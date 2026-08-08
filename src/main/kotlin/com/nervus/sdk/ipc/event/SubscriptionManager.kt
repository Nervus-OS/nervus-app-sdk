package com.nervus.sdk.ipc.event

import io.github.nervusos.ipc.v1.DeliveryClass
import io.github.nervusos.ipc.v1.Event
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.receiveAsFlow
import java.util.logging.Logger

/** 本地缓冲溢出导致 RELIABLE 订阅终止。 */
class SubscriptionOverflowException(subscriptionId: Long) : Exception(
    "subscription $subscriptionId overflowed locally; " +
        "RELIABLE events cannot be dropped, so the subscription was terminated"
)

private const val DEFAULT_BUFFER = 64

/**
 * 客户端侧的订阅缓冲。
 *
 * # 为什么不能都用 DROP_OLDEST
 *
 * 这里曾经给所有订阅都开 `BufferOverflow.DROP_OLDEST` 的 Channel。那样
 * `trySend` 【永远成功】，于是：
 *
 * - RELIABLE 的事件在消费者慢的时候被**静默丢掉**。内核那边保证了
 *   `Event.dropped == 0`，调用方据此认为自己拿到了完整序列——而缺口是在
 *   它自己进程里产生的。
 * - 丢弃计数那段代码是死的（`trySend` 不会失败），日志一行也不会打。
 * - STATE 丢的是**最旧**的一条。STATE 的语义是「只要最新值」，可它丢旧留新
 *   的方向对了、计数却没进任何一条事件的 dropped，合并就成了静默丢弃。
 *
 * 所以现在按类别分开处理，并且【本地丢弃数要搭在下一条真正送达的事件上】——
 * 只有这样调用方看到 sequence 跳号时才拿得到条数。
 *
 * # 绝不阻塞
 *
 * [pushEvent] 由读循环调用，那是整条连接共用的。阻塞在这里会让同一连接上的
 * 请求响应、Ping、其它订阅一起饿死——一个慢消费者拖垮全连接，正是
 * delivery_class 机制要防的事。
 */
internal class SubscriptionManager {

    private class Entry(val subscriptionId: Long, buffer: Int) {
        val channel = Channel<Event>(buffer)
        var deliveryClass: DeliveryClass = DeliveryClass.DELIVERY_CLASS_UNSPECIFIED

        /** 还没搭上任何一条已投递事件的本地丢弃数。投递成功时清零并计进那条。 */
        var pendingDropped: Long = 0

        /** 累计本地丢弃条数，仅用于排查（非 0 = 本进程消费太慢，不必去查内核）。 */
        var localDropped: Long = 0
    }

    private val entries = HashMap<Long, Entry>()
    private val pendingBindings = HashMap<Long, Long>()
    private val logger = Logger.getLogger(SubscriptionManager::class.java.name)

    @Synchronized
    fun subscribe(
        subscriptionId: Long,
        deliveryClass: DeliveryClass = DeliveryClass.DELIVERY_CLASS_UNSPECIFIED,
        buffer: Int = DEFAULT_BUFFER
    ): Flow<Event> {
        val entry = entries.getOrPut(subscriptionId) { Entry(subscriptionId, buffer) }
        entry.deliveryClass = deliveryClass
        return entry.channel.receiveAsFlow()
    }

    @Synchronized
    fun bind(requestId: Long, serverSubscriptionId: Long) {
        pendingBindings[requestId] = serverSubscriptionId
        entries.getOrPut(serverSubscriptionId) { Entry(serverSubscriptionId, DEFAULT_BUFFER) }
    }

    /**
     * 投递一条事件。返回 false 表示这条没进到消费者手里（订阅不存在，或本地丢弃）。
     */
    @Synchronized
    fun pushEvent(event: Event): Boolean {
        val entry = entries[event.subscriptionId] ?: return false

        // carried 是「你上次实际收到之后本地丢了多少」。它必须搭上某一条真正
        // 送达的事件才算报出去了，所以只在成功之后才清零。
        var carried = entry.pendingDropped
        if (trySend(entry, event, carried)) {
            entry.pendingDropped = 0
            return true
        }

        return when (entry.deliveryClass) {
            DeliveryClass.DELIVERY_CLASS_STATE -> {
                // 合并：挤掉队列里【最旧】的一条，把最新的塞进去。STATE 的语义
                // 是「只要最新值」，丢新留旧正好把语义反过来。
                if (entry.channel.tryReceive().isSuccess) {
                    carried++
                    if (trySend(entry, event, carried)) {
                        entry.pendingDropped = 0
                        return true
                    }
                }
                entry.localDropped++
                entry.pendingDropped = carried + 1
                false
            }

            DeliveryClass.DELIVERY_CLASS_LOSSY -> {
                entry.localDropped++
                entry.pendingDropped = carried + 1
                logger.fine("event dropped for subscription ${event.subscriptionId}")
                false
            }

            else -> {
                // RELIABLE，以及任何本 build 不认识的类别。
                //
                // 【未知类别按 RELIABLE 处理】：协议新增一个值时，把它当成
                // 「可以随便丢」是最危险的默认——调用方会以为自己拿到了完整
                // 序列。宁可终止订阅，让它知道。
                logger.severe(
                    "subscription ${event.subscriptionId} overflowed locally " +
                        "(class=${entry.deliveryClass}); terminating"
                )
                entry.channel.close(SubscriptionOverflowException(event.subscriptionId))
                entries.remove(event.subscriptionId)
                pendingBindings.values.removeAll { it == event.subscriptionId }
                false
            }
        }
    }

    /** 把本地丢弃数加进事件自带的 dropped 一起报出去。 */
    private fun trySend(entry: Entry, event: Event, carried: Long): Boolean {
        val outgoing = if (carried == 0L) {
            event
        } else {
            event.toBuilder().setDropped(event.dropped + carried).build()
        }
        return entry.channel.trySend(outgoing).isSuccess
    }

    @Synchronized
    fun setDeliveryClass(subscriptionId: Long, deliveryClass: DeliveryClass) {
        entries[subscriptionId]?.deliveryClass = deliveryClass
    }

    @Synchronized
    fun getDeliveryClass(subscriptionId: Long): DeliveryClass =
        entries[subscriptionId]?.deliveryClass ?: DeliveryClass.DELIVERY_CLASS_UNSPECIFIED

    /**
     * 本 SDK 缓冲累计丢弃的条数（不含 nervud 侧丢弃）。
     *
     * 与 `Event.dropped` 分开只为排查：它非 0 说明【本进程消费太慢】，该加大
     * 缓冲或让 collector 更快，而不是去查内核。
     */
    @Synchronized
    fun localDropped(subscriptionId: Long): Long = entries[subscriptionId]?.localDropped ?: 0

    @Synchronized
    fun unsubscribe(subscriptionId: Long): Flow<Event>? {
        pendingBindings.values.removeAll { it == subscriptionId }
        val entry = entries.remove(subscriptionId) ?: return null
        entry.channel.close()
        return entry.channel.receiveAsFlow()
    }

    @Synchronized
    fun closeSubscription(subscriptionId: Long) {
        pendingBindings.values.removeAll { it == subscriptionId }
        entries.remove(subscriptionId)?.channel?.close()
    }

    @Synchronized
    fun clear() {
        entries.values.forEach { it.channel.close() }
        entries.clear()
        pendingBindings.clear()
    }

    @Synchronized
    fun subscriptionCount(): Int = entries.size
}
