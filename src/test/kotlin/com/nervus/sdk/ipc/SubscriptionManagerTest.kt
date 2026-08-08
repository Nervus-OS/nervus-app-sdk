package com.nervus.sdk.ipc

import com.nervus.sdk.ipc.event.SubscriptionManager
import com.nervus.sdk.ipc.event.SubscriptionOverflowException
import io.github.nervusos.ipc.v1.DeliveryClass
import io.github.nervusos.ipc.v1.Event
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.Flow
import kotlin.test.Test
import kotlin.test.*

class SubscriptionManagerTest {

    @Test
    fun `subscribe and receive event`() = runBlocking {
        val mgr = SubscriptionManager()
        val flow = mgr.subscribe(1L)

        val collector = mutableListOf<Event>()
        val job = launch(Dispatchers.Unconfined) {
            flow.collect { collector.add(it) }
        }

        yield()

        val event = Event.newBuilder()
            .setSubscriptionId(1L)
            .setSequence(1)
            .build()
        mgr.pushEvent(event)

        yield()
        assertTrue(collector.size >= 1)
        if (collector.isNotEmpty()) {
            assertEquals(1L, collector[0].subscriptionId)
            assertEquals(1, collector[0].sequence)
        }

        job.cancel()
    }

    @Test
    fun `event to unknown subscription returns false`() {
        val mgr = SubscriptionManager()
        val event = Event.newBuilder()
            .setSubscriptionId(999L)
            .setSequence(1)
            .build()
        assertFalse(mgr.pushEvent(event))
    }

    @Test
    fun `subscribe after unsubscribe`(): Unit = runBlocking {
        val mgr = SubscriptionManager()
        mgr.subscribe(1L)

        val flow1 = mgr.unsubscribe(1L)
        assertNotNull(flow1)

        val flow2 = mgr.subscribe(1L)
        assertNotNull(flow2)
    }

    @Test
    fun `bind request id to subscription`() = runBlocking {
        val mgr = SubscriptionManager()
        mgr.bind(100L, 1L)

        val event = Event.newBuilder()
            .setSubscriptionId(1L)
            .setSequence(1)
            .build()
        assertTrue(mgr.pushEvent(event))
    }

    @Test
    fun `clear removes all subscriptions`() = runBlocking {
        val mgr = SubscriptionManager()
        mgr.subscribe(1L)
        mgr.subscribe(2L)
        assertEquals(2, mgr.subscriptionCount())

        mgr.clear()
        assertEquals(0, mgr.subscriptionCount())

        val event = Event.newBuilder()
            .setSubscriptionId(1L)
            .setSequence(1)
            .build()
        assertFalse(mgr.pushEvent(event))
    }

    @Test
    fun `close subscription stops events`() = runBlocking {
        val mgr = SubscriptionManager()
        mgr.subscribe(1L)

        mgr.closeSubscription(1L)

        val event = Event.newBuilder()
            .setSubscriptionId(1L)
            .setSequence(1)
            .build()
        assertFalse(mgr.pushEvent(event))
    }

    @Test
    fun `unsubscribe cleans up pending bindings`() = runBlocking {
        val mgr = SubscriptionManager()
        mgr.bind(42L, 1L)

        val event = Event.newBuilder()
            .setSubscriptionId(1L)
            .setSequence(1)
            .build()
        assertTrue(mgr.pushEvent(event))

        mgr.unsubscribe(1L)

        assertFalse(mgr.pushEvent(event))
    }

    @Test
    fun `close subscription cleans up pending bindings`() = runBlocking {
        val mgr = SubscriptionManager()
        mgr.bind(42L, 1L)

        mgr.closeSubscription(1L)

        val event = Event.newBuilder()
            .setSubscriptionId(1L)
            .setSequence(1)
            .build()
        assertFalse(mgr.pushEvent(event))
    }
}

// ---- delivery_class 分类处理 ----------------------------------------------
//
// 这几条守的是「所有类别都用 DROP_OLDEST」那个旧实现不会回来。它的问题是
// trySend 永不失败，于是 RELIABLE 的事件在消费者慢的时候被静默丢掉，而调用方
// 根据内核给的 dropped == 0 认为自己拿到了完整序列。

private const val BUF = 4

private fun evt(subId: Long, seq: Long, dropped: Long = 0) = Event.newBuilder()
    .setSubscriptionId(subId)
    .setSequence(seq)
    .setDropped(dropped)
    .build()

class SubscriptionDeliveryClassTest {

    /** RELIABLE 溢出【终止订阅】，不静默丢弃。 */
    @Test
    fun `reliable overflow terminates the subscription`() = runBlocking {
        val mgr = SubscriptionManager()
        val flow = mgr.subscribe(1L, DeliveryClass.DELIVERY_CLASS_RELIABLE, buffer = BUF)

        repeat(BUF) { assertTrue(mgr.pushEvent(evt(1L, (it + 1).toLong()))) }
        assertFalse(mgr.pushEvent(evt(1L, (BUF + 1).toLong())), "第 ${BUF + 1} 条该溢出")

        // 消费者拿到已缓冲的那几条，然后收到异常——它必须【知道】自己丢了东西。
        val seen = mutableListOf<Event>()
        assertFailsWith<SubscriptionOverflowException> {
            flow.collect { seen.add(it) }
        }
        assertEquals(BUF, seen.size)

        // 订阅已经作废，后续事件不再被接收。
        assertFalse(mgr.pushEvent(evt(1L, 99)))
        assertEquals(0, mgr.subscriptionCount())
    }

    /** 未知类别按 RELIABLE 处理：新增一个枚举值不能变成「可以随便丢」。 */
    @Test
    fun `unknown delivery class is treated as reliable`() = runBlocking {
        val mgr = SubscriptionManager()
        mgr.subscribe(1L, DeliveryClass.DELIVERY_CLASS_UNSPECIFIED, buffer = BUF)

        repeat(BUF) { mgr.pushEvent(evt(1L, (it + 1).toLong())) }
        mgr.pushEvent(evt(1L, (BUF + 1).toLong()))

        assertEquals(0, mgr.subscriptionCount(), "未知类别溢出时该终止订阅")
    }

    /**
     * STATE 挤掉【最旧】的一条，并把被挤掉的计进新那条的 dropped。
     *
     * 方向搞反（丢新留旧）会把 STATE 的语义整个颠倒；只丢不计数则让调用方
     * 看到 sequence 跳号却拿不到条数，而那正是 dropped 存在的理由。
     */
    @Test
    fun `state coalesces oldest and carries the drop count`() = runBlocking {
        val mgr = SubscriptionManager()
        val flow = mgr.subscribe(1L, DeliveryClass.DELIVERY_CLASS_STATE, buffer = BUF)

        repeat(BUF) { mgr.pushEvent(evt(1L, (it + 1).toLong())) }
        // 满了：这一条挤掉 seq=1，自己进队
        assertTrue(mgr.pushEvent(evt(1L, 5)))

        mgr.closeSubscription(1L)
        val seen = mutableListOf<Event>()
        flow.collect { seen.add(it) }

        assertEquals(listOf(2L, 3L, 4L, 5L), seen.map { it.sequence }, "该丢的是最旧那条")
        assertEquals(1L, seen.last().dropped, "被挤掉的那条要计进 dropped")
    }

    /** 服务端已报的 dropped 与本地丢弃【相加】，不能互相覆盖。 */
    @Test
    fun `server reported drops add to local drops`() = runBlocking {
        val mgr = SubscriptionManager()
        val flow = mgr.subscribe(1L, DeliveryClass.DELIVERY_CLASS_LOSSY, buffer = 1)

        assertTrue(mgr.pushEvent(evt(1L, 1)))
        assertFalse(mgr.pushEvent(evt(1L, 2)), "缓冲满，本地丢 1 条")

        // 消费掉队里那条，腾出位置
        assertEquals(1L, flow.takeOne().sequence)

        // 下一条自带服务端丢弃 3 条，加上本地那 1 条 = 4
        assertTrue(mgr.pushEvent(evt(1L, 3, dropped = 3)))
        assertEquals(1L, mgr.localDropped(1L))

        mgr.closeSubscription(1L)
        val rest = mutableListOf<Event>()
        flow.collect { rest.add(it) }

        assertEquals(1, rest.size)
        assertEquals(4L, rest[0].dropped)
    }

    /** LOSSY 丢弃后，欠的账要搭在【下一条真正送达】的事件上。 */
    @Test
    fun `lossy carries pending drops onto the next delivered event`() = runBlocking {
        val mgr = SubscriptionManager()
        val flow = mgr.subscribe(1L, DeliveryClass.DELIVERY_CLASS_LOSSY, buffer = 1)

        mgr.pushEvent(evt(1L, 1))
        assertFalse(mgr.pushEvent(evt(1L, 2)))
        assertFalse(mgr.pushEvent(evt(1L, 3)))

        val drained = mutableListOf<Event>()
        drained.add(flow.takeOne())

        assertTrue(mgr.pushEvent(evt(1L, 4)))
        assertEquals(2L, mgr.localDropped(1L))

        mgr.closeSubscription(1L)
        flow.collect { drained.add(it) }

        assertEquals(listOf(1L, 4L), drained.map { it.sequence })
        assertEquals(2L, drained[1].dropped, "欠的两条要搭在 seq=4 上")
    }
}

/** 从 Flow 里取一条就走——用来在测试中间腾出缓冲位置。 */
private suspend fun Flow<Event>.takeOne(): Event {
    var out: Event? = null
    try {
        collect { out = it; throw StopCollect() }
    } catch (_: StopCollect) {
    }
    return out!!
}

private class StopCollect : RuntimeException(null, null, false, false)
