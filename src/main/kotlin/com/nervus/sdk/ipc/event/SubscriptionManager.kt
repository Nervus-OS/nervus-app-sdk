package com.nervus.sdk.ipc.event

import io.github.nervusos.ipc.v1.DeliveryClass
import io.github.nervusos.ipc.v1.Event
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.receiveAsFlow
import java.util.logging.Logger

internal class SubscriptionManager {
    private val subscriptions = HashMap<Long, Channel<Event>>()
    private val deliveryClasses = HashMap<Long, DeliveryClass>()
    private val pendingBindings = HashMap<Long, Long>()
    private val logger = Logger.getLogger(SubscriptionManager::class.java.name)

    fun subscribe(subscriptionId: Long, deliveryClass: DeliveryClass = DeliveryClass.DELIVERY_CLASS_UNSPECIFIED): Flow<Event> {
        deliveryClasses[subscriptionId] = deliveryClass
        return subscriptions.getOrPut(subscriptionId) {
            Channel<Event>(Channel.BUFFERED, onBufferOverflow = BufferOverflow.DROP_OLDEST)
        }.receiveAsFlow()
    }

    fun bind(requestId: Long, serverSubscriptionId: Long) {
        pendingBindings[requestId] = serverSubscriptionId
        subscriptions.getOrPut(serverSubscriptionId) {
            Channel<Event>(Channel.BUFFERED, onBufferOverflow = BufferOverflow.DROP_OLDEST)
        }
    }

    fun pushEvent(event: Event): Boolean {
        val channel = subscriptions[event.subscriptionId] ?: return false
        val dc = deliveryClasses[event.subscriptionId]
        if (dc == DeliveryClass.DELIVERY_CLASS_RELIABLE) {
            return channel.trySend(event).isSuccess
        }
        val emitted = channel.trySend(event).isSuccess
        if (!emitted) {
            logger.fine("event dropped for subscription ${event.subscriptionId}")
        }
        return emitted
    }

    fun setDeliveryClass(subscriptionId: Long, deliveryClass: DeliveryClass) {
        deliveryClasses[subscriptionId] = deliveryClass
    }

    fun getDeliveryClass(subscriptionId: Long): DeliveryClass {
        return deliveryClasses[subscriptionId] ?: DeliveryClass.DELIVERY_CLASS_UNSPECIFIED
    }

    fun unsubscribe(subscriptionId: Long): Flow<Event>? {
        pendingBindings.values.removeAll { it == subscriptionId }
        deliveryClasses.remove(subscriptionId)
        val channel = subscriptions.remove(subscriptionId) ?: return null
        channel.close()
        return channel.receiveAsFlow()
    }

    fun closeSubscription(subscriptionId: Long) {
        pendingBindings.values.removeAll { it == subscriptionId }
        deliveryClasses.remove(subscriptionId)
        subscriptions.remove(subscriptionId)?.close()
    }

    fun clear() {
        subscriptions.values.forEach { it.close() }
        subscriptions.clear()
        deliveryClasses.clear()
        pendingBindings.clear()
    }

    fun subscriptionCount(): Int = subscriptions.size
}
