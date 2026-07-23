package com.nervus.sdk.ipc.event

import io.github.nervusos.ipc.v1.Event
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.asSharedFlow

class SubscriptionManager {
    private val subscriptions = HashMap<Long, MutableSharedFlow<Event>>()
    private val pendingBindings = HashMap<Long, Long>()

    fun subscribe(subscriptionId: Long): Flow<Event> {
        return subscriptions.getOrPut(subscriptionId) {
            MutableSharedFlow(replay = 0, extraBufferCapacity = 64)
        }.asSharedFlow()
    }

    fun bind(requestId: Long, serverSubscriptionId: Long) {
        pendingBindings[requestId] = serverSubscriptionId
        subscriptions.getOrPut(serverSubscriptionId) {
            MutableSharedFlow(replay = 0, extraBufferCapacity = 64)
        }
    }

    fun pushEvent(event: Event): Boolean {
        val flow = subscriptions[event.subscriptionId] ?: return false
        return flow.tryEmit(event)
    }

    fun unsubscribe(subscriptionId: Long): Flow<Event>? {
        return subscriptions.remove(subscriptionId)
    }

    fun closeSubscription(subscriptionId: Long) {
        subscriptions.remove(subscriptionId)
    }

    fun clear() {
        subscriptions.clear()
        pendingBindings.clear()
    }

    fun subscriptionCount(): Int = subscriptions.size
}
