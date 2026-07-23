package com.nervus.sdk.ipc

import com.nervus.sdk.ipc.event.SubscriptionManager
import io.github.nervusos.ipc.v1.Event
import kotlinx.coroutines.*
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
    fun `subscribe after unsubscribe`() = runBlocking {
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
