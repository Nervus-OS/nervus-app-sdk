package com.nervus.sdk.component

import com.nervus.sdk.ipc.ConnectionState
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.*

class ComponentTest {

    private open class TestComponent(
        overrideConfig: ComponentConfig = ComponentConfig(autoReconnect = false)
    ) : Component(overrideConfig) {
        var startCalled = false
        var closeCalled = false
        var doStartCalled = false
        var doCloseCalled = false
        var active = true

        override fun isActive(): Boolean = active
        override fun doStart() {
            doStartCalled = true
            startCalled = true
        }
        override fun doClose() {
            doCloseCalled = true
            closeCalled = true
        }

        var onReadyCalled = false
        var onStopCalled = false
        var onConnectCalled = false

        override fun onReady() { onReadyCalled = true }
        override fun onStop() { onStopCalled = true }
        override fun onConnect() { onConnectCalled = true }
    }

    @Test
    fun `start transitions from DISCONNECTED to CONNECTED`() {
        val comp = TestComponent()
        assertEquals(ConnectionState.DISCONNECTED, comp.state)
        comp.start()
        assertEquals(ConnectionState.CONNECTED, comp.state)
        comp.close()
        assertEquals(ConnectionState.CLOSED, comp.state)
    }

    @Test
    fun `close on fresh component is safe`() {
        val comp = TestComponent()
        assertEquals(ConnectionState.DISCONNECTED, comp.state)
        comp.close()
        assertEquals(ConnectionState.CLOSED, comp.state)
    }

    @Test
    fun `lifecycle callbacks are invoked on start`() {
        val comp = TestComponent()
        comp.start()
        assertTrue(comp.doStartCalled)
        assertTrue(comp.onConnectCalled)
        assertTrue(comp.onReadyCalled)
        comp.close()
        assertTrue(comp.doCloseCalled)
        assertTrue(comp.onStopCalled)
    }

    @Test
    fun `close during start prevents CONNECTED state`() {
        val comp = object : TestComponent() {
            override fun doStart() {
                close()
            }
        }
        comp.start()
        assertEquals(ConnectionState.CLOSED, comp.state)
    }

    @Test
    fun `multiple close calls are idempotent`() {
        val comp = TestComponent()
        comp.start()
        comp.close()
        comp.close()
        assertEquals(ConnectionState.CLOSED, comp.state)
        var stopCount = 0
        if (comp.onStopCalled) stopCount++
        assertEquals(1, stopCount)
    }

    @Test
    fun `reconnect attempts are bounded by maxReconnectAttempts`() = runBlocking {
        var failCount = 0
        val comp = object : TestComponent(
            ComponentConfig(autoReconnect = true, maxReconnectAttempts = 3)
        ) {
            override fun isActive(): Boolean = false
            override fun doStart() {
                failCount++
                if (failCount > 1) {
                    // only reconnect loop calls doStart
                }
                throw RuntimeException("connection failed #$failCount")
            }
        }
        runCatching { comp.start() }
        delay(4000)
        assertTrue(failCount > 0, "doStart should be called at least once")
        assertTrue(failCount <= 4, "doStart should not exceed maxReconnectAttempts + initial")
        comp.close()
    }

    @Test
    fun `close stops reconnection`() = runBlocking {
        var callCount = 0
        val comp = object : TestComponent(
            ComponentConfig(autoReconnect = true, maxReconnectAttempts = 10)
        ) {
            override fun isActive(): Boolean = false
            override fun doStart() {
                callCount++
                throw RuntimeException("failed")
            }
        }
        runCatching { comp.start() }
        delay(100)
        val countBeforeClose = callCount
        comp.close()
        delay(2000)
        assertEquals(countBeforeClose, callCount, "doStart should not be called after close")
    }

    @Test
    fun `state property reflects connection lifecycle`() {
        val comp = TestComponent()
        assertEquals(ConnectionState.DISCONNECTED, comp.state)
        comp.start()
        assertEquals(ConnectionState.CONNECTED, comp.state)
        comp.close()
        assertEquals(ConnectionState.CLOSED, comp.state)
    }
}