package com.nervus.sdk.ipc

import com.nervus.sdk.ipc.connection.*
import com.nervus.sdk.ipc.endpoint.EndpointCache
import com.nervus.sdk.ipc.endpoint.EndpointCacheKey
import com.nervus.sdk.ipc.endpoint.EndpointCacheValue
import com.nervus.sdk.ipc.event.SubscriptionManager
import com.nervus.sdk.ipc.rpc.PendingMap
import com.nervus.sdk.ipc.rpc.RequestIdGenerator
import io.github.nervusos.ipc.v1.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.Flow
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit

class NervusClient(
    val sdkName: String = "nervus-app-sdk",
    val sdkVersion: String = "0.1.0",
    val minProtocolMajor: Int = 1,
    val maxProtocolMajor: Int = 1,
    val maxProtocolMinor: Int = 0
) : AutoCloseable {
    private var udSocket: UnixDomainSocket? = null
    private var frameReader: FrameReader? = null
    private var frameWriter: FrameWriter? = null
    private var handshakeResult: HandshakeResult? = null

    private val requestIdGenerator = RequestIdGenerator()
    private val pendingMap = PendingMap()
    val endpointCache = EndpointCache()
    val subscriptionManager = SubscriptionManager()

    private var readerJob: Job? = null
    private var scope: CoroutineScope? = null

    val limits: ConnectionLimits? get() = handshakeResult?.limits

    fun connect(socketPath: String): HandshakeResult {
        val socket = UnixDomainSocket.connect(socketPath)
        try {
            val reader = FrameReader(socket.inputStream)
            val writer = FrameWriter(socket.outputStream)

            val handshake = HelloHandshake(reader, writer)
            val result = handshake.negotiate(
                minMajor = minProtocolMajor,
                maxMajor = maxProtocolMajor,
                maxMinor = maxProtocolMinor,
                sdkName = sdkName,
                sdkVersion = sdkVersion
            )

            udSocket = socket
            frameReader = reader
            frameWriter = writer
            handshakeResult = result

            scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
            readerJob = scope?.launch {
                runReaderLoop()
            }

            return result
        } catch (e: Exception) {
            socket.close()
            throw e
        }
    }

    private suspend fun runReaderLoop() {
        try {
            while (udSocket?.isOpen == true) {
                val envelope = frameReader?.readFrame() ?: break
                when (envelope.bodyCase) {
                    Envelope.BodyCase.RESPONSE -> {
                        handleResponse(envelope.response)
                    }
                    Envelope.BodyCase.EVENT -> {
                        handleEvent(envelope.event)
                    }
                    Envelope.BodyCase.ENDPOINT_DIED -> {
                        handleEndpointDied(envelope.endpointDied)
                    }
                    Envelope.BodyCase.PING -> {
                        handlePing()
                    }
                    else -> {
                    }
                }
            }
        } catch (_: Exception) {
        } finally {
            pendingMap.failAll()
            endpointCache.invalidateAll()
        }
    }

    private fun handleResponse(response: Response) {
        pendingMap.complete(response.requestId, response)
    }

    private fun handleEvent(event: Event) {
        subscriptionManager.pushEvent(event)
    }

    private fun handleEndpointDied(died: EndpointDied) {
        endpointCache.invalidateAll()
    }

    private fun handlePing() {
        val pong = Envelope.newBuilder().setPong(Pong.getDefaultInstance()).build()
        frameWriter?.writeFrame(pong)
    }

    fun call(
        endpointId: Long,
        methodId: Int,
        payload: ByteArray = ByteArray(0),
        timeoutMs: Int = handshakeResult?.limits?.defaultTimeoutMs ?: 5000
    ): CompletableFuture<Response> {
        val writer = frameWriter ?: throw IllegalStateException("not connected")
        val requestId = requestIdGenerator.next()
        val future = pendingMap.register(requestId)

        val request = Request.newBuilder()
            .setRequestId(requestId)
            .setEndpointId(endpointId)
            .setMethodId(methodId)
            .setTimeoutMs(timeoutMs)
            .setPayload(com.google.protobuf.ByteString.copyFrom(payload))
            .build()

        val envelope = Envelope.newBuilder()
            .setRequest(request)
            .build()

        writer.writeFrame(envelope)
        return future
    }

    override fun close() {
        readerJob?.cancel()
        scope?.cancel()
        udSocket?.close()
        pendingMap.failAll()
        endpointCache.invalidateAll()
        subscriptionManager.clear()
    }
}
