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
import java.io.EOFException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import java.util.logging.Logger

enum class ConnectionState {
    DISCONNECTED,
    CONNECTING,
    CONNECTED,
    CLOSED
}

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
    private val resolvePending = PendingMap()
    private val _endpointCache = EndpointCache()
    private val _subscriptionManager = SubscriptionManager()

    private var readerJob: Job? = null
    private var scope: CoroutineScope? = null

    @Volatile
    var connectionState: ConnectionState = ConnectionState.DISCONNECTED
        private set

    val limits: ConnectionLimits? get() = handshakeResult?.limits
    val endpointCache: EndpointCache get() = _endpointCache
    val subscriptionManager: SubscriptionManager get() = _subscriptionManager

    private val logger = Logger.getLogger(NervusClient::class.java.name)

    fun connect(
        socketPath: String,
        handshakeTimeoutMs: Long = 5000
    ): HandshakeResult {
        connectionState = ConnectionState.CONNECTING
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
                sdkVersion = sdkVersion,
                timeoutMs = handshakeTimeoutMs
            )

            udSocket = socket
            frameReader = reader
            frameWriter = writer
            handshakeResult = result
            connectionState = ConnectionState.CONNECTED

            scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
            readerJob = scope?.launch {
                runReaderLoop()
            }

            return result
        } catch (e: Exception) {
            connectionState = ConnectionState.CLOSED
            socket.close()
            throw e
        }
    }

    private suspend fun runReaderLoop() {
        try {
            while (udSocket?.isOpen == true && connectionState == ConnectionState.CONNECTED) {
                val envelope = frameReader?.readFrame() ?: break
                when (envelope.bodyCase) {
                    Envelope.BodyCase.RESPONSE -> {
                        handleResponse(envelope.response)
                    }
                    Envelope.BodyCase.EVENT -> {
                        handleEvent(envelope.event)
                    }
                    Envelope.BodyCase.RESOLVE_ENDPOINT_RESULT -> {
                        handleResolveEndpointResult(envelope.resolveEndpointResult)
                    }
                    Envelope.BodyCase.ENDPOINT_DIED -> {
                        handleEndpointDied(envelope.endpointDied)
                    }
                    Envelope.BodyCase.ENDPOINT_REVOKED -> {
                        handleEndpointRevoked(envelope.endpointRevoked)
                    }
                    Envelope.BodyCase.SUBSCRIBE_RESULT -> {
                        handleSubscribeResult(envelope.subscribeResult)
                    }
                    Envelope.BodyCase.UNSUBSCRIBE_RESULT -> {
                        handleUnsubscribeResult(envelope.unsubscribeResult)
                    }
                    Envelope.BodyCase.SUBSCRIPTION_CLOSED -> {
                        handleSubscriptionClosed(envelope.subscriptionClosed)
                    }
                    Envelope.BodyCase.PING -> {
                        handlePing(envelope.ping)
                    }
                    Envelope.BodyCase.PONG -> {
                    }
                    else -> {
                    }
                }
            }
        } catch (e: java.net.SocketTimeoutException) {
            logger.warning("read timeout: ${e.message}")
        } catch (e: EOFException) {
            logger.info("connection closed by server")
        } catch (e: ProtocolViolationException) {
            logger.severe("protocol violation: ${e.message}")
        } catch (e: Exception) {
            logger.fine("reader loop exited: ${e.message}")
        } finally {
            connectionState = ConnectionState.CLOSED
            pendingMap.failAll()
            resolvePending.failAll()
            _endpointCache.invalidateAll()
        }
    }

    private fun handleResponse(response: Response) {
        pendingMap.complete(response.requestId, response)
    }

    private fun handleResolveEndpointResult(result: ResolveEndpointResult) {
        when (result.outcomeCase) {
            ResolveEndpointResult.OutcomeCase.SUCCESS -> {
                val success = result.success
                val value = EndpointCacheValue(
                    endpointId = success.endpointId,
                    interfaceMajor = success.interfaceMajor,
                    interfaceMinor = success.interfaceMinor,
                    schemaHash = success.interfaceSchemaHash.toByteArray(),
                    resourceHandle = success.resourceHandle
                )
                _endpointCache.put(
                    EndpointCacheKey(
                        interfaceId = "",
                        interfaceMajor = success.interfaceMajor,
                        interfaceMinor = success.interfaceMinor,
                        schemaHash = success.interfaceSchemaHash.toByteArray()
                    ),
                    value
                )
                resolvePending.complete(result.requestId, Response.newBuilder()
                    .setSuccess(Success.newBuilder()
                        .setCode(StatusCode.STATUS_CODE_OK)
                        .setPayload(success.toByteString())
                        .build())
                    .build())
            }
            ResolveEndpointResult.OutcomeCase.FAILURE -> {
                resolvePending.complete(result.requestId, Response.newBuilder()
                    .setFailure(result.failure)
                    .build())
            }
            else -> {
                logger.warning("resolve_endpoint_result with no outcome")
            }
        }
    }

    private fun handleEvent(event: Event) {
        _subscriptionManager.pushEvent(event)
    }

    private fun handleEndpointDied(died: EndpointDied) {
        _endpointCache.invalidateAll()
    }

    private fun handleEndpointRevoked(revoked: EndpointRevoked) {
        _endpointCache.invalidateAll()
    }

    private fun handleSubscribeResult(result: SubscribeResult) {
        val response = Response.newBuilder().apply {
            when (result.outcomeCase) {
                SubscribeResult.OutcomeCase.SUCCESS -> {
                    val success = result.success
                    _subscriptionManager.bind(result.requestId, success.subscriptionId)
                    setSuccess(Success.newBuilder()
                        .setCode(StatusCode.STATUS_CODE_OK)
                        .setPayload(success.toByteString())
                        .build())
                }
                SubscribeResult.OutcomeCase.FAILURE -> {
                    setFailure(result.failure)
                }
                else -> {
                    setFailure(Failure.newBuilder()
                        .setCode(StatusCode.STATUS_CODE_INTERNAL)
                        .setPublicMessage("subscribe_result with no outcome")
                        .build())
                }
            }
        }.build()
        resolvePending.complete(result.requestId, response)
    }

    private fun handleUnsubscribeResult(result: UnsubscribeResult) {
        val response = Response.newBuilder().apply {
            when (result.outcomeCase) {
                UnsubscribeResult.OutcomeCase.SUCCESS -> {
                    setSuccess(Success.newBuilder()
                        .setCode(StatusCode.STATUS_CODE_OK)
                        .build())
                }
                UnsubscribeResult.OutcomeCase.FAILURE -> {
                    setFailure(result.failure)
                }
                else -> {}
            }
        }.build()
        resolvePending.complete(result.requestId, response)
    }

    private fun handleSubscriptionClosed(closed: SubscriptionClosed) {
        _subscriptionManager.closeSubscription(closed.subscriptionId)
    }

    private fun handlePing(ping: Ping) {
        val pong = Pong.newBuilder().setNonce(ping.nonce).build()
        frameWriter?.writeFrame(
            Envelope.newBuilder().setPong(pong).build()
        )
    }

    fun resolveEndpoint(
        interfaceId: String,
        minInterfaceMajor: Int = 1,
        maxInterfaceMajor: Int = 1,
        resourceType: String = "",
        resourceRole: String = "",
        timeoutMs: Int = handshakeResult?.limits?.defaultTimeoutMs ?: 5000
    ): CompletableFuture<Response> {
        val writer = frameWriter ?: throw IllegalStateException("not connected")
        val requestId = requestIdGenerator.next()
        val future = resolvePending.register(requestId)

        val builder = ResolveEndpoint.newBuilder()
            .setRequestId(requestId)
            .setInterfaceId(interfaceId)
            .setMinInterfaceMajor(minInterfaceMajor)
            .setMaxInterfaceMajor(maxInterfaceMajor)

        if (resourceType.isNotEmpty() || resourceRole.isNotEmpty()) {
            builder.selector = ResourceSelector.newBuilder()
                .setType(resourceType)
                .setRole(resourceRole)
                .build()
        }

        writer.writeFrame(
            Envelope.newBuilder().setResolveEndpoint(builder.build()).build()
        )

        if (timeoutMs > 0) {
            future.orTimeout(timeoutMs.toLong(), TimeUnit.MILLISECONDS)
        }
        return future
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

        writer.writeFrame(
            Envelope.newBuilder().setRequest(request).build()
        )

        if (timeoutMs > 0) {
            future.orTimeout(timeoutMs.toLong(), TimeUnit.MILLISECONDS)
        }
        return future
    }

    fun cancel(requestId: Long) {
        val writer = frameWriter ?: throw IllegalStateException("not connected")
        val cancel = Cancel.newBuilder().setRequestId(requestId).build()
        writer.writeFrame(
            Envelope.newBuilder().setCancel(cancel).build()
        )
        pendingMap.cancelOne(requestId)
    }

    fun subscribe(
        endpointId: Long,
        eventId: Int,
        payload: ByteArray = ByteArray(0),
        timeoutMs: Int = handshakeResult?.limits?.defaultTimeoutMs ?: 5000
    ): CompletableFuture<Flow<Event>> {
        val writer = frameWriter ?: throw IllegalStateException("not connected")
        val requestId = requestIdGenerator.next()
        val future = resolvePending.register(requestId)

        val subscribe = Subscribe.newBuilder()
            .setRequestId(requestId)
            .setEndpointId(endpointId)
            .setEventId(eventId)
            .setPayload(com.google.protobuf.ByteString.copyFrom(payload))
            .build()

        writer.writeFrame(
            Envelope.newBuilder().setSubscribe(subscribe).build()
        )

        val resultFuture = future.thenApply { response ->
            if (response.hasSuccess()) {
                val subResult = SubscribeSuccess.parseFrom(response.success.payload)
                _subscriptionManager.subscribe(subResult.subscriptionId)
            } else {
                throw RuntimeException("subscribe failed: ${response.failure.publicMessage}")
            }
        }

        if (timeoutMs > 0) {
            future.orTimeout(timeoutMs.toLong(), TimeUnit.MILLISECONDS)
        }
        return resultFuture
    }

    fun unsubscribe(subscriptionId: Long, timeoutMs: Int = 5000): CompletableFuture<Response> {
        val writer = frameWriter ?: throw IllegalStateException("not connected")
        val requestId = requestIdGenerator.next()
        val future = resolvePending.register(requestId)

        writer.writeFrame(
            Envelope.newBuilder()
                .setUnsubscribe(Unsubscribe.newBuilder()
                    .setRequestId(requestId)
                    .setSubscriptionId(subscriptionId)
                    .build())
                .build()
        )

        if (timeoutMs > 0) {
            future.orTimeout(timeoutMs.toLong(), TimeUnit.MILLISECONDS)
        }
        return future
    }

    override fun close() {
        connectionState = ConnectionState.CLOSED
        readerJob?.cancel()
        scope?.cancel()
        udSocket?.close()
        pendingMap.failAll()
        resolvePending.failAll()
        _endpointCache.invalidateAll()
        _subscriptionManager.clear()
    }
}
