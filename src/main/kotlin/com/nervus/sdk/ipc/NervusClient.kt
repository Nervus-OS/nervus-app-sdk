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
import java.util.concurrent.ConcurrentHashMap
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
    @Volatile
    private var udSocket: UnixDomainSocket? = null
    @Volatile
    private var frameReader: FrameReader? = null
    @Volatile
    private var frameWriter: FrameWriter? = null
    private var handshakeResult: HandshakeResult? = null

    private val requestIdGenerator = RequestIdGenerator()
    private val pendingMap = PendingMap()
    private val resolvePending = PendingMap()
    private val resolveEndpointIds = ConcurrentHashMap<Long, String>()
    private val endpointSubscriptions = ConcurrentHashMap<Long, MutableSet<Long>>()
    private val _endpointCache = EndpointCache()
    private val _subscriptionManager = SubscriptionManager()

    private var readerJob: Job? = null
    private var pingJob: Job? = null
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
        componentId: String = "",
        handshakeTimeoutMs: Long = 5000
    ): HandshakeResult {
        if (connectionState != ConnectionState.DISCONNECTED) {
            throw IllegalStateException("already connected or connecting")
        }
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
                componentId = componentId,
                timeoutMs = handshakeTimeoutMs
            )

            udSocket = socket
            frameReader = reader
            frameWriter = writer
            handshakeResult = result
            connectionState = ConnectionState.CONNECTED

            val idleTimeout = result.limits.idleTimeoutMs
            scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
            readerJob = scope?.launch {
                runReaderLoop()
            }
            if (idleTimeout > 0) {
                pingJob = scope?.launch {
                    runPingLoop(idleTimeout)
                }
            }

            return result
        } catch (e: Exception) {
            connectionState = ConnectionState.CLOSED
            socket.close()
            throw e
        }
    }

    private suspend fun runPingLoop(idleTimeoutMs: Int) {
        val interval = idleTimeoutMs.toLong() / 2
        while (udSocket?.isOpen == true && connectionState == ConnectionState.CONNECTED) {
            delay(interval)
            val nonce = requestIdGenerator.next()
            frameWriter?.writeFrame(
                Envelope.newBuilder().setPing(
                    Ping.newBuilder().setNonce(nonce).build()
                ).build()
            )
        }
    }

    private fun closeConnection() {
        connectionState = ConnectionState.CLOSED
        udSocket?.close()
        readerJob?.cancel()
        pingJob?.cancel()
        scope?.cancel()
        pendingMap.failAll()
        resolvePending.failAll()
        resolveEndpointIds.clear()
        _endpointCache.invalidateAll()
        _subscriptionManager.clear()
    }

    private suspend fun runReaderLoop() {
        try {
            while (udSocket?.isOpen == true && connectionState == ConnectionState.CONNECTED) {
                val envelope = try {
                    frameReader?.readFrame()
                } catch (e: java.net.SocketTimeoutException) {
                    logger.fine("read timeout, checking connection")
                    continue
                } ?: break
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
                        logger.severe("unknown envelope body: ${envelope.bodyCase} - closing connection")
                        closeConnection()
                        return
                    }
                }
            }
        } catch (e: EOFException) {
            logger.info("connection closed by server")
        } catch (e: ProtocolViolationException) {
            logger.severe("protocol violation: ${e.message}")
        } catch (e: Exception) {
            logger.fine("reader loop exited: ${e.message}")
        } finally {
            closeConnection()
        }
    }

    private fun handleResponse(response: Response) {
        if (response.hasSuccess()) {
            val code = response.success.code
            if (code != StatusCode.STATUS_CODE_OK && code != StatusCode.STATUS_CODE_ACCEPTED) {
                logger.severe("invalid success code: $code")
                return
            }
        } else if (response.hasFailure()) {
            val code = response.failure.code
            if (code == StatusCode.STATUS_CODE_UNSPECIFIED || code == StatusCode.STATUS_CODE_OK || code == StatusCode.STATUS_CODE_ACCEPTED) {
                logger.severe("invalid failure code: $code")
                return
            }
        } else {
            logger.warning("response with no outcome")
            return
        }
        pendingMap.complete(response.requestId, response)
    }

    private fun handleResolveEndpointResult(result: ResolveEndpointResult) {
        val interfaceId = resolveEndpointIds.remove(result.requestId) ?: return
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
                        interfaceId = interfaceId,
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
        closeEndpointSubscriptions(died.endpointId)
        when (died.reason) {
            EndpointDiedReason.ENDPOINT_DIED_REASON_SERVICE_SHUTTING_DOWN,
            EndpointDiedReason.ENDPOINT_DIED_REASON_SERVICE_RESTARTED -> {
                logger.info("endpoint ${died.endpointId} died (${died.reason}), may be resolved again later")
            }
            EndpointDiedReason.ENDPOINT_DIED_REASON_SERVICE_GONE,
            EndpointDiedReason.ENDPOINT_DIED_REASON_RESOURCE_FAULT -> {
                logger.warning("endpoint ${died.endpointId} died (${died.reason}), retry with backoff")
            }
            else -> {
                logger.warning("endpoint ${died.endpointId} died (unknown reason)")
            }
        }
    }

    private fun handleEndpointRevoked(revoked: EndpointRevoked) {
        _endpointCache.invalidateAll()
        closeEndpointSubscriptions(revoked.endpointId)
        logger.severe("endpoint ${revoked.endpointId} revoked (${revoked.reason}) - do not retry")
    }

    private fun closeEndpointSubscriptions(endpointId: Long) {
        val subs = endpointSubscriptions.remove(endpointId) ?: return
        for (subId in subs) {
            _subscriptionManager.closeSubscription(subId)
        }
    }

    private fun handleSubscribeResult(result: SubscribeResult) {
        val response = Response.newBuilder().apply {
            when (result.outcomeCase) {
                SubscribeResult.OutcomeCase.SUCCESS -> {
                    val success = result.success
                    _subscriptionManager.bind(result.requestId, success.subscriptionId)
                    _subscriptionManager.setDeliveryClass(success.subscriptionId, success.deliveryClass)
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
                else -> {
                    logger.warning("unsubscribe_result with no outcome")
                    setFailure(Failure.newBuilder()
                        .setCode(StatusCode.STATUS_CODE_INTERNAL)
                        .setPublicMessage("unsubscribe_result with no outcome")
                        .build())
                }
            }
        }.build()
        resolvePending.complete(result.requestId, response)
    }

    private fun handleSubscriptionClosed(closed: SubscriptionClosed) {
        _subscriptionManager.closeSubscription(closed.subscriptionId)
        endpointSubscriptions.values.forEach { it.remove(closed.subscriptionId) }
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
        explicitComponent: String = "",
        timeoutMs: Int = handshakeResult?.limits?.defaultTimeoutMs ?: 5000
    ): CompletableFuture<Response> {
        val writer = frameWriter ?: throw IllegalStateException("not connected")
        val requestId = requestIdGenerator.next()
        val future = resolvePending.register(requestId)
        resolveEndpointIds[requestId] = interfaceId

        try {
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
            if (explicitComponent.isNotEmpty()) {
                builder.explicitComponent = explicitComponent
            }

            writer.writeFrame(
                Envelope.newBuilder().setResolveEndpoint(builder.build()).build()
            )

            if (timeoutMs > 0) {
                future.orTimeout(timeoutMs.toLong(), TimeUnit.MILLISECONDS)
            }
        } catch (e: Exception) {
            resolvePending.failOne(requestId, StatusCode.STATUS_CODE_UNAVAILABLE, "write failed: ${e.message}")
        }
        return future
    }

    fun call(
        endpointId: Long,
        methodId: Int,
        payload: ByteArray = ByteArray(0),
        timeoutMs: Int = handshakeResult?.limits?.defaultTimeoutMs ?: 5000
    ): CompletableFuture<Response> {
        val limits = handshakeResult?.limits
        if (limits != null) {
            if (payload.size > limits.defaultMethodPayloadBytes && limits.defaultMethodPayloadBytes > 0) {
                val f = CompletableFuture<Response>()
                f.completeExceptionally(IllegalStateException("payload too large: ${payload.size} > ${limits.defaultMethodPayloadBytes}"))
                return f
            }
            if (pendingMap.size() >= limits.maxInflightRequests) {
                val f = CompletableFuture<Response>()
                f.completeExceptionally(IllegalStateException("max inflight requests (${limits.maxInflightRequests}) reached"))
                return f
            }
        }
        val effectiveTimeout = if (limits != null && limits.maxTimeoutMs > 0) timeoutMs.coerceAtMost(limits.maxTimeoutMs) else timeoutMs
        val writer = frameWriter ?: throw IllegalStateException("not connected")
        val requestId = requestIdGenerator.next()
        val future = pendingMap.register(requestId, payload.size)

        try {
            val request = Request.newBuilder()
                .setRequestId(requestId)
                .setEndpointId(endpointId)
                .setMethodId(methodId)
                .setTimeoutMs(effectiveTimeout)
                .setPayload(com.google.protobuf.ByteString.copyFrom(payload))
                .build()

            writer.writeFrame(
                Envelope.newBuilder().setRequest(request).build()
            )

            if (effectiveTimeout > 0) {
                future.orTimeout(effectiveTimeout.toLong(), TimeUnit.MILLISECONDS)
            }
        } catch (e: Exception) {
            pendingMap.failOne(requestId, StatusCode.STATUS_CODE_UNAVAILABLE, "write failed: ${e.message}")
        }
        return future
    }

    fun cancel(requestId: Long) {
        val writer = frameWriter ?: throw IllegalStateException("not connected")
        pendingMap.cancelOne(requestId)
        val cancel = Cancel.newBuilder().setRequestId(requestId).build()
        try {
            writer.writeFrame(
                Envelope.newBuilder().setCancel(cancel).build()
            )
        } catch (e: Exception) {
            logger.fine("failed to write cancel frame: ${e.message}")
        }
    }

    fun subscribe(
        endpointId: Long,
        eventId: Int,
        payload: ByteArray = ByteArray(0),
        timeoutMs: Int = handshakeResult?.limits?.defaultTimeoutMs ?: 5000
    ): CompletableFuture<Flow<Event>> {
        val limits = handshakeResult?.limits
        if (limits != null && _subscriptionManager.subscriptionCount() >= limits.maxSubscriptions) {
            val future = CompletableFuture<Flow<Event>>()
            future.completeExceptionally(IllegalStateException("max subscriptions (${limits.maxSubscriptions}) reached"))
            return future
        }
        val writer = frameWriter ?: throw IllegalStateException("not connected")
        val requestId = requestIdGenerator.next()
        val future = resolvePending.register(requestId)

        try {
            val subscribe = Subscribe.newBuilder()
                .setRequestId(requestId)
                .setEndpointId(endpointId)
                .setEventId(eventId)
                .setPayload(com.google.protobuf.ByteString.copyFrom(payload))
                .build()

            writer.writeFrame(
                Envelope.newBuilder().setSubscribe(subscribe).build()
            )
        } catch (e: Exception) {
            resolvePending.failOne(requestId, StatusCode.STATUS_CODE_UNAVAILABLE, "write failed: ${e.message}")
        }

        val resultFuture: CompletableFuture<Flow<Event>> = future.thenApply { response ->
            if (response.hasSuccess()) {
                val subResult = SubscribeSuccess.parseFrom(response.success.payload)
                val subId = subResult.subscriptionId
                val flow = _subscriptionManager.subscribe(subId, subResult.deliveryClass)
                endpointSubscriptions.computeIfAbsent(endpointId) { ConcurrentHashMap.newKeySet() }.add(subId)
                flow
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

        try {
            writer.writeFrame(
                Envelope.newBuilder()
                    .setUnsubscribe(Unsubscribe.newBuilder()
                        .setRequestId(requestId)
                        .setSubscriptionId(subscriptionId)
                        .build())
                    .build()
            )
        } catch (e: Exception) {
            resolvePending.failOne(requestId, StatusCode.STATUS_CODE_UNAVAILABLE, "write failed: ${e.message}")
        }

        if (timeoutMs > 0) {
            future.orTimeout(timeoutMs.toLong(), TimeUnit.MILLISECONDS)
        }
        return future
    }

    override fun close() {
        connectionState = ConnectionState.CLOSED
        udSocket?.close()
        readerJob?.cancel()
        pingJob?.cancel()
        scope?.cancel()
        pendingMap.failAll()
        resolvePending.failAll()
        resolveEndpointIds.clear()
        endpointSubscriptions.clear()
        _endpointCache.invalidateAll()
        _subscriptionManager.clear()
    }
}
