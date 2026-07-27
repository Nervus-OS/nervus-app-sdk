package com.nervus.sdk.ipc

import com.nervus.sdk.ipc.connection.*
import com.nervus.sdk.ipc.dispatch.DispatchHandler
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

internal open class NervusClient(
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

    // ---- 提供侧（RegisterEndpoint / Dispatch）--------------------------------
    //
    // 为什么消费侧的 Client 也要能注册 endpoint：内核【没有】"启动某个 app"的
    // IPC——EnsureStarted 只能被 endpoint.Resolve 在拉起 on-demand 组件时触发。
    // 于是任何"能被启动"的 app 都必须导出一个接口并注册它，否则谁也叫不醒它。
    //
    // 也就是说"纯消费者"这个角色在 Nervus 上不存在：一个有界面、要能被点开的
    // app 天然同时是提供者。把注册能力放在这里，而不是逼调用方再开一条连接，
    // 是因为 endpoint_id 是【连接作用域】的，两条连接意味着两套句柄和两次
    // 组件核对，出问题时很难对上号。
    private val registerPending = PendingMap()
    private val launchPending = PendingMap()
    private val dispatchHandler = DispatchHandler()
    private val registrations = ConcurrentHashMap<String, Long>()
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
        connectionState = ConnectionState.DISCONNECTED
        udSocket?.close()
        readerJob?.cancel()
        pingJob?.cancel()
        scope?.cancel()
        pendingMap.failAll()
        resolvePending.failAll()
        resolveEndpointIds.clear()
        _endpointCache.invalidateAll()
        _subscriptionManager.clear()
        // 提供侧状态同样要清：连接断了，注册在这条连接上的 endpoint_id 全部失效
        // （endpoint_id 是连接作用域的），重连后必须重新 RegisterEndpoint。
        // 不清的话重连后会拿着旧句柄去回 DispatchResult，被内核当迟到结果丢弃
        registerPending.failAll()
        launchPending.failAll()
        dispatchHandler.clear()
        registrations.clear()
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
                    Envelope.BodyCase.REGISTER_ENDPOINT_RESULT -> {
                        handleRegisterEndpointResult(envelope.registerEndpointResult)
                    }
                    Envelope.BodyCase.LAUNCH_COMPONENT_RESULT -> {
                        handleLaunchComponentResult(envelope.launchComponentResult)
                    }
                    Envelope.BodyCase.DISPATCH -> {
                        handleDispatch(envelope.dispatch)
                    }
                    Envelope.BodyCase.CANCEL_DISPATCH -> {
                        dispatchHandler.cancelDispatch(envelope.cancelDispatch.routeId)
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

    /** 提供侧的 dispatch 分发器。ServiceStub 往它上面挂 method handler */
    val handler: DispatchHandler get() = dispatchHandler

    /**
     * 向 nervud 报到一个本组件实现的 Interface。
     *
     * 只能注册 manifest 的 `components[].exports` 里已声明的接口，且要有对应权限
     * （`visibility: public` 需 perm.service.register，`package` 需
     * perm.service.register.private）——内核 endpoint/register.go 的七步准入。
     *
     * 返回服务端侧的 endpoint_id。它与调用方 Resolve 得到的 endpoint_id 是
     * **两个命名空间**：同一个数字在两条连接上毫无关系。
     */
    fun registerEndpoint(
        interfaceId: String,
        interfaceMajor: Int = 1,
        interfaceMinor: Int = 0,
        schemaHash: ByteArray = ByteArray(0),
        resourceHandle: String = "",
        timeoutMs: Int = 5000
    ): CompletableFuture<Long> {
        val writer = frameWriter ?: throw IllegalStateException("not connected")
        val requestId = requestIdGenerator.next()
        val future = registerPending.register(requestId)

        try {
            writer.writeFrame(
                Envelope.newBuilder().setRegisterEndpoint(
                    RegisterEndpoint.newBuilder()
                        .setRequestId(requestId)
                        .setInterfaceId(interfaceId)
                        .setInterfaceMajor(interfaceMajor)
                        .setInterfaceMinor(interfaceMinor)
                        .setInterfaceSchemaHash(com.google.protobuf.ByteString.copyFrom(schemaHash))
                        .setResourceHandle(resourceHandle)
                        .build()
                ).build()
            )
            if (timeoutMs > 0) {
                future.orTimeout(timeoutMs.toLong(), TimeUnit.MILLISECONDS)
            }
        } catch (e: Exception) {
            registerPending.failOne(requestId, StatusCode.STATUS_CODE_UNAVAILABLE, "write failed: ${e.message}")
        }

        return future.thenApply { response ->
            if (response.hasFailure()) {
                throw RuntimeException("register endpoint '$interfaceId' rejected: ${response.failure.publicMessage}")
            }
            val endpointId = RegisterEndpointSuccess.parseFrom(response.success.payload).endpointId
            registrations[interfaceId] = endpointId
            endpointId
        }
    }

    private fun handleRegisterEndpointResult(result: RegisterEndpointResult) {
        val response = when (result.outcomeCase) {
            RegisterEndpointResult.OutcomeCase.SUCCESS ->
                Response.newBuilder().setSuccess(
                    Success.newBuilder()
                        .setCode(StatusCode.STATUS_CODE_OK)
                        .setPayload(result.success.toByteString())
                        .build()
                ).build()
            RegisterEndpointResult.OutcomeCase.FAILURE ->
                Response.newBuilder().setFailure(result.failure).build()
            else ->
                Response.newBuilder().setFailure(
                    Failure.newBuilder()
                        .setCode(StatusCode.STATUS_CODE_INTERNAL)
                        .setPublicMessage("register_endpoint_result with no outcome")
                        .build()
                ).build()
        }
        registerPending.complete(result.requestId, response)
    }

    /**
     * 请求 nervud 拉起一个已安装的组件（Envelope 的 LaunchComponent，body 80）。
     *
     * 需要 `perm.system.launch`（MinTrust=Platform）——只有随系统镜像发布、
     * 平台签名的包拿得到。Launcher 与会话服务是它的使用者。
     *
     * 成功【只】意味着那个组件现在在跑，不建立任何调用关系。想和它通信仍要走
     * [resolveEndpoint]，该有的权限与可见性裁决一条不少。
     *
     * @return true 表示该组件在本次请求之前就已经在运行
     */
    fun launchComponent(
        packageId: String,
        componentId: String,
        timeoutMs: Int = 30_000,
    ): CompletableFuture<Boolean> {
        val writer = frameWriter ?: throw IllegalStateException("not connected")
        val requestId = requestIdGenerator.next()
        val future = launchPending.register(requestId)

        try {
            writer.writeFrame(
                Envelope.newBuilder().setLaunchComponent(
                    LaunchComponent.newBuilder()
                        .setRequestId(requestId)
                        .setPackageId(packageId)
                        .setComponentId(componentId)
                        .build()
                ).build()
            )
            if (timeoutMs > 0) {
                future.orTimeout(timeoutMs.toLong(), TimeUnit.MILLISECONDS)
            }
        } catch (e: Exception) {
            launchPending.failOne(requestId, StatusCode.STATUS_CODE_UNAVAILABLE, "write failed: ${e.message}")
        }

        return future.thenApply { response ->
            if (response.hasFailure()) {
                throw RuntimeException(
                    "launch $packageId/$componentId rejected: ${response.failure.publicMessage} " +
                        "(code=${response.failure.code})"
                )
            }
            LaunchComponentSuccess.parseFrom(response.success.payload).alreadyRunning
        }
    }

    private fun handleLaunchComponentResult(result: LaunchComponentResult) {
        val response = when (result.outcomeCase) {
            LaunchComponentResult.OutcomeCase.SUCCESS ->
                Response.newBuilder().setSuccess(
                    Success.newBuilder()
                        .setCode(StatusCode.STATUS_CODE_OK)
                        .setPayload(result.success.toByteString())
                        .build()
                ).build()
            LaunchComponentResult.OutcomeCase.FAILURE ->
                Response.newBuilder().setFailure(result.failure).build()
            else ->
                Response.newBuilder().setFailure(
                    Failure.newBuilder()
                        .setCode(StatusCode.STATUS_CODE_INTERNAL)
                        .setPublicMessage("launch_component_result with no outcome")
                        .build()
                ).build()
        }
        launchPending.complete(result.requestId, response)
    }

    private fun handleDispatch(dispatch: Dispatch) {
        val dispatchResult = dispatchHandler.dispatch(dispatch)
        try {
            frameWriter?.writeFrame(
                Envelope.newBuilder().setDispatchResult(dispatchResult).build()
            )
        } catch (e: Exception) {
            logger.severe("failed to write dispatch result: ${e.message}")
        }
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
