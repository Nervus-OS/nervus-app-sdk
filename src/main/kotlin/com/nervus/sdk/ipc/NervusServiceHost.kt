package com.nervus.sdk.ipc

import com.nervus.sdk.ipc.connection.*
import com.nervus.sdk.ipc.dispatch.DispatchHandler
import io.github.nervusos.ipc.v1.*
import kotlinx.coroutines.*
import java.io.EOFException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import java.util.logging.Logger
import com.nervus.sdk.ipc.rpc.PendingMap
import com.nervus.sdk.ipc.rpc.RequestIdGenerator

internal data class Registration(
    val endpointId: Long,
    val interfaceId: String,
    val interfaceMajor: Int,
    val interfaceMinor: Int
)

internal class NervusServiceHost(
    val sdkName: String = "nervus-app-sdk",
    val sdkVersion: String = "0.1.0"
) : AutoCloseable {
    @Volatile
    private var udSocket: UnixDomainSocket? = null
    @Volatile
    private var frameReader: FrameReader? = null
    @Volatile
    private var frameWriter: FrameWriter? = null
    private var handshakeResult: HandshakeResult? = null

    private val requestIdGenerator = RequestIdGenerator()
    private val registerPending = PendingMap()
    private val dispatchHandler = DispatchHandler()
    private val registrations = HashMap<String, Registration>()

    private var readerJob: Job? = null
    private var pingJob: Job? = null
    private var scope: CoroutineScope? = null

    @Volatile
    var isConnected: Boolean = false
        private set

    private val logger = Logger.getLogger(NervusServiceHost::class.java.name)

    fun connect(
        socketPath: String,
        componentId: String = "",
        handshakeTimeoutMs: Long = 5000
    ) {
        if (isConnected) {
            throw IllegalStateException("already connected")
        }
        val socket = UnixDomainSocket.connect(socketPath)
        try {
            val reader = FrameReader(socket.inputStream)
            val writer = FrameWriter(socket.outputStream)

            val handshake = HelloHandshake(reader, writer)
            val result = handshake.negotiate(
                minMajor = PROTOCOL_MAJOR_MIN,
                maxMajor = PROTOCOL_MAJOR_MAX,
                maxMinor = PROTOCOL_MINOR_MAX,
                sdkName = sdkName,
                sdkVersion = sdkVersion,
                componentId = componentId,
                timeoutMs = handshakeTimeoutMs
            )

            udSocket = socket
            frameReader = reader
            frameWriter = writer
            handshakeResult = result
            isConnected = true

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
        } catch (e: Exception) {
            isConnected = false
            socket.close()
            throw e
        }
    }

    val handler: DispatchHandler get() = dispatchHandler

    fun registerEndpoint(
        interfaceId: String,
        interfaceMajor: Int,
        interfaceMinor: Int,
        schemaHash: ByteArray = ByteArray(0),
        resourceHandle: String = "",
        timeoutMs: Int = 5000
    ): CompletableFuture<Registration> {
        val writer = frameWriter ?: throw IllegalStateException("not connected")
        val requestId = requestIdGenerator.next()
        val future = registerPending.register(requestId)

        try {
            val register = RegisterEndpoint.newBuilder()
                .setRequestId(requestId)
                .setInterfaceId(interfaceId)
                .setInterfaceMajor(interfaceMajor)
                .setInterfaceMinor(interfaceMinor)
                .setInterfaceSchemaHash(com.google.protobuf.ByteString.copyFrom(schemaHash))
                .setResourceHandle(resourceHandle)
                .build()

            writer.writeFrame(
                Envelope.newBuilder().setRegisterEndpoint(register).build()
            )
        } catch (e: Exception) {
            registerPending.failOne(requestId, StatusCode.STATUS_CODE_UNAVAILABLE, "write failed: ${e.message}")
        }

        if (timeoutMs > 0) {
            future.orTimeout(timeoutMs.toLong(), TimeUnit.MILLISECONDS)
        }

        return future.thenApply { response ->
            if (response.hasFailure()) {
                throw RuntimeException("register endpoint rejected: ${response.failure.publicMessage}")
            }
            val success = RegisterEndpointSuccess.parseFrom(response.success.payload)
            val registration = Registration(
                endpointId = success.endpointId,
                interfaceId = interfaceId,
                interfaceMajor = interfaceMajor,
                interfaceMinor = interfaceMinor
            )
            registrations[interfaceId] = registration
            registration
        }
    }

    // ---- 提供侧事件 ---------------------------------------------------------
    //
    // 【这三个方法只有 Service 能调】。nervud 收到普通 App 发来的 PublishEvent
    // 或 BindEventScope 会【直接关掉连接】——它把方向错误当协议违规审计，不是
    // 回一个错误码。所以 NervusClient（App 那条路径）上没有对应的方法，不是漏了。

    /**
     * 推一条事件给所有订阅方。
     *
     * 单向：没有请求 ID，没有回执。事件被拒（endpoint 不属于你、契约里没声明过
     * 这个 event_id、载荷超限）表现为订阅方【什么也没收到】，本地不会抛异常。
     * 排查要看 nervud 的审计日志。
     *
     * [monotonicTimestampNanos] 是【事件发生的时刻】，由你自己取。SDK 不替你
     * 填当前时间：那样填出来的是「推送时刻」，跟采样时刻差多少只有你知道，而
     * 一个看起来合理的错时间比一个明摆着的 0 更难发现。不提供就留 0。
     */
    fun publishEvent(
        endpointId: Long,
        eventId: Int,
        payload: ByteArray = ByteArray(0),
        monotonicTimestampNanos: Long = 0
    ) {
        val writer = frameWriter ?: throw IllegalStateException("not connected")
        writer.writeFrame(
            Envelope.newBuilder()
                .setPublishEvent(
                    PublishEvent.newBuilder()
                        .setEndpointId(endpointId)
                        .setEventId(eventId)
                        .setPayload(com.google.protobuf.ByteString.copyFrom(payload))
                        .setMonotonicTimestampNanos(monotonicTimestampNanos)
                        .build()
                )
                .build()
        )
    }

    /**
     * 登记一个实例的归属：`scope` 这个实例属于 [originRouteId] 那次调用的调用方。
     *
     * 声明了 `EventMeta.scoped` 的事件，订阅方必须带 scope 才订得上，而 nervud
     * 只让【登记过的所有者】订。所以造出实例的那一刻就要登记：
     *
     * ```kotlin
     * // OpenStream 的 handler 里，ctx 是 DispatchContext
     * val streamId = openDevice(...)
     * host.bindEventScope(endpointId, scope = streamId, originRouteId = ctx.routeId)
     * ```
     *
     * 【归属靠 route 证明，不靠自报】。你说的是「属于我正在处理的这次调用的
     * 调用方」，那次调用是谁发起的 nervud 自己知道——所以你没法把别人塞进来，
     * 也没法把自己塞进别人的事件流。
     *
     * 单向，无回执。失败（endpoint 不属于你、route 已经结束）的症状是订阅方
     * 拿到 NOT_FOUND。所以要么在把句柄返回给调用方【之前】登记，要么把失败
     * 告诉它——别让它拿着一个永远收不到事件的 stream_id。
     *
     * 同一个 scope 重复登记直接覆盖，不报错。
     */
    fun bindEventScope(endpointId: Long, scope: Long, originRouteId: Long) {
        sendEventScope(endpointId, scope, originRouteId, released = false)
    }

    /**
     * 撤销一个实例的归属。实例关掉时【必须调】，正常路径和异常路径都要走到。
     *
     * 连接断开和 endpoint 撤下时 nervud 会连带清干净，但那兜的是崩溃；运行期间
     * 反复开关实例不撤销，内核的归属表会无界增长。
     *
     * 【先发终态事件，再撤归属】。撤了之后订阅方就收不到了，而「停了」那一条
     * 正是它最需要的。
     */
    fun releaseEventScope(endpointId: Long, scope: Long) {
        sendEventScope(endpointId, scope, originRouteId = 0, released = true)
    }

    private fun sendEventScope(
        endpointId: Long,
        scope: Long,
        originRouteId: Long,
        released: Boolean
    ) {
        val writer = frameWriter ?: throw IllegalStateException("not connected")
        writer.writeFrame(
            Envelope.newBuilder()
                .setBindEventScope(
                    BindEventScope.newBuilder()
                        .setEndpointId(endpointId)
                        .setScope(scope)
                        .setOriginRouteId(originRouteId)
                        .setReleased(released)
                        .build()
                )
                .build()
        )
    }

    fun unregisterEndpoint(
        endpointId: Long,
        drain: Boolean = false,
        timeoutMs: Int = 5000
    ): CompletableFuture<Unit> {
        val writer = frameWriter ?: throw IllegalStateException("not connected")
        val requestId = requestIdGenerator.next()
        val future = registerPending.register(requestId)

        try {
            val unregister = UnregisterEndpoint.newBuilder()
                .setRequestId(requestId)
                .setEndpointId(endpointId)
                .setDrain(drain)
                .build()

            writer.writeFrame(
                Envelope.newBuilder().setUnregisterEndpoint(unregister).build()
            )
        } catch (e: Exception) {
            registerPending.failOne(requestId, StatusCode.STATUS_CODE_UNAVAILABLE, "write failed: ${e.message}")
        }

        if (timeoutMs > 0) {
            future.orTimeout(timeoutMs.toLong(), TimeUnit.MILLISECONDS)
        }

        return future.thenApply { response ->
            if (response.hasFailure()) {
                throw RuntimeException("unregister endpoint rejected: ${response.failure.publicMessage}")
            }
            registrations.entries.removeAll { it.value.endpointId == endpointId }
        }
    }

    private suspend fun runPingLoop(idleTimeoutMs: Int) {
        val interval = idleTimeoutMs.toLong() / 2
        while (udSocket?.isOpen == true && isConnected) {
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
        for (reg in registrations.values) {
            try {
                frameWriter?.writeFrame(
                    Envelope.newBuilder().setUnregisterEndpoint(
                        UnregisterEndpoint.newBuilder()
                            .setRequestId(requestIdGenerator.next())
                            .setEndpointId(reg.endpointId)
                            .build()
                    ).build()
                )
            } catch (_: Exception) {
            }
        }
        isConnected = false
        udSocket?.close()
        readerJob?.cancel()
        pingJob?.cancel()
        scope?.cancel()
        registerPending.failAll()
        dispatchHandler.clear()
        registrations.clear()
    }

    private suspend fun runReaderLoop() {
        try {
            while (udSocket?.isOpen == true && isConnected) {
                val envelope = try {
                    frameReader?.readFrame()
                } catch (e: java.net.SocketTimeoutException) {
                    logger.fine("read timeout, checking connection")
                    continue
                } ?: break
                when (envelope.bodyCase) {
                    Envelope.BodyCase.REGISTER_ENDPOINT_RESULT -> {
                        handleRegisterResult(envelope.registerEndpointResult)
                    }
                    Envelope.BodyCase.UNREGISTER_ENDPOINT_RESULT -> {
                        handleUnregisterResult(envelope.unregisterEndpointResult)
                    }
                    Envelope.BodyCase.DISPATCH -> {
                        handleDispatch(envelope.dispatch)
                    }
                    Envelope.BodyCase.CANCEL_DISPATCH -> {
                        handleCancelDispatch(envelope.cancelDispatch)
                    }
                    Envelope.BodyCase.PING -> {
                        handlePing(envelope.ping)
                    }
                    Envelope.BodyCase.PONG -> {}
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

    private fun handleRegisterResult(result: RegisterEndpointResult) {
        when (result.outcomeCase) {
            RegisterEndpointResult.OutcomeCase.SUCCESS -> {
                val success = result.success
                registerPending.complete(result.requestId, Response.newBuilder()
                    .setSuccess(Success.newBuilder()
                        .setCode(StatusCode.STATUS_CODE_OK)
                        .setPayload(success.toByteString())
                        .build())
                    .build())
            }
            RegisterEndpointResult.OutcomeCase.FAILURE -> {
                registerPending.complete(result.requestId, Response.newBuilder()
                    .setFailure(result.failure)
                    .build())
            }
            else -> {
                logger.warning("register_endpoint_result with no outcome")
                registerPending.complete(result.requestId, Response.newBuilder()
                    .setFailure(Failure.newBuilder()
                        .setCode(StatusCode.STATUS_CODE_INTERNAL)
                        .setPublicMessage("register_endpoint_result with no outcome")
                        .build())
                    .build())
            }
        }
    }

    private fun handleUnregisterResult(result: UnregisterEndpointResult) {
        when (result.outcomeCase) {
            UnregisterEndpointResult.OutcomeCase.SUCCESS -> {
                registerPending.complete(result.requestId, Response.newBuilder()
                    .setSuccess(Success.newBuilder()
                        .setCode(StatusCode.STATUS_CODE_OK)
                        .build())
                    .build())
            }
            UnregisterEndpointResult.OutcomeCase.FAILURE -> {
                registerPending.complete(result.requestId, Response.newBuilder()
                    .setFailure(result.failure)
                    .build())
            }
            else -> {
                logger.warning("unregister_endpoint_result with no outcome")
                registerPending.complete(result.requestId, Response.newBuilder()
                    .setFailure(Failure.newBuilder()
                        .setCode(StatusCode.STATUS_CODE_INTERNAL)
                        .setPublicMessage("unregister_endpoint_result with no outcome")
                        .build())
                    .build())
            }
        }
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

    private fun handleCancelDispatch(cancelDispatch: CancelDispatch) {
        dispatchHandler.cancelDispatch(cancelDispatch.routeId)
        logger.info("cancel dispatch received for route_id=${cancelDispatch.routeId}, reason=${cancelDispatch.reason}")
    }

    private fun handlePing(ping: Ping) {
        val pong = Pong.newBuilder().setNonce(ping.nonce).build()
        try {
            frameWriter?.writeFrame(
                Envelope.newBuilder().setPong(pong).build()
            )
        } catch (e: Exception) {
            logger.fine("failed to write pong: ${e.message}")
        }
    }

    override fun close() {
        closeConnection()
    }
}
