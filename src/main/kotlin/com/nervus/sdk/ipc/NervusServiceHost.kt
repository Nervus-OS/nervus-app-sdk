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
