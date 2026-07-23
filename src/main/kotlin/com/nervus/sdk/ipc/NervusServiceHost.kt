package com.nervus.sdk.ipc

import com.nervus.sdk.ipc.connection.*
import com.nervus.sdk.ipc.dispatch.DispatchHandler
import io.github.nervusos.ipc.v1.*
import kotlinx.coroutines.*

class NervusServiceHost(
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

    private val registeredEndpoints = HashMap<Long, DispatchHandler>()
    private var readerJob: Job? = null
    private var scope: CoroutineScope? = null

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
                sdkVersion = sdkVersion,
                componentId = "service-host"
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
                    Envelope.BodyCase.DISPATCH -> {
                        handleDispatch(envelope.dispatch)
                    }
                    Envelope.BodyCase.CANCEL_DISPATCH -> {
                        handleCancelDispatch(envelope.cancelDispatch)
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
            registeredEndpoints.clear()
        }
    }

    private fun handleDispatch(dispatch: Dispatch) {
        val handler = registeredEndpoints[dispatch.endpointId]
        if (handler == null) {
            val failure = Failure.newBuilder()
                .setCode(StatusCode.STATUS_CODE_NOT_FOUND)
                .setPublicMessage("endpoint not registered: ${dispatch.endpointId}")
                .build()
            val result = DispatchResult.newBuilder()
                .setRouteId(dispatch.routeId)
                .setFailure(failure)
                .build()
            frameWriter?.writeFrame(
                Envelope.newBuilder().setDispatchResult(result).build()
            )
            return
        }

        val result = handler.handle(dispatch)
        frameWriter?.writeFrame(
            Envelope.newBuilder().setDispatchResult(result).build()
        )
    }

    private fun handleCancelDispatch(cancel: CancelDispatch) {
    }

    private fun handlePing() {
        frameWriter?.writeFrame(
            Envelope.newBuilder().setPong(Pong.getDefaultInstance()).build()
        )
    }

    fun registerEndpoint(
        interfaceId: String,
        interfaceMajor: Int,
        interfaceMinor: Int,
        schemaHash: ByteArray = ByteArray(0),
        resourceHandle: String = "",
        handler: DispatchHandler
    ): Long {
        val writer = frameWriter ?: throw IllegalStateException("not connected")
        val requestId = nextRequestId()

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
        return requestId
    }

    private var reqIdCounter = 1L

    private fun nextRequestId(): Long = reqIdCounter++

    fun registerHandler(endpointId: Long, handler: DispatchHandler) {
        registeredEndpoints[endpointId] = handler
    }

    override fun close() {
        readerJob?.cancel()
        scope?.cancel()
        udSocket?.close()
        registeredEndpoints.clear()
    }
}
