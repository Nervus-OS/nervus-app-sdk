package com.nervus.sdk.ipc

import com.nervus.sdk.ipc.connection.FrameReader
import com.nervus.sdk.ipc.connection.FrameWriter
import com.nervus.sdk.ipc.dispatch.DispatchHandler
import com.nervus.sdk.ipc.rpc.PendingMap
import com.nervus.sdk.ipc.rpc.RequestIdGenerator
import io.github.nervusos.ipc.v1.*
import kotlin.test.Test
import java.io.PipedInputStream
import java.io.PipedOutputStream
import java.util.concurrent.TimeUnit
import kotlin.test.*

class InMemoryRoundTripTest {

    @Test
    fun `send request and receive response through pipes`() {
        val clientOut = PipedOutputStream()
        val clientIn = PipedInputStream()
        val serverOut = PipedOutputStream()
        val serverIn = PipedInputStream()

        clientOut.connect(serverIn)
        serverOut.connect(clientIn)

        val clientWriter = FrameWriter(clientOut)
        val clientReader = FrameReader(clientIn)
        val serverWriter = FrameWriter(serverOut)
        val serverReader = FrameReader(serverIn)

        val idGen = RequestIdGenerator()
        val pending = PendingMap()

        val requestId = idGen.next()
        val future = pending.register(requestId)

        val request = Request.newBuilder()
            .setRequestId(requestId)
            .setEndpointId(100L)
            .setMethodId(200)
            .setTimeoutMs(5000)
            .setPayload(com.google.protobuf.ByteString.copyFromUtf8("hello"))
            .build()

        clientWriter.writeFrame(
            Envelope.newBuilder().setRequest(request).build()
        )

        val serverReceived = serverReader.readFrame()
        assertTrue(serverReceived.hasRequest())
        assertEquals(requestId, serverReceived.request.requestId)
        assertEquals(100L, serverReceived.request.endpointId)
        assertEquals(200, serverReceived.request.methodId)

        val response = Response.newBuilder()
            .setRequestId(requestId)
            .setSuccess(
                Success.newBuilder()
                    .setCode(StatusCode.STATUS_CODE_OK)
                    .setPayload(com.google.protobuf.ByteString.copyFromUtf8("world"))
                    .build()
            )
            .build()

        serverWriter.writeFrame(
            Envelope.newBuilder().setResponse(response).build()
        )

        val clientReceived = clientReader.readFrame()
        assertTrue(clientReceived.hasResponse())
        assertEquals(requestId, clientReceived.response.requestId)
        assertTrue(clientReceived.response.hasSuccess())
        assertEquals("world", clientReceived.response.success.payload.toStringUtf8())
    }
}
