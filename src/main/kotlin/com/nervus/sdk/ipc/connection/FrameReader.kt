package com.nervus.sdk.ipc.connection

import com.google.protobuf.InvalidProtocolBufferException
import io.github.nervusos.ipc.v1.Envelope
import java.io.EOFException
import java.io.InputStream
import java.nio.ByteBuffer

class ProtocolViolationException(message: String) : Exception(message)

class FrameReader(private val input: InputStream) {
    private val headerBuf = ByteBuffer.allocate(HEADER_SIZE)

    fun readFrame(): Envelope {
        headerBuf.clear()
        readFully(input, headerBuf)
        headerBuf.flip()
        val length = headerBuf.getInt().toLong() and 0xFFFFFFFFL

        if (length == 0L) {
            throw ProtocolViolationException("zero-length frame")
        }
        if (length > MAX_FRAME_BYTES) {
            throw ProtocolViolationException(
                "frame too large: $length > $MAX_FRAME_BYTES"
            )
        }

        val body = ByteArray(length.toInt())
        readFully(input, ByteBuffer.wrap(body))
        return try {
            Envelope.parseFrom(body)
        } catch (e: InvalidProtocolBufferException) {
            throw ProtocolViolationException("malformed envelope: ${e.message}")
        }
    }

    private fun readFully(stream: InputStream, buf: ByteBuffer) {
        while (buf.hasRemaining()) {
            val n = stream.read(buf.array(), buf.position(), buf.remaining())
            if (n == -1) {
                throw EOFException("connection closed unexpectedly")
            }
            buf.position(buf.position() + n)
        }
    }

    companion object {
        const val HEADER_SIZE = 4
        const val MAX_FRAME_BYTES = 128 * 1024
    }
}
