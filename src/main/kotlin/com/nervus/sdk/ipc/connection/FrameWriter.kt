package com.nervus.sdk.ipc.connection

import io.github.nervusos.ipc.v1.Envelope
import java.io.OutputStream
import java.nio.ByteBuffer

class FrameWriter(private val output: OutputStream) {
    private val lock = Any()

    fun writeFrame(envelope: Envelope) {
        val body = envelope.toByteArray()
        val length = body.size

        if (length > FrameReader.MAX_FRAME_BYTES) {
            throw ProtocolViolationException(
                "frame too large to write: $length > ${FrameReader.MAX_FRAME_BYTES}"
            )
        }

        val header = ByteBuffer.allocate(FrameReader.HEADER_SIZE)
        header.putInt(length)
        header.flip()

        synchronized(lock) {
            output.write(header.array())
            output.write(body)
            output.flush()
        }
    }
}
