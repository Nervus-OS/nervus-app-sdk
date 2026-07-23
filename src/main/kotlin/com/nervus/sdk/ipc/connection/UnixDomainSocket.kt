package com.nervus.sdk.ipc.connection

import java.io.InputStream
import java.io.OutputStream
import java.net.StandardProtocolFamily
import java.net.UnixDomainSocketAddress
import java.nio.channels.SocketChannel

class UnixDomainSocket(
    val socketChannel: SocketChannel,
    val inputStream: InputStream,
    val outputStream: OutputStream,
    val remoteAddress: UnixDomainSocketAddress
) : AutoCloseable {
    override fun close() {
        socketChannel.close()
    }

    val isOpen: Boolean get() = socketChannel.isOpen

    companion object {
        fun connect(socketPath: String): UnixDomainSocket {
            val address = UnixDomainSocketAddress.of(socketPath)
            val channel = SocketChannel.open(StandardProtocolFamily.UNIX)
            channel.connect(address)
            val input = channel.socket().getInputStream()
            val output = channel.socket().getOutputStream()
            return UnixDomainSocket(channel, input, output, address)
        }
    }
}
