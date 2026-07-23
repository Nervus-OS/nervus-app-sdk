package com.nervus.sdk.ipc.rpc

import io.github.nervusos.ipc.v1.Failure
import io.github.nervusos.ipc.v1.Response
import io.github.nervusos.ipc.v1.StatusCode
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap

class PendingMap {
    private val map = ConcurrentHashMap<Long, CompletableFuture<Response>>()

    fun register(requestId: Long): CompletableFuture<Response> {
        val future = CompletableFuture<Response>()
        val existing = map.putIfAbsent(requestId, future)
        if (existing != null) {
            throw IllegalStateException("duplicate request_id: $requestId")
        }
        return future
    }

    fun complete(requestId: Long, response: Response): Boolean {
        val future = map.remove(requestId) ?: return false
        return future.complete(response)
    }

    fun cancelOne(requestId: Long): Boolean {
        val future = map.remove(requestId) ?: return false
        return future.cancel(false)
    }

    fun failAll() {
        val failure = Failure.newBuilder()
            .setCode(StatusCode.STATUS_CODE_UNAVAILABLE)
            .setPublicMessage("connection closed")
            .build()
        val response = Response.newBuilder()
            .setFailure(failure)
            .build()

        val iter = map.entries.iterator()
        while (iter.hasNext()) {
            val entry = iter.next()
            iter.remove()
            entry.value.complete(response)
        }
    }

    fun size(): Int = map.size
}
