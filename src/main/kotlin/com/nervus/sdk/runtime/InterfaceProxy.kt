package com.nervus.sdk.runtime

import com.nervus.sdk.annotations.Method
import com.nervus.sdk.ipc.NervusClient
import io.github.nervusos.ipc.v1.Response
import io.github.nervusos.ipc.v1.StatusCode
import java.lang.reflect.Proxy
import java.util.concurrent.CompletableFuture
import kotlin.coroutines.Continuation
import kotlin.coroutines.intrinsics.COROUTINE_SUSPENDED

data class ResolvedEndpoint(
    val endpointId: Long,
    val interfaceId: String,
    val interfaceMajor: Int,
    val interfaceMinor: Int,
)

internal class InterfaceProxy(private val client: NervusClient, private val endpoint: ResolvedEndpoint) {

    @Suppress("UNCHECKED_CAST")
    fun <T : Any> create(interfaceClass: kotlin.reflect.KClass<T>): T {
        return Proxy.newProxyInstance(
            interfaceClass.java.classLoader,
            arrayOf(interfaceClass.java)
        ) { proxy, method, args ->
            val annotation = method.getAnnotation(Method::class.java)
                ?: throw UnsupportedOperationException(
                    "Method '${method.name}' is not annotated with @Method"
                )
            val methodId = annotation.id

            val isSuspend = method.parameterTypes.lastOrNull()
                ?.let { Continuation::class.java.isAssignableFrom(it) } == true

            val appArgs = if (isSuspend && args != null) {
                args.dropLast(1).toTypedArray()
            } else {
                args ?: emptyArray()
            }

            val payload = if (appArgs.isNotEmpty()) {
                serializeArgs(appArgs)
            } else {
                ByteArray(0)
            }

            val future = client.call(endpoint.endpointId, methodId, payload)
            val returnType = if (isSuspend) {
                method.genericReturnType
            } else {
                method.returnType
            }

            if (isSuspend) {
                @Suppress("UNCHECKED_CAST")
                val continuation = args?.last() as Continuation<Any?>
                val resultFuture = future.thenApply { response -> convertReturnType(response, returnType) }
                resultFuture.whenComplete { result, ex ->
                    if (ex != null) {
                        continuation.resumeWith(Result.failure(ex))
                    } else {
                        continuation.resumeWith(Result.success(result))
                    }
                }
                COROUTINE_SUSPENDED
            } else {
                future.thenApply { response -> convertReturnType(response, returnType) }
            }
        } as T
    }

    private fun convertReturnType(response: Response, returnType: java.lang.reflect.Type): Any? {
        val raw = extractPayload(response)
        return when {
            returnType == Void.TYPE || returnType == java.lang.Void::class.java -> null
            returnType == ByteArray::class.java -> raw
            returnType == String::class.java -> raw.toString(Charsets.UTF_8)
            returnType == Int::class.java || returnType == Int::class.javaPrimitiveType -> {
                if (raw.size >= 4) ((raw[0].toInt() and 0xFF) shl 24) or
                    ((raw[1].toInt() and 0xFF) shl 16) or
                    ((raw[2].toInt() and 0xFF) shl 8) or
                    (raw[3].toInt() and 0xFF) else 0
            }
            returnType == Long::class.java || returnType == Long::class.javaPrimitiveType -> {
                if (raw.size >= 8) ((raw[0].toLong() and 0xFF) shl 56) or
                    ((raw[1].toLong() and 0xFF) shl 48) or
                    ((raw[2].toLong() and 0xFF) shl 40) or
                    ((raw[3].toLong() and 0xFF) shl 32) or
                    ((raw[4].toLong() and 0xFF) shl 24) or
                    ((raw[5].toLong() and 0xFF) shl 16) or
                    ((raw[6].toLong() and 0xFF) shl 8) or
                    (raw[7].toLong() and 0xFF) else 0L
            }
            returnType == Float::class.java || returnType == Float::class.javaPrimitiveType -> {
                if (raw.size >= 4) Float.fromBits(
                    ((raw[0].toInt() and 0xFF) shl 24) or
                    ((raw[1].toInt() and 0xFF) shl 16) or
                    ((raw[2].toInt() and 0xFF) shl 8) or
                    (raw[3].toInt() and 0xFF)
                ) else 0f
            }
            returnType == Double::class.java || returnType == Double::class.javaPrimitiveType -> {
                if (raw.size >= 8) Double.fromBits(
                    ((raw[0].toLong() and 0xFF) shl 56) or
                    ((raw[1].toLong() and 0xFF) shl 48) or
                    ((raw[2].toLong() and 0xFF) shl 40) or
                    ((raw[3].toLong() and 0xFF) shl 32) or
                    ((raw[4].toLong() and 0xFF) shl 24) or
                    ((raw[5].toLong() and 0xFF) shl 16) or
                    ((raw[6].toLong() and 0xFF) shl 8) or
                    (raw[7].toLong() and 0xFF)
                ) else 0.0
            }
            returnType == Boolean::class.java || returnType == Boolean::class.javaPrimitiveType -> {
                raw.isNotEmpty() && raw[0] != 0.toByte()
            }
            com.google.protobuf.Message::class.java.isAssignableFrom(returnType as Class<*>) -> {
                val parseMethod = returnType.getMethod("parseFrom", ByteArray::class.java)
                parseMethod.invoke(null, raw)
            }
            else -> raw
        }
    }

    internal fun serializeArgs(args: Array<out Any?>): ByteArray {
        if (args.size == 1) {
            val arg = args[0]
            if (arg is ByteArray) return arg
            if (arg is com.google.protobuf.Message) return arg.toByteArray()
        }
        val buffers = args.map { arg ->
            val bytes = when (arg) {
                is ByteArray -> arg
                is com.google.protobuf.Message -> arg.toByteArray()
                is String -> arg.toByteArray()
                is Int -> intToBytes(arg)
                is Long -> longToBytes(arg)
                else -> arg?.toString()?.toByteArray() ?: ByteArray(0)
            }
            intToBytes(bytes.size) + bytes
        }
        return buffers.fold(ByteArray(0)) { acc, b -> acc + b }
    }

    internal fun extractPayload(response: Response): ByteArray {
        if (response.hasFailure()) {
            throw RuntimeException("RPC call failed: ${response.failure.publicMessage} (code=${response.failure.code})")
        }
        val success = response.success
        if (success.code != StatusCode.STATUS_CODE_OK && success.code != StatusCode.STATUS_CODE_ACCEPTED) {
            throw RuntimeException("RPC call returned unexpected status: ${success.code}")
        }
        return success.payload.toByteArray()
    }

    private fun intToBytes(value: Int): ByteArray = byteArrayOf(
        (value shr 24).toByte(),
        (value shr 16).toByte(),
        (value shr 8).toByte(),
        value.toByte()
    )

    private fun longToBytes(value: Long): ByteArray = byteArrayOf(
        (value shr 56).toByte(),
        (value shr 48).toByte(),
        (value shr 40).toByte(),
        (value shr 32).toByte(),
        (value shr 24).toByte(),
        (value shr 16).toByte(),
        (value shr 8).toByte(),
        value.toByte()
    )
}
