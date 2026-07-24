package com.nervus.sdk.runtime

import com.nervus.sdk.annotations.Method
import com.nervus.sdk.ipc.dispatch.DispatchHandler
import com.nervus.sdk.ipc.dispatch.MethodHandler
import io.github.nervusos.ipc.v1.CallerContext
import io.github.nervusos.ipc.v1.DispatchResult
import io.github.nervusos.ipc.v1.Failure
import io.github.nervusos.ipc.v1.StatusCode
import io.github.nervusos.ipc.v1.Success
import java.lang.reflect.InvocationTargetException
import java.util.concurrent.ConcurrentHashMap
import kotlin.coroutines.Continuation
import kotlin.coroutines.intrinsics.COROUTINE_SUSPENDED
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine
import kotlinx.coroutines.runBlocking

internal data class MethodRegistration(
    val methodId: Int,
    val methodName: String,
    val isSuspend: Boolean,
    val invoke: (ByteArray) -> ByteArray,
)

internal class ServiceStub {

    private val registrations = ConcurrentHashMap<Int, MethodRegistration>()

    fun registerInterface(
        instance: Any,
        handler: DispatchHandler,
        dispatchCallback: ((methodId: Int, payload: ByteArray, caller: CallerContext) -> DispatchResult)? = null
    ) {
        val methods = instance::class.java.methods
        val seenIds = HashSet<Int>()
        for (method in methods) {
            val annotation = resolveMethodAnnotation(method) ?: continue
            val methodId = annotation.id
            val allParamTypes = method.parameterTypes

            if (!seenIds.add(methodId)) {
                throw IllegalArgumentException(
                    "duplicate @Method(id=$methodId) on '${method.name}' " +
                    "in class '${instance::class.java.name}'"
                )
            }

            val isSuspend = allParamTypes.lastOrNull()
                ?.let { Continuation::class.java.isAssignableFrom(it) } == true

            val appParamTypes = if (isSuspend) {
                allParamTypes.dropLast(1).toTypedArray()
            } else {
                allParamTypes
            }

            val registration = MethodRegistration(
                methodId = methodId,
                methodName = method.name,
                isSuspend = isSuspend,
                invoke = { payload ->
                    val resolvedArgs = resolveArguments(payload, appParamTypes)
                    try {
                        val result = if (isSuspend) {
                            runBlocking {
                                suspendCoroutine { cont ->
                                    try {
                                        val r = method.invoke(instance, *resolvedArgs, cont)
                                        if (r !== COROUTINE_SUSPENDED) {
                                            cont.resume(r)
                                        }
                                    } catch (e: InvocationTargetException) {
                                        cont.resumeWithException(e.cause ?: e)
                                    } catch (e: Exception) {
                                        cont.resumeWithException(e)
                                    }
                                }
                            }
                        } else {
                            method.invoke(instance, *resolvedArgs)
                        }
                        serializeResult(result)
                    } catch (e: InvocationTargetException) {
                        throw e.cause ?: e
                    }
                }
            )
            registrations[methodId] = registration
            java.util.logging.Logger.getLogger(ServiceStub::class.java.name)
                .info("registered method id=$methodId name=${method.name} suspend=$isSuspend class=${instance::class.java.name}")

            handler.register(methodId, MethodHandler { payload, context ->
                if (dispatchCallback != null) {
                    val result = dispatchCallback(methodId, payload, context.caller)
                    result.toBuilder().setRouteId(context.routeId).build()
                } else {
                    invokeDirect(methodId, payload, context.routeId)
                }
            })
        }
        if (registrations.isEmpty()) {
            java.util.logging.Logger.getLogger(ServiceStub::class.java.name)
                .warning("no @Method found on ${instance::class.java.name} (checked interfaces too)")
        }
    }

    /** @Method may live on the interface; Kotlin does not copy it onto the impl method. */
    private fun resolveMethodAnnotation(method: java.lang.reflect.Method): Method? {
        method.getAnnotation(Method::class.java)?.let { return it }
        val paramTypes = method.parameterTypes
        for (iface in method.declaringClass.interfaces) {
            try {
                val m = iface.getMethod(method.name, *paramTypes)
                m.getAnnotation(Method::class.java)?.let { return it }
            } catch (_: NoSuchMethodException) {
            }
        }
        var superCls = method.declaringClass.superclass
        while (superCls != null && superCls != Any::class.java) {
            try {
                val m = superCls.getMethod(method.name, *paramTypes)
                m.getAnnotation(Method::class.java)?.let { return it }
            } catch (_: NoSuchMethodException) {
            }
            for (iface in superCls.interfaces) {
                try {
                    val m = iface.getMethod(method.name, *paramTypes)
                    m.getAnnotation(Method::class.java)?.let { return it }
                } catch (_: NoSuchMethodException) {
                }
            }
            superCls = superCls.superclass
        }
        return null
    }

    private fun invokeDirect(methodId: Int, payload: ByteArray, routeId: Long): DispatchResult {
        val reg = registrations[methodId]
        if (reg == null) {
            return DispatchResult.newBuilder()
                .setRouteId(routeId)
                .setFailure(Failure.newBuilder()
                    .setCode(StatusCode.STATUS_CODE_NOT_FOUND)
                    .setPublicMessage("no handler for method_id=$methodId")
                    .build())
                .build()
        }
        return try {
            val resultBytes = reg.invoke(payload)
            DispatchResult.newBuilder()
                .setRouteId(routeId)
                .setSuccess(Success.newBuilder()
                    .setCode(StatusCode.STATUS_CODE_OK)
                    .setPayload(com.google.protobuf.ByteString.copyFrom(resultBytes))
                    .build())
                .build()
        } catch (e: Exception) {
            DispatchResult.newBuilder()
                .setRouteId(routeId)
                .setFailure(Failure.newBuilder()
                    .setCode(StatusCode.STATUS_CODE_INTERNAL)
                    .setPublicMessage("method '${reg.methodName}' failed: ${e.message}")
                    .build())
                .build()
        }
    }

    fun dispatch(methodId: Int, payload: ByteArray, caller: CallerContext): DispatchResult? {
        val reg = registrations[methodId] ?: return null
        return try {
            val resultBytes = reg.invoke(payload)
            DispatchResult.newBuilder()
                .setSuccess(Success.newBuilder()
                    .setCode(StatusCode.STATUS_CODE_OK)
                    .setPayload(com.google.protobuf.ByteString.copyFrom(resultBytes))
                    .build())
                .build()
        } catch (e: Exception) {
            DispatchResult.newBuilder()
                .setFailure(Failure.newBuilder()
                    .setCode(StatusCode.STATUS_CODE_INTERNAL)
                    .setPublicMessage("method '${reg.methodName}' failed: ${e.message}")
                    .build())
                .build()
        }
    }

    fun unregisterAll(handler: DispatchHandler) {
        for (methodId in registrations.keys) {
            handler.unregister(methodId)
        }
        registrations.clear()
    }

    internal fun resolveArguments(payload: ByteArray, paramTypes: Array<Class<*>>): Array<Any?> {
        if (paramTypes.isEmpty()) return emptyArray()
        if (paramTypes.size == 1) {
            val type = paramTypes[0]
            if (type == ByteArray::class.java) return arrayOf(payload)
            if (com.google.protobuf.Message::class.java.isAssignableFrom(type)) {
                val parseFrom = type.getMethod("parseFrom", ByteArray::class.java)
                return arrayOf(parseFrom.invoke(null, payload))
            }
        }
        val args = mutableListOf<Any?>()
        var offset = 0
        for (paramType in paramTypes) {
            if (offset + 4 > payload.size) {
                args.add(null)
                break
            }
            val len = bytesToInt(payload, offset)
            offset += 4
            val dataEnd = (offset + len).coerceAtMost(payload.size)
            val data = payload.copyOfRange(offset, dataEnd)
            offset = dataEnd
            when {
                paramType == ByteArray::class.java -> args.add(data)
                paramType == String::class.java -> args.add(String(data))
                paramType == Int::class.java || paramType == Int::class.javaPrimitiveType -> args.add(bytesToInt(data, 0))
                paramType == Long::class.java || paramType == Long::class.javaPrimitiveType -> args.add(bytesToLong(data, 0))
                com.google.protobuf.Message::class.java.isAssignableFrom(paramType) -> {
                    val parseFrom = paramType.getMethod("parseFrom", ByteArray::class.java)
                    args.add(parseFrom.invoke(null, data))
                }
                else -> args.add(data)
            }
        }
        return args.toTypedArray()
    }

    internal fun serializeResult(result: Any?): ByteArray {
        return when (result) {
            is ByteArray -> result
            is com.google.protobuf.Message -> result.toByteArray()
            is String -> result.toByteArray()
            null -> ByteArray(0)
            else -> result.toString().toByteArray()
        }
    }

    private fun bytesToInt(data: ByteArray, offset: Int): Int {
        if (offset + 4 > data.size) return 0
        return ((data[offset].toInt() and 0xFF) shl 24) or
                ((data[offset + 1].toInt() and 0xFF) shl 16) or
                ((data[offset + 2].toInt() and 0xFF) shl 8) or
                (data[offset + 3].toInt() and 0xFF)
    }

    private fun bytesToLong(data: ByteArray, offset: Int): Long {
        if (offset + 8 > data.size) return 0L
        var result = 0L
        for (i in 0 until 8) {
            result = (result shl 8) or (data[offset + i].toLong() and 0xFF)
        }
        return result
    }
}
