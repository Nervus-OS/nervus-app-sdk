package com.nervus.sdk.ipc.dispatch

import io.github.nervusos.ipc.v1.*

internal data class DispatchContext(
    val routeId: Long,
    val remainingMs: Int,
    val caller: CallerContext
) {
    val isCancelled: Boolean get() = routeId in cancelledRouteIds

    internal var cancelledRouteIds: Set<Long> = emptySet()
}

internal fun interface MethodHandler {
    fun handle(payload: ByteArray, context: DispatchContext): DispatchResult
}

internal class DispatchHandler {
    private val handlers = HashMap<Int, MethodHandler>()
    private val cancelledRoutes = HashSet<Long>()

    fun register(methodId: Int, handler: MethodHandler) {
        handlers[methodId] = handler
    }

    fun unregister(methodId: Int) {
        handlers.remove(methodId)
    }

    fun cancelDispatch(routeId: Long) {
        cancelledRoutes.add(routeId)
    }

    fun dispatch(dispatch: Dispatch): DispatchResult {
        val handler = handlers[dispatch.methodId]
            ?: return DispatchResult.newBuilder()
                .setRouteId(dispatch.routeId)
                .setFailure(Failure.newBuilder()
                    .setCode(StatusCode.STATUS_CODE_NOT_FOUND)
                    .setPublicMessage("unknown method_id: ${dispatch.methodId}")
                    .build())
                .build()
        val ctx = DispatchContext(
            routeId = dispatch.routeId,
            remainingMs = dispatch.remainingMs,
            caller = dispatch.caller
        ).apply { cancelledRouteIds = cancelledRoutes.toSet() }
        if (ctx.isCancelled) {
            cancelledRoutes.remove(dispatch.routeId)
            return DispatchResult.newBuilder()
                .setRouteId(dispatch.routeId)
                .setFailure(Failure.newBuilder()
                    .setCode(StatusCode.STATUS_CODE_CANCELLED)
                    .setPublicMessage("dispatch cancelled")
                    .build())
                .build()
        }
        val result = handler.handle(dispatch.payload.toByteArray(), ctx)
        cancelledRoutes.remove(dispatch.routeId)
        return result.toBuilder().setRouteId(dispatch.routeId).build()
    }

    fun clear() {
        handlers.clear()
        cancelledRoutes.clear()
    }
}
