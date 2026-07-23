package com.nervus.sdk.ipc.dispatch

import io.github.nervusos.ipc.v1.Dispatch
import io.github.nervusos.ipc.v1.DispatchResult

fun interface DispatchHandler {
    fun handle(dispatch: Dispatch): DispatchResult
}
