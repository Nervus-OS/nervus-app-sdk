package com.nervus.sdk.ipc

import io.github.nervusos.ipc.v1.Failure
import io.github.nervusos.ipc.v1.ResolveEndpointErrorDetail
import io.github.nervusos.ipc.v1.ResolveEndpointReason

/**
 * 把一个 [Failure] 渲染成人能看懂的一行。
 *
 * # 为什么需要它
 *
 * nervud 【不填】`public_message`——那个字段只允许由受审计的模板生成，绝大多数
 * 内建路径干脆留空。真正的细因在 `error_detail` 里，是一段 typed protobuf。
 *
 * 少了这一层，`"${failure.publicMessage} (code=${failure.code})"` 渲染出来就是
 * 一句光秃秃的
 *
 *     (code=STATUS_CODE_FAILED_PRECONDITION)
 *
 * 而 FAILED_PRECONDITION 在 Resolve 这条路径上至少对应四种完全不同的处置：
 * 接口没注册（等服务起来 / 查它为什么没起来）、版本不匹配（改 min/maxMajor）、
 * 资源找不到（改 selector）、资源不唯一（加 labels 或改 policy）。
 * 只给一个码等于让人四条路各试一遍。
 *
 * 细因就在线上，是客户端把它丢了。
 */
internal fun describeFailure(failure: Failure): String {
    val parts = mutableListOf<String>()
    decodeReason(failure)?.let { parts += it }
    failure.publicMessage.takeIf { it.isNotBlank() }?.let { parts += it }
    parts += "code=${failure.code}"
    return parts.joinToString(" ")
}

/**
 * 解 `error_detail`。
 *
 * 【类型由外层消息决定，不由本字段自述】——这里只在 Resolve 的失败路径上调用，
 * 所以按 [ResolveEndpointErrorDetail] 解。换一条路径要换一个解法，不能在这里
 * 按"猜"来解：按发送方自报的类型解码正是 status.proto 反复在防的事。
 *
 * 解不开就返回 null：一个新版内核加了字段不该让客户端把一次可用的错误报告
 * 变成一次异常。
 */
private fun decodeReason(failure: Failure): String? {
    if (failure.errorDetail.isEmpty) return null
    return try {
        val detail = ResolveEndpointErrorDetail.parseFrom(failure.errorDetail)
        detail.reason
            .takeIf { it != ResolveEndpointReason.RESOLVE_ENDPOINT_REASON_UNSPECIFIED }
            ?.let { "${it.name}${hintFor(it.name)}" }
    } catch (_: Exception) {
        null
    }
}

/**
 * 给每种 reason 附一句「那该怎么办」。
 *
 * 这几句是这个 SDK 里少见的、把运维知识写进代码的地方。理由是它们的读者恰好
 * 是最没有上下文的那个人：应用作者看到一个内核返回的枚举名，通常并不知道
 * 该去改自己的 manifest 还是去查某个系统服务。
 */
private fun hintFor(reason: String): String = when (reason) {
    "RESOLVE_ENDPOINT_REASON_INTERFACE_NOT_FOUND" ->
        "（没有服务注册这个接口：确认提供方在跑，或它注册时被拒了——看 nervud 的 endpoint.RegisterEndpoint 审计）"
    "RESOLVE_ENDPOINT_REASON_VERSION_MISMATCH" ->
        "（接口 major 版本对不上：调整 InterfaceRequirement 的 minMajor/maxMajor）"
    "RESOLVE_ENDPOINT_REASON_RESOURCE_NOT_FOUND" ->
        "（selector 选不到设备：绑资源的接口必须自己给出 resourceType + labels/role，v2 没有隐式默认）"
    "RESOLVE_ENDPOINT_REASON_RESOURCE_AMBIGUOUS" ->
        "（selector 命中多个：加 labels 收窄，或把 selectionPolicy 设成 SYSTEM_PREFERRED）"
    else -> ""
}
