package com.nervus.sdk.ipc

import com.nervus.sdk.ipc.connection.FrameReader
import com.nervus.sdk.ipc.connection.FrameWriter
import com.nervus.sdk.ipc.connection.HelloHandshake
import com.nervus.sdk.ipc.connection.PROTOCOL_MAJOR_MAX
import com.nervus.sdk.ipc.connection.PROTOCOL_MAJOR_MIN
import com.nervus.sdk.ipc.connection.PROTOCOL_MINOR_MAX
import com.nervus.sdk.ipc.connection.ProtocolViolationException
import io.github.nervusos.ipc.v1.*
import java.io.PipedInputStream
import java.io.PipedOutputStream
import kotlin.test.*

/**
 * V2 线协议这一层的用例。
 *
 * 这里验的都是【错了不会当场报错】的东西：版本谈错了照样能连上、scope 漏了
 * 照样能发出去。症状要到运行时才显形，而且看起来像别的问题。
 */
class ProtocolV2Test {

    // ---- 版本 -------------------------------------------------------------

    @Test
    fun `sdk speaks protocol 2 only`() {
        assertEquals(2, PROTOCOL_MAJOR_MIN)
        assertEquals(2, PROTOCOL_MAJOR_MAX)
        assertEquals(0, PROTOCOL_MINOR_MAX)
    }

    /**
     * 【下界是 2 的意义】：v1 的 nervud 必须连不上，而不是连上之后语义悄悄
     * 分叉。写 min = 1 的话它会握手成功，然后在空 selector 的 Resolve 上给出
     * 跟 v2 不同的设备——两边都不报错。
     */
    @Test
    fun `server choosing protocol 1 is rejected`() {
        val (client, server) = pipePair()

        // 内核侧：收 Hello，回一个 v1 的 HelloAck
        Thread {
            server.reader.readFrame()
            server.writer.writeFrame(
                Envelope.newBuilder().setHelloAck(
                    HelloAck.newBuilder().setSuccess(
                        HelloAckSuccess.newBuilder()
                            .setProtocolMajor(1)
                            .setProtocolMinor(0)
                            .setLimits(ConnectionLimits.getDefaultInstance())
                            .build()
                    ).build()
                ).build()
            )
        }.start()

        assertFailsWith<ProtocolViolationException> {
            HelloHandshake(client.reader, client.writer).negotiate(
                minMajor = PROTOCOL_MAJOR_MIN,
                maxMajor = PROTOCOL_MAJOR_MAX,
                maxMinor = PROTOCOL_MINOR_MAX,
                sdkName = "test",
                sdkVersion = "0",
            )
        }
    }

    @Test
    fun `hello advertises protocol 2`() {
        val (client, server) = pipePair()

        Thread {
            HelloHandshake(client.reader, client.writer).runCatching {
                negotiate(
                    minMajor = PROTOCOL_MAJOR_MIN,
                    maxMajor = PROTOCOL_MAJOR_MAX,
                    maxMinor = PROTOCOL_MINOR_MAX,
                    sdkName = "test",
                    sdkVersion = "0",
                )
            }
        }.start()

        val hello = server.reader.readFrame().hello
        assertEquals(2, hello.minProtocolMajor)
        assertEquals(2, hello.maxProtocolMajor)
    }

    // ---- 实例作用域 -------------------------------------------------------

    /**
     * scope 在 Envelope 上，不在 payload 里——nervud 要自己看懂它才能裁决归属，
     * 不能按 Provider 的 schema 去解一段 bytes。这条用例守的是它没被挪回 payload。
     */
    @Test
    fun `subscribe carries scope on the envelope`() {
        val (client, server) = pipePair()

        client.writer.writeFrame(
            Envelope.newBuilder().setSubscribe(
                Subscribe.newBuilder()
                    .setRequestId(1)
                    .setEndpointId(5)
                    .setEventId(1)
                    .setScope(4242)
                    .build()
            ).build()
        )

        val sub = server.reader.readFrame().subscribe
        assertEquals(4242L, sub.scope)
        assertTrue(sub.payload.isEmpty, "scope 不该被塞进 payload")
    }

    @Test
    fun `bind and release event scope round trip`() {
        val (client, server) = pipePair()

        client.writer.writeFrame(
            Envelope.newBuilder().setBindEventScope(
                BindEventScope.newBuilder()
                    .setEndpointId(7)
                    .setScope(99)
                    .setOriginRouteId(1234)
                    .build()
            ).build()
        )
        client.writer.writeFrame(
            Envelope.newBuilder().setBindEventScope(
                BindEventScope.newBuilder()
                    .setEndpointId(7)
                    .setScope(99)
                    .setReleased(true)
                    .build()
            ).build()
        )

        val bind = server.reader.readFrame().bindEventScope
        assertEquals(7L, bind.endpointId)
        assertEquals(99L, bind.scope)
        assertEquals(1234L, bind.originRouteId)
        assertFalse(bind.released)

        val release = server.reader.readFrame().bindEventScope
        assertTrue(release.released)
        assertEquals(99L, release.scope)
    }

    @Test
    fun `publish event round trip`() {
        val (client, server) = pipePair()

        client.writer.writeFrame(
            Envelope.newBuilder().setPublishEvent(
                PublishEvent.newBuilder()
                    .setEndpointId(7)
                    .setEventId(2)
                    .setPayload(com.google.protobuf.ByteString.copyFromUtf8("state"))
                    .setMonotonicTimestampNanos(555)
                    .build()
            ).build()
        )

        val ev = server.reader.readFrame().publishEvent
        assertEquals(7L, ev.endpointId)
        assertEquals(2, ev.eventId)
        assertEquals("state", ev.payload.toStringUtf8())
        assertEquals(555L, ev.monotonicTimestampNanos)
    }

    // ---- selector ---------------------------------------------------------

    @Test
    fun `no selector when nothing is specified`() {
        assertNull(
            buildResourceSelector(
                "", "", emptyMap(),
                ResourceSelectionPolicy.RESOURCE_SELECTION_POLICY_UNSPECIFIED
            )
        )
    }

    /**
     * 【纯 labels 的选择必须发得出去】。只按 type/role 判断「有没有 selector」
     * 的话，一个按语义选设备的调用会把整个 selector 丢掉——而按语义选正是 v2
     * 推荐的写法。错在客户端，报在服务端。
     */
    @Test
    fun `labels alone produce a selector`() {
        val sel = buildResourceSelector(
            "", "", mapOf("nervus.camera.facing" to "front"),
            ResourceSelectionPolicy.RESOURCE_SELECTION_POLICY_UNSPECIFIED
        )
        assertNotNull(sel)
        assertEquals("front", sel.labelsMap["nervus.camera.facing"])
        assertEquals("", sel.type)
    }

    @Test
    fun `policy alone produces a selector`() {
        val sel = buildResourceSelector(
            "", "", emptyMap(),
            ResourceSelectionPolicy.RESOURCE_SELECTION_POLICY_SYSTEM_PREFERRED
        )
        assertNotNull(sel)
        assertEquals(
            ResourceSelectionPolicy.RESOURCE_SELECTION_POLICY_SYSTEM_PREFERRED,
            sel.policy
        )
    }

    @Test
    fun `all four fields are carried through`() {
        val sel = buildResourceSelector(
            "nervus.resource.camera",
            "front",
            mapOf("a" to "1", "b" to "2"),
            ResourceSelectionPolicy.RESOURCE_SELECTION_POLICY_REQUIRE_UNIQUE
        )
        assertNotNull(sel)
        assertEquals("nervus.resource.camera", sel.type)
        assertEquals("front", sel.role)
        assertEquals(2, sel.labelsCount)
        assertEquals(
            ResourceSelectionPolicy.RESOURCE_SELECTION_POLICY_REQUIRE_UNIQUE,
            sel.policy
        )
    }

    // ---- 管道对 -----------------------------------------------------------

    private class Side(val reader: FrameReader, val writer: FrameWriter)

    private fun pipePair(): Pair<Side, Side> {
        val aOut = PipedOutputStream()
        val aIn = PipedInputStream()
        val bOut = PipedOutputStream()
        val bIn = PipedInputStream()
        aOut.connect(bIn)
        bOut.connect(aIn)
        return Side(FrameReader(aIn), FrameWriter(aOut)) to
            Side(FrameReader(bIn), FrameWriter(bOut))
    }
}

/**
 * Failure 的细因渲染。
 *
 * nervud 不填 public_message，细因在 error_detail 里。少了解码，应用作者看到的
 * 就是一句光秃秃的 `(code=STATUS_CODE_FAILED_PRECONDITION)`——而这个码在 Resolve
 * 路径上对应四种完全不同的处置。
 */
class FailureTextTest {

    private fun failureWith(reason: ResolveEndpointReason): Failure =
        Failure.newBuilder()
            .setCode(StatusCode.STATUS_CODE_FAILED_PRECONDITION)
            .setErrorDetail(
                ResolveEndpointErrorDetail.newBuilder().setReason(reason).build().toByteString()
            )
            .build()

    /** 【核心】：细因必须出现在消息里，光有 code 等于让人四条路各试一遍。 */
    @Test
    fun `reason is surfaced, not just the code`() {
        val text = describeFailure(
            failureWith(ResolveEndpointReason.RESOLVE_ENDPOINT_REASON_INTERFACE_NOT_FOUND)
        )
        assertContains(text, "INTERFACE_NOT_FOUND")
        assertContains(text, "STATUS_CODE_FAILED_PRECONDITION")
    }

    @Test
    fun `each resolve reason carries an actionable hint`() {
        val reasons = listOf(
            ResolveEndpointReason.RESOLVE_ENDPOINT_REASON_INTERFACE_NOT_FOUND,
            ResolveEndpointReason.RESOLVE_ENDPOINT_REASON_VERSION_MISMATCH,
            ResolveEndpointReason.RESOLVE_ENDPOINT_REASON_RESOURCE_NOT_FOUND,
            ResolveEndpointReason.RESOLVE_ENDPOINT_REASON_RESOURCE_AMBIGUOUS,
        )
        for (r in reasons) {
            val text = describeFailure(failureWith(r))
            assertTrue(text.contains("（"), "reason $r 没有给出该怎么办：$text")
        }
    }

    /** 没有 detail 时不崩，退化成 code。 */
    @Test
    fun `bare failure degrades to the code`() {
        val text = describeFailure(
            Failure.newBuilder().setCode(StatusCode.STATUS_CODE_NOT_FOUND).build()
        )
        assertEquals("code=STATUS_CODE_NOT_FOUND", text)
    }

    /**
     * 【解不开的 detail 不能抛】。新版内核换了 detail 类型时，客户端该退化成
     * 报 code，而不是把一次可用的错误报告变成一次解析异常——那会掩盖真正的错误。
     */
    @Test
    fun `undecodable detail degrades instead of throwing`() {
        val text = describeFailure(
            Failure.newBuilder()
                .setCode(StatusCode.STATUS_CODE_INTERNAL)
                .setErrorDetail(com.google.protobuf.ByteString.copyFrom(byteArrayOf(-1, -1, -1, -1)))
                .build()
        )
        assertContains(text, "STATUS_CODE_INTERNAL")
    }

    /** public_message 非空时一并带上，不互相覆盖。 */
    @Test
    fun `public message is kept alongside the reason`() {
        val text = describeFailure(
            Failure.newBuilder()
                .setCode(StatusCode.STATUS_CODE_FAILED_PRECONDITION)
                .setPublicMessage("provider not ready")
                .setErrorDetail(
                    ResolveEndpointErrorDetail.newBuilder()
                        .setReason(ResolveEndpointReason.RESOLVE_ENDPOINT_REASON_INTERFACE_NOT_FOUND)
                        .build().toByteString()
                )
                .build()
        )
        assertContains(text, "provider not ready")
        assertContains(text, "INTERFACE_NOT_FOUND")
    }
}
