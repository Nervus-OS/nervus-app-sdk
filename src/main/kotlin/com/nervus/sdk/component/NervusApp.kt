package com.nervus.sdk.component

import com.nervus.sdk.annotations.Event as EventAnnotation
import com.nervus.sdk.annotations.Interface
import com.nervus.sdk.annotations.Method
import com.nervus.sdk.ipc.ConnectionState
import com.nervus.sdk.ipc.NervusClient
import com.nervus.sdk.runtime.InterfaceProxy
import com.nervus.sdk.runtime.ResolvedEndpoint
import com.nervus.sdk.runtime.ServiceStub
import io.github.nervusos.ipc.v1.CallerContext
import io.github.nervusos.ipc.v1.DispatchResult
import io.github.nervusos.ipc.v1.Event
import io.github.nervusos.ipc.v1.Failure
import io.github.nervusos.ipc.v1.ResolveEndpointSuccess
import io.github.nervusos.ipc.v1.Response
import io.github.nervusos.ipc.v1.StatusCode
import kotlinx.coroutines.flow.Flow
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import kotlin.reflect.KClass

data class InterfaceRequirement(
    val id: String,
    val minMajor: Int = 1,
    val maxMajor: Int = 1,
    val resourceType: String = "",
    val resourceRole: String = "",
    val isRequired: Boolean = true,
)

abstract class NervusApp(
    config: ComponentConfig = ComponentConfig()
) : Component(config) {

    private var client: NervusClient? = null
    private val resolvedEndpoints = ConcurrentHashMap<String, ResolvedEndpoint>()
    private val proxyCache = ConcurrentHashMap<Class<*>, Any>()
    private val stubs = mutableListOf<ServiceStub>()

    protected abstract val requiredInterfaces: List<InterfaceRequirement>

    /**
     * 本组件对外提供的 Interface。缺省为空。
     *
     * ## 为什么一个"App"也要提供接口
     *
     * 内核【没有】"启动某个 app"的 IPC：`EnsureStarted` 只能由
     * `endpoint.Resolve` 在拉起 `launch_mode: on-demand` 的组件时触发，而
     * `manual` 模式没有任何 wire 路径能叫醒它。
     *
     * 所以"能被点开的 app"在 Nervus 上必须：
     *   1. manifest 里 `launch_mode: on-demand`
     *   2. `exports` 声明一个接口（哪怕它一个方法都没有）
     *   3. 启动后 RegisterEndpoint 报到
     *
     * 别人"启动"它的方式就是对那个接口发 ResolveEndpoint —— 内核发现没有活着的
     * 提供者，于是把组件拉起来，等它注册完再返回。
     *
     * 接口没有方法时 [instance] 传 null 即可，只占一个可被 Resolve 的名字。
     */
    protected open val providedInterfaces: List<ProvidedInterface> = emptyList()

    override fun isActive(): Boolean = client?.connectionState == ConnectionState.CONNECTED

    override fun doStart() {
        client?.close()
        val c = NervusClient(
            sdkName = config.sdkName,
            sdkVersion = config.sdkVersion
        )
        c.connect(
            socketPath = config.socketPath,
            componentId = config.componentId,
            handshakeTimeoutMs = config.handshakeTimeoutMs
        )
        client = c
        // 先注册再解析：注册是"让别人能叫醒我"，解析是"我要用别人"。
        // 顺序反过来的话，若解析阻塞在拉起某个组件上，本组件迟迟没报到，
        // 那个正在等我们的 Resolve 就会超时
        registerProvidedInterfaces(c)
        resolveRequiredInterfaces(c)
    }

    private fun registerProvidedInterfaces(c: NervusClient) {
        for (iface in providedInterfaces) {
            val endpointId = try {
                c.registerEndpoint(
                    interfaceId = iface.id,
                    interfaceMajor = iface.major,
                    interfaceMinor = iface.minor,
                    schemaHash = iface.schemaHash,
                    resourceHandle = iface.resourceHandle,
                ).get(30, TimeUnit.SECONDS)
            } catch (e: Exception) {
                // 注册失败不能降级为警告：注册不上就意味着这个组件永远叫不醒，
                // 而症状会表现为"点了图标没反应"，离根因很远。直接失败让
                // supervisor 退避重启，日志里留下准确原因
                throw RuntimeException("failed to register interface '${iface.id}': ${e.message}", e)
            }
            iface.instance?.let { impl ->
                val stub = ServiceStub()
                stub.registerInterface(impl, c.handler) { methodId, payload, caller ->
                    dispatch(methodId, payload, caller)
                }
                stubs.add(stub)
            }
            logger.info("registered interface '${iface.id}' -> endpoint $endpointId")
        }
    }

    /**
     * 未命中任何 method handler 时的兜底。子类可覆盖。
     *
     * 缺省回 NOT_FOUND —— 协议的 StatusCode 里没有 UNIMPLEMENTED，而
     * UNAVAILABLE 的语义是"稍后重试"，对一个根本不存在的方法是误导。
     */
    protected open fun dispatch(
        methodId: Int,
        payload: ByteArray,
        context: CallerContext
    ): DispatchResult =
        DispatchResult.newBuilder()
            .setFailure(
                Failure.newBuilder()
                    .setCode(StatusCode.STATUS_CODE_NOT_FOUND)
                    .setPublicMessage("no handler for method_id=$methodId")
                    .build()
            )
            .build()

    /**
     * 运行期解析一个接口，返回其 endpoint_id。
     *
     * 与 [requiredInterfaces] 的区别：那批在 start 时一次性解析；本方法用于
     * 【运行期才知道要用谁】的场景——启动器点开一个 app 就是典型例子，
     * 装了哪些 app 是运行期才从 pkgmanager 查出来的。
     *
     * 对"启动 app"这个用途，返回值通常用不上：调用本身就是启动动作
     * （内核为此拉起了那个组件）。
     */
    /**
     * 请求内核拉起一个已安装的组件。
     *
     * 需要 `perm.system.launch`（MinTrust=Platform）。Launcher 与会话服务用它。
     *
     * @return true 表示该组件在本次请求之前就已经在运行
     */
    protected fun launchComponent(
        packageId: String,
        componentId: String,
        timeoutSeconds: Long = 30,
    ): Boolean {
        val c = client ?: throw IllegalStateException("component not started")
        return c.launchComponent(packageId, componentId)
            .get(timeoutSeconds, TimeUnit.SECONDS)
    }

    protected fun resolveNow(
        interfaceId: String,
        minMajor: Int = 1,
        maxMajor: Int = 1,
        timeoutSeconds: Long = 30,
    ): Long {
        val c = client ?: throw IllegalStateException("component not started")
        val response = c.resolveEndpoint(
            interfaceId = interfaceId,
            minInterfaceMajor = minMajor,
            maxInterfaceMajor = maxMajor,
        ).get(timeoutSeconds, TimeUnit.SECONDS)
        if (response.hasFailure()) {
            throw RuntimeException("resolve '$interfaceId' failed: ${response.failure.publicMessage}")
        }
        return ResolveEndpointSuccess.parseFrom(response.success.payload).endpointId
    }

    override fun doClose() {
        client?.let { c -> stubs.forEach { it.unregisterAll(c.handler) } }
        stubs.clear()
        client?.close()
        client = null
        resolvedEndpoints.clear()
        proxyCache.clear()
    }

    protected fun subscribe(
        interfaceClass: KClass<*>,
        eventId: Int,
        payload: ByteArray = ByteArray(0)
    ): CompletableFuture<Flow<Event>> {
        val c = client ?: throw IllegalStateException("component not started")
        val endpoint = findEndpointForInterface(interfaceClass)
        return c.subscribe(endpoint.endpointId, eventId, payload)
    }

    private fun resolveRequiredInterfaces(c: NervusClient) {
        for (req in requiredInterfaces) {
            val future = c.resolveEndpoint(
                interfaceId = req.id,
                minInterfaceMajor = req.minMajor,
                maxInterfaceMajor = req.maxMajor,
                resourceType = req.resourceType,
                resourceRole = req.resourceRole,
            )
            val response: Response
            try {
                response = future.get(30, TimeUnit.SECONDS)
            } catch (e: Exception) {
                if (req.isRequired) {
                    throw RuntimeException("failed to resolve required interface '${req.id}': ${e.message}", e)
                }
                logger.warning("failed to resolve optional interface '${req.id}': ${e.message}")
                continue
            }
            if (response.hasFailure()) {
                if (req.isRequired) {
                    throw RuntimeException("failed to resolve required interface '${req.id}': ${response.failure.publicMessage}")
                }
                logger.warning("failed to resolve optional interface '${req.id}': ${response.failure.publicMessage}")
                continue
            }
            val success = ResolveEndpointSuccess.parseFrom(response.success.payload)
            val endpoint = ResolvedEndpoint(
                endpointId = success.endpointId,
                interfaceId = req.id,
                interfaceMajor = success.interfaceMajor,
                interfaceMinor = success.interfaceMinor,
            )
            resolvedEndpoints[req.id] = endpoint
            logger.info("resolved interface '${req.id}' -> endpoint ${endpoint.endpointId}")
        }
    }

    /**
     * 对已解析的接口发一次调用，返回响应 payload 的原始字节。
     *
     * ## 为什么提供它，而不是都走 [use] 的动态代理
     *
     * [use] 靠反射从方法签名推断返回类型，但 Kotlin 的 `suspend fun` 与
     * `CompletableFuture<T>` 在字节码里的返回类型分别是 `Object` 和
     * `CompletableFuture`——**泛型参数在运行期拿不到**，于是 InterfaceProxy 的
     * 类型转换会退化成返回原始 ByteArray，调用方拿到手再 ClassCastException。
     *
     * 对于 payload 就是 protobuf 消息的接口（系统接口都是），显式写
     * `XxxResult.parseFrom(call(...))` 既准确又看得懂，比让反射去猜强。
     *
     * @param interfaceId 必须已在 [requiredInterfaces] 中解析过
     * @param methodId 接口 schema 里的稳定数字方法 ID
     */
    protected fun call(
        interfaceId: String,
        methodId: Int,
        payload: ByteArray = ByteArray(0),
        timeoutSeconds: Long = 30,
    ): ByteArray {
        val c = client ?: throw IllegalStateException("component not started")
        val endpoint = resolvedEndpoints[interfaceId]
            ?: throw IllegalStateException(
                "interface '$interfaceId' not resolved; declare it in requiredInterfaces"
            )
        val response = c.call(endpoint.endpointId, methodId, payload)
            .get(timeoutSeconds, TimeUnit.SECONDS)
        if (response.hasFailure()) {
            throw RuntimeException(
                "call $interfaceId#$methodId failed: ${response.failure.publicMessage} " +
                    "(code=${response.failure.code})"
            )
        }
        return response.success.payload.toByteArray()
    }

    @Suppress("UNCHECKED_CAST")
    protected fun <T : Any> use(interfaceClass: KClass<T>): T {
        val endpoint = findEndpointForInterface(interfaceClass)
        val cached = proxyCache[interfaceClass.java]
        if (cached != null) return cached as T
        val proxy = InterfaceProxy(
            client ?: throw IllegalStateException("component not started"),
            endpoint
        )
        val instance = proxy.create(interfaceClass)
        proxyCache[interfaceClass.java] = instance
        return instance
    }

    private fun <T : Any> findEndpointForInterface(interfaceClass: KClass<T>): ResolvedEndpoint {
        val interfaceId = resolveInterfaceId(interfaceClass)
        return resolvedEndpoints[interfaceId]
            ?: throw IllegalStateException(
                "interface '${interfaceClass.qualifiedName}' was not resolved. " +
                "Make sure to declare it in requiredInterfaces with a matching 'id' field."
            )
    }

    private fun resolveInterfaceId(interfaceClass: KClass<*>): String {
        val annotation = interfaceClass.annotations.filterIsInstance<Interface>().firstOrNull()
        val customId = annotation?.id?.takeIf { it.isNotEmpty() }
        return customId
            ?: interfaceClass.qualifiedName
            ?: interfaceClass.simpleName
            ?: "unknown"
    }
}
