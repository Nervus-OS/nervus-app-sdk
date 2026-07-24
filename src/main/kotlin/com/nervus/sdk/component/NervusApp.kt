package com.nervus.sdk.component

import com.nervus.sdk.annotations.Event as EventAnnotation
import com.nervus.sdk.annotations.Interface
import com.nervus.sdk.annotations.Method
import com.nervus.sdk.ipc.ConnectionState
import com.nervus.sdk.ipc.NervusClient
import com.nervus.sdk.runtime.InterfaceProxy
import com.nervus.sdk.runtime.ResolvedEndpoint
import io.github.nervusos.ipc.v1.Event
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

    protected abstract val requiredInterfaces: List<InterfaceRequirement>

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
        resolveRequiredInterfaces(c)
    }

    override fun doClose() {
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
