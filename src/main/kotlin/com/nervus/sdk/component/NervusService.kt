package com.nervus.sdk.component

import com.nervus.sdk.ipc.NervusServiceHost
import com.nervus.sdk.runtime.ServiceStub
import io.github.nervusos.ipc.v1.CallerContext
import io.github.nervusos.ipc.v1.DispatchResult
import io.github.nervusos.ipc.v1.Failure
import io.github.nervusos.ipc.v1.StatusCode
import java.util.concurrent.TimeUnit

data class ProvidedInterface(
    val id: String,
    val major: Int,
    val minor: Int = 0,
    val instance: Any,
    val resourceHandle: String = "",
    val schemaHash: ByteArray = ByteArray(0),
)

abstract class NervusService(
    config: ComponentConfig = ComponentConfig()
) : Component(config) {

    private var host: NervusServiceHost? = null
    private val stubs = mutableListOf<ServiceStub>()
    private val endpointRegistrations = mutableListOf<EndpointRegistration>()

    protected abstract val providedInterfaces: List<ProvidedInterface>

    override fun isActive(): Boolean = host?.isConnected == true

    override fun doStart() {
        host?.close()
        val h = NervusServiceHost(
            sdkName = config.sdkName,
            sdkVersion = config.sdkVersion
        )
        h.connect(
            socketPath = config.socketPath,
            componentId = config.componentId,
            handshakeTimeoutMs = config.handshakeTimeoutMs
        )
        host = h
        registerProvidedInterfaces(h)
    }

    override fun doClose() {
        for (stub in stubs) {
            host?.let { stub.unregisterAll(it.handler) }
        }
        stubs.clear()
        host?.close()
        host = null
        endpointRegistrations.clear()
    }

    protected open fun dispatch(
        methodId: Int,
        payload: ByteArray,
        context: CallerContext
    ): DispatchResult {
        for (stub in stubs) {
            val result = stub.dispatch(methodId, payload, context)
            if (result != null) return result
        }
        return DispatchResult.newBuilder()
            .setFailure(Failure.newBuilder()
                .setCode(StatusCode.STATUS_CODE_NOT_FOUND)
                .setPublicMessage("no handler for method_id=$methodId")
                .build())
            .build()
    }

    private fun registerProvidedInterfaces(h: NervusServiceHost) {
        for (iface in providedInterfaces) {
            val future = h.registerEndpoint(
                interfaceId = iface.id,
                interfaceMajor = iface.major,
                interfaceMinor = iface.minor,
                schemaHash = iface.schemaHash,
                resourceHandle = iface.resourceHandle,
            )
            val registration: com.nervus.sdk.ipc.Registration
            try {
                registration = future.get(30, TimeUnit.SECONDS)
            } catch (e: Exception) {
                throw RuntimeException("failed to register interface '${iface.id}': ${e.message}", e)
            }
            logger.info("registered interface '${iface.id}' -> endpoint ${registration.endpointId}")

            val stub = ServiceStub()
            stub.registerInterface(iface.instance, h.handler) { methodId, payload, caller ->
                dispatch(methodId, payload, caller)
            }
            stubs.add(stub)
            endpointRegistrations.add(
                EndpointRegistration(
                    endpointId = registration.endpointId,
                    interfaceId = iface.id,
                    interfaceMajor = iface.major,
                    interfaceMinor = iface.minor
                )
            )
        }
    }

    private data class EndpointRegistration(
        val endpointId: Long,
        val interfaceId: String,
        val interfaceMajor: Int,
        val interfaceMinor: Int,
    )
}
