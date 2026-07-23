package com.nervus.sdk.ipc.endpoint

data class EndpointCacheKey(
    val interfaceId: String,
    val interfaceVersion: String,
    val schemaHash: ByteArray
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is EndpointCacheKey) return false
        return interfaceId == other.interfaceId &&
                interfaceVersion == other.interfaceVersion &&
                schemaHash.contentEquals(other.schemaHash)
    }

    override fun hashCode(): Int {
        var result = interfaceId.hashCode()
        result = 31 * result + interfaceVersion.hashCode()
        result = 31 * result + schemaHash.contentHashCode()
        return result
    }
}

data class EndpointCacheValue(
    val endpointId: Long,
    val resolvedAtMs: Long = System.currentTimeMillis()
)

class EndpointCache {
    private val cache = HashMap<EndpointCacheKey, EndpointCacheValue>()

    @Synchronized
    fun get(key: EndpointCacheKey): EndpointCacheValue? {
        return cache[key]
    }

    @Synchronized
    fun put(key: EndpointCacheKey, value: EndpointCacheValue) {
        cache[key] = value
    }

    @Synchronized
    fun invalidateAll() {
        cache.clear()
    }

    @Synchronized
    fun size(): Int = cache.size
}
