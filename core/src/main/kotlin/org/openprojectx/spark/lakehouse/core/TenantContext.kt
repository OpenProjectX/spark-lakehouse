package org.openprojectx.spark.lakehouse.core

import com.typesafe.config.Config

/**
 * Runtime identity of the tenant a job runs for. This repo never stores
 * tenant data — the orchestration repo injects a `tenant { }` block in the
 * submitted HOCON, and this class is the only place it is interpreted.
 *
 * Storage layout convention: `<storage-root>/<layer>/<table>`, table
 * namespaces `<tenant>_<layer>`.
 */
data class TenantContext(
    val tenantId: String,
    val storageRoot: String,
    val tags: Map<String, String> = emptyMap(),
) {
    init {
        if (!TENANT_ID_PATTERN.matches(tenantId)) {
            throw JobConfigException(
                "Config 'tenant.id' must match ${TENANT_ID_PATTERN.pattern} (got '$tenantId')"
            )
        }
        if (storageRoot.isBlank()) {
            throw JobConfigException("Config 'tenant.storage-root' must not be blank")
        }
    }

    fun layerPath(layer: Layer, table: String): String =
        "${storageRoot.trimEnd('/')}/${layer.id}/$table"

    fun namespace(layer: Layer): String = "${tenantId}_${layer.id}"

    companion object {
        private val TENANT_ID_PATTERN = Regex("[a-z][a-z0-9_]*")

        fun from(config: Config): TenantContext {
            if (!config.hasPath("tenant")) {
                throw JobConfigException("Missing required config 'tenant' block (tenant.id, tenant.storage-root)")
            }
            return TenantContext(
                tenantId = ConfigSupport.requiredString(config, "tenant.id"),
                storageRoot = ConfigSupport.requiredString(config, "tenant.storage-root"),
                tags = ConfigSupport.stringMap(config, "tenant.tags"),
            )
        }
    }
}
