package org.openprojectx.spark.lakehouse.jobs.ops

import com.typesafe.config.Config
import org.openprojectx.spark.boot.core.FlowDefinition
import org.openprojectx.spark.boot.core.NodeDefinition
import org.openprojectx.spark.lakehouse.core.ConfigSupport
import org.openprojectx.spark.lakehouse.core.JobConfigException
import org.openprojectx.spark.lakehouse.core.LakehouseNodeKinds
import org.openprojectx.spark.lakehouse.core.Layer
import org.openprojectx.spark.lakehouse.core.TenantContext
import org.openprojectx.spark.lakehouse.job.api.AbstractJobTemplate

/**
 * Metadata-only append of one Iceberg table into another within a tenant:
 * commits the source's current data files into the target in one snapshot,
 * moving no data (UNION ALL semantics, no dedup). Both tables reference the
 * same physical files afterwards, so the source must be retired after the
 * run — the accepted constraints are documented in
 * `docs/jobs/iceberg-zero-copy-append.adoc` and enforced fail-fast where the
 * job can detect them (field-ID schema identity, no row-level deletes,
 * compatible partition specs).
 *
 * Schema (version 1):
 * ```hocon
 * job    { template = "iceberg-zero-copy-append", schema-version = 1, name = "acme-orders-consolidate" }
 * tenant { id = "acme", storage-root = "s3a://lake/acme" }
 * source { table = "orders_2025", layer = "silver" }   # layer defaults to silver
 * target { table = "orders",      layer = "silver", catalog = "hms" }
 * ```
 */
object IcebergZeroCopyAppendJob : AbstractJobTemplate() {

    override val name = "iceberg-zero-copy-append"
    override val schemaVersion = 1

    override fun buildFlow(config: Config): FlowDefinition {
        validateHeader(config)

        val tenant = TenantContext.from(config)
        val catalog = ConfigSupport.optionalString(config, "target.catalog") ?: "hms"

        val sourceTable = tableIdentifier(config, "source", tenant, catalog)
        val targetTable = tableIdentifier(config, "target", tenant, catalog)
        if (sourceTable == targetTable) {
            throw JobConfigException("'source' and 'target' resolve to the same table: $sourceTable")
        }

        val jobName = ConfigSupport.optionalString(config, "job.name")
            ?: "${tenant.tenantId}-${ConfigSupport.requiredString(config, "target.table")}-zero-copy-append"

        return FlowDefinition(
            name = jobName,
            nodes = listOf(
                NodeDefinition(
                    id = "append",
                    type = LakehouseNodeKinds.ICEBERG_ZERO_COPY_APPEND_ACTION,
                    config = mapOf(
                        "source_table" to sourceTable,
                        "target_table" to targetTable,
                    ),
                ),
            ),
            edges = emptyList(),
        )
    }

    private fun tableIdentifier(
        config: Config,
        block: String,
        tenant: TenantContext,
        catalog: String,
    ): String {
        val table = ConfigSupport.optionalString(config, "$block.table")
            ?: throw JobConfigException("Missing required config '$block.table'")
        val layer = layer(config, "$block.layer")
        return "$catalog.${tenant.namespace(layer)}.$table"
    }

    private fun layer(config: Config, path: String): Layer {
        val id = ConfigSupport.optionalString(config, path) ?: return Layer.SILVER
        return Layer.entries.find { it.id == id }
            ?: throw JobConfigException(
                "Config '$path' must be one of ${Layer.entries.joinToString(", ") { it.id }} (got '$id')"
            )
    }
}
