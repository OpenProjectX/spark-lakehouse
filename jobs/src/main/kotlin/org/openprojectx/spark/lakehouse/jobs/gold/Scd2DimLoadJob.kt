package org.openprojectx.spark.lakehouse.jobs.gold

import com.typesafe.config.Config
import org.openprojectx.spark.boot.core.EdgeDefinition
import org.openprojectx.spark.boot.core.FlowDefinition
import org.openprojectx.spark.boot.core.NodeDefinition
import org.openprojectx.spark.lakehouse.core.ConfigSupport
import org.openprojectx.spark.lakehouse.core.JobConfigException
import org.openprojectx.spark.lakehouse.core.LakehouseNodeKinds
import org.openprojectx.spark.lakehouse.core.Layer
import org.openprojectx.spark.lakehouse.core.SparkBootNodeKinds
import org.openprojectx.spark.lakehouse.core.TenantContext
import org.openprojectx.spark.lakehouse.job.api.AbstractJobTemplate

/**
 * Loads a Kimball SCD Type 2 dimension in the tenant's gold layer from the
 * current state of a silver table: changed tracked columns close the current
 * version and open a new one; new keys open a first version.
 *
 * Schema (version 1):
 * ```hocon
 * job    { template = "scd2-dim-load", schema-version = 1, name = "acme-dim-customers" }
 * tenant { id = "acme", storage-root = "s3a://lake/acme" }
 * source {
 *   table = "customers"             # silver table → <catalog>.<tenant>_silver.<table>
 *   catalog = "hms"
 *   # or identifier = "hms.acme_silver.customers"; where = "…" filters rows
 * }
 * dimension {
 *   natural-key = ["id"]
 *   tracked-columns = []            # type-2 columns; default: all non-key columns
 *   exclude-columns = []            # columns kept out of the dimension
 *   effective-at = "2026-07-09T00:00:00Z"   # optional version boundary; default load time
 * }
 * target { table = "dim_customers", catalog = "hms" }   # → <catalog>.<tenant>_gold.<table>
 * ```
 */
object Scd2DimLoadJob : AbstractJobTemplate() {

    override val name = "scd2-dim-load"
    override val schemaVersion = 1

    override fun buildFlow(config: Config): FlowDefinition {
        validateHeader(config)

        val tenant = TenantContext.from(config)
        val sourceCatalog = ConfigSupport.optionalString(config, "source.catalog") ?: "hms"
        val sourceIdentifier = ConfigSupport.optionalString(config, "source.identifier")
            ?: ConfigSupport.optionalString(config, "source.table")
                ?.let { "$sourceCatalog.${tenant.namespace(Layer.SILVER)}.$it" }
            ?: throw JobConfigException("Config 'source' needs 'source.table' (silver table) or 'source.identifier'")

        val naturalKeys = ConfigSupport.stringList(config, "dimension.natural-key")
        if (naturalKeys.isEmpty()) {
            throw JobConfigException("Missing required config 'dimension.natural-key'")
        }

        val targetTable = ConfigSupport.requiredString(config, "target.table")
        val targetCatalog = ConfigSupport.optionalString(config, "target.catalog") ?: "hms"
        val dimTable = "$targetCatalog.${tenant.namespace(Layer.GOLD)}.$targetTable"

        val jobName = ConfigSupport.optionalString(config, "job.name")
            ?: "${tenant.tenantId}-$targetTable-scd2"
        val where = ConfigSupport.optionalString(config, "source.where")

        val nodes = mutableListOf(
            NodeDefinition(
                id = "source",
                type = LakehouseNodeKinds.TABLE_SOURCE,
                config = mapOf("table" to sourceIdentifier),
            ),
        )
        if (where != null) {
            nodes += NodeDefinition(
                id = "filter",
                type = SparkBootNodeKinds.SQL_FILTER_TRANSFORM,
                config = mapOf("condition" to where),
            )
        }
        nodes += NodeDefinition(
            id = "dim",
            type = LakehouseNodeKinds.SCD2_DIM_SINK,
            config = buildMap {
                put("table", dimTable)
                put("keys", naturalKeys)
                val tracked = ConfigSupport.stringList(config, "dimension.tracked-columns")
                if (tracked.isNotEmpty()) put("tracked_columns", tracked)
                val excluded = ConfigSupport.stringList(config, "dimension.exclude-columns")
                if (excluded.isNotEmpty()) put("exclude_columns", excluded)
                ConfigSupport.optionalString(config, "dimension.effective-at")?.let { put("effective_at", it) }
            },
        )

        val ids = nodes.map { it.id }
        val edges = ids.zip(ids.drop(1)).map { (from, to) -> EdgeDefinition(from, to) }

        return FlowDefinition(name = jobName, nodes = nodes, edges = edges)
    }
}
