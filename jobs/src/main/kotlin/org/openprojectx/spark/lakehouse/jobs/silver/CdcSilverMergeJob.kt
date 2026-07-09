package org.openprojectx.spark.lakehouse.jobs.silver

import com.typesafe.config.Config
import org.openprojectx.spark.boot.core.EdgeDefinition
import org.openprojectx.spark.boot.core.FlowDefinition
import org.openprojectx.spark.boot.core.NodeDefinition
import org.openprojectx.spark.lakehouse.core.BronzeColumns
import org.openprojectx.spark.lakehouse.core.ConfigSupport
import org.openprojectx.spark.lakehouse.core.JobConfigException
import org.openprojectx.spark.lakehouse.core.LakehouseNodeKinds
import org.openprojectx.spark.lakehouse.core.Layer
import org.openprojectx.spark.lakehouse.core.SparkBootNodeKinds
import org.openprojectx.spark.lakehouse.core.TenantContext
import org.openprojectx.spark.lakehouse.job.api.AbstractJobTemplate

/**
 * Resolves CDC events from the tenant's bronze layer to the latest event per
 * business key and merges them into a silver Iceberg table
 * (`<catalog>.<tenant>_silver.<table>`): deletes applied, updates upserted.
 *
 * Schema (version 1):
 * ```hocon
 * job    { template = "cdc-silver-merge", schema-version = 1, name = "acme-orders-merge" }
 * tenant { id = "acme", storage-root = "s3a://lake/acme" }
 * source {
 *   table = "orders_cdc"            # bronze dataset (parquet); or explicit path = "…"
 *   where = "_snapshot_date = '2026-07-05'"   # optional event filter
 * }
 * cdc {
 *   primary-key = ["id"]            # business key columns
 *   sequence-by = "ts"              # ordering column; latest event per key wins
 *   op-column = "op"                # optional; omit for pure upsert sources
 *   delete-values = ["d"]           # op values meaning delete
 *   exclude-columns = []            # extra columns to keep out of silver
 * }
 * target {
 *   table = "orders"                # silver table name
 *   catalog = "hms"                 # iceberg catalog (spark.boot.hms / iceberg.catalogs)
 * }
 * ```
 *
 * Bronze metadata columns (`_lake_*`, `_snapshot_date`) are excluded from
 * silver automatically; `cdc.sequence-by` may still reference them.
 */
object CdcSilverMergeJob : AbstractJobTemplate() {

    override val name = "cdc-silver-merge"
    override val schemaVersion = 1

    private val bronzeMetadataColumns = listOf(
        BronzeColumns.TENANT,
        BronzeColumns.SOURCE,
        BronzeColumns.INGESTED_AT,
        BronzeColumns.SNAPSHOT_DATE,
    )

    override fun buildFlow(config: Config): FlowDefinition {
        validateHeader(config)

        val tenant = TenantContext.from(config)
        val sourceTable = ConfigSupport.optionalString(config, "source.table")
        val sourcePath = ConfigSupport.optionalString(config, "source.path")
            ?: sourceTable?.let { tenant.layerPath(Layer.BRONZE, it) }
            ?: throw JobConfigException("Config 'source' needs 'source.table' (bronze dataset) or 'source.path'")

        val keys = ConfigSupport.stringList(config, "cdc.primary-key")
        if (keys.isEmpty()) {
            throw JobConfigException("Missing required config 'cdc.primary-key' (business key columns)")
        }
        val sequenceBy = ConfigSupport.requiredString(config, "cdc.sequence-by")
        val opColumn = ConfigSupport.optionalString(config, "cdc.op-column")
        val deleteValues = ConfigSupport.stringList(config, "cdc.delete-values").ifEmpty { listOf("d") }
        val excludeColumns = ConfigSupport.stringList(config, "cdc.exclude-columns")

        val targetTable = ConfigSupport.requiredString(config, "target.table")
        val catalog = ConfigSupport.optionalString(config, "target.catalog") ?: "hms"
        val silverTable = "$catalog.${tenant.namespace(Layer.SILVER)}.$targetTable"

        val jobName = ConfigSupport.optionalString(config, "job.name")
            ?: "${tenant.tenantId}-$targetTable-cdc-merge"
        val where = ConfigSupport.optionalString(config, "source.where")

        val nodes = mutableListOf(
            NodeDefinition(id = "source", type = SparkBootNodeKinds.PARQUET_SOURCE, config = mapOf("path" to sourcePath)),
        )
        if (where != null) {
            nodes += NodeDefinition(
                id = "filter",
                type = SparkBootNodeKinds.SQL_FILTER_TRANSFORM,
                config = mapOf("condition" to where),
            )
        }
        nodes += NodeDefinition(
            id = "resolve",
            type = LakehouseNodeKinds.CDC_RESOLVE_TRANSFORM,
            config = mapOf("keys" to keys, "sequence_by" to sequenceBy),
        )
        nodes += NodeDefinition(
            id = "merge",
            type = LakehouseNodeKinds.ICEBERG_MERGE_SINK,
            config = buildMap {
                put("table", silverTable)
                put("keys", keys)
                opColumn?.let { put("op_column", it) }
                put("delete_values", deleteValues)
                put("exclude_columns", bronzeMetadataColumns + excludeColumns)
            },
        )

        val ids = nodes.map { it.id }
        val edges = ids.zip(ids.drop(1)).map { (from, to) -> EdgeDefinition(from, to) }

        return FlowDefinition(name = jobName, nodes = nodes, edges = edges)
    }
}
