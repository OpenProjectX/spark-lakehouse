package org.openprojectx.spark.lakehouse.jobs

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

/**
 * Snapshots one RDBMS table into the tenant's bronze layer as append-only,
 * metadata-stamped parquet partitioned by snapshot date.
 *
 * Schema (version 1):
 * ```hocon
 * job    { template = "jdbc-snapshot-ingest", schema-version = 1, name = "acme-orders" }
 * tenant { id = "acme", storage-root = "s3a://lake/acme" }
 * source {
 *   table = "public.orders"
 *   # either a named connection from spark.boot.jdbc.connections…
 *   # connection = "orders-db"
 *   # …or inline:
 *   url = "jdbc:postgresql://…", user = "…", password = "…", driver = "org.postgresql.Driver"
 * }
 * target { table = "orders", snapshot-date = "2026-07-05", partition-by = [] }
 * ```
 */
object JdbcSnapshotIngestJob : JobTemplate {

    override val name = "jdbc-snapshot-ingest"
    override val schemaVersion = 1

    override fun buildFlow(config: Config): FlowDefinition {
        validateHeader(config)

        val tenant = TenantContext.from(config)
        val sourceTable = ConfigSupport.requiredString(config, "source.table")
        val connection = ConfigSupport.optionalString(config, "source.connection")
        val url = ConfigSupport.optionalString(config, "source.url")
        if (connection == null && url == null) {
            throw JobConfigException("Config 'source' needs either 'source.connection' or inline 'source.url'")
        }

        val targetTable = ConfigSupport.requiredString(config, "target.table")
        val jobName = ConfigSupport.optionalString(config, "job.name")
            ?: "${tenant.tenantId}-$targetTable-snapshot"

        val sourceConfig = buildMap<String, Any?> {
            put("table", sourceTable)
            connection?.let { put("connection", it) }
            url?.let { put("url", it) }
            ConfigSupport.optionalString(config, "source.user")?.let { put("user", it) }
            ConfigSupport.optionalString(config, "source.password")?.let { put("password", it) }
            ConfigSupport.optionalString(config, "source.driver")?.let { put("driver", it) }
        }

        val sinkConfig = buildMap<String, Any?> {
            put("path", tenant.layerPath(Layer.BRONZE, targetTable))
            put("tenant", tenant.tenantId)
            put("source", sourceTable)
            ConfigSupport.optionalString(config, "target.snapshot-date")?.let { put("snapshot_date", it) }
            val partitionBy = ConfigSupport.stringList(config, "target.partition-by")
            if (partitionBy.isNotEmpty()) put("partition_by", partitionBy)
        }

        return FlowDefinition(
            name = jobName,
            nodes = listOf(
                NodeDefinition(id = "source", type = SparkBootNodeKinds.JDBC_SOURCE, config = sourceConfig),
                NodeDefinition(id = "bronze", type = LakehouseNodeKinds.BRONZE_SNAPSHOT_SINK, config = sinkConfig),
            ),
            edges = listOf(EdgeDefinition(from = "source", to = "bronze")),
        )
    }
}
