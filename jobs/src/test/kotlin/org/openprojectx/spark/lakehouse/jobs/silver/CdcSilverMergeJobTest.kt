package org.openprojectx.spark.lakehouse.jobs.silver

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.openprojectx.spark.boot.core.EdgeDefinition
import org.openprojectx.spark.lakehouse.core.JobConfigException
import org.openprojectx.spark.lakehouse.jobs.LakehouseJobs

class CdcSilverMergeJobTest {

    private fun config(text: String): Config = ConfigFactory.parseString(text.trimIndent())

    private val valid = config(
        """
        job { template = "cdc-silver-merge", schema-version = 1 }
        tenant { id = "acme", storage-root = "file:///lake/acme" }
        source { table = "orders_cdc" }
        cdc {
          primary-key = ["id"]
          sequence-by = "ts"
          op-column = "op"
        }
        target { table = "orders" }
        """
    )

    @Test
    fun `builds source-resolve-merge flow with derived names`() {
        val flow = CdcSilverMergeJob.buildFlow(valid)

        assertEquals("acme-orders-cdc-merge", flow.name)
        assertEquals(listOf("source", "resolve", "merge"), flow.nodes.map { it.id })
        assertEquals(
            listOf(EdgeDefinition("source", "resolve"), EdgeDefinition("resolve", "merge")),
            flow.edges,
        )

        val source = flow.nodes.first { it.id == "source" }
        assertEquals("ParquetSource", source.type)
        assertEquals("file:///lake/acme/bronze/orders_cdc", source.config["path"])

        val resolve = flow.nodes.first { it.id == "resolve" }
        assertEquals("CdcResolveTransform", resolve.type)
        assertEquals(listOf("id"), resolve.config["keys"])
        assertEquals("ts", resolve.config["sequence_by"])

        val merge = flow.nodes.first { it.id == "merge" }
        assertEquals("IcebergMergeSink", merge.type)
        assertEquals("hms.acme_silver.orders", merge.config["table"])
        assertEquals("op", merge.config["op_column"])
        assertEquals(listOf("d"), merge.config["delete_values"])
        @Suppress("UNCHECKED_CAST")
        val excluded = merge.config["exclude_columns"] as List<String>
        assertTrue(excluded.containsAll(listOf("_lake_tenant", "_lake_source", "_lake_ingested_at", "_snapshot_date")))
    }

    @Test
    fun `inserts filter node when where is set`() {
        val flow = CdcSilverMergeJob.buildFlow(
            config("""source.where = "_snapshot_date = '2026-07-05'"""").withFallback(valid)
        )
        assertEquals(listOf("source", "filter", "resolve", "merge"), flow.nodes.map { it.id })
        val filter = flow.nodes.first { it.id == "filter" }
        assertEquals("SqlFilterTransform", filter.type)
        assertEquals("_snapshot_date = '2026-07-05'", filter.config["condition"])
        assertEquals(
            listOf(
                EdgeDefinition("source", "filter"),
                EdgeDefinition("filter", "resolve"),
                EdgeDefinition("resolve", "merge"),
            ),
            flow.edges,
        )
    }

    @Test
    fun `honours explicit catalog and source path`() {
        val flow = CdcSilverMergeJob.buildFlow(
            config(
                """
                source.path = "s3a://elsewhere/raw/orders"
                target.catalog = "prod"
                """
            ).withFallback(valid)
        )
        assertEquals("s3a://elsewhere/raw/orders", flow.nodes.first { it.id == "source" }.config["path"])
        assertEquals("prod.acme_silver.orders", flow.nodes.first { it.id == "merge" }.config["table"])
    }

    @Test
    fun `rejects missing primary key`() {
        val broken = config(
            """
            tenant { id = "acme", storage-root = "file:///lake/acme" }
            source { table = "orders_cdc" }
            cdc { sequence-by = "ts" }
            target { table = "orders" }
            """
        )
        val error = assertThrows<JobConfigException> { CdcSilverMergeJob.buildFlow(broken) }
        assertTrue(error.message!!.contains("cdc.primary-key"))
    }

    @Test
    fun `rejects missing source table and path`() {
        val broken = config(
            """
            tenant { id = "acme", storage-root = "file:///lake/acme" }
            cdc { primary-key = ["id"], sequence-by = "ts" }
            target { table = "orders" }
            """
        )
        val error = assertThrows<JobConfigException> { CdcSilverMergeJob.buildFlow(broken) }
        assertTrue(error.message!!.contains("source.table"))
    }

    @Test
    fun `is registered in the catalog`() {
        assertEquals(CdcSilverMergeJob, LakehouseJobs.catalog.require("cdc-silver-merge"))
    }
}
