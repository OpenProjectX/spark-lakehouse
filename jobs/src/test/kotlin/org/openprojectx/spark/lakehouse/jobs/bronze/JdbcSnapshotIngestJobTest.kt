package org.openprojectx.spark.lakehouse.jobs.bronze

import com.typesafe.config.ConfigFactory
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.openprojectx.spark.boot.core.EdgeDefinition
import org.openprojectx.spark.lakehouse.core.JobConfigException
import org.openprojectx.spark.lakehouse.jobs.LakehouseJobs

class JdbcSnapshotIngestJobTest {

    private fun config(text: String) = ConfigFactory.parseString(text.trimIndent())

    private val valid = config(
        """
        job { template = "jdbc-snapshot-ingest", schema-version = 1 }
        tenant { id = "acme", storage-root = "file:///lake/acme" }
        source {
          url = "jdbc:postgresql://db:5432/shop"
          user = "spark"
          password = "secret"
          driver = "org.postgresql.Driver"
          table = "public.orders"
        }
        target { table = "orders" }
        """
    )

    @Test
    fun `builds a two-node flow wired source to bronze`() {
        val flow = JdbcSnapshotIngestJob.buildFlow(valid)

        assertEquals("acme-orders-snapshot", flow.name)
        assertEquals(listOf("source", "bronze"), flow.nodes.map { it.id })
        assertEquals(listOf(EdgeDefinition("source", "bronze")), flow.edges)

        val source = flow.nodes.first { it.id == "source" }
        assertEquals("JdbcSource", source.type)
        assertEquals("public.orders", source.config["table"])
        assertEquals("jdbc:postgresql://db:5432/shop", source.config["url"])

        val sink = flow.nodes.first { it.id == "bronze" }
        assertEquals("BronzeSnapshotSink", sink.type)
        assertEquals("file:///lake/acme/bronze/orders", sink.config["path"])
        assertEquals("acme", sink.config["tenant"])
        assertEquals("public.orders", sink.config["source"])
        assertNull(sink.config["snapshot_date"])
    }

    @Test
    fun `passes optional snapshot date and partitions through`() {
        val flow = JdbcSnapshotIngestJob.buildFlow(
            valid.withFallback(config("""target { snapshot-date = "2026-07-01", partition-by = ["region"] }"""))
        )
        val sink = flow.nodes.first { it.id == "bronze" }
        assertEquals("2026-07-01", sink.config["snapshot_date"])
        assertEquals(listOf("region"), sink.config["partition_by"])
    }

    @Test
    fun `rejects config without connection or url`() {
        val broken = config(
            """
            tenant { id = "acme", storage-root = "file:///lake/acme" }
            source { table = "public.orders" }
            target { table = "orders" }
            """
        )
        val error = assertThrows<JobConfigException> { JdbcSnapshotIngestJob.buildFlow(broken) }
        assertTrue(error.message!!.contains("source.connection"))
    }

    @Test
    fun `rejects unsupported schema version`() {
        val futureConfig = config("""job.schema-version = 2""").withFallback(valid)
        val error = assertThrows<JobConfigException> { JdbcSnapshotIngestJob.buildFlow(futureConfig) }
        assertTrue(error.message!!.contains("schema-version"))
    }

    @Test
    fun `rejects config submitted to the wrong template`() {
        val mismatched = config("""job.template = "gold-mart-build"""").withFallback(valid)
        val error = assertThrows<JobConfigException> { JdbcSnapshotIngestJob.buildFlow(mismatched) }
        assertTrue(error.message!!.contains("job.template"))
    }

    @Test
    fun `catalog resolves templates and names unknown ones`() {
        assertEquals(JdbcSnapshotIngestJob, LakehouseJobs.catalog.require("jdbc-snapshot-ingest"))
        val error = assertThrows<JobConfigException> { LakehouseJobs.catalog.require("nope") }
        assertTrue(error.message!!.contains("jdbc-snapshot-ingest"))
    }
}
