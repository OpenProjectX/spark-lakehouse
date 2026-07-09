package org.openprojectx.spark.lakehouse.jobs.gold

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.openprojectx.spark.boot.core.EdgeDefinition
import org.openprojectx.spark.lakehouse.core.JobConfigException
import org.openprojectx.spark.lakehouse.jobs.LakehouseJobs

class Scd2DimLoadJobTest {

    private fun config(text: String): Config = ConfigFactory.parseString(text.trimIndent())

    private val valid = config(
        """
        job { template = "scd2-dim-load", schema-version = 1 }
        tenant { id = "acme", storage-root = "file:///lake/acme" }
        source { table = "customers" }
        dimension { natural-key = ["id"] }
        target { table = "dim_customers" }
        """
    )

    @Test
    fun `builds source-dim flow with derived identifiers`() {
        val flow = Scd2DimLoadJob.buildFlow(valid)

        assertEquals("acme-dim_customers-scd2", flow.name)
        assertEquals(listOf("source", "dim"), flow.nodes.map { it.id })
        assertEquals(listOf(EdgeDefinition("source", "dim")), flow.edges)

        val source = flow.nodes.first { it.id == "source" }
        assertEquals("TableSource", source.type)
        assertEquals("hms.acme_silver.customers", source.config["table"])

        val dim = flow.nodes.first { it.id == "dim" }
        assertEquals("Scd2DimSink", dim.type)
        assertEquals("hms.acme_gold.dim_customers", dim.config["table"])
        assertEquals(listOf("id"), dim.config["keys"])
        assertNull(dim.config["tracked_columns"])
        assertNull(dim.config["effective_at"])
    }

    @Test
    fun `passes dimension options and filter through`() {
        val flow = Scd2DimLoadJob.buildFlow(
            config(
                """
                source.where = "active = true"
                dimension {
                  tracked-columns = ["tier"]
                  exclude-columns = ["updated_at"]
                  effective-at = "2026-07-09T00:00:00Z"
                }
                """
            ).withFallback(valid)
        )
        assertEquals(listOf("source", "filter", "dim"), flow.nodes.map { it.id })
        assertEquals("active = true", flow.nodes.first { it.id == "filter" }.config["condition"])

        val dim = flow.nodes.first { it.id == "dim" }
        assertEquals(listOf("tier"), dim.config["tracked_columns"])
        assertEquals(listOf("updated_at"), dim.config["exclude_columns"])
        assertEquals("2026-07-09T00:00:00Z", dim.config["effective_at"])
    }

    @Test
    fun `honours explicit source identifier and target catalog`() {
        val flow = Scd2DimLoadJob.buildFlow(
            config(
                """
                source.identifier = "prod.shared.customers"
                target.catalog = "prod"
                """
            ).withFallback(valid)
        )
        assertEquals("prod.shared.customers", flow.nodes.first { it.id == "source" }.config["table"])
        assertEquals("prod.acme_gold.dim_customers", flow.nodes.first { it.id == "dim" }.config["table"])
    }

    @Test
    fun `rejects missing natural key`() {
        val broken = config(
            """
            tenant { id = "acme", storage-root = "file:///lake/acme" }
            source { table = "customers" }
            target { table = "dim_customers" }
            """
        )
        val error = assertThrows<JobConfigException> { Scd2DimLoadJob.buildFlow(broken) }
        assertTrue(error.message!!.contains("dimension.natural-key"))
    }

    @Test
    fun `rejects missing source`() {
        val broken = config(
            """
            tenant { id = "acme", storage-root = "file:///lake/acme" }
            dimension { natural-key = ["id"] }
            target { table = "dim_customers" }
            """
        )
        val error = assertThrows<JobConfigException> { Scd2DimLoadJob.buildFlow(broken) }
        assertTrue(error.message!!.contains("source.table"))
    }

    @Test
    fun `is registered in the catalog`() {
        assertEquals(Scd2DimLoadJob, LakehouseJobs.catalog.require("scd2-dim-load"))
    }
}
