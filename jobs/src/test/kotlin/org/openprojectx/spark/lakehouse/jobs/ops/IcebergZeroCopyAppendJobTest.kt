package org.openprojectx.spark.lakehouse.jobs.ops

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.openprojectx.spark.lakehouse.core.JobConfigException
import org.openprojectx.spark.lakehouse.jobs.LakehouseJobs

class IcebergZeroCopyAppendJobTest {

    private fun config(text: String): Config = ConfigFactory.parseString(text.trimIndent())

    private val valid = config(
        """
        job { template = "iceberg-zero-copy-append", schema-version = 1 }
        tenant { id = "acme", storage-root = "file:///lake/acme" }
        source { table = "orders_2025" }
        target { table = "orders" }
        """
    )

    @Test
    fun `builds a single action node flow`() {
        val flow = IcebergZeroCopyAppendJob.buildFlow(valid)

        assertEquals("acme-orders-zero-copy-append", flow.name)
        assertEquals(1, flow.nodes.size)
        assertTrue(flow.edges.isEmpty())

        val append = flow.nodes.single()
        assertEquals("append", append.id)
        assertEquals("IcebergZeroCopyAppendAction", append.type)
        assertEquals("hms.acme_silver.orders_2025", append.config["source_table"])
        assertEquals("hms.acme_silver.orders", append.config["target_table"])
        assertEquals(false, append.config["skip_validation"])
    }

    @Test
    fun `passes skip-validation through when explicitly enabled`() {
        val flow = IcebergZeroCopyAppendJob.buildFlow(
            config("options.skip-validation = true").withFallback(valid)
        )
        assertEquals(true, flow.nodes.single().config["skip_validation"])
    }

    @Test
    fun `resolves layers and catalog`() {
        val flow = IcebergZeroCopyAppendJob.buildFlow(
            config(
                """
                source.layer = "bronze"
                target.layer = "gold"
                target.catalog = "prod"
                """
            ).withFallback(valid)
        )

        val append = flow.nodes.single()
        assertEquals("prod.acme_bronze.orders_2025", append.config["source_table"])
        assertEquals("prod.acme_gold.orders", append.config["target_table"])
    }

    @Test
    fun `rejects source equal to target`() {
        val ex = assertThrows<JobConfigException> {
            IcebergZeroCopyAppendJob.buildFlow(
                config("""source.table = "orders"""").withFallback(valid)
            )
        }
        assertTrue(ex.message!!.contains("same table"))
    }

    @Test
    fun `rejects missing source table`() {
        assertThrows<JobConfigException> {
            IcebergZeroCopyAppendJob.buildFlow(
                config(
                    """
                    job { template = "iceberg-zero-copy-append" }
                    tenant { id = "acme", storage-root = "file:///lake/acme" }
                    target { table = "orders" }
                    """
                )
            )
        }
    }

    @Test
    fun `rejects unknown layer`() {
        val ex = assertThrows<JobConfigException> {
            IcebergZeroCopyAppendJob.buildFlow(
                config("""source.layer = "platinum"""").withFallback(valid)
            )
        }
        assertTrue(ex.message!!.contains("platinum"))
    }

    @Test
    fun `rejects unsupported schema version`() {
        assertThrows<JobConfigException> {
            IcebergZeroCopyAppendJob.buildFlow(
                config("job.schema-version = 2").withFallback(valid)
            )
        }
    }

    @Test
    fun `is registered in the job catalog`() {
        assertTrue("iceberg-zero-copy-append" in LakehouseJobs.catalog.names())
    }
}
