package org.openprojectx.spark.lakehouse.it

import com.typesafe.config.ConfigFactory
import java.nio.file.Files
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.openprojectx.bigdata.test.core.BigDataService
import org.openprojectx.bigdata.test.core.BigDataTestKit
import org.openprojectx.bigdata.test.junit5.BigDataTest
import org.openprojectx.spark.boot.dagger.SparkBootComponent
import org.openprojectx.spark.lakehouse.app.DaggerLakehouseComponent
import org.openprojectx.spark.lakehouse.app.LakehouseJobRunner

/**
 * End-to-end SCD2 dimension load: an HMS-backed silver Iceberg table drives a
 * gold dimension across two loads — the second changes one tracked column and
 * adds a key, so history must close the old version and open new ones.
 */
@BigDataTest(hiveMetastore = true)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class Scd2DimLoadIntegrationTest {

    private var component: SparkBootComponent? = null

    @AfterAll
    fun tearDown() {
        component?.sparkSession()?.stop()
        System.clearProperty("spark.boot.hms.uri")
        System.clearProperty("spark.boot.hms.warehouse")
    }

    @Test
    fun `tracks history across two dimension loads`(kit: BigDataTestKit) {
        val metastoreUri = kit.endpoint(BigDataService.HIVE_METASTORE).property("hive.metastore.uris")
        val warehouse = Files.createTempDirectory("gold-warehouse").toUri().toString()
        System.setProperty("spark.boot.hms.uri", metastoreUri)
        System.setProperty("spark.boot.hms.warehouse", warehouse)

        val component = DaggerLakehouseComponent.create().also { this.component = it }
        val spark = component.sparkSession()

        spark.sql("CREATE NAMESPACE IF NOT EXISTS hms.acme_silver")
        spark.sql("CREATE TABLE hms.acme_silver.customers (id INT, name STRING, tier STRING) USING iceberg")
        spark.sql("INSERT INTO hms.acme_silver.customers VALUES (1, 'alice', 'gold'), (2, 'bob', 'silver')")

        fun runLoad(effectiveAt: String) {
            val config = ConfigFactory.parseString(
                """
                job { template = "scd2-dim-load", schema-version = 1 }
                tenant { id = "acme", storage-root = "file:///unused" }
                source { table = "customers" }
                dimension {
                  natural-key = ["id"]
                  tracked-columns = ["name", "tier"]
                  effective-at = "$effectiveAt"
                }
                target { table = "dim_customers" }
                """.trimIndent()
            )
            LakehouseJobRunner.run(null, config, component)
        }

        // initial load: two first versions
        runLoad("2026-07-01T00:00:00Z")
        val dim = "hms.acme_gold.dim_customers"
        assertEquals(2, spark.table(dim).count())
        assertEquals(2, spark.table(dim).filter("_scd_current").count())

        // idempotent: reloading unchanged state adds no versions
        runLoad("2026-07-02T00:00:00Z")
        assertEquals(2, spark.table(dim).count())

        // bob changes tier, carol appears
        spark.sql("UPDATE hms.acme_silver.customers SET tier = 'gold' WHERE id = 2")
        spark.sql("INSERT INTO hms.acme_silver.customers VALUES (3, 'carol', 'bronze')")
        runLoad("2026-07-03T00:00:00Z")

        val rows = spark.table(dim)
            .selectExpr("id", "tier", "_scd_current", "_scd_effective_to", "_dim_key")
            .collectAsList()
        assertEquals(4, rows.size)

        val currentByKey = rows.filter { it.getBoolean(2) }.associateBy { it.getInt(0) }
        assertEquals(setOf(1, 2, 3), currentByKey.keys)
        assertEquals("gold", currentByKey[2]!!.getString(1))
        assertNull(currentByKey[2]!!.get(3))

        val closedBob = rows.single { it.getInt(0) == 2 && !it.getBoolean(2) }
        assertEquals("silver", closedBob.getString(1))
        assertNotNull(closedBob.get(3))

        // surrogate keys are distinct across versions
        assertEquals(4, rows.map { it.getLong(4) }.toSet().size)
    }
}
