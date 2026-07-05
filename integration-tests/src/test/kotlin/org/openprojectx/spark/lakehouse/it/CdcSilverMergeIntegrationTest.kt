package org.openprojectx.spark.lakehouse.it

import com.typesafe.config.ConfigFactory
import java.nio.file.Files
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.openprojectx.bigdata.test.core.BigDataService
import org.openprojectx.bigdata.test.core.BigDataTestKit
import org.openprojectx.bigdata.test.junit5.BigDataTest
import org.openprojectx.spark.boot.dagger.SparkBootComponent
import org.openprojectx.spark.lakehouse.app.DaggerLakehouseComponent
import org.openprojectx.spark.lakehouse.app.LakehouseJobRunner

/**
 * End-to-end CDC merge: bronze CDC parquet → resolve latest per key →
 * MERGE INTO an HMS-backed Iceberg silver table. The Hive Metastore runs in
 * a bigdata-test container; the warehouse is a local temp dir (metadata
 * pointers in HMS, data files written by the local Spark driver).
 */
@BigDataTest(hiveMetastore = true)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CdcSilverMergeIntegrationTest {

    private var component: SparkBootComponent? = null

    @AfterAll
    fun tearDown() {
        component?.sparkSession()?.stop()
        System.clearProperty("spark.boot.hms.uri")
        System.clearProperty("spark.boot.hms.warehouse")
    }

    @Test
    fun `merges cdc batches into a silver iceberg table`(kit: BigDataTestKit) {
        val metastoreUri = kit.endpoint(BigDataService.HIVE_METASTORE).property("hive.metastore.uris")
        val warehouse = Files.createTempDirectory("silver-warehouse").toUri().toString()
        // SparkBootConfigLoader resolves system properties ahead of application.conf.
        System.setProperty("spark.boot.hms.uri", metastoreUri)
        System.setProperty("spark.boot.hms.warehouse", warehouse)

        val component = DaggerLakehouseComponent.create().also { this.component = it }
        val spark = component.sparkSession()

        val lakeRoot = Files.createTempDirectory("lake-acme").toUri().toString().trimEnd('/')
        val bronzePath = "$lakeRoot/bronze/orders_cdc"
        spark.sql(
            """
            SELECT * FROM VALUES
              (1, 'alice', 100, 'c', 1, '2026-07-01'),
              (2, 'bob',   200, 'c', 2, '2026-07-01'),
              (3, 'carol', 300, 'c', 3, '2026-07-01'),
              (2, 'bob',   250, 'u', 4, '2026-07-02'),
              (3, 'carol', 300, 'd', 5, '2026-07-02'),
              (4, 'dave',  400, 'c', 6, '2026-07-02')
            AS t(id, name, amount, op, ts, _snapshot_date)
            """.trimIndent()
        ).write().parquet(bronzePath)

        fun runForSnapshot(snapshotDate: String) {
            val config = ConfigFactory.parseString(
                """
                job { template = "cdc-silver-merge", schema-version = 1 }
                tenant { id = "acme", storage-root = "$lakeRoot" }
                source { table = "orders_cdc", where = "_snapshot_date = '$snapshotDate'" }
                cdc {
                  primary-key = ["id"]
                  sequence-by = "ts"
                  op-column = "op"
                }
                target { table = "orders" }
                """.trimIndent()
            )
            LakehouseJobRunner.run(null, config, component)
        }

        // initial load: three inserts
        runForSnapshot("2026-07-01")
        val afterLoad = spark.table("hms.acme_silver.orders")
        assertEquals(3, afterLoad.count())
        assertEquals(setOf("id", "name", "amount", "ts"), afterLoad.columns().toSet())

        // incremental: update bob, delete carol, insert dave
        runForSnapshot("2026-07-02")
        val silver = spark.table("hms.acme_silver.orders")
            .selectExpr("id", "name", "amount")
            .collectAsList()
            .associateBy { it.getInt(0) }

        assertEquals(setOf(1, 2, 4), silver.keys)
        assertEquals("alice", silver[1]!!.getString(1))
        assertEquals("bob", silver[2]!!.getString(1))
        assertEquals(250, silver[2]!!.getInt(2))
        assertEquals("dave", silver[4]!!.getString(1))
    }
}
