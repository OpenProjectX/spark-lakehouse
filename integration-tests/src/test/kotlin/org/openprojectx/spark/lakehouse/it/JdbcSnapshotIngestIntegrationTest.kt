package org.openprojectx.spark.lakehouse.it

import com.typesafe.config.ConfigFactory
import java.nio.file.Files
import java.sql.DriverManager
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.openprojectx.spark.lakehouse.app.DaggerLakehouseComponent
import org.openprojectx.spark.lakehouse.app.LakehouseJobRunner
import org.openprojectx.spark.lakehouse.core.BronzeColumns
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class JdbcSnapshotIngestIntegrationTest {

    companion object {
        @Container
        @JvmStatic
        val postgres: PostgreSQLContainer<*> = PostgreSQLContainer("postgres:16-alpine")
    }

    private val component = DaggerLakehouseComponent.create()

    @AfterAll
    fun stopSpark() {
        component.sparkSession().stop()
    }

    @Test
    fun `snapshots a postgres table into bronze parquet`() {
        DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password).use { connection ->
            connection.createStatement().use { statement ->
                statement.execute("CREATE TABLE orders (id INT PRIMARY KEY, status TEXT, amount NUMERIC)")
                statement.execute(
                    "INSERT INTO orders VALUES (1, 'PAID', 10.5), (2, 'PAID', 20.0), (3, 'CANCELLED', 5.0)"
                )
            }
        }

        val lakeRoot = Files.createTempDirectory("lake-acme").toUri().toString().trimEnd('/')
        val config = ConfigFactory.parseString(
            """
            job { template = "jdbc-snapshot-ingest", schema-version = 1, name = "it-orders-snapshot" }
            tenant { id = "acme", storage-root = "$lakeRoot" }
            source {
              url = "${postgres.jdbcUrl}"
              user = "${postgres.username}"
              password = "${postgres.password}"
              driver = "org.postgresql.Driver"
              table = "public.orders"
            }
            target { table = "orders", snapshot-date = "2026-07-05" }
            """.trimIndent()
        )

        LakehouseJobRunner.run(null, config, component)

        val bronze = component.sparkSession().read().parquet("$lakeRoot/bronze/orders")
        assertEquals(3, bronze.count())

        val columns = bronze.columns().toSet()
        assertTrue(columns.containsAll(setOf(BronzeColumns.TENANT, BronzeColumns.SOURCE, BronzeColumns.INGESTED_AT, BronzeColumns.SNAPSHOT_DATE)))

        val row = bronze.filter("id = 1").collectAsList().single()
        assertEquals("acme", row.getString(row.fieldIndex(BronzeColumns.TENANT)))
        assertEquals("public.orders", row.getString(row.fieldIndex(BronzeColumns.SOURCE)))
        // partition-column inference reads the snapshot date back as a DATE
        assertEquals("2026-07-05", row.get(row.fieldIndex(BronzeColumns.SNAPSHOT_DATE)).toString())
        assertEquals("PAID", row.getString(row.fieldIndex("status")))
    }
}
