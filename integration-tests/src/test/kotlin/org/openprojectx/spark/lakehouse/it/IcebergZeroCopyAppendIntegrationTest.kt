package org.openprojectx.spark.lakehouse.it

import com.typesafe.config.ConfigFactory
import java.nio.file.Files
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertThrows
import org.openprojectx.bigdata.test.core.BigDataService
import org.openprojectx.bigdata.test.core.BigDataTestKit
import org.openprojectx.spark.boot.dagger.SparkBootComponent
import org.openprojectx.spark.lakehouse.app.DaggerLakehouseComponent
import org.openprojectx.spark.lakehouse.app.LakehouseJobRunner

/**
 * Zero-copy append against an HMS-backed Iceberg catalog. The happy path
 * asserts that no data moved (the target's new files ARE the source's files);
 * the remaining tests pin every refusal that guards against a silently
 * inconsistent target, and demonstrate the documented ownership hazard.
 */
@org.openprojectx.bigdata.test.junit5.BigDataTest(hiveMetastore = true)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class IcebergZeroCopyAppendIntegrationTest {

    private var component: SparkBootComponent? = null

    private fun spark(kit: BigDataTestKit): SparkSession {
        component?.let { return it.sparkSession() }
        val metastoreUri = kit.endpoint(BigDataService.HIVE_METASTORE).property("hive.metastore.uris")
        val warehouse = Files.createTempDirectory("zca-warehouse").toUri().toString()
        System.setProperty("spark.boot.hms.uri", metastoreUri)
        System.setProperty("spark.boot.hms.warehouse", warehouse)
        val created = DaggerLakehouseComponent.create().also { component = it }
        created.sparkSession().sql("CREATE NAMESPACE IF NOT EXISTS hms.acme_silver")
        return created.sparkSession()
    }

    @AfterAll
    fun tearDown() {
        component?.sparkSession()?.stop()
        System.clearProperty("spark.boot.hms.uri")
        System.clearProperty("spark.boot.hms.warehouse")
    }

    private fun runJob(source: String, target: String, skipValidation: Boolean = false) {
        val config = ConfigFactory.parseString(
            """
            job { template = "iceberg-zero-copy-append", schema-version = 1 }
            tenant { id = "acme", storage-root = "file:///tmp/lake-acme" }
            source { table = "$source" }
            target { table = "$target" }
            options { skip-validation = $skipValidation }
            """.trimIndent()
        )
        LakehouseJobRunner.run(null, config, component!!)
    }

    private fun q(name: String) = "hms.acme_silver.$name"

    private fun SparkSession.filePaths(name: String): Set<String> =
        sql("SELECT file_path FROM ${q(name)}.files").collectAsList()
            .map { it.getString(0) }.toSet()

    private fun SparkSession.snapshotCount(name: String): Long =
        sql("SELECT snapshot_id FROM ${q(name)}.snapshots").count()

    @Test
    fun `appends the source's files into the target without moving data`(kit: BigDataTestKit) {
        val spark = spark(kit)
        spark.sql("CREATE TABLE ${q("src_happy")} (id INT, name STRING) USING iceberg")
        spark.sql("CREATE TABLE ${q("tgt_happy")} (id INT, name STRING) USING iceberg")
        spark.sql("INSERT INTO ${q("src_happy")} VALUES (1, 'alice'), (2, 'bob')")
        spark.sql("INSERT INTO ${q("tgt_happy")} VALUES (10, 'zoe')")

        val sourceFiles = spark.filePaths("src_happy")
        val targetFilesBefore = spark.filePaths("tgt_happy")

        runJob("src_happy", "tgt_happy")

        assertEquals(3, spark.table(q("tgt_happy")).count())
        // metadata-only: the target's new files are exactly the source's
        // physical files, still under the source table's location
        assertEquals(targetFilesBefore + sourceFiles, spark.filePaths("tgt_happy"))
        assertTrue(sourceFiles.all { it.contains("src_happy") })
        // source untouched
        assertEquals(2, spark.table(q("src_happy")).count())
    }

    @Test
    fun `rerun with an unchanged source is a no-op`(kit: BigDataTestKit) {
        val spark = spark(kit)
        spark.sql("CREATE TABLE ${q("src_rerun")} (id INT) USING iceberg")
        spark.sql("CREATE TABLE ${q("tgt_rerun")} (id INT) USING iceberg")
        spark.sql("INSERT INTO ${q("src_rerun")} VALUES (1), (2)")

        runJob("src_rerun", "tgt_rerun")
        val snapshotsAfterFirst = spark.snapshotCount("tgt_rerun")

        runJob("src_rerun", "tgt_rerun")

        assertEquals(2, spark.table(q("tgt_rerun")).count())
        assertEquals(snapshotsAfterFirst, spark.snapshotCount("tgt_rerun"))
    }

    @Test
    fun `refuses identical-looking schemas whose field ids diverged`(kit: BigDataTestKit) {
        val spark = spark(kit)
        spark.sql("CREATE TABLE ${q("src_ids")} (id INT, name STRING) USING iceberg")
        spark.sql("CREATE TABLE ${q("tgt_ids")} (id INT, name STRING) USING iceberg")
        // drop + re-add: the schema prints identically to the target's but
        // 'name' now has a fresh field ID — appended files would read wrong
        spark.sql("ALTER TABLE ${q("src_ids")} DROP COLUMN name")
        spark.sql("ALTER TABLE ${q("src_ids")} ADD COLUMN name STRING")
        spark.sql("INSERT INTO ${q("src_ids")} VALUES (1, 'alice')")
        spark.sql("INSERT INTO ${q("tgt_ids")} VALUES (10, 'zoe')")

        val ex = assertThrows<IllegalStateException> { runJob("src_ids", "tgt_ids") }
        assertTrue(ex.message!!.contains("field ID"), ex.message)
        // refusal happened before any commit: target unchanged
        assertEquals(1, spark.table(q("tgt_ids")).count())
        assertEquals(1, spark.snapshotCount("tgt_ids"))
    }

    @Test
    fun `refuses a source whose dropped columns outrun the target id counter`(kit: BigDataTestKit) {
        val spark = spark(kit)
        // after dropping 'tmp' the source schema equals the target's, field
        // IDs included — but ID 3 lives on in the source's files, and the
        // target would hand out ID 3 on its next ADD COLUMN
        spark.sql("CREATE TABLE ${q("src_ctr")} (id INT, name STRING, tmp INT) USING iceberg")
        spark.sql("CREATE TABLE ${q("tgt_ctr")} (id INT, name STRING) USING iceberg")
        spark.sql("INSERT INTO ${q("src_ctr")} VALUES (1, 'alice', 99)")
        spark.sql("ALTER TABLE ${q("src_ctr")} DROP COLUMN tmp")

        val ex = assertThrows<IllegalStateException> { runJob("src_ctr", "tgt_ctr") }
        assertTrue(ex.message!!.contains("field IDs up to"), ex.message)
        assertEquals(0, spark.snapshotCount("tgt_ctr"))
    }

    @Test
    fun `imports position delete files - target reads exactly the source's live rows`(kit: BigDataTestKit) {
        val spark = spark(kit)
        spark.sql(
            """
            CREATE TABLE ${q("src_mor")} (id INT, name STRING) USING iceberg
            TBLPROPERTIES ('format-version' = '2', 'write.delete.mode' = 'merge-on-read')
            """.trimIndent()
        )
        spark.sql("CREATE TABLE ${q("tgt_mor")} (id INT, name STRING) USING iceberg")
        // single range slice → one 100-row data file, so the single-row DELETE
        // can never be a metadata (whole-file) delete and must write a position
        // delete file. With core-count slicing (local[*]), a wide-enough box
        // puts id 50 alone in its own file and the DELETE degrades to a
        // metadata delete — no delete files at all.
        spark.sql("INSERT INTO ${q("src_mor")} SELECT id, concat('name', id) FROM range(0, 100, 1, 1)")
        spark.sql("DELETE FROM ${q("src_mor")} WHERE id = 50")

        runJob("src_mor", "tgt_mor")

        // position deletes reference (file_path, pos) — absolute pointers that
        // stay valid because the paths never changed. The target reads 99 rows.
        assertEquals(99, spark.table(q("tgt_mor")).count())
        assertEquals(0, spark.table(q("tgt_mor")).filter("id = 50").count())
        // still zero copy: the imported delete file is the source's own file
        val deletePaths = spark.sql("SELECT file_path FROM ${q("tgt_mor")}.delete_files")
            .collectAsList().map { it.getString(0) }
        assertEquals(1, deletePaths.size)
        assertTrue(deletePaths.single().contains("src_mor"), deletePaths.single())
    }

    @Test
    fun `refuses incompatible partition specs`(kit: BigDataTestKit) {
        val spark = spark(kit)
        spark.sql(
            "CREATE TABLE ${q("src_part")} (id INT, name STRING) USING iceberg PARTITIONED BY (bucket(4, id))"
        )
        spark.sql("CREATE TABLE ${q("tgt_part")} (id INT, name STRING) USING iceberg")
        spark.sql("INSERT INTO ${q("src_part")} VALUES (1, 'alice')")

        val ex = assertThrows<IllegalStateException> { runJob("src_part", "tgt_part") }
        assertTrue(ex.message!!.contains("partition specs"), ex.message)
        assertEquals(0, spark.snapshotCount("tgt_part"))
    }

    @Test
    fun `empty source is a successful no-op`(kit: BigDataTestKit) {
        val spark = spark(kit)
        spark.sql("CREATE TABLE ${q("src_empty")} (id INT) USING iceberg")
        spark.sql("CREATE TABLE ${q("tgt_empty")} (id INT) USING iceberg")
        spark.sql("INSERT INTO ${q("tgt_empty")} VALUES (10)")
        val snapshotsBefore = spark.snapshotCount("tgt_empty")

        runJob("src_empty", "tgt_empty")

        assertEquals(1, spark.table(q("tgt_empty")).count())
        assertEquals(snapshotsBefore, spark.snapshotCount("tgt_empty"))
    }

    // ------------------------------------------------------------------
    // skip-validation demonstrations: what the queries actually return when
    // options.skip-validation = true overrides a refused precondition. These
    // pin the corruption modes documented in the adoc — none of them error;
    // they all return wrong data, which is the point.
    // ------------------------------------------------------------------

    @Test
    fun `skip validation - diverged field ids silently null out a column`(kit: BigDataTestKit) {
        val spark = spark(kit)
        spark.sql("CREATE TABLE ${q("src_skids")} (id INT, name STRING) USING iceberg")
        spark.sql("CREATE TABLE ${q("tgt_skids")} (id INT, name STRING) USING iceberg")
        spark.sql("ALTER TABLE ${q("src_skids")} DROP COLUMN name")
        spark.sql("ALTER TABLE ${q("src_skids")} ADD COLUMN name STRING")
        spark.sql("INSERT INTO ${q("src_skids")} VALUES (1, 'alice')")
        spark.sql("INSERT INTO ${q("tgt_skids")} VALUES (10, 'zoe')")

        runJob("src_skids", "tgt_skids", skipValidation = true)

        // no error anywhere — but alice's name is gone: her file binds 'name'
        // to the source's re-added field ID, the target projects its own ID
        // and finds nothing. The value exists in the file yet reads as NULL.
        val rows = spark.table(q("tgt_skids")).collectAsList().associateBy { it.getInt(0) }
        assertEquals(setOf(1, 10), rows.keys)
        assertEquals("zoe", rows[10]!!.getString(1))
        assertTrue(rows[1]!!.isNullAt(1), "imported row's name should misbind to NULL")
        // the source still reads its own file correctly — same bytes, two truths
        assertEquals("alice", spark.table(q("src_skids")).collectAsList().single().getString(1))
    }

    @Test
    fun `equality deletes - refused, and resurrect when skipped`(kit: BigDataTestKit) {
        val spark = spark(kit)
        spark.sql("CREATE TABLE ${q("src_skeq")} (id INT, name STRING) USING iceberg TBLPROPERTIES ('format-version' = '2')")
        spark.sql("CREATE TABLE ${q("tgt_skeq")} (id INT, name STRING) USING iceberg")
        spark.sql("INSERT INTO ${q("src_skeq")} SELECT id, concat('name', id) FROM range(0, 100, 1, 1)")
        // a real equality delete file (id = 50), the kind Flink CDC writes;
        // committed after the data so it applies within the source
        commitEqualityDelete(spark, "src_skeq", 50)
        spark.sql("REFRESH TABLE ${q("src_skeq")}")
        assertEquals(99, spark.table(q("src_skeq")).count())

        // refused by default: no sequence placement makes it correct
        val ex = assertThrows<IllegalStateException> { runJob("src_skeq", "tgt_skeq") }
        assertTrue(ex.message!!.contains("equality delete files"), ex.message)
        assertEquals(0, spark.snapshotCount("tgt_skeq"))

        // skipped: data files import, the equality delete is dropped — the
        // row the source deleted is alive again in the target
        runJob("src_skeq", "tgt_skeq", skipValidation = true)
        assertEquals(100, spark.table(q("tgt_skeq")).count())
        assertEquals(1, spark.table(q("tgt_skeq")).filter("id = 50").count())
    }

    @Test
    fun `demonstration - importing equality deletes one commit later deletes the target's own rows`(kit: BigDataTestKit) {
        val spark = spark(kit)
        spark.sql("CREATE TABLE ${q("src_eqx")} (id INT, name STRING) USING iceberg TBLPROPERTIES ('format-version' = '2')")
        spark.sql("CREATE TABLE ${q("tgt_eqx")} (id INT, name STRING) USING iceberg TBLPROPERTIES ('format-version' = '2')")
        spark.sql("INSERT INTO ${q("src_eqx")} SELECT id, concat('src', id) FROM range(0, 100, 1, 1)")
        commitEqualityDelete(spark, "src_eqx", 50)
        // the target has its OWN, unrelated row with id = 50
        spark.sql("INSERT INTO ${q("tgt_eqx")} SELECT id, concat('own', id) FROM range(0, 100, 1, 1)")

        // the only sequence placement where an imported equality delete works
        // for the imported rows: data files first, delete one commit later —
        // done manually here because the job refuses to do this
        val sourceTable = org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark, q("src_eqx"))
        val targetTable = org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark, q("tgt_eqx"))
        val append = targetTable.newFastAppend()
        sourceTable.newScan().planFiles().use { tasks -> tasks.forEach { append.appendFile(it.file().copy()) } }
        append.commit()
        val delete = sourceTable.newScan().planFiles().use { tasks -> tasks.first().deletes().single().copy() }
        targetTable.newRowDelta().addDeletes(delete).commit()
        spark.sql("REFRESH TABLE ${q("tgt_eqx")}")

        // the imported rows are correct (source's id 50 stays deleted)…
        // but equality deletes match values, not files: the target's OWN
        // 'own50' row is gone too. 198 rows instead of the correct 199.
        assertEquals(198, spark.table(q("tgt_eqx")).count())
        assertEquals(0, spark.table(q("tgt_eqx")).filter("id = 50").count())
        assertEquals(0, spark.table(q("tgt_eqx")).filter("name = 'own50'").count())
    }

    /** Writes a real one-row equality delete file (`id = [id]`) and commits it. */
    private fun commitEqualityDelete(spark: SparkSession, table: String, id: Int) {
        val icebergTable = org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark, q(table))
        val idField = icebergTable.schema().findField("id")
        val deleteSchema = icebergTable.schema().select("id")
        val factory = org.apache.iceberg.data.GenericAppenderFactory(
            icebergTable.schema(), icebergTable.spec(), intArrayOf(idField.fieldId()), deleteSchema, null
        )
        val output = org.apache.iceberg.encryption.EncryptedFiles.plainAsEncryptedOutput(
            icebergTable.io().newOutputFile("${icebergTable.location()}/data/eq-delete-$id.parquet")
        )
        val writer = factory.newEqDeleteWriter(output, org.apache.iceberg.FileFormat.PARQUET, null)
        writer.use { it.write(org.apache.iceberg.data.GenericRecord.create(deleteSchema).copy(mapOf("id" to id))) }
        icebergTable.newRowDelta().addDeletes(writer.toDeleteFile()).commit()
    }

    @Test
    fun `skip validation - dropped column values resurface under a future target column`(kit: BigDataTestKit) {
        val spark = spark(kit)
        spark.sql("CREATE TABLE ${q("src_skctr")} (id INT, name STRING, tmp INT) USING iceberg")
        spark.sql("CREATE TABLE ${q("tgt_skctr")} (id INT, name STRING) USING iceberg")
        spark.sql("INSERT INTO ${q("src_skctr")} VALUES (1, 'alice', 99)")
        spark.sql("ALTER TABLE ${q("src_skctr")} DROP COLUMN tmp")

        runJob("src_skctr", "tgt_skctr", skipValidation = true)
        // the append looked clean: schemas were identical at append time
        assertEquals(1, spark.table(q("tgt_skctr")).count())

        // months later, an innocent evolution on the target...
        spark.sql("ALTER TABLE ${q("tgt_skctr")} ADD COLUMN extra INT")

        // ...and the source's dropped 'tmp' value materializes in 'extra':
        // the new column reused the imported file's dead field ID
        val extra = spark.sql("SELECT extra FROM ${q("tgt_skctr")} WHERE id = 1").collectAsList().single()
        assertEquals(99, extra.getInt(0))
    }

    @Test
    fun `documented hazard - purging the source breaks the target`(kit: BigDataTestKit) {
        val spark = spark(kit)
        spark.sql("CREATE TABLE ${q("src_purge")} (id INT) USING iceberg")
        spark.sql("CREATE TABLE ${q("tgt_purge")} (id INT) USING iceberg")
        spark.sql("INSERT INTO ${q("src_purge")} VALUES (1), (2)")

        runJob("src_purge", "tgt_purge")
        assertEquals(2, spark.table(q("tgt_purge")).count())

        // the accepted constraint: the source owns the physical files, so
        // DROP … PURGE tears the shared files out from under the target.
        // The source must instead be dropped WITHOUT purge.
        spark.sql("DROP TABLE ${q("src_purge")} PURGE")
        assertThrows<Exception> {
            spark.table(q("tgt_purge")).collectAsList()
        }
    }
}
