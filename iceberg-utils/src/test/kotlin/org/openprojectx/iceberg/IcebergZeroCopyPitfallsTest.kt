package org.openprojectx.iceberg

import java.nio.file.Path
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.DataFile
import org.apache.iceberg.DataFiles
import org.apache.iceberg.FileFormat
import org.apache.iceberg.FileMetadata
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.Schema
import org.apache.iceberg.Table
import org.apache.iceberg.hadoop.HadoopTables
import org.apache.iceberg.types.Types
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

/**
 * Demonstrations, not guards: each test shows the concrete inconsistency the
 * target ends up with when `skipValidation = true` overrides a precondition
 * (plus one pitfall that exists regardless of validation). These pin the
 * behaviors the documentation warns about — if one starts failing, the risk
 * story in docs/jobs/iceberg-zero-copy-append.adoc must be re-verified, not
 * the test silenced. Data-level consequences (wrong values returned by
 * queries) are demonstrated in the Spark integration suite; this suite
 * proves the corruption at the metadata level, where it originates.
 */
class IcebergZeroCopyPitfallsTest {

    @TempDir
    lateinit var tmp: Path

    private val tables = HadoopTables(Configuration())

    private val schema = Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "name", Types.StringType.get()),
    )

    private fun create(
        name: String,
        schema: Schema = this.schema,
        spec: PartitionSpec = PartitionSpec.unpartitioned(),
    ): Table = tables.create(schema, spec, mapOf("format-version" to "2"), "$tmp/$name")

    private fun dataFile(table: Table, name: String, partitionPath: String? = null): DataFile {
        val builder = DataFiles.builder(table.spec())
            .withPath("${table.location()}/data/$name.parquet")
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(128)
            .withRecordCount(2)
        partitionPath?.let(builder::withPartitionPath)
        return builder.build()
    }

    private fun addRows(table: Table, name: String, partitionPath: String? = null) {
        table.newFastAppend().appendFile(dataFile(table, name, partitionPath)).commit()
    }

    private fun addPositionDelete(table: Table, name: String) {
        val delete = FileMetadata.deleteFileBuilder(table.spec())
            .ofPositionDeletes()
            .withPath("${table.location()}/data/$name.parquet")
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(64)
            .withRecordCount(1)
            .build()
        table.newRowDelta().addDeletes(delete).commit()
    }

    @Test
    fun `diverged field ids - append succeeds and the corruption leaves no trace in the target`() {
        val source = create("src")
        source.updateSchema().deleteColumn("name").commit()
        source.updateSchema().addColumn("name", Types.StringType.get()).commit()
        addRows(source, "f1")
        val target = create("tgt")

        val result = IcebergZeroCopy.append(source, target, skipValidation = true)

        assertEquals(IcebergZeroCopy.Outcome.APPENDED, result.outcome)
        assertTrue(result.violations.any { "field IDs" in it })
        // the imported file's 'name' column is bound to the source's fresh
        // field ID; the target will project its own ID and read nulls — and
        // nothing in the target's metadata records that this happened. The
        // returned violations list is the only trace, gone after this call.
        val imported = target.newScan().planFiles().use { tasks -> tasks.map { it.file().location() } }
        assertTrue(imported.any { "src/data/f1" in it })
        val summary = target.currentSnapshot().summary()
        assertTrue(summary.keys.none { "violation" in it.lowercase() })
    }

    @Test
    fun `delete files are dropped - deleted rows resurrect in the target`() {
        val source = create("src").also { addRows(it, "f1") }
        addPositionDelete(source, "f1-deletes")
        // the source's own scans apply the delete...
        val sourceDeletes = source.newScan().planFiles().use { tasks -> tasks.sumOf { it.deletes().size } }
        assertEquals(1, sourceDeletes)
        val target = create("tgt")

        val result = IcebergZeroCopy.append(source, target, skipValidation = true)

        assertEquals(IcebergZeroCopy.Outcome.APPENDED, result.outcome)
        assertTrue(result.violations.any { "resurrects" in it })
        // ...but the target scans the same physical file with no deletes
        // attached: every row the source deleted is alive again here
        target.newScan().planFiles().use { tasks ->
            val task = tasks.single { "src/data/f1" in it.file().location() }
            assertEquals(0, task.deletes().size)
        }
    }

    @Test
    fun `dropped-column ids collide with columns the target adds later`() {
        val threeColumns = Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "tmp", Types.IntegerType.get()),
        )
        val source = create("src", schema = threeColumns).also { addRows(it, "f1") }
        val droppedTmpId = source.schema().findField("tmp").fieldId()
        source.updateSchema().deleteColumn("tmp").commit()
        val target = create("tgt")
        // current schemas are identical field-for-field; only the counter
        // guard knows the imported files still carry values under ID 3
        assertTrue(source.schema().sameSchema(target.schema()))

        val result = IcebergZeroCopy.append(source, target, skipValidation = true)
        assertTrue(result.violations.any { "field IDs up to" in it })

        // an ordinary, locally-safe evolution on the target...
        target.updateSchema().addColumn("extra", Types.IntegerType.get()).commit()

        // ...reuses exactly the imported files' dead ID: from now on the
        // source's dropped 'tmp' values are served as the target's 'extra'
        assertEquals(droppedTmpId, target.schema().findField("extra").fieldId())
    }

    @Test
    fun `partitioned source into unpartitioned target - partition metadata silently stripped`() {
        val partitioned = PartitionSpec.builderFor(schema).identity("id").build()
        val source = create("src", spec = partitioned).also { addRows(it, "f1", partitionPath = "id=1") }
        val target = create("tgt")

        val result = IcebergZeroCopy.append(source, target, skipValidation = true)

        // no error, no warning from Iceberg itself: the commit succeeds and
        // the file's partition tuple is simply gone in the target
        assertEquals(IcebergZeroCopy.Outcome.APPENDED, result.outcome)
        assertTrue(result.violations.any { "partition specs" in it })
        target.newScan().planFiles().use { tasks ->
            val file = tasks.single().file()
            assertEquals(0, file.partition().size())
        }
    }

    @Test
    fun `unpartitioned source into partitioned target - files land with null partition values`() {
        val source = create("src").also { addRows(it, "f1") }
        val partitioned = PartitionSpec.builderFor(schema).identity("id").build()
        val target = create("tgt", spec = partitioned)

        val result = IcebergZeroCopy.append(source, target, skipValidation = true)

        assertEquals(IcebergZeroCopy.Outcome.APPENDED, result.outcome)
        // the imported file claims partition id=null while actually holding
        // arbitrary ids — partition pruning now reasons from a lie
        target.newScan().planFiles().use { tasks ->
            val partition = tasks.single().file().partition()
            assertEquals(1, partition.size())
            assertNull(partition.get(0, Integer::class.java))
        }
    }

    @Test
    fun `pitfall regardless of validation - re-run after source changed duplicates files`() {
        val source = create("src").also { addRows(it, "f1") }
        val target = create("tgt")

        IcebergZeroCopy.append(source, target)
        // the source keeps living (already a contract violation) and commits
        // a new snapshot; the idempotency marker only matches exact snapshots
        addRows(source, "f2")
        val result = IcebergZeroCopy.append(source, target)

        // second run appends the source's FULL current file list: f1 again
        assertEquals(IcebergZeroCopy.Outcome.APPENDED, result.outcome)
        assertEquals(2, result.appendedFiles)
        val locations = target.newScan().planFiles().use { tasks -> tasks.map { it.file().location() } }
        assertEquals(3, locations.size)
        assertEquals(2, locations.count { "src/data/f1" in it })
        // every row of f1 is now counted twice by the target
    }
}
