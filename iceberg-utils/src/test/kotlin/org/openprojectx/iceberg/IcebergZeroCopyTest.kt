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
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir

/**
 * Pure-metadata tests: tables are HadoopTables in a temp dir and the "data
 * files" are fabricated manifest entries — nothing ever writes a data file,
 * which is exactly the contract under test. Each guard test pins a scenario
 * that would silently corrupt the target if the append went through.
 */
class IcebergZeroCopyTest {

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

    private fun dataFileLocations(table: Table): Set<String> =
        table.newScan().planFiles().use { tasks -> tasks.map { it.file().location() }.toSet() }

    @Test
    fun `appends the source's files without moving data`() {
        val source = create("src").also { addRows(it, "f1"); addRows(it, "f2") }
        val target = create("tgt").also { addRows(it, "t1") }
        val sourceFiles = dataFileLocations(source)
        val targetFilesBefore = dataFileLocations(target)

        val result = IcebergZeroCopy.append(source, target)

        assertEquals(IcebergZeroCopy.Outcome.APPENDED, result.outcome)
        assertEquals(2, result.appendedFiles)
        assertEquals(source.currentSnapshot().snapshotId(), result.sourceSnapshotId)
        // the target's new files ARE the source's files, still under src/
        assertEquals(targetFilesBefore + sourceFiles, dataFileLocations(target))
        val summary = target.currentSnapshot().summary()
        assertEquals(source.name(), summary[IcebergZeroCopy.SOURCE_TABLE_PROPERTY])
        assertEquals(
            source.currentSnapshot().snapshotId().toString(),
            summary[IcebergZeroCopy.SOURCE_SNAPSHOT_PROPERTY],
        )
    }

    @Test
    fun `rerun with an unchanged source is a no-op`() {
        val source = create("src").also { addRows(it, "f1") }
        val target = create("tgt")

        IcebergZeroCopy.append(source, target)
        val snapshotsAfterFirst = target.snapshots().count()

        val result = IcebergZeroCopy.append(source, target)

        assertEquals(IcebergZeroCopy.Outcome.ALREADY_APPENDED, result.outcome)
        assertEquals(0, result.appendedFiles)
        assertEquals(snapshotsAfterFirst, target.snapshots().count())
    }

    @Test
    fun `source with no snapshot is an empty no-op`() {
        val source = create("src")
        val target = create("tgt").also { addRows(it, "t1") }
        val snapshotsBefore = target.snapshots().count()

        val result = IcebergZeroCopy.append(source, target)

        assertEquals(IcebergZeroCopy.Outcome.EMPTY_SOURCE, result.outcome)
        assertNull(result.sourceSnapshotId)
        assertEquals(snapshotsBefore, target.snapshots().count())
    }

    @Test
    fun `refuses schemas whose field ids diverged`() {
        // table creation reassigns IDs 1..N, so divergence must come from
        // evolution: drop + re-add leaves a schema that prints identically to
        // the target's but binds 'name' to a fresh field ID
        val source = create("src")
        source.updateSchema().deleteColumn("name").commit()
        source.updateSchema().addColumn("name", Types.StringType.get()).commit()
        addRows(source, "f1")
        val target = create("tgt")

        val ex = assertThrows<IcebergZeroCopy.ValidationException> {
            IcebergZeroCopy.append(source, target)
        }
        assertTrue("field ID" in ex.message!!, ex.message)
        assertNull(target.currentSnapshot())
    }

    @Test
    fun `refuses a source whose dropped columns outrun the target id counter`() {
        val threeColumns = Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "tmp", Types.IntegerType.get()),
        )
        val source = create("src", schema = threeColumns).also { addRows(it, "f1") }
        // after the drop, current schemas match field-for-field — only the
        // source's ID counter betrays that ID 3 lives on in its files
        source.updateSchema().deleteColumn("tmp").commit()
        val target = create("tgt")
        assertTrue(source.schema().sameSchema(target.schema()))

        val ex = assertThrows<IcebergZeroCopy.ValidationException> {
            IcebergZeroCopy.append(source, target)
        }
        assertTrue("field IDs up to" in ex.message!!, ex.message)
        assertNull(target.currentSnapshot())
    }

    @Test
    fun `refuses incompatible partition specs`() {
        val partitioned = PartitionSpec.builderFor(schema).identity("id").build()
        val source = create("src", spec = partitioned).also { addRows(it, "f1", partitionPath = "id=1") }
        val target = create("tgt")

        val ex = assertThrows<IcebergZeroCopy.ValidationException> {
            IcebergZeroCopy.append(source, target)
        }
        assertTrue("partition specs" in ex.message!!, ex.message)
        assertNull(target.currentSnapshot())
    }

    @Test
    fun `refuses a source carrying row-level delete files`() {
        val source = create("src").also { addRows(it, "f1") }
        val positionDelete = FileMetadata.deleteFileBuilder(source.spec())
            .ofPositionDeletes()
            .withPath("${source.location()}/data/f1-deletes.parquet")
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(64)
            .withRecordCount(1)
            .build()
        source.newRowDelta().addDeletes(positionDelete).commit()
        val target = create("tgt")

        val ex = assertThrows<IcebergZeroCopy.ValidationException> {
            IcebergZeroCopy.append(source, target)
        }
        assertTrue("delete files" in ex.message!!, ex.message)
        assertNull(target.currentSnapshot())
    }

    @Test
    fun `validation failure after a previous append leaves the target intact`() {
        val source = create("src").also { addRows(it, "f1") }
        val target = create("tgt")
        IcebergZeroCopy.append(source, target)
        val filesAfterAppend = dataFileLocations(target)

        // source mutates incompatibly afterwards: gains a delete file
        addRows(source, "f2")
        val positionDelete = FileMetadata.deleteFileBuilder(source.spec())
            .ofPositionDeletes()
            .withPath("${source.location()}/data/f2-deletes.parquet")
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(64)
            .withRecordCount(1)
            .build()
        source.newRowDelta().addDeletes(positionDelete).commit()

        assertThrows<IcebergZeroCopy.ValidationException> {
            IcebergZeroCopy.append(source, target)
        }
        assertEquals(filesAfterAppend, dataFileLocations(target))
    }
}
