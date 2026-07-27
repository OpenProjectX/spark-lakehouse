package org.openprojectx.spark.lakehouse.catalog

import javax.inject.Inject
import org.apache.iceberg.DataFile
import org.apache.iceberg.HasTableOperations
import org.apache.iceberg.Table
import org.apache.iceberg.spark.Spark3Util
import org.openprojectx.spark.boot.core.ConfigNodeFactory
import org.openprojectx.spark.boot.core.FlowNode
import org.openprojectx.spark.boot.core.NodeFactory
import org.openprojectx.spark.boot.runtime.spark.SparkActionNode
import org.openprojectx.spark.boot.runtime.spark.SparkExecutionContext
import org.slf4j.LoggerFactory

/**
 * Metadata-only append of one Iceberg table into another: the data files
 * referenced by the source's current snapshot are committed into the target
 * in a single fast-append snapshot. No data is read, rewritten, or moved —
 * afterwards both tables reference the same physical files, so the source
 * must be treated as retired (see docs/jobs/iceberg-zero-copy-append.adoc
 * for the ownership rules this node cannot enforce).
 *
 * Semantics are UNION ALL, guarded by fail-fast checks against every way a
 * manifest-level append can silently corrupt the target:
 *  - schemas must match by Iceberg field ID, not just by name/type
 *  - the source's schema history must not hold field IDs above the target's
 *    ID counter (a later `ALTER TABLE ADD COLUMN` on the target would reuse
 *    them and misread the imported files)
 *  - every source data file's partition spec must be compatible with the
 *    target's current spec
 *  - the source must carry no row-level delete files (they cannot be
 *    re-sequenced into the target; deleted rows would resurrect)
 *
 * All checks run before the commit; a failure leaves the target untouched.
 * Re-runs are no-ops while the appended snapshot's summary is still in the
 * target's retained history.
 */
class IcebergZeroCopyAppendActionNode : SparkActionNode {
    /** Full identifier: `catalog.namespace.table`. */
    lateinit var sourceTable: String

    /** Full identifier: `catalog.namespace.table`. */
    lateinit var targetTable: String

    override val name: String = "iceberg-zero-copy-append"

    override fun execute(input: Unit, context: SparkExecutionContext) {
        val source = Spark3Util.loadIcebergTable(context.spark, sourceTable)
        val target = Spark3Util.loadIcebergTable(context.spark, targetTable)

        val sourceSnapshot = source.currentSnapshot()
        if (sourceSnapshot == null) {
            log.info("Source {} has no snapshot; nothing to append", sourceTable)
            return
        }
        if (alreadyAppended(target, sourceSnapshot.snapshotId())) {
            log.info(
                "Source snapshot {} of {} already appended to {}; no-op",
                sourceSnapshot.snapshotId(), sourceTable, targetTable
            )
            return
        }

        validateSchemas(source, target)
        validatePartitionSpecs(source, target)
        val files = collectDataFiles(source)
        if (files.isEmpty()) {
            log.info("Source {} has no data files; nothing to append", sourceTable)
            return
        }

        val append = target.newFastAppend()
        files.forEach(append::appendFile)
        append.set(SOURCE_TABLE_PROPERTY, sourceTable)
        append.set(SOURCE_SNAPSHOT_PROPERTY, sourceSnapshot.snapshotId().toString())
        append.commit()
        log.info(
            "Appended {} data files of {} (snapshot {}) to {} without moving data",
            files.size, sourceTable, sourceSnapshot.snapshotId(), targetTable
        )
    }

    private fun alreadyAppended(target: Table, sourceSnapshotId: Long): Boolean =
        target.snapshots().any { snapshot ->
            snapshot.summary()[SOURCE_TABLE_PROPERTY] == sourceTable &&
                snapshot.summary()[SOURCE_SNAPSHOT_PROPERTY] == sourceSnapshotId.toString()
        }

    private fun validateSchemas(source: Table, target: Table) {
        check(source.schema().sameSchema(target.schema())) {
            "Zero-copy append requires identical schemas including Iceberg field IDs; " +
                "$sourceTable and $targetTable differ (identical-looking columns still mismatch " +
                "when the tables have different schema-evolution histories). " +
                "Fall back to a copying INSERT INTO … SELECT."
        }
        // Field IDs above the target's ID counter exist only in the source's
        // history (dropped columns). The target would hand out the same IDs on
        // its next ADD COLUMN and misread the imported files.
        val sourceLastColumnId = lastColumnId(source)
        val targetLastColumnId = lastColumnId(target)
        if (sourceLastColumnId != null && targetLastColumnId != null) {
            check(sourceLastColumnId <= targetLastColumnId) {
                "Zero-copy append refused: $sourceTable has assigned field IDs up to " +
                    "$sourceLastColumnId but $targetTable only up to $targetLastColumnId. " +
                    "The source's dropped columns would collide with columns the target adds later."
            }
        }
    }

    private fun lastColumnId(table: Table): Int? =
        (table as? HasTableOperations)?.operations()?.current()?.lastColumnId()

    private fun validatePartitionSpecs(source: Table, target: Table) {
        check(source.spec().compatibleWith(target.spec())) {
            "Zero-copy append requires compatible partition specs; " +
                "$sourceTable has ${source.spec()} but $targetTable has ${target.spec()}"
        }
    }

    private fun collectDataFiles(source: Table): List<DataFile> {
        // Scan planning may split one data file across several tasks and
        // re-appending it would duplicate rows, so files dedupe by location.
        val files = LinkedHashMap<String, DataFile>()
        source.newScan().planFiles().use { tasks ->
            tasks.forEach { task ->
                check(task.deletes().isEmpty()) {
                    "Zero-copy append refused: $sourceTable carries row-level delete files. " +
                        "Appending its data files would resurrect deleted rows; compact the " +
                        "source first (rewrite_data_files) or use a copying insert."
                }
                files.putIfAbsent(task.file().location(), task.file().copy())
            }
        }
        return files.values.toList()
    }

    companion object {
        const val SOURCE_TABLE_PROPERTY = "lakehouse.zero-copy-append.source-table"
        const val SOURCE_SNAPSHOT_PROPERTY = "lakehouse.zero-copy-append.source-snapshot-id"

        private val log = LoggerFactory.getLogger(IcebergZeroCopyAppendActionNode::class.java)
    }
}

class IcebergZeroCopyAppendActionNodeFactory @Inject constructor() :
    NodeFactory<IcebergZeroCopyAppendActionNode> {
    override fun create(): IcebergZeroCopyAppendActionNode = IcebergZeroCopyAppendActionNode()
}

class IcebergZeroCopyAppendActionConfigFactory @Inject constructor() : ConfigNodeFactory {
    override fun create(config: Map<String, Any?>): FlowNode<*, *> {
        return IcebergZeroCopyAppendActionNode().apply {
            sourceTable = requiredString(config, "source_table")
            targetTable = requiredString(config, "target_table")
        }
    }

    private fun requiredString(config: Map<String, Any?>, key: String): String {
        val value = config[key] as? String
        require(!value.isNullOrBlank()) { "Missing required config '$key'" }
        return value
    }
}
