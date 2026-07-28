package org.openprojectx.iceberg

import org.apache.iceberg.DataFile
import org.apache.iceberg.DeleteFile
import org.apache.iceberg.FileContent
import org.apache.iceberg.HasTableOperations
import org.apache.iceberg.Table

/**
 * Metadata-only append of one Iceberg table into another: the data files
 * referenced by the source's current snapshot are committed into the target
 * in a single fast-append snapshot. No data is read, rewritten, or moved —
 * afterwards both tables reference the same physical files, so the source
 * must be treated as retired (dropped without purge, no snapshot expiry);
 * that ownership contract is the caller's to enforce.
 *
 * Semantics are UNION ALL, guarded fail-fast against every way a
 * manifest-level append can silently corrupt the target:
 *  - schemas must match by Iceberg field ID, not just by name/type
 *  - the source's schema history must not hold field IDs above the target's
 *    ID counter (a later `ALTER TABLE ADD COLUMN` on the target would reuse
 *    them and misread the imported files)
 *  - the source's partition spec must be compatible with the target's
 *  - the source must carry no equality delete files (there is no sequence
 *    placement that makes them correct in the target: committed with the
 *    data files they are silent no-ops, committed later they delete the
 *    target's own matching rows)
 *
 * Position delete files ARE imported: their `(file_path, pos)` rows point at
 * absolute physical rows, and committing them in the same RowDelta as the
 * data files (same sequence number) makes them apply — the `>=` rule — while
 * never touching the target's own files (different paths).
 *
 * All checks run before the commit; a [ValidationException] leaves the
 * target untouched. With `skipValidation = true` the same checks still run
 * but only as warnings collected into [Result.violations] — the append
 * proceeds into the corrupt states described above (see the
 * skip-validation test suites for demonstrations of each). Re-runs are
 * no-ops while the appended snapshot's summary is still in the target's
 * retained history. This type is Spark-free; load the [Table] handles
 * through whatever catalog integration the caller uses.
 */
object IcebergZeroCopy {

    /** Snapshot-summary key recording the appended source table name. */
    const val SOURCE_TABLE_PROPERTY = "zero-copy-append.source-table"

    /** Snapshot-summary key recording the appended source snapshot id. */
    const val SOURCE_SNAPSHOT_PROPERTY = "zero-copy-append.source-snapshot-id"

    enum class Outcome {
        /** Files were committed to the target. */
        APPENDED,

        /** The source's current snapshot was appended earlier; nothing done. */
        ALREADY_APPENDED,

        /** The source has no snapshot or no data files; nothing to append. */
        EMPTY_SOURCE,
    }

    data class Result(
        val outcome: Outcome,
        val appendedFiles: Int = 0,
        val sourceSnapshotId: Long? = null,
        /** Violated preconditions; non-empty only with `skipValidation = true`. */
        val violations: List<String> = emptyList(),
        /** Position delete files imported alongside the data files. */
        val appendedDeleteFiles: Int = 0,
    )

    /** A precondition failed; the target was not modified. */
    class ValidationException(message: String) : IllegalStateException(message)

    /**
     * Appends [source]'s current data files into [target] by metadata only.
     * [sourceName]/[targetName] override the identifiers used in messages and
     * in the snapshot-summary idempotency marker (defaults: [Table.name]).
     *
     * [skipValidation] = true turns precondition failures from
     * [ValidationException]s into [Result.violations] and commits anyway.
     * Every violated precondition is a way the target silently corrupts —
     * misbound columns, resurrected rows, partition metadata Iceberg may
     * reject at commit time. Only for recovery tooling that has verified the
     * risk externally.
     */
    @JvmStatic
    @JvmOverloads
    fun append(
        source: Table,
        target: Table,
        sourceName: String = source.name(),
        targetName: String = target.name(),
        skipValidation: Boolean = false,
    ): Result {
        val sourceSnapshot = source.currentSnapshot()
            ?: return Result(Outcome.EMPTY_SOURCE)
        if (alreadyAppended(target, sourceName, sourceSnapshot.snapshotId())) {
            return Result(Outcome.ALREADY_APPENDED, sourceSnapshotId = sourceSnapshot.snapshotId())
        }

        val violations = mutableListOf<String>()
        violations += schemaViolations(source, target, sourceName, targetName)
        violations += partitionSpecViolations(source, target, sourceName, targetName)
        if (violations.isNotEmpty() && !skipValidation) {
            throw ValidationException(violations.joinToString("\n"))
        }

        val collected = collectFiles(source)
        if (collected.equalityDeleteFiles > 0) {
            val violation =
                "Zero-copy append of $sourceName drops ${collected.equalityDeleteFiles} equality " +
                    "delete files: no sequence placement makes them correct in $targetName " +
                    "(committed with the data files they are silent no-ops and their deleted " +
                    "rows resurrect; committed later they delete the target's own matching " +
                    "rows). This operation is metadata-only and never rewrites data itself; if " +
                    "the merge is still required, first compact the source outside it " +
                    "(rewrite_data_files — a data-rewriting step), or run a copying insert instead."
            if (!skipValidation) throw ValidationException(violation)
            violations += violation
        }
        if (collected.dataFiles.isEmpty()) {
            return Result(Outcome.EMPTY_SOURCE, sourceSnapshotId = sourceSnapshot.snapshotId(), violations = violations)
        }

        // Position deletes ride along in one RowDelta: same commit -> same
        // sequence number, and position deletes apply to data files with a
        // sequence <= their own, so they keep deleting exactly the imported
        // (file_path, pos) rows and can never touch the target's own files.
        if (collected.positionDeleteFiles.isEmpty()) {
            val append = target.newFastAppend()
            collected.dataFiles.forEach(append::appendFile)
            append.set(SOURCE_TABLE_PROPERTY, sourceName)
            append.set(SOURCE_SNAPSHOT_PROPERTY, sourceSnapshot.snapshotId().toString())
            append.commit()
        } else {
            val rowDelta = target.newRowDelta()
            collected.dataFiles.forEach(rowDelta::addRows)
            collected.positionDeleteFiles.forEach(rowDelta::addDeletes)
            rowDelta.set(SOURCE_TABLE_PROPERTY, sourceName)
            rowDelta.set(SOURCE_SNAPSHOT_PROPERTY, sourceSnapshot.snapshotId().toString())
            rowDelta.commit()
        }
        return Result(
            outcome = Outcome.APPENDED,
            appendedFiles = collected.dataFiles.size,
            sourceSnapshotId = sourceSnapshot.snapshotId(),
            violations = violations,
            appendedDeleteFiles = collected.positionDeleteFiles.size,
        )
    }

    private fun alreadyAppended(target: Table, sourceName: String, sourceSnapshotId: Long): Boolean =
        target.snapshots().any { snapshot ->
            snapshot.summary()[SOURCE_TABLE_PROPERTY] == sourceName &&
                snapshot.summary()[SOURCE_SNAPSHOT_PROPERTY] == sourceSnapshotId.toString()
        }

    private fun schemaViolations(
        source: Table,
        target: Table,
        sourceName: String,
        targetName: String,
    ): List<String> {
        val violations = mutableListOf<String>()
        if (!source.schema().sameSchema(target.schema())) {
            violations +=
                "Zero-copy append requires identical schemas including Iceberg field IDs; " +
                    "$sourceName and $targetName differ (identical-looking columns still mismatch " +
                    "when the tables have different schema-evolution histories). " +
                    "This operation is metadata-only and never copies data itself; if the merge " +
                    "is still required, run a copying INSERT INTO … SELECT outside it."
        }
        // Field IDs above the target's ID counter exist only in the source's
        // history (dropped columns). The target would hand out the same IDs on
        // its next ADD COLUMN and misread the imported files.
        val sourceLastColumnId = lastColumnId(source)
        val targetLastColumnId = lastColumnId(target)
        if (sourceLastColumnId != null && targetLastColumnId != null &&
            sourceLastColumnId > targetLastColumnId
        ) {
            violations +=
                "Zero-copy append refused: $sourceName has assigned field IDs up to " +
                    "$sourceLastColumnId but $targetName only up to $targetLastColumnId. " +
                    "The source's dropped columns would collide with columns the target adds later."
        }
        return violations
    }

    private fun lastColumnId(table: Table): Int? =
        (table as? HasTableOperations)?.operations()?.current()?.lastColumnId()

    private fun partitionSpecViolations(
        source: Table,
        target: Table,
        sourceName: String,
        targetName: String,
    ): List<String> =
        if (source.spec().compatibleWith(target.spec())) {
            emptyList()
        } else {
            listOf(
                "Zero-copy append requires compatible partition specs; " +
                    "$sourceName has ${source.spec()} but $targetName has ${target.spec()}"
            )
        }

    private class CollectedFiles(
        val dataFiles: List<DataFile>,
        val positionDeleteFiles: List<DeleteFile>,
        val equalityDeleteFiles: Int,
    )

    private fun collectFiles(source: Table): CollectedFiles {
        // Scan planning may split one data file across several tasks (and one
        // delete file usually serves many data files), so both dedupe by
        // location or the target would double-reference them.
        val files = LinkedHashMap<String, DataFile>()
        val positionDeletes = LinkedHashMap<String, DeleteFile>()
        val equalityDeletes = mutableSetOf<String>()
        source.newScan().planFiles().use { tasks ->
            tasks.forEach { task ->
                task.deletes().forEach { delete ->
                    when (delete.content()) {
                        FileContent.POSITION_DELETES ->
                            positionDeletes.putIfAbsent(delete.location(), delete.copy())
                        else -> equalityDeletes += delete.location()
                    }
                }
                files.putIfAbsent(task.file().location(), task.file().copy())
            }
        }
        return CollectedFiles(files.values.toList(), positionDeletes.values.toList(), equalityDeletes.size)
    }
}
