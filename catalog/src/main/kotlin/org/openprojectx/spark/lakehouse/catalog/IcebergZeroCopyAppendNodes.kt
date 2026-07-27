package org.openprojectx.spark.lakehouse.catalog

import javax.inject.Inject
import org.apache.iceberg.spark.Spark3Util
import org.openprojectx.iceberg.IcebergZeroCopy
import org.openprojectx.spark.boot.core.ConfigNodeFactory
import org.openprojectx.spark.boot.core.FlowNode
import org.openprojectx.spark.boot.core.NodeFactory
import org.openprojectx.spark.boot.runtime.spark.SparkActionNode
import org.openprojectx.spark.boot.runtime.spark.SparkExecutionContext
import org.slf4j.LoggerFactory

/**
 * spark-boot adapter for [IcebergZeroCopy]: resolves both tables through the
 * session's Iceberg catalogs and delegates the metadata-only append. All
 * semantics, consistency guards, and the source-retirement contract live in
 * the library — see its docs and docs/jobs/iceberg-zero-copy-append.adoc.
 */
class IcebergZeroCopyAppendActionNode : SparkActionNode {
    /** Full identifier: `catalog.namespace.table`. */
    lateinit var sourceTable: String

    /** Full identifier: `catalog.namespace.table`. */
    lateinit var targetTable: String

    /**
     * UNSAFE: turns precondition failures into warnings and appends anyway —
     * the target can end up silently corrupt (misbound columns, resurrected
     * rows). Only for recovery tooling that verified the risk externally.
     */
    var skipValidation: Boolean = false

    override val name: String = "iceberg-zero-copy-append"

    override fun execute(input: Unit, context: SparkExecutionContext) {
        val source = Spark3Util.loadIcebergTable(context.spark, sourceTable)
        val target = Spark3Util.loadIcebergTable(context.spark, targetTable)

        val result = IcebergZeroCopy.append(source, target, sourceTable, targetTable, skipValidation)
        result.violations.forEach { violation ->
            log.warn("Zero-copy append precondition violated (skip_validation=true): {}", violation)
        }
        when (result.outcome) {
            IcebergZeroCopy.Outcome.APPENDED -> log.info(
                "Appended {} data files of {} (snapshot {}) to {} without moving data",
                result.appendedFiles, sourceTable, result.sourceSnapshotId, targetTable
            )
            IcebergZeroCopy.Outcome.ALREADY_APPENDED -> log.info(
                "Source snapshot {} of {} already appended to {}; no-op",
                result.sourceSnapshotId, sourceTable, targetTable
            )
            IcebergZeroCopy.Outcome.EMPTY_SOURCE -> log.info(
                "Source {} has no data to append; no-op", sourceTable
            )
        }
    }

    private companion object {
        val log = LoggerFactory.getLogger(IcebergZeroCopyAppendActionNode::class.java)!!
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
            skipValidation = when (val value = config["skip_validation"]) {
                null -> false
                is Boolean -> value
                is String -> value.toBooleanStrict()
                else -> error("Config 'skip_validation' must be a boolean")
            }
        }
    }

    private fun requiredString(config: Map<String, Any?>, key: String): String {
        val value = config[key] as? String
        require(!value.isNullOrBlank()) { "Missing required config '$key'" }
        return value
    }
}
