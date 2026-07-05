package org.openprojectx.spark.lakehouse.ingestion

import javax.inject.Inject
import org.openprojectx.spark.boot.core.ConfigNodeFactory
import org.openprojectx.spark.boot.core.FlowNode
import org.openprojectx.spark.boot.core.NodeFactory

class BronzeSnapshotSinkNodeFactory @Inject constructor() : NodeFactory<BronzeSnapshotSinkNode> {
    override fun create(): BronzeSnapshotSinkNode = BronzeSnapshotSinkNode()
}

class BronzeSnapshotSinkConfigFactory @Inject constructor() : ConfigNodeFactory {
    override fun create(config: Map<String, Any?>): FlowNode<*, *> {
        return BronzeSnapshotSinkNode().apply {
            path = requiredString(config, "path")
            tenant = requiredString(config, "tenant")
            source = requiredString(config, "source")
            (config["snapshot_date"] as? String)?.let { snapshotDate = it }
            partitionBy = stringList(config, "partition_by")
        }
    }
}

private fun requiredString(config: Map<String, Any?>, key: String): String {
    val value = config[key] as? String
    require(!value.isNullOrBlank()) { "Missing required config '$key'" }
    return value
}

private fun stringList(config: Map<String, Any?>, key: String): List<String> =
    when (val value = config[key]) {
        null -> emptyList()
        is List<*> -> value.map { it.toString() }
        is String -> value.split(",").map { it.trim() }.filter { it.isNotEmpty() }
        else -> error("Config '$key' must be a list or comma-separated string")
    }
