package org.openprojectx.spark.lakehouse.silver

import javax.inject.Inject
import org.openprojectx.spark.boot.core.ConfigNodeFactory
import org.openprojectx.spark.boot.core.FlowNode
import org.openprojectx.spark.boot.core.NodeFactory

class CdcResolveTransformNodeFactory @Inject constructor() : NodeFactory<CdcResolveTransformNode> {
    override fun create(): CdcResolveTransformNode = CdcResolveTransformNode()
}

class IcebergMergeSinkNodeFactory @Inject constructor() : NodeFactory<IcebergMergeSinkNode> {
    override fun create(): IcebergMergeSinkNode = IcebergMergeSinkNode()
}

class CdcResolveTransformConfigFactory @Inject constructor() : ConfigNodeFactory {
    override fun create(config: Map<String, Any?>): FlowNode<*, *> {
        return CdcResolveTransformNode().apply {
            keys = requiredStringList(config, "keys")
            sequenceBy = requiredString(config, "sequence_by")
        }
    }
}

class IcebergMergeSinkConfigFactory @Inject constructor() : ConfigNodeFactory {
    override fun create(config: Map<String, Any?>): FlowNode<*, *> {
        return IcebergMergeSinkNode().apply {
            table = requiredString(config, "table")
            keys = requiredStringList(config, "keys")
            opColumn = config["op_column"] as? String
            stringList(config, "delete_values")?.let { deleteValues = it }
            stringList(config, "exclude_columns")?.let { excludeColumns = it }
        }
    }
}

private fun requiredString(config: Map<String, Any?>, key: String): String {
    val value = config[key] as? String
    require(!value.isNullOrBlank()) { "Missing required config '$key'" }
    return value
}

private fun requiredStringList(config: Map<String, Any?>, key: String): List<String> =
    stringList(config, key).takeUnless { it.isNullOrEmpty() }
        ?: throw IllegalArgumentException("Missing required config '$key'")

private fun stringList(config: Map<String, Any?>, key: String): List<String>? =
    when (val value = config[key]) {
        null -> null
        is List<*> -> value.map { it.toString() }
        is String -> value.split(",").map { it.trim() }.filter { it.isNotEmpty() }
        else -> error("Config '$key' must be a list or comma-separated string")
    }
