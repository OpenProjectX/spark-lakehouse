package org.openprojectx.spark.lakehouse.gold

import javax.inject.Inject
import org.openprojectx.spark.boot.core.ConfigNodeFactory
import org.openprojectx.spark.boot.core.FlowNode
import org.openprojectx.spark.boot.core.NodeFactory

class Scd2DimSinkNodeFactory @Inject constructor() : NodeFactory<Scd2DimSinkNode> {
    override fun create(): Scd2DimSinkNode = Scd2DimSinkNode()
}

class Scd2DimSinkConfigFactory @Inject constructor() : ConfigNodeFactory {
    override fun create(config: Map<String, Any?>): FlowNode<*, *> {
        return Scd2DimSinkNode().apply {
            table = requiredString(config, "table")
            naturalKeys = requiredStringList(config, "keys")
            stringList(config, "tracked_columns")?.let { trackedColumns = it }
            stringList(config, "exclude_columns")?.let { excludeColumns = it }
            effectiveAt = config["effective_at"] as? String
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
