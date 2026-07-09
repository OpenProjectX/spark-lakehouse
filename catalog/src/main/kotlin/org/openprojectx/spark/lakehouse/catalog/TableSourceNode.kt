package org.openprojectx.spark.lakehouse.catalog

import javax.inject.Inject
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.openprojectx.spark.boot.core.ConfigNodeFactory
import org.openprojectx.spark.boot.core.FlowNode
import org.openprojectx.spark.boot.core.NodeFactory
import org.openprojectx.spark.boot.runtime.spark.SparkExecutionContext
import org.openprojectx.spark.boot.runtime.spark.SparkSourceNode

/** Reads a catalog table by full identifier (`catalog.namespace.table`). */
class TableSourceNode : SparkSourceNode<Dataset<Row>> {
    lateinit var table: String

    override val name: String = "table-source"

    override fun execute(input: Unit, context: SparkExecutionContext): Dataset<Row> =
        context.spark.table(table)
}

class TableSourceNodeFactory @Inject constructor() : NodeFactory<TableSourceNode> {
    override fun create(): TableSourceNode = TableSourceNode()
}

class TableSourceConfigFactory @Inject constructor() : ConfigNodeFactory {
    override fun create(config: Map<String, Any?>): FlowNode<*, *> {
        return TableSourceNode().apply {
            val value = config["table"] as? String
            require(!value.isNullOrBlank()) { "Missing required config 'table'" }
            table = value
        }
    }
}
