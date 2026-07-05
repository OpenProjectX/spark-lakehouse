package org.openprojectx.spark.lakehouse.silver

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.openprojectx.spark.boot.runtime.spark.SparkExecutionContext
import org.openprojectx.spark.boot.runtime.spark.SparkSinkNode
import org.openprojectx.spark.boot.runtime.spark.SparkTransformNode
import org.openprojectx.spark.lakehouse.catalog.IcebergNamespaces

/**
 * Collapses a batch of CDC events to the latest event per business key:
 * row_number over (partition by keys order by sequence desc) = 1. The
 * sequence column must be monotonic per key (commit LSN, event timestamp, …).
 */
class CdcResolveTransformNode : SparkTransformNode<Dataset<Row>, Dataset<Row>> {
    lateinit var keys: List<String>
    lateinit var sequenceBy: String

    override val name: String = "cdc-resolve"

    override fun execute(input: Dataset<Row>, context: SparkExecutionContext): Dataset<Row> {
        val window = Window
            .partitionBy(*keys.map { functions.col(it) }.toTypedArray())
            .orderBy(functions.col(sequenceBy).desc())
        return input
            .withColumn(ROW_NUMBER_COLUMN, functions.row_number().over(window))
            .filter("$ROW_NUMBER_COLUMN = 1")
            .drop(ROW_NUMBER_COLUMN)
    }

    private companion object {
        const val ROW_NUMBER_COLUMN = "_cdc_rn"
    }
}

/**
 * Merges resolved CDC rows into a silver Iceberg table: delete on matched
 * delete events, update on matched upserts, insert on unmatched upserts.
 * Creates the namespace and table on first run. Without [opColumn] the merge
 * degrades to a pure upsert.
 */
class IcebergMergeSinkNode : SparkSinkNode<Dataset<Row>> {
    /** Full identifier: `catalog.namespace.table`. */
    lateinit var table: String
    lateinit var keys: List<String>
    var opColumn: String? = null
    var deleteValues: List<String> = listOf("d")
    /** Columns never written to silver (bronze metadata, transport fields). */
    var excludeColumns: List<String> = emptyList()

    override val name: String = "iceberg-merge-sink"

    override fun execute(input: Dataset<Row>, context: SparkExecutionContext) {
        val spark = context.spark
        val excluded = (excludeColumns + listOfNotNull(opColumn)).toSet()
        val dataColumns = input.columns().filter { it !in excluded }
        require(dataColumns.isNotEmpty()) { "No data columns left after excluding $excluded" }
        keys.forEach { key ->
            require(key in dataColumns) { "Merge key '$key' missing from data columns $dataColumns" }
        }

        IcebergNamespaces.ensureNamespaceFor(spark, table)
        createTableIfMissing(input, context, dataColumns)

        val view = "cdc_updates_${java.util.UUID.randomUUID().toString().replace("-", "")}"
        input.localCheckpoint(true).createOrReplaceTempView(view)
        try {
            spark.sql(mergeSql(view, dataColumns))
        } finally {
            spark.catalog().dropTempView(view)
        }
    }

    private fun createTableIfMissing(input: Dataset<Row>, context: SparkExecutionContext, dataColumns: List<String>) {
        val fields = input.schema().fields().filter { it.name() in dataColumns.toSet() }
        val ddl = StructType(fields.toTypedArray<StructField>()).toDDL()
        context.spark.sql("CREATE TABLE IF NOT EXISTS $table ($ddl) USING iceberg")
    }

    private fun mergeSql(view: String, dataColumns: List<String>): String {
        val on = keys.joinToString(" AND ") { "t.`$it` = s.`$it`" }
        val setClause = dataColumns.joinToString(", ") { "t.`$it` = s.`$it`" }
        val insertColumns = dataColumns.joinToString(", ") { "`$it`" }
        val insertValues = dataColumns.joinToString(", ") { "s.`$it`" }
        val op = opColumn
        return if (op == null) {
            """
            MERGE INTO $table t USING $view s ON $on
            WHEN MATCHED THEN UPDATE SET $setClause
            WHEN NOT MATCHED THEN INSERT ($insertColumns) VALUES ($insertValues)
            """.trimIndent()
        } else {
            val deletes = deleteValues.joinToString(", ") { "'${it.replace("'", "''")}'" }
            """
            MERGE INTO $table t USING $view s ON $on
            WHEN MATCHED AND s.`$op` IN ($deletes) THEN DELETE
            WHEN MATCHED THEN UPDATE SET $setClause
            WHEN NOT MATCHED AND s.`$op` NOT IN ($deletes) THEN INSERT ($insertColumns) VALUES ($insertValues)
            """.trimIndent()
        }
    }
}
