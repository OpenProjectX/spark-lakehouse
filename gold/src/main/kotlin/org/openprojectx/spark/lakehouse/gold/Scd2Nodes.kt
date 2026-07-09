package org.openprojectx.spark.lakehouse.gold

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.openprojectx.spark.boot.runtime.spark.SparkExecutionContext
import org.openprojectx.spark.boot.runtime.spark.SparkSinkNode
import org.openprojectx.spark.lakehouse.catalog.IcebergNamespaces
import org.openprojectx.spark.lakehouse.core.Scd2Columns

/**
 * Kimball SCD Type 2 loader. Input is the current source state (one row per
 * natural key, e.g. a silver table); the sink maintains a versioned Iceberg
 * dimension:
 *
 *  - new natural keys open a first version,
 *  - a change in any tracked column closes the current version
 *    (`_scd_effective_to`, `_scd_current = false`) and opens a new one,
 *  - unchanged rows are untouched,
 *  - keys absent from the source are left current (no delete detection in v1).
 *
 * `_dim_key` is a deterministic hash of natural key + effective-from, so
 * replays with the same `effectiveAt` produce stable surrogate keys.
 */
class Scd2DimSinkNode : SparkSinkNode<Dataset<Row>> {
    /** Full identifier: `catalog.namespace.table`. */
    lateinit var table: String
    lateinit var naturalKeys: List<String>
    /** Type-2 columns; a change opens a new version. Empty = all non-key columns. */
    var trackedColumns: List<String> = emptyList()
    /** Columns never written to the dimension. */
    var excludeColumns: List<String> = emptyList()
    /** ISO-8601 instant for the version boundary; defaults to load time. */
    var effectiveAt: String? = null

    override val name: String = "scd2-dim-sink"

    override fun execute(input: Dataset<Row>, context: SparkExecutionContext) {
        val spark = context.spark
        val excluded = excludeColumns.toSet()
        val dataColumns = input.columns().filter { it !in excluded }
        keysMustExist(dataColumns)
        val tracked = trackedColumns.ifEmpty { dataColumns - naturalKeys.toSet() }
        require(tracked.isNotEmpty()) { "No tracked columns left for SCD2 on $table" }

        val source = input
            .select(dataColumns.first(), *dataColumns.drop(1).toTypedArray())
            .dropDuplicates(naturalKeys.toTypedArray())

        IcebergNamespaces.ensureNamespaceFor(spark, table)
        createTableIfMissing(source, context)

        val effectiveFrom = Timestamp.from(effectiveAt?.let { Instant.parse(it) } ?: Instant.now())
        val current = spark.table(table).filter(functions.col(Scd2Columns.CURRENT))

        val joinCondition = naturalKeys
            .map { functions.expr("s.`$it` <=> d.`$it`") }
            .reduce { a, b -> a.and(b) }
        val hasChange = tracked
            .map { functions.expr("NOT (s.`$it` <=> d.`$it`)") }
            .reduce { a, b -> a.or(b) }
        val isNewKey = functions.expr("d.`${naturalKeys.first()}` IS NULL")
            .and(functions.expr("d.`${Scd2Columns.CURRENT}` IS NULL"))

        val stage = source.alias("s")
            .join(current.alias("d"), joinCondition, "left_outer")
            .filter(isNewKey.or(hasChange))
            .selectExpr("s.*")
            .withColumn(Scd2Columns.EFFECTIVE_FROM, functions.lit(effectiveFrom))
            .withColumn(Scd2Columns.EFFECTIVE_TO, functions.lit(null).cast("timestamp"))
            .withColumn(Scd2Columns.CURRENT, functions.lit(true))
            .withColumn(
                Scd2Columns.DIM_KEY,
                functions.xxhash64(
                    *(naturalKeys.map { functions.col(it) } + functions.col(Scd2Columns.EFFECTIVE_FROM))
                        .toTypedArray()
                ),
            )
            .localCheckpoint(true)

        val view = "scd2_stage_${UUID.randomUUID().toString().replace("-", "")}"
        stage.createOrReplaceTempView(view)
        try {
            val on = naturalKeys.joinToString(" AND ") { "t.`$it` = s.`$it`" }
            context.spark.sql(
                """
                MERGE INTO $table t USING $view s
                ON $on AND t.`${Scd2Columns.CURRENT}` = true
                WHEN MATCHED THEN UPDATE SET
                  t.`${Scd2Columns.CURRENT}` = false,
                  t.`${Scd2Columns.EFFECTIVE_TO}` = s.`${Scd2Columns.EFFECTIVE_FROM}`
                """.trimIndent()
            )
            val orderedColumns = spark.table(table).columns().joinToString(", ") { "`$it`" }
            context.spark.sql("INSERT INTO $table ($orderedColumns) SELECT $orderedColumns FROM $view")
        } finally {
            spark.catalog().dropTempView(view)
        }
    }

    private fun keysMustExist(dataColumns: List<String>) {
        require(naturalKeys.isNotEmpty()) { "SCD2 sink for $table needs at least one natural key" }
        naturalKeys.forEach { key ->
            require(key in dataColumns) { "Natural key '$key' missing from input columns $dataColumns" }
        }
    }

    private fun createTableIfMissing(source: Dataset<Row>, context: SparkExecutionContext) {
        val ddl = StructType(source.schema().fields().toList().toTypedArray<StructField>()).toDDL()
        context.spark.sql(
            """
            CREATE TABLE IF NOT EXISTS $table (
              $ddl,
              `${Scd2Columns.EFFECTIVE_FROM}` TIMESTAMP,
              `${Scd2Columns.EFFECTIVE_TO}` TIMESTAMP,
              `${Scd2Columns.CURRENT}` BOOLEAN,
              `${Scd2Columns.DIM_KEY}` BIGINT
            ) USING iceberg
            """.trimIndent()
        )
    }
}
