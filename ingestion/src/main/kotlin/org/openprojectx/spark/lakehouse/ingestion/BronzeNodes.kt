package org.openprojectx.spark.lakehouse.ingestion

import java.time.LocalDate
import java.time.ZoneOffset
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions
import org.openprojectx.spark.boot.runtime.spark.SparkExecutionContext
import org.openprojectx.spark.boot.runtime.spark.SparkSinkNode
import org.openprojectx.spark.lakehouse.core.BronzeColumns

/**
 * Bronze snapshot writer: stamps lineage metadata columns onto the incoming
 * frame and appends it as parquet partitioned by snapshot date. Bronze is
 * append-only by contract — reruns for the same snapshot date must be handled
 * upstream (orchestration clears the partition or picks a new date).
 */
class BronzeSnapshotSinkNode : SparkSinkNode<Dataset<Row>> {
    lateinit var path: String
    lateinit var tenant: String
    lateinit var source: String
    var snapshotDate: String = LocalDate.now(ZoneOffset.UTC).toString()
    var partitionBy: List<String> = emptyList()

    override val name: String = "bronze-snapshot-sink"

    override fun execute(input: Dataset<Row>, context: SparkExecutionContext) {
        val stamped = input
            .withColumn(BronzeColumns.TENANT, functions.lit(tenant))
            .withColumn(BronzeColumns.SOURCE, functions.lit(source))
            .withColumn(BronzeColumns.INGESTED_AT, functions.current_timestamp())
            .withColumn(BronzeColumns.SNAPSHOT_DATE, functions.lit(snapshotDate))

        stamped.write()
            .mode(SaveMode.Append)
            .partitionBy(BronzeColumns.SNAPSHOT_DATE, *partitionBy.toTypedArray())
            .parquet(path)
    }
}
