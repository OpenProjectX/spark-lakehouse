package org.openprojectx.spark.lakehouse.core

/**
 * Metadata columns every bronze writer stamps onto source-faithful data.
 * Prefixed to stay clear of source schemas; bronze is append-only, so these
 * columns are the audit trail for replays and late reprocessing.
 */
object BronzeColumns {
    const val TENANT = "_lake_tenant"
    const val SOURCE = "_lake_source"
    const val INGESTED_AT = "_lake_ingested_at"
    const val SNAPSHOT_DATE = "_snapshot_date"
}

/**
 * Node kind strings contributed by spark-lakehouse to the spark-boot factory
 * registries. Kept here (Spark-free) so job templates can reference kinds
 * without a dependency on the Spark-facing connector modules.
 */
object LakehouseNodeKinds {
    const val BRONZE_SNAPSHOT_SINK = "BronzeSnapshotSink"
}

/** spark-boot built-in kinds the job templates compose with. */
object SparkBootNodeKinds {
    const val JDBC_SOURCE = "JdbcSource"
    const val PARQUET_SOURCE = "ParquetSource"
    const val PARQUET_SINK = "ParquetSink"
    const val ICEBERG_SINK = "IcebergSink"
    const val SQL_TRANSFORM = "SqlTransform"
}
