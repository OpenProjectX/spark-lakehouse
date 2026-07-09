package org.openprojectx.spark.lakehouse.jobs

import org.openprojectx.spark.lakehouse.job.api.JobCatalog
import org.openprojectx.spark.lakehouse.jobs.bronze.JdbcSnapshotIngestJob
import org.openprojectx.spark.lakehouse.jobs.gold.Scd2DimLoadJob
import org.openprojectx.spark.lakehouse.jobs.silver.CdcSilverMergeJob

/** The job catalog shipped in this image, one entry per medallion pattern. */
object LakehouseJobs {

    val catalog: JobCatalog = JobCatalog(
        listOf(
            // bronze
            JdbcSnapshotIngestJob,
            // silver
            CdcSilverMergeJob,
            // gold
            Scd2DimLoadJob,
        )
    )
}
