package org.openprojectx.spark.lakehouse.jobs

import org.openprojectx.spark.lakehouse.core.JobConfigException

/**
 * The catalog of job templates shipped in this image. The orchestration repo
 * selects one via `--job <name>` or `job.template` in the submitted config.
 */
object JobCatalog {

    private val templates: Map<String, JobTemplate> =
        listOf<JobTemplate>(
            JdbcSnapshotIngestJob,
        ).associateBy { it.name }

    fun names(): Set<String> = templates.keys

    fun require(name: String): JobTemplate =
        templates[name] ?: throw JobConfigException(
            "Unknown job template '$name'. Available templates: ${templates.keys.sorted().joinToString(", ")}"
        )
}
