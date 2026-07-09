package org.openprojectx.spark.lakehouse.job.api

import org.openprojectx.spark.lakehouse.core.JobConfigException

/**
 * A named set of job templates. The shipped image assembles one from the
 * `jobs` module; tests or extensions can assemble their own.
 */
class JobCatalog(templates: Collection<JobTemplate>) {

    private val byName: Map<String, JobTemplate> = templates
        .groupBy { it.name }
        .mapValues { (name, matches) ->
            require(matches.size == 1) { "Duplicate job template name '$name'" }
            matches.single()
        }

    fun names(): Set<String> = byName.keys

    fun require(name: String): JobTemplate =
        byName[name] ?: throw JobConfigException(
            "Unknown job template '$name'. Available templates: ${byName.keys.sorted().joinToString(", ")}"
        )
}
