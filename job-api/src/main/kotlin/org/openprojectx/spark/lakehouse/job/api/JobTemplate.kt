package org.openprojectx.spark.lakehouse.job.api

import com.typesafe.config.Config
import org.openprojectx.spark.boot.core.FlowDefinition

/**
 * An abstract, tenant-agnostic Spark job. A template turns a submitted HOCON
 * config (owned by the orchestration repo) into a spark-boot [FlowDefinition];
 * it never embeds tenant knowledge or business SQL of its own.
 *
 * The config schema is the versioned contract between this image and the
 * orchestration repo: templates must reject configs whose declared
 * `job.schema-version` they do not support, with messages that make the fix
 * obvious from the Airflow side.
 */
interface JobTemplate {
    /** Stable template id, e.g. `jdbc-snapshot-ingest`. Referenced by `job.template`. */
    val name: String

    /** Config schema version this template implements. */
    val schemaVersion: Int

    /** Builds the flow, validating the config fail-fast. */
    fun buildFlow(config: Config): FlowDefinition
}
