package org.openprojectx.spark.lakehouse.app

import com.typesafe.config.Config
import org.openprojectx.spark.boot.core.FlowAssembler
import org.openprojectx.spark.boot.dagger.SparkBootComponent
import org.openprojectx.spark.lakehouse.core.ConfigSupport
import org.openprojectx.spark.lakehouse.core.JobConfigException
import org.openprojectx.spark.lakehouse.jobs.JobCatalog

/**
 * Resolves a job template, builds its flow from the submitted config, and
 * runs it on the component's Spark runtime. Does not stop the SparkSession —
 * the caller owns the session lifecycle (the CLI stops it, tests reuse it).
 */
object LakehouseJobRunner {

    fun run(jobName: String?, config: Config, component: SparkBootComponent) {
        val templateName = jobName
            ?: ConfigSupport.optionalString(config, "job.template")
            ?: throw JobConfigException(
                "No job template selected: pass --job <name> or set 'job.template'. " +
                    "Available templates: ${JobCatalog.names().sorted().joinToString(", ")}"
            )
        val template = JobCatalog.require(templateName)
        val definition = template.buildFlow(config)
        val flow = FlowAssembler(component.nodeFactoryRegistry()).assemble(definition)
        component.sparkRuntime().run(flow)
    }
}
