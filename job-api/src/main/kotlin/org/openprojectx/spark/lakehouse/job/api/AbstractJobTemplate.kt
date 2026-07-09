package org.openprojectx.spark.lakehouse.job.api

import com.typesafe.config.Config
import org.openprojectx.spark.lakehouse.core.ConfigSupport
import org.openprojectx.spark.lakehouse.core.JobConfigException

/**
 * Base contract for job templates: shared header validation (template id and
 * schema version). Concrete templates call [validateHeader] first in
 * [buildFlow] and then interpret their own sections.
 */
abstract class AbstractJobTemplate : JobTemplate {

    protected fun validateHeader(config: Config) {
        val declared = ConfigSupport.optionalString(config, "job.template")
        if (declared != null && declared != name) {
            throw JobConfigException("Config 'job.template' is '$declared' but was submitted to template '$name'")
        }
        val version = ConfigSupport.optionalInt(config, "job.schema-version") ?: 1
        if (version != schemaVersion) {
            throw JobConfigException(
                "Template '$name' supports schema-version $schemaVersion, config declares $version"
            )
        }
    }
}
