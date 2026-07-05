package org.openprojectx.spark.lakehouse.core

/**
 * Raised when a submitted job configuration does not satisfy a job template's
 * schema. The message must be actionable on its own: tenant configs live in a
 * separate orchestration repo, so this text is often the only feedback loop.
 */
class JobConfigException(message: String) : RuntimeException(message)
