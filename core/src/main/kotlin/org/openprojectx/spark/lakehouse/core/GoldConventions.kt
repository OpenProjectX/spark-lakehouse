package org.openprojectx.spark.lakehouse.core

/**
 * History-tracking columns every SCD2 dimension carries. The surrogate key is
 * a deterministic hash of the natural key and the version's effective-from
 * timestamp, so reloads produce stable keys.
 */
object Scd2Columns {
    const val DIM_KEY = "_dim_key"
    const val EFFECTIVE_FROM = "_scd_effective_from"
    const val EFFECTIVE_TO = "_scd_effective_to"
    const val CURRENT = "_scd_current"
}
