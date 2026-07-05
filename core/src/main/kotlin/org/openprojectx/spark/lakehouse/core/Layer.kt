package org.openprojectx.spark.lakehouse.core

/**
 * Medallion layers. Bronze is source-faithful and append-only, silver is the
 * business-detail truth (CDC-resolved, deduplicated), gold serves marts and
 * aggregates.
 */
enum class Layer(val id: String) {
    BRONZE("bronze"),
    SILVER("silver"),
    GOLD("gold"),
}
