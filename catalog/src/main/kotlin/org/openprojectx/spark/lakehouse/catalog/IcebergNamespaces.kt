package org.openprojectx.spark.lakehouse.catalog

import org.apache.spark.sql.SparkSession

/**
 * Namespace bootstrap for tenant tables. Tenant namespaces
 * (`<tenant>_<layer>`) are created on demand so provisioning a tenant needs
 * no manual catalog step.
 */
object IcebergNamespaces {

    /** Ensures the namespace of a `catalog.namespace.table` identifier exists. */
    fun ensureNamespaceFor(spark: SparkSession, tableIdentifier: String) {
        val parts = tableIdentifier.split(".")
        if (parts.size >= 3) {
            val namespace = parts.dropLast(1).joinToString(".")
            spark.sql("CREATE NAMESPACE IF NOT EXISTS $namespace")
        }
    }
}
