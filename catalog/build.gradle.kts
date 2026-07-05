// Catalog bootstrap: Iceberg/HMS namespace + table-property conventions per
// tenant namespace.
plugins {
    id("buildsrc.convention.kotlin-jvm")
    id("org.openprojectx.spark.platform")
}

sparkPlatform {
    line.set("spark4")
    managedConfigurations.set(listOf("compileOnly", "testImplementation", "testRuntimeOnly"))
}

dependencies {
    api(project(":core"))

    compileOnly("org.apache.spark:spark-sql_2.13")
}
