// Catalog interactions: Iceberg/HMS namespace bootstrap, table-property
// conventions, and catalog-table nodes shared by silver and gold.
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
    api(libs.sparkBootCore)
    api(libs.sparkBootRuntimeSpark)
    implementation(libs.dagger)

    compileOnly("org.apache.spark:spark-sql_2.13")
}
