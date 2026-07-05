// Silver-layer node library: CDC merge, dedup, SCD1/SCD2, DV2.0 hub/link/sat
// builders.
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
    api(project(":catalog"))
    api(libs.sparkBootCore)
    api(libs.sparkBootRuntimeSpark)
    implementation(libs.dagger)

    compileOnly("org.apache.spark:spark-sql_2.13")
}
