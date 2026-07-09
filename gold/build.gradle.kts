// Gold-layer node library: Kimball dimension/fact builders and serving-store
// publish nodes.
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
