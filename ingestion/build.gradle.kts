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

    testImplementation(libs.junitJupiter)
    testRuntimeOnly(libs.junitPlatformLauncher)
    testImplementation("org.apache.spark:spark-sql_2.13")
}
