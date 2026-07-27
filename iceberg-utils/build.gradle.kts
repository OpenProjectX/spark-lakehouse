// Spark-free Iceberg utilities (zero-copy append). Depends only on the
// Iceberg table API so it can be reused outside spark-boot flows; consumers
// bring their own Iceberg runtime (the platform image ships
// iceberg-spark-runtime, which contains these classes unshaded).
plugins {
    id("buildsrc.convention.kotlin-jvm")
}

dependencies {
    compileOnly(libs.icebergApi)
    compileOnly(libs.icebergCore)

    testImplementation(libs.icebergApi)
    testImplementation(libs.icebergCore)
    testImplementation(libs.hadoopCommon)
    testImplementation(libs.junitJupiter)
    testRuntimeOnly(libs.junitPlatformLauncher)
}
