// The job SPI: template interface, abstract contract, and catalog. Spark-free
// so orchestration-side tooling can validate configs against it.
plugins {
    id("buildsrc.convention.kotlin-jvm")
}

dependencies {
    api(project(":core"))
    api(libs.sparkBootCore)
    implementation(libs.typesafeConfig)

    testImplementation(libs.junitJupiter)
    testRuntimeOnly(libs.junitPlatformLauncher)
}
