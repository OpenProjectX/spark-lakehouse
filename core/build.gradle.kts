plugins {
    id("buildsrc.convention.kotlin-jvm")
}

dependencies {
    api(libs.typesafeConfig)

    testImplementation(libs.junitJupiter)
    testRuntimeOnly(libs.junitPlatformLauncher)
}
