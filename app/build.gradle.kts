plugins {
    application
    id("buildsrc.convention.kotlin-jvm")
    kotlin("kapt")
    id("org.openprojectx.spark.platform")
}

sparkPlatform {
    line.set("spark4")
    // Published platform image profile: all variants + hadoopAws/hadoopGcs/icebergAws.
    profile.set("lakehouse")
    platformVersion.set("0.1.42")
    managedConfigurations.set(listOf("compileOnly", "testImplementation", "testRuntimeOnly"))
}

dependencies {
    api(project(":core"))
    api(project(":ingestion"))
    api(project(":silver"))
    api(project(":gold"))
    api(project(":jobs"))
    api(libs.sparkBootDagger)
    implementation(libs.typesafeConfig)
    implementation(libs.dagger)
    kapt(libs.daggerCompiler)

    // JDBC drivers are application-owned: they ship in the app image layer,
    // not the platform image.
    implementation(libs.postgresqlDriver)

    compileOnly("org.apache.spark:spark-sql_2.13")

    testImplementation(libs.junitJupiter)
    testRuntimeOnly(libs.junitPlatformLauncher)
    testImplementation("org.apache.spark:spark-sql_2.13")
}

application {
    mainClass.set("org.openprojectx.spark.lakehouse.app.LakehouseCliKt")
}
