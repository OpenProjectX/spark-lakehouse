// Catalog bootstrap: Iceberg/HMS namespace + table-property conventions per
// tenant namespace. Skeleton — populated with the first Iceberg silver job.
plugins {
    id("buildsrc.convention.kotlin-jvm")
}

dependencies {
    api(project(":core"))
}
