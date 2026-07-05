// Governance: OpenLineage wiring, data-contract validation, schema-evolution
// policy. Skeleton — populated once the first quality gate lands.
plugins {
    id("buildsrc.convention.kotlin-jvm")
}

dependencies {
    api(project(":core"))
}
