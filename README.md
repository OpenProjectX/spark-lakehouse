# Spark Lakehouse

A common, tenant-agnostic catalog of abstract Spark job templates implementing
medallion architecture (bronze / silver / gold) and standard data-model load
patterns. Departments do not write Spark code: an orchestration repo (Airflow)
submits a job template name plus a HOCON config, and this project's docker
image runs it.

Built on the OpenProjectX stack:

| Layer | Project | Role |
| --- | --- | --- |
| Build/runtime contract | [spark-platform](https://github.com/OpenProjectX/spark-platform) | Spark/Hadoop/Iceberg version matrix, base + platform docker image layers, app image build (Jib) |
| Pipeline engine | [spark-boot](https://github.com/OpenProjectX/spark-boot) | Flow/Node/Edge model, Kotlin + HOCON DSL, Dagger node factories, Spark 4 runtime |
| Test infrastructure | [bigdata-test](https://github.com/OpenProjectX/bigdata-test) | Testcontainers fixtures (S3, HMS, Kafka, …) |

## Design rules

- **No tenant data in this repo.** Multi-tenancy is a code contract
  (`TenantContext`, storage-layout and namespace conventions) resolved from the
  submitted config at runtime. Tests use synthetic tenants only.
- **Abstract the load patterns, not the model designs.** Templates implement
  the mechanical parts (snapshot ingest, CDC merge, SCD2, DV2.0 hub/link/sat
  loads); business decisions (keys, grain, transform SQL) arrive as config.
- **The config schema is the contract.** Every template declares a
  `schema-version` and fails fast with actionable messages, because tenant
  configs live in the orchestration repo and evolve independently.
- **The docker image is the deliverable.** One image, job selected by config.

## Modules

| Module | Purpose |
| --- | --- |
| `core` | Tenant/layer contracts, naming conventions, config validation. Spark-free. |
| `ingestion` | Bronze node library (`BronzeSnapshotSink`, …) contributed to spark-boot registries. |
| `silver` | CDC merge, dedup, SCD, DV2.0 builders (skeleton). |
| `gold` | Dimensional builders and serving-store publish nodes (skeleton). |
| `catalog` | Iceberg/HMS namespace + table-property conventions (skeleton). |
| `governance` | Lineage, data contracts, quality gates (skeleton). |
| `jobs` | The job catalog: `JobTemplate` SPI + templates. Spark-free, unit-testable. |
| `app` | Dagger component, CLI entrypoint, and the docker image build. |
| `integration-tests` | End-to-end tests against containers. |

## Job catalog

| Template | Schema | What it does |
| --- | --- | --- |
| `jdbc-snapshot-ingest` | v1 | Snapshot one RDBMS table into the tenant's bronze layer as append-only, metadata-stamped parquet partitioned by `_snapshot_date`. |

Submitted config shape (owned by the orchestration repo):

```hocon
job    { template = "jdbc-snapshot-ingest", schema-version = 1, name = "acme-orders" }
tenant { id = "acme", storage-root = "s3a://lake/acme" }
source {
  table = "public.orders"
  # either a named connection from spark.boot.jdbc.connections, or inline:
  url = "jdbc:postgresql://…", user = "…", password = "…", driver = "org.postgresql.Driver"
}
target { table = "orders", snapshot-date = "2026-07-05", partition-by = [] }
```

Bronze rows are stamped with `_lake_tenant`, `_lake_source`, `_lake_ingested_at`,
and `_snapshot_date`; the layout is `<storage-root>/<layer>/<table>`.

Environment (S3 endpoints, HMS, named JDBC connections, Iceberg catalogs) uses
spark-boot's starter config under `spark.boot { }` — see the spark-boot README.

## Running

```bash
# build + unit tests
./gradlew build

# integration tests (needs Docker)
./gradlew :integration-tests:test

# app docker image (layers on ghcr.io/openprojectx/spark-platform:spark4-lakehouse-<ver>;
# pull or build that base image into the local docker daemon first)
./gradlew :app:jibDockerBuild
```

Running the image (what the orchestration repo submits — the Spark Operator
entrypoint does not forward `SPARK_EXTRA_CLASSPATH` to the driver, so
`spark.driver.extraClassPath` must be set explicitly):

```bash
docker run --rm -e SPARK_DRIVER_BIND_ADDRESS=0.0.0.0 \
  org.openprojectx.spark.lakehouse.core/app:0.1.0-snapshot \
  driver --master 'local[*]' \
  --conf spark.driver.host=127.0.0.1 \
  --conf 'spark.driver.extraClassPath=/opt/spark/app/resources:/opt/spark/app/classes:/opt/spark/app/libs/*' \
  --class org.openprojectx.spark.lakehouse.app.LakehouseCliKt \
  local:///opt/spark/app/app.jar \
  --job jdbc-snapshot-ingest --config /mnt/config/tenant-job.conf
```
