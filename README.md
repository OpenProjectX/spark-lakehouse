# Spark Lakehouse

A tenant-agnostic catalog of abstract Spark job templates implementing
medallion architecture (bronze / silver / gold). Departments do not write
Spark code: an orchestration repo (Airflow) submits a template name plus a
HOCON config, and this project's docker image runs it.

Built on [spark-platform](https://github.com/OpenProjectX/spark-platform)
(version matrix, image layers), [spark-boot](https://github.com/OpenProjectX/spark-boot)
(pipeline engine), and [bigdata-test](https://github.com/OpenProjectX/bigdata-test)
(integration-test containers).

## Job catalog

| Template | Layer | What it does |
| --- | --- | --- |
| `jdbc-snapshot-ingest` | bronze | Snapshot an RDBMS table into append-only, metadata-stamped parquet |
| `cdc-silver-merge` | silver | Resolve CDC events and `MERGE INTO` a silver Iceberg table |
| `scd2-dim-load` | gold | Maintain a Kimball SCD Type 2 dimension from a silver table |

## Quick start

```bash
./gradlew build                      # build + unit tests
./gradlew :integration-tests:test    # end-to-end tests (needs Docker)
./gradlew :app:jibDockerBuild        # app docker image
```

```bash
spark-lakehouse --job cdc-silver-merge --config /mnt/config/tenant-job.conf
```

## Documentation

- [Design](docs/design.adoc) — architecture, modules, how a job runs
- [Principles](docs/principles.adoc) — the rules that shape this project
- [Usage](docs/usage.adoc) — CLI, environment config, docker, orchestration
- Job reference — [jdbc-snapshot-ingest](docs/jobs/jdbc-snapshot-ingest.adoc),
  [cdc-silver-merge](docs/jobs/cdc-silver-merge.adoc),
  [scd2-dim-load](docs/jobs/scd2-dim-load.adoc)

## License

Apache License 2.0
