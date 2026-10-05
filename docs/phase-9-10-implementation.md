# Phase 9–10 implementation and decisions

## Phase 9: infrastructure and delivery

Terraform provisions GCS Bronze/Silver buckets, three BigQuery datasets, reconciliation and
benchmark evidence tables, four workload identities, scoped bucket/dataset IAM, Secret Manager
containers, required APIs, and a cloud-resource dashboard. Production deletion protection is
forced on. There is no always-on cloud compute by default, which keeps development inexpensive and
leaves compute choice reversible.

Key tradeoffs:

- Bronze uses versioning and a longer lifecycle because raw replay is the recovery anchor; Silver
  is reproducible and receives shorter retention.
- Terraform owns stable infrastructure, the loader owns evolving Silver landing schemas, and dbt
  owns Gold. Shared table ownership was rejected because it turns legitimate schema evolution into
  perpetual Terraform drift.
- Secret resources are code; secret values are not. This avoids plaintext credentials and secret
  payloads in Terraform state.
- Service accounts are split by workload. This adds IAM resources but limits blast radius and
  keeps the dashboard read-only. The processor can create/read Bronze objects but cannot update or
  delete them; Silver remains object-admin because replay replaces reproducible outputs.
- No GKE, VM, or permanent Dataproc cluster is created. Such capacity should be added only after
  measured workload and cost justify it.

The multi-target application image gives ingestion, Bronze, Spark, and warehouse jobs the same
locked dependency base while retaining distinct commands and independently deployable images. It
runs as a non-root user under `tini`; Java 17 is present for Spark. Airflow retains its dedicated
image because its dependency constraints and lifecycle differ.

Pull requests run formatting, linting, strict typing, unit/contract/integration Spark tests, dbt
compile/build/idempotent rerun/docs, Terraform format/validate, and every Docker target build. A
tagged release reruns integration tests before publishing commit- and tag-addressed GHCR images
with provenance and SBOM. Cloud deployment is intentionally not automatic because no deployment
runtime was selected in the architecture; image publication is the safe delivery boundary.

## Phase 10: reliability and benchmark evidence

The deterministic suite injects restart, broker rejection, malformed input, additive drift,
duplicate delivery, BigQuery failure, Spark checkpoint replay, and dbt-test failure. These tests
assert data invariants and run in CI. The Docker drill utility introduces real service outages and
measures service recovery, after which reconciliation must prove row-level recovery.

The benchmark harness generates deterministic Jetstream frames without depending on the public
network. Named profiles encode the exact 30-minute sustained, 5-minute burst, 5-million replay,
and 10%-duplicate workloads. Kafka mode includes durable broker acknowledgement; memory mode is
only a parser/generator diagnostic. The BigQuery comparator uses identical parameterized queries,
cache disabled, and defaults to a cost-safe dry run.

Results are machine-written JSON with workload, method, environment, Git SHA, counts, bytes,
throughput, percentile latency, process resource use, errors, and duplicates. A target-size result
without a Git SHA is still `VALIDATION`, not `MEASURED`. Full end-to-end claims additionally require
Bronze/Silver/Gold reconciliation and Spark/runtime resource measurements.

## Remaining operational prerequisites, not placeholders

Local and live measurements are now recorded in `docs/benchmark.md`, including failed targets.
Cloud-only claims still require renewed GCP authentication: BigQuery bytes/runtime and layout
reduction, Gold freshness, deployed dashboard latency, Terraform plan/apply evidence, and scheduled
Airflow success. These remain `BLOCKED` or `NOT MEASURED`; local DuckDB and DAG-import checks are
not substitutes. This is an evidence requirement, not missing pipeline logic.
