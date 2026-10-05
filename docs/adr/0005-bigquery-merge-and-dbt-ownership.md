# ADR 0005: BigQuery event-id merges and dbt-owned analytics

Status: accepted

## Decision

Silver Parquet is loaded into deterministic BigQuery temporary tables. Each load has a job ID
derived from the dataset name and sorted GCS URIs. The loader creates a native destination table
partitioned by `event_date`, clustered by its common filter keys, synchronizes additive top-level
columns, and performs an insert-only `MERGE` on `event_id`. The merge restricts the target scan to
the incoming event-date range. The temporary table is deleted after either success or failure.

dbt owns all warehouse transformations after the Silver landing tables: deduplicated staging
views, latest-mutation intermediate state, dimensions, facts, marts, tests, descriptions, and
lineage. Large current-state, fact, and mart models use incremental materializations with stable
unique keys. Dashboard SQL must remain thin.

## Tradeoffs

- Batch-loading Parquet is simpler and cheaper than row-by-row warehouse streaming, but warehouse
  freshness depends on the bounded load schedule.
- An insert-only event-history merge prevents replay duplication. Mutated logical records are not
  updated in landing tables because each source mutation has its own `event_id`; current-state dbt
  models fold mutations by AT URI.
- Requiring partition filters on Silver landing tables protects users from accidental full scans,
  but every maintenance query must supply a usable event-date predicate.
- Top-level additive columns are appended automatically. Nested record evolution requires an
  explicit migration because silent nested schema rewriting is harder to review safely.
- DuckDB provides deterministic local dbt execution without GCP credentials. BigQuery remains the
  production SQL and physical-layout target; a cloud smoke test requires a user-supplied GCP
  project and Application Default Credentials.
- Dimensions are rebuilt as tables because the entity sets are comparatively small. Mutation
  state, facts, and marts are incremental because those relations grow with event history.

## Authoritative references

- [Load Parquet from Cloud Storage](https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-parquet)
- [BigQuery DML and MERGE](https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax)
- [BigQuery partitioned tables](https://cloud.google.com/bigquery/docs/partitioned-tables)
- [dbt incremental models](https://docs.getdbt.com/docs/build/incremental-models)
- [dbt BigQuery configurations](https://docs.getdbt.com/reference/resource-configs/bigquery-configs)
- [dbt source freshness](https://docs.getdbt.com/docs/deploy/source-freshness)
