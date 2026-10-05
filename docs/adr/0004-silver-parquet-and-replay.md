# ADR 0004: Checkpointed Silver event history plus deterministic batch rebuild

Status: accepted

## Decision

Spark uses explicit schemas and common transformation functions for streaming and bounded Bronze
replay. Streaming writes canonical mutation history to partitioned Parquet with independent durable
checkpoints. It deduplicates deterministic event IDs within a 48-hour event-time watermark,
covering the documented live Jetstream replay window plus margin. Long-range replay uses the batch
path, which performs unbounded event-ID deduplication and overwrites a separately selected rebuild
target.

Live deduplication uses Spark's RocksDB state-store provider with bounded native memory and a
configurable Kafka `maxOffsetsPerTrigger`. The disk-backed store is required because a replay
window can contain millions of event IDs; the default HDFS-backed state store retains state objects
on the JVM heap and failed the measured 9.3-million-delivery local workload. Existing checkpoints
must not be reused when changing state-store providers, so this migration uses a new checkpoint
prefix while retaining the failed checkpoint as diagnostic evidence.

Creates, updates, and deletes remain explicit mutations in Silver. Current state is derived by the
latest `(event_time, cursor)` mutation per AT URI; a latest delete produces `is_deleted=true`.

## Tradeoffs

- Plain Parquet is interoperable and matches the architecture, but it has no transactional `MERGE`.
  Therefore streaming Silver is an immutable canonical mutation log, while current-state folding
  happens in bounded rebuild/warehouse MERGE jobs. Adding Delta/Iceberg solely for convenient
  upserts would add a platform not required by the source-of-truth design.
- Watermarking bounds state but can exclude extremely late live events. The 48-hour value is tied
  to the source's live recovery envelope; historical recovery must use Bronze batch replay, which
  has no watermark.
- RocksDB adds local-disk I/O and native-memory tuning, but avoids making the JVM heap the capacity
  limit for millions of deduplication keys. Kafka input is capped per trigger to bound recovery
  micro-batches; excess load remains durably buffered in Redpanda and is observable as lag. The
  local Compose profile gives the Spark driver a 2 GiB heap and therefore requires an 8 GiB Colima
  VM when it runs beside Redpanda; managed deployments must size this from measured state and load.
- Independent output queries isolate failures and checkpoints but reread Kafka for each dataset.
  This costs more broker bandwidth than a single multi-sink `foreachBatch`; the latter only offers
  at-least-once custom-sink semantics and would require a separate transactional batch ledger.
- Full overwrite is safe only for a new/rebuild target. Production promotion will use versioned
  GCS prefixes and a controlled manifest pointer, never in-place mutation of the active dataset.
- Spark's file-sink progress reports source input rows, not filtered output rows. Operational
  telemetry must not label that value as quarantine output. Exact quarantine and canonical counts
  therefore come from the bounded Parquet verifier/reconciliation job; switching quarantine to a
  custom `foreachBatch` counter was rejected because it would weaken the file sink's commit log.

## Schema decisions

- Posts retain replies, quotes, embed type, external URL, text, and language arrays.
- Likes and reposts share a canonical engagement shape.
- Follows become graph edges.
- Identity, account, and sync lifecycle events remain queryable rather than being dropped.
- Invalid required fields go to Silver quarantine with all original envelope fields.
