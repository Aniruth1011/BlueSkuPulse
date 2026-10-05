# ADR 0004: Checkpointed Silver event history plus deterministic batch rebuild

Status: accepted

## Decision

Spark uses explicit schemas and common transformation functions for streaming and bounded Bronze
replay. Streaming writes canonical mutation history to partitioned Parquet with independent durable
checkpoints. It deduplicates deterministic event IDs within a 48-hour event-time watermark,
covering the documented live Jetstream replay window plus margin. Long-range replay uses the batch
path, which performs unbounded event-ID deduplication and overwrites a separately selected rebuild
target.

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
- Independent output queries isolate failures and checkpoints but reread Kafka for each dataset.
  This costs more broker bandwidth than a single multi-sink `foreachBatch`; the latter only offers
  at-least-once custom-sink semantics and would require a separate transactional batch ledger.
- Full overwrite is safe only for a new/rebuild target. Production promotion will use versioned
  GCS prefixes and a controlled manifest pointer, never in-place mutation of the active dataset.

## Schema decisions

- Posts retain replies, quotes, embed type, external URL, text, and language arrays.
- Likes and reposts share a canonical engagement shape.
- Follows become graph edges.
- Identity, account, and sync lifecycle events remain queryable rather than being dropped.
- Invalid required fields go to Silver quarantine with all original envelope fields.

