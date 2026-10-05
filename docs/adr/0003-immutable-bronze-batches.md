# ADR 0003: Bronze uses deterministic immutable micro-batches

Status: accepted

## Decision

The Bronze consumer buffers Kafka records, groups them by event date, event hour, and collection,
and writes compressed JSONL objects. A deterministic batch ID is computed from Kafka
topic/partition/offset coordinates. Data objects are created before a manifest; consumer offsets
are committed only after all objects and the manifest are durable.

Local development uses atomic hard-link creation. GCS uses `if_generation_match=0`, so retries
cannot replace an existing object. Every manifest records counts, offset ranges, compressed size,
object keys, and SHA-256 digests.

## Tradeoffs

- JSONL retains the source-shaped envelope and is easy to replay, but scans less efficiently than
  Parquet. Bronze optimizes durability and auditability; Silver optimizes analytics.
- Bronze intentionally retains duplicate deliveries. Removing them here would destroy evidence
  needed to measure source behavior and debug recovery.
- Partitioning by collection accelerates selective replay but can create small files for rare
  collections. Buffered multi-record files and later compaction bound this cost.
- Manifest-after-data can leave unreferenced data if a process dies between writes. Deterministic
  names make the next retry finish the same manifest without duplication.
- Malformed Kafka values are stored under `$malformed` with their exact bytes base64-encoded. The
  sink does not silently discard data it cannot parse.

