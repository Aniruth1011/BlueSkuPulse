# AtmosPulse: problem statement and Phase 1–4 implementation

## 1. Problem statement

AtmosPulse is a production-style real-time data engineering platform for public activity on the
Bluesky / AT Protocol network. Bluesky produces a continuous event stream containing repository
commits and account lifecycle events. These events include posts, likes, reposts, follows,
updates, deletes, identity changes, and account changes.

The engineering problem is not simply to read those events. AtmosPulse must turn an unreliable,
evolving, at-least-once event stream into durable and reproducible analytical data without losing
events, silently discarding malformed records, or creating duplicate canonical records.

The target end-to-end architecture is:

```text
Bluesky Jetstream
        |
        v
Asynchronous Python ingestion
        |
        v
Kafka / Redpanda
        |
        +----------------------+
        |                      |
        v                      v
Immutable Bronze         Spark Structured Streaming
raw storage                    |
        |                      v
        +--------------> Canonical Silver Parquet
                               |
                               v
                         BigQuery + dbt
                               |
                               v
                         Analytics marts
```

Phases 1–4 implement the source-through-Silver portion of this architecture. BigQuery, dbt,
analytics marts, Airflow workflows, Terraform, full monitoring dashboards, and performance
benchmarking belong to later phases.

## 2. What data engineering work are we doing?

The platform performs five fundamental data engineering jobs:

1. **Capture:** maintain a long-running connection to Jetstream and translate version-specific
   source messages into a stable AtmosPulse event contract.
2. **Buffer and decouple:** publish events to Kafka/Redpanda so source availability and downstream
   processing failures are independent.
3. **Preserve:** write immutable raw records to Bronze storage so any downstream state can be
   reconstructed later.
4. **Standardize:** use Spark to validate, deduplicate, and normalize heterogeneous events into
   analytics-ready Silver datasets.
5. **Recover:** persist cursors and checkpoints so processes can restart, replay overlap safely,
   and prove that a failure caused redelivery rather than data loss.

The permanent WebSocket process is an ingestion service. It is not an Airflow task. Airflow will
later orchestrate bounded activities such as hourly loads, reconciliation, and historical
backfills.

## 3. Correctness model

AtmosPulse assumes at-least-once delivery. A source or broker retry may therefore deliver the same
mutation more than once. The design favors safe redelivery over a cursor gap.

The important invariants are:

- Every mutation receives a deterministic `event_id` derived from stable source fields.
- A source cursor advances only after the corresponding event or DLQ record is acknowledged by
  Kafka.
- A crash after Kafka acknowledgement but before cursor persistence can cause a duplicate, but it
  cannot cause silent loss.
- Bronze records and manifests use deterministic names and create-only writes.
- Bronze intentionally preserves duplicate deliveries as operational evidence.
- Silver removes duplicate event identities and retains creates, updates, and deletes as explicit
  immutable mutations.
- Malformed records are routed to a DLQ, preserved in Bronze, or written to Silver quarantine;
  they are never silently discarded.
- Unknown additive source fields remain in `payload_json` and do not crash ingestion.
- Jetstream v1 and v2 cursors are versioned and cannot be used interchangeably.

## 4. Phase 1 — Foundation

### Goal

Establish a maintainable project foundation and prove the first vertical slice:

```text
Fixture or Jetstream -> asynchronous Python -> Kafka/Redpanda
```

### Implemented components

#### Project and configuration

- Python project configuration and locked dependencies using `pyproject.toml`, `uv`, and
  `uv.lock`.
- Environment-driven settings with safe local defaults.
- `.env.example` covering sources, Kafka, metrics, Bronze, and Spark.
- Make targets for installation, validation, infrastructure startup, ingestion, Bronze, and Spark.
- `.gitignore` for secrets, virtual environments, generated data, checkpoints, and local state.

#### Stable event contract

The `EventEnvelope` gives downstream systems one stable schema regardless of source adapter. It
contains:

- deterministic event ID;
- source version and source host;
- version-specific cursor value;
- event kind and operation;
- DID, collection, record key, AT URI, and CID where applicable;
- source event time and ingestion time;
- complete canonical source JSON in `payload_json`;
- AtmosPulse schema version.

The raw payload is retained because the stable envelope cannot anticipate every future additive
source field.

#### Source abstraction

All sources implement one asynchronous interface:

- `JetstreamV1Source` supports the legacy `/subscribe` format and Unix-microsecond `time_us`
  cursors.
- `JetstreamV2Source` supports `network.bsky.jetstream.subscribeEvents`, the `xrpc.v1.json`
  subprotocol, and inclusive monotonic `seq` cursors.
- `FixtureSource` replays deterministic JSONL for offline and repeatable tests.

The fixture is a first-class source rather than a mock hidden inside tests. This makes recovery,
duplicate, schema-evolution, and replay scenarios reproducible without relying on the live Bluesky
network.

#### Redpanda development environment

Docker Compose starts:

- a single-node Redpanda broker;
- deterministic topic initialization;
- Redpanda Console for local inspection.

Redpanda is Kafka-protocol compatible and provides a lighter local environment. Production-facing
code uses Kafka APIs, so the design is not coupled to Redpanda-specific client behavior.

### Phase 1 tradeoffs

| Decision | Benefit | Cost |
|---|---|---|
| Stable envelope plus raw JSON | Downstream schema stability and future recovery | Stores some data twice |
| Source interface with three adapters | Version isolation and deterministic testing | More adapter code |
| Redpanda locally | Fast, lightweight Kafka-compatible development | Production Kafka differences still require integration testing |
| File cursor store initially | Transparent and easy to inspect | Single-process ownership only; distributed coordination comes later |

## 5. Phase 2 — Reliable ingestion

### Goal

Make the first vertical slice safe under disconnects, retries, malformed input, and process
restarts.

### Durability boundary

The Kafka acknowledgement is the ingestion durability boundary:

```text
receive source frame
       |
       v
parse and validate
       |
       +-- invalid --> publish DLQ --+
       |                             |
       +-- valid ----> publish raw --+
                                     |
                             wait for Kafka ack
                                     |
                                     v
                             persist source cursor
```

The cursor never advances first. If publishing fails, the process raises an error and leaves the
cursor unchanged. If the process crashes after acknowledgement but before cursor persistence, the
source may resend the event, which is safe because its identity is deterministic.

### Reconnection and cursor recovery

- WebSocket connections reconnect indefinitely.
- Backoff is exponential, capped, and randomized with jitter.
- Idle timeouts and WebSocket heartbeats detect dead connections.
- V1 reconnects with a two-second cursor rewind, following its time-based replay model.
- V2 reconnects with the last durable inclusive sequence, intentionally replaying that event.
- Cursor files include cursor kind and source host.
- A version or host mismatch fails closed instead of resetting to the live stream tip.

### Kafka publishing

- `acks=all` waits for broker durability acknowledgement.
- Idempotent producer mode is enabled.
- The DID is used as the Kafka key to preserve per-actor ordering within a partition.
- Event ID and source version are included as Kafka headers.
- Source processing remains sequential across the durability boundary, keeping cursor ownership
  simple and auditable.

### DLQ behavior

Parse or contract failures are published to `bluesky.dlq` with:

- source version and host;
- best-effort cursor where recoverable;
- failure reason;
- original raw text;
- failure timestamp.

A cursor may advance after the DLQ acknowledgement because the problematic bytes have crossed a
durability boundary and can be repaired and replayed later.

### Phase 2 logging and metrics

- `DEBUG`: parsed event, Kafka acknowledgement, durable cursor movement.
- `INFO`: ingestion startup, resume cursor, shutdown, and run summary.
- `WARNING`: source advisories, malformed source data, reconnect attempts, and acknowledged DLQ
  records.
- `ERROR`: Kafka or DLQ publishing failures with stack traces.

Metrics include events received, events acknowledged, DLQ count, producer errors, ingestion lag,
reconnect attempts, and durable checkpoint age.

### Phase 2 tradeoffs

| Decision | Benefit | Cost |
|---|---|---|
| Prefer overlap/redelivery | Prevents cursor gaps and silent loss | Downstream deduplication is mandatory |
| Acknowledge sequentially | Clear durability and cursor semantics | Lower peak producer throughput |
| Unlimited reconnect attempts with capped delay | Temporary outages recover automatically | Permanent outages require alerting and operator action |
| Durable DLQ before cursor advance | Bad data does not block the stream or disappear | Requires repair and replay tooling |

## 6. Phase 3 — Immutable Bronze storage

### Goal

Create a durable, replayable raw history independently of Kafka retention.

### Bronze write flow

```text
Kafka consumer with auto-commit disabled
                |
                v
buffer records by count or time
                |
                v
group by event date / hour / collection
                |
                v
write deterministic gzip JSONL objects
                |
                v
write deterministic manifest
                |
                v
commit Kafka consumer offsets
```

No Kafka offset is committed before all data objects and the manifest are durable.

### File layout

```text
bronze/
└── bluesky/
    ├── event_date=YYYY-MM-DD/
    │   └── event_hour=HH/
    │       └── collection=<collection>/
    │           └── batch-<deterministic-id>.jsonl.gz
    └── _manifests/
        └── batch-<deterministic-id>.json
```

Batch IDs are computed from the sorted Kafka topic, partition, and offset coordinates. A retry of
the same Kafka batch therefore produces the same object names and content.

Each manifest records:

- schema version and batch ID;
- record count and compressed bytes;
- first and last offsets per topic-partition;
- object paths and SHA-256 digests;
- manifest creation time.

### Immutability

- Local development uses a temporary file, `fsync`, and atomic hard-link creation.
- GCS uses `if_generation_match=0` with CRC32C checking.
- Existing object keys are never overwritten.
- Data objects are written before the manifest.

A failure between data and manifest creation can leave an orphan object. That object is harmless:
the deterministic retry reuses the same name and completes the manifest.

### Malformed Kafka values

If a Kafka value is not a valid event envelope, Bronze still writes it. The record contains:

- SHA-256 identity derived from the original bytes;
- exact original bytes encoded as base64;
- parse error;
- Kafka topic, partition, offset, key, and timestamp;
- schema version `0` and the `$malformed` partition.

### Phase 3 logging and metrics

- Batch commits log batch ID, offsets, counts, object count, bytes, malformed count, and whether the
  write was a replay.
- Malformed records produce warnings with Kafka coordinates, not the entire user payload.
- Consumer failures produce error logs and stack traces.
- Metrics track Bronze records, files, compressed bytes, batch duration, and freshness.

### Phase 3 tradeoffs

| Decision | Benefit | Cost |
|---|---|---|
| Gzip JSONL Bronze | Faithful, debuggable, replayable raw history | Less scan-efficient than Parquet |
| Buffered files | Avoids one object per event | A small amount of data waits in memory before flushing |
| Retain duplicates | Preserves source-delivery evidence | Bronze storage is larger |
| Date/hour/collection partitions | Selective replay and easier investigation | Rare collections can produce small files |
| Manifest after data | Manifest represents complete batches | Crash can temporarily leave an unreferenced object |

## 7. Phase 4 — Spark normalization and Silver Parquet

### Goal

Convert the heterogeneous event stream into validated, deduplicated, analytics-ready canonical
datasets while using the same business transformations for live processing and replay.

### Shared transformations

Transformation functions accept and return Spark DataFrames. They do not contain source or sink
management. Both modes reuse them:

- continuous Kafka Structured Streaming;
- bounded Bronze replay.

This prevents the live and backfill paths from developing different business rules.

### Explicit schemas and evolution

Spark uses an explicit stable envelope schema instead of inferring every micro-batch. Nested record
schemas are defined for posts, engagement subjects, and follows.

Unknown additive fields do not break processing because the full source object remains in
`payload_json`. A newly promoted analytical field requires an intentional Silver schema change.

### Canonical outputs

The Spark job writes independent Parquet datasets for:

- `events`: canonical immutable mutation history;
- `posts`: text, languages, reply root/parent, external URL, quote URI, embed and media flags;
- `engagements`: likes and reposts with actor and subject identity;
- `graph_edges`: follows between actor and subject DIDs;
- `identity`: identity, account, and sync lifecycle events;
- `quarantine`: invalid canonical envelopes with quality errors.

### Updates and deletes

Silver does not overwrite a previous record mutation. Creates, updates, and deletes remain
explicit immutable rows. Current state is derived deterministically by selecting the latest
`(event_time, cursor)` mutation for each AT URI. If the latest mutation is a delete, the derived
record has `is_deleted=true`.

This mutation-history approach supports audits and reproducible reconstruction. BigQuery/dbt will
later materialize consumer-facing current-state tables.

### Deduplication

- Live streaming uses `event_id` deduplication with a 48-hour event-time watermark.
- The watermark bounds the size of Spark state.
- Historical Bronze replay has no live watermark and performs unbounded event-ID deduplication.
- Events too old for the live recovery window are recovered through Bronze, not by silently
  relaxing live state limits.

### Checkpointing and storage

- Every streaming output has an independent checkpoint directory.
- Streaming Parquet writes are append-only and checkpointed.
- Silver is partitioned primarily by event date, with relevant secondary partitions.
- Batch replay writes to a separately selected rebuild target.
- Production promotion should use a new versioned GCS prefix and controlled pointer switch rather
  than overwriting the active dataset.

### Data quality

Required checks cover:

- event ID;
- valid DID shape;
- event timestamp;
- supported event kinds;
- valid commit operation;
- collection, record key, and AT URI for commits.

Failures are written to quarantine with all envelope fields, a list of failed checks, and the
quarantine time.

### Phase 4 logging and metrics

Spark logs query start, progress, state-operator information, clean termination, and failures.
Metrics expose:

- input and processed rows per second;
- micro-batch duration;
- state-store row count;
- duplicate rows dropped;
- event-time lag;
- quarantined rows;
- query failures.

### Phase 4 tradeoffs

| Decision | Benefit | Cost |
|---|---|---|
| Plain Parquet | Interoperable and matches the required architecture | No transactional `MERGE` |
| Immutable Silver mutations | Auditable create/update/delete history | Current state must be materialized downstream |
| 48-hour live watermark | Bounded state and stable streaming operation | Very old events require Bronze replay |
| Independent output queries/checkpoints | Failure isolation and clear recovery | Kafka is read separately for each output |
| Shared DataFrame transformations | Live/replay semantic consistency | Input/output wrappers remain mode-specific |
| Explicit schemas | Predictable contracts and safer evolution | New analytical columns require deliberate schema updates |

## 8. End-to-end data flow implemented through Phase 4

```text
Jetstream v1 / Jetstream v2 / Fixture
                  |
         parse stable envelope
                  |
       valid -----+----- malformed
         |                    |
         v                    v
bluesky.raw.events       bluesky.dlq
         |
         +---- Kafka acknowledgement ----> durable source cursor
         |
         +---- Bronze consumer
         |          |
         |          v
         |   immutable gzip JSONL
         |   + deterministic manifest
         |
         +---- Spark Structured Streaming
                    |
                    v
          validate and quarantine
                    |
          event-ID deduplication
                    |
                    v
       canonical Silver Parquet datasets

Bronze archive ----> bounded Spark replay ----> versioned Silver rebuild
```

## 9. Testing and validation completed

The Phase 1–4 suite covers:

- deterministic v1 and v2 parsing;
- incompatible cursor rejection;
- cursor advancement only after acknowledgement;
- broker failure without cursor advancement;
- durable DLQ behavior;
- forced WebSocket disconnect, reconnect, and inclusive replay;
- fixture cursor resume;
- deterministic Bronze file and manifest generation;
- Bronze replay idempotency;
- local create-only storage;
- exact malformed-byte preservation;
- duplicate Silver event elimination;
- additive source-field tolerance;
- invalid required-field quarantine;
- post, engagement, graph, and identity normalization;
- replies, external links, quotes, and media flags;
- create/update/delete current-state folding;
- Bronze-to-Silver bounded replay;
- streaming and batch canonical equivalence;
- streaming checkpoint restart and redelivery deduplication;
- Kafka producer success and failure paths;
- structured logging behavior.

Validation results at implementation time:

- 30 tests passing;
- Ruff lint and formatting checks passing;
- strict mypy checks passing;
- ingestion coverage: 84.21%;
- combined core Bronze/transformation coverage: 88.65%;
- transformation module coverage: 100%.

A local live smoke test also exercised:

```text
Fixture -> Redpanda -> Bronze -> bounded Spark Silver
Fixture -> Redpanda -> continuous Kafka/Spark -> checkpointed Silver
```

The streaming smoke test intentionally delivered four event identities twice. Spark retained four
canonical identities and reported four duplicate deliveries. This is correctness evidence, not a
throughput or latency benchmark.

No performance SLO is marked achieved. Throughput, latency, recovery time, resource usage, and
BigQuery scan-reduction values remain `TARGET` until the formal benchmark phase records the
dataset, environment, Git commit, and measurement method.

## 10. Main implementation locations

| Area | Location |
|---|---|
| Configuration and logging | `ingestion/config.py`, `ingestion/logging.py` |
| Event contract | `ingestion/models/events.py` |
| Source adapters | `ingestion/sources/` |
| Cursor storage | `ingestion/cursor_store.py` |
| Kafka ingestion and durability | `ingestion/producer.py` |
| Bronze storage abstraction | `bronze/storage.py` |
| Bronze batching and manifests | `bronze/writer.py` |
| Bronze Kafka consumer | `bronze/consumer.py` |
| Spark schemas | `streaming/schemas/events.py` |
| Canonical transformations | `streaming/jobs/transformations.py` |
| Streaming and replay jobs | `streaming/jobs/normalize_events.py` |
| Spark operational metrics | `streaming/observability.py` |
| Tests | `tests/unit/`, `tests/integration/`, `tests/fixtures/` |
| Detailed decisions | `docs/adr/`, `docs/phase-2-4-decisions.md` |
| Operations | `docs/operations/phase-2-4-runbook.md` |

## 11. What remains after Phase 4

Phase 5 has since been implemented; see `docs/phase-5-warehouse.md`. The list below describes the
roadmap as it stood at the Phase 4 boundary.

The implemented source-to-Silver platform is the foundation for the remaining work:

1. **Phase 5:** BigQuery loading and dbt staging, dimensions, facts, marts, tests, lineage, and
   documentation.
2. **Phase 6:** network, topic, domain, creator, engagement, conversation, and pipeline-health
   analytical products.
3. **Phase 7:** Airflow orchestration for bounded hourly workflows, reconciliation, backfill, and
   optional compaction.
4. **Phase 8:** operational dashboards and alerts covering the complete platform.
5. **Phase 9:** Terraform, deployable images, CI/CD, secret handling, and infrastructure
   validation.
6. **Phase 10:** controlled failure injection and reproducible sustained, burst, replay, duplicate,
   recovery, resource, and BigQuery layout benchmarks.

SQLite and generic database-query tools are intentionally not in the production data path. They
do not solve a required platform problem here: Bronze needs durable object storage, Spark needs
distributed replay and normalization, and analytical serving is assigned to BigQuery and dbt.
