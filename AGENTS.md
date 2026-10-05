# AGENTS.md --- AtmosPulse: Production-Grade Bluesky Data Engineering Platform

## 0. Mission

Build **AtmosPulse**, a production-style data platform for the
Bluesky/AT Protocol ecosystem.

This is not an API-demo project. The repository must demonstrate the
engineering concerns expected in a real data engineering system:

-   continuous event ingestion;
-   durable raw storage;
-   at-least-once processing with idempotent downstream writes;
-   cursor/checkpoint recovery after disconnects;
-   replay/backfill;
-   schema evolution;
-   delete/update handling;
-   streaming + batch processing;
-   data quality and reconciliation;
-   dimensional/analytics modeling;
-   orchestration;
-   observability;
-   infrastructure as code;
-   CI/CD;
-   cost-aware warehouse design;
-   measurable SLOs and benchmark results.

The finished project should answer:

> **What is happening on Bluesky, how is activity changing over time,
> what topics/links/accounts drive engagement, and can the pipeline
> deliver those answers reliably under real streaming conditions?**

The engineering system is the product. The dashboard is only a consumer.

------------------------------------------------------------------------

## 1. Source-of-truth assumptions

### Bluesky / AT Protocol

Primary source: - Bluesky/AT Protocol **Jetstream** event stream. -
Subscribe primarily to: - `app.bsky.feed.post` - `app.bsky.feed.like` -
`app.bsky.feed.repost` - `app.bsky.graph.follow` - Also retain
identity/account lifecycle events when supplied by the selected
Jetstream wire version.

Secondary source: - Bluesky public AppView HTTP API for selective
enrichment, reconciliation, and debugging. - Public `app.bsky.*` GET
endpoints can generally be queried through the public AppView without
authentication.

Do **not** design the primary pipeline as repeated REST polling.
Jetstream is the streaming source.

### Jetstream compatibility rule

The codebase must isolate the Jetstream wire implementation behind a
source adapter.

Implement: - `JetstreamV1Source` --- compatible with currently public
legacy Jetstream hosts. - `JetstreamV2Source` --- archive/replay/live
implementation when a compatible v2 service is available. -
`FixtureSource` --- deterministic local source for tests.

Never assume cursors are interchangeable between Jetstream versions.

### Delivery semantics

Assume source delivery can be **at least once**.

Therefore: - raw landing may contain duplicate deliveries; -
Silver/current-state tables must be idempotent; - every event needs a
deterministic event identity; - cursor advancement must occur only after
the event has crossed the chosen durability boundary; - reconnect must
resume from the persisted cursor with a safety overlap where required; -
delete/update/account lifecycle events must not be ignored.

------------------------------------------------------------------------

## 2. Architecture

``` text
                         ┌─────────────────────────────┐
                         │ Bluesky / AT Protocol       │
                         │ Jetstream WebSocket         │
                         └──────────────┬──────────────┘
                                        │
                                        ▼
                         ┌─────────────────────────────┐
                         │ Python async Ingestion      │
                         │ source adapter + validation │
                         └──────────────┬──────────────┘
                                        │
                                        ▼
                         ┌─────────────────────────────┐
                         │ Kafka / Redpanda            │
                         │ durable event buffer        │
                         └───────┬─────────────┬───────┘
                                 │             │
                      raw sink   │             │ streaming consumer
                                 ▼             ▼
                    ┌─────────────────┐  ┌──────────────────────┐
                    │ GCS Bronze      │  │ Spark Structured     │
                    │ immutable JSON  │  │ Streaming / PySpark  │
                    └────────┬────────┘  └──────────┬───────────┘
                             │                      │
                             │ batch/replay         ▼
                             │             ┌──────────────────────┐
                             └────────────►│ GCS Silver Parquet   │
                                           │ canonical events     │
                                           └──────────┬───────────┘
                                                      │
                                                      ▼
                                           ┌──────────────────────┐
                                           │ BigQuery              │
                                           │ staging + warehouse   │
                                           └──────────┬───────────┘
                                                      │
                                                      ▼
                                           ┌──────────────────────┐
                                           │ dbt                  │
                                           │ facts/dims/marts      │
                                           └──────────┬───────────┘
                                                      │
                              ┌───────────────────────┼───────────────────┐
                              ▼                       ▼                   ▼
                        BI dashboard             SQL analysis       data-quality
                        Metabase/Looker           notebooks          monitoring
```

Control plane:

``` text
Airflow / Cloud Composer
    ├── backfill jobs
    ├── Spark batch jobs
    ├── BigQuery load/merge jobs
    ├── dbt build/test
    ├── reconciliation
    ├── retention/compaction
    └── daily data-quality report

Terraform
    ├── GCS
    ├── BigQuery datasets/tables
    ├── service accounts/IAM
    ├── monitoring resources
    └── optional managed compute

GitHub Actions
    ├── lint/type-check
    ├── unit/integration tests
    ├── dbt compile/test
    ├── Terraform validate
    └── Docker build
```

------------------------------------------------------------------------

## 3. Why each tool exists

  ------------------------------------------------------------------------
  Tool                    Use                      Do not use it for
  ----------------------- ------------------------ -----------------------
  Python 3.12 + asyncio   WebSocket client, source large historical
                          adapters, validation,    transformations
                          producer                 

  Kafka or Redpanda       durable stream buffer,   final analytics storage
                          decoupling, replay       
                          within retained log      

  GCS                     cheap immutable Bronze   low-latency dashboard
                          and Parquet Silver       serving
                          storage                  

  PySpark                 streaming normalization  tiny control-plane
                          and large                scripts
                          backfills/reprocessing   

  BigQuery                analytical warehouse and raw event durability
                          serving layer            

  dbt Core                SQL transformations,     source ingestion
                          tests, docs, lineage     

  Airflow                 batch orchestration,     keeping a WebSocket
                          retries, dependencies,   alive 24/7
                          backfills                

  Terraform               reproducible cloud       application logic
                          resources/IAM            

  Docker Compose          reproducible local       production HA
                          Kafka/Redpanda +         
                          Airflow + services       

  Prometheus/Grafana or   pipeline health/SLO      business
  Cloud Monitoring        telemetry                transformations

  Great Expectations/Soda raw/Silver contract      duplicating every dbt
  (optional)              checks                   test

  Metabase / Looker       thin analytics/demo      core project value
  Studio                  layer                    
  ------------------------------------------------------------------------

**Rule:** Do not add technology merely to increase the tool count. Every
component must have an explicit failure mode or scalability problem that
it solves.

------------------------------------------------------------------------

## 4. Repository layout

``` text
atmospulse/
├── AGENTS.md
├── SKILLS.md
├── README.md
├── Makefile
├── pyproject.toml
├── docker-compose.yml
├── .env.example
├── ingestion/
│   ├── sources/
│   │   ├── base.py
│   │   ├── jetstream_v1.py
│   │   ├── jetstream_v2.py
│   │   └── fixture.py
│   ├── models/
│   │   └── events.py
│   ├── producer.py
│   ├── cursor_store.py
│   ├── dead_letter.py
│   └── metrics.py
├── streaming/
│   ├── jobs/
│   │   ├── normalize_events.py
│   │   ├── posts.py
│   │   ├── engagements.py
│   │   └── graph_edges.py
│   └── schemas/
├── batch/
│   ├── backfill.py
│   ├── compact.py
│   └── reconcile.py
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   ├── macros/
│   ├── tests/
│   ├── snapshots/
│   └── seeds/
├── airflow/
│   └── dags/
│       ├── hourly_pipeline.py
│       ├── daily_reconciliation.py
│       └── historical_backfill.py
├── infra/
│   └── terraform/
├── monitoring/
│   ├── dashboards/
│   └── alerts/
├── dashboards/
├── tests/
│   ├── unit/
│   ├── integration/
│   ├── contract/
│   └── load/
├── scripts/
└── docs/
    ├── architecture.md
    ├── data_contract.md
    ├── runbook.md
    ├── benchmark.md
    └── adr/
```

------------------------------------------------------------------------

## 5. Event contract

Every raw event envelope should expose, when available:

``` text
event_id
source_version
source_host
cursor
event_kind
operation
did
collection
rkey
at_uri
cid
event_time
ingested_at
payload_json
schema_version
```

### Event ID

Generate deterministic identity from stable source fields. Preferred
logical key:

``` text
(source_version, did, collection, rkey, operation, cid-or-record-version)
```

If the selected wire protocol exposes a globally useful sequence, store
it, but do not make cross-host/cross-version assumptions not guaranteed
by the source.

### Timestamps

Maintain separately: - `event_time` --- source event timestamp; -
`record_created_at` --- timestamp inside a post/record, if applicable; -
`ingested_at` --- first time this platform observed the event; -
`processed_at` --- transformation completion time.

Never replace source time with processing time.

### Raw policy

Bronze is append-only.

Never mutate a raw object to "fix" a bad record. Corrections happen in
later layers.

Recommended object layout:

``` text
gs://<bucket>/bronze/bluesky/
  event_date=YYYY-MM-DD/
  event_hour=HH/
  collection=<nsid>/
  part-*.jsonl.gz
```

------------------------------------------------------------------------

## 6. Kafka / Redpanda design

Topics:

``` text
bluesky.raw.events
bluesky.posts
bluesky.engagements
bluesky.graph
bluesky.identity
bluesky.dlq
```

Minimum production-like behavior: - key records by DID or stable
repository identity to preserve useful local ordering; - enable producer
acknowledgements; - configure bounded retries and exponential backoff; -
track producer errors; - retain enough stream history for local replay
tests; - DLQ malformed/unprocessable messages with reason + original
payload; - do not acknowledge source progress before the durable publish
succeeds.

For a low-cost portfolio deployment, Redpanda is acceptable as a
Kafka-compatible local/demo broker. Document how managed
Kafka/Confluent/MSK would replace it in a production deployment.

------------------------------------------------------------------------

## 7. Spark responsibilities

### Structured Streaming

Consume raw topics and: 1. parse event envelope; 2. enforce schema; 3.
quarantine malformed records; 4. normalize nested payloads; 5. derive
`at_uri`, actor DID, collection, operation; 6. extract post text
metadata, languages, reply/quote references, embeds and links; 7.
separate likes/reposts/follows; 8. deduplicate; 9. apply watermarking
where event-time windows are used; 10. write partitioned Parquet Silver
data; 11. expose streaming metrics.

### Batch PySpark

Use the same transformation functions for: - replay/backfill; -
rebuilding Silver from Bronze; - schema migrations; - compaction of
small Parquet files; - benchmark/load tests.

The code must demonstrate that the streaming and batch paths converge to
the same canonical schema.

------------------------------------------------------------------------

## 8. Warehouse model

### Staging

``` text
stg_events
stg_posts
stg_likes
stg_reposts
stg_follows
stg_identity
```

### Dimensions

``` text
dim_actor
dim_date
dim_hour
dim_language
dim_domain
```

### Facts

``` text
fct_posts
fct_likes
fct_reposts
fct_follows
fct_post_engagement
```

### Marts

``` text
mart_network_activity_hourly
mart_topic_velocity
mart_domain_velocity
mart_creator_growth
mart_post_engagement
mart_conversation_health
mart_pipeline_health
```

### Suggested semantics

`fct_posts` - one canonical post record per AT URI; - include author
DID; - created time; - language; - reply flag; - quote flag; -
has_media; - external domain; - deleted/current-state flag where
derivable.

`fct_post_engagement` - post URI; - likes; - reposts; - replies; - quote
posts; - engagement velocity at defined observation windows.

`mart_topic_velocity` - token/topic; - window; - post count; - unique
authors; - engagement; - previous-window count; - velocity; -
acceleration; - z-score/anomaly score.

Do not call a token "trending" without a documented ranking definition.

------------------------------------------------------------------------

## 9. BigQuery physical design

Use: - partitioning by event/date field; - clustering on frequently
filtered dimensions such as collection, actor DID, or post URI where
appropriate; - incremental dbt models; - `MERGE` for
canonical/current-state tables; - append-only tables for immutable event
history; - explicit column selection rather than `SELECT *` in
production marts.

Track: - bytes processed per representative query; - partition
pruning; - incremental build duration; - full-refresh duration; -
storage footprint.

------------------------------------------------------------------------

## 10. dbt requirements

Every model must have: - description; - owner/domain metadata where
useful; - column descriptions for important keys; - tests; - explicit
materialization; - documented grain.

Required tests: - `not_null` on primary identifiers; - `unique` where
grain requires it; - accepted values for event kinds/operations; -
relationships between facts and dimensions where logically valid; -
custom test for impossible timestamps; - custom test for duplicate
current-state AT URIs; - freshness checks on streaming-derived sources.

Use incremental models for large facts/marts.

Generate dbt docs and include a lineage screenshot in the project
README.

------------------------------------------------------------------------

## 11. Airflow DAGs

### `hourly_pipeline`

Tasks: 1. verify source/broker heartbeat; 2. verify Bronze freshness; 3.
launch/verify incremental Silver processing; 4. load/merge BigQuery
staging; 5. `dbt build --select tag:hourly`; 6. publish quality metrics;
7. notify on SLA miss.

### `daily_reconciliation`

Tasks: 1. sample source records; 2. compare raw/Silver/warehouse counts;
3. validate delete/update folding; 4. check duplicate rate; 5. check
freshness; 6. check null/contract failures; 7. write
`mart_pipeline_health`; 8. publish report.

### `historical_backfill`

Parameters: - start time; - end time; - collections; - source mode; -
target layer; - overwrite/rebuild flag.

Backfill must be rerunnable and idempotent.

Airflow must orchestrate jobs; it must not be the process responsible
for a permanent WebSocket connection.

------------------------------------------------------------------------

## 12. Analytics functionality

The project must expose useful products, not only tables.

### A. Network activity

-   posts/minute and posts/hour;
-   likes/reposts/follows over time;
-   unique active authors;
-   event mix;
-   language distribution.

### B. Topic velocity

-   rising keywords/hashtags/topics;
-   rolling 5m/15m/1h counts;
-   velocity vs prior window;
-   acceleration;
-   anomaly score;
-   unique-author guardrail to reduce single-account spam effects.

### C. Link/domain intelligence

-   most-shared external domains;
-   fastest-rising domains;
-   unique sharers;
-   engagement per shared domain;
-   domain concentration.

### D. Creator/account analytics

-   post frequency;
-   received engagement;
-   engagement per post;
-   follower-edge growth observed by the pipeline;
-   rolling creator activity.

Do not equate popularity with quality or authority.

### E. Conversation analytics

-   reply depth;
-   replies per root;
-   branching factor;
-   time-to-first-reply;
-   conversation duration;
-   unique participants.

### F. Data platform health

-   source lag;
-   ingest rate;
-   processing rate;
-   DLQ rate;
-   duplicate rate;
-   reconnect count;
-   Bronze/Silver/Gold row reconciliation;
-   Airflow success rate;
-   dbt freshness.

------------------------------------------------------------------------

## 13. Target engineering SLOs and benchmark metrics

These are **targets to measure**, not numbers to claim before running
benchmarks.

  -------------------------------------------------------------------------
  Category              Metric                                       Target
  --------------------- ---------------------- ----------------------------
  Freshness             p50 source-event →                           \< 5 s
                        Bronze                 

  Freshness             p95 source-event →                          \< 15 s
                        Bronze                 

  Freshness             p95 source-event →                          \< 60 s
                        queryable Silver       

  Freshness             p95 source-event →                        \< 10 min
                        hourly Gold mart       

  Reliability           accepted-event           \>= 99.9% during benchmark
                        durable-ingest success 

  Reliability           unplanned data loss                      0 observed
                        across forced          
                        reconnect test         

  Recovery              WebSocket disconnect →                  \< 60 s p95
                        resumed consumption    

  Recovery              replay/backfill job        \>= 99% across test runs
                        success                

  Correctness           duplicate rows in                                 0
                        canonical Silver by    
                        logical key            

  Correctness           duplicate rows in                                 0
                        canonical Gold facts   

  Quality               required-field             \>= 99.9% or quarantined
                        validity               

  Quality               dbt critical tests                             100%
                        passing                

  Reconciliation        Bronze→Silver                               \< 0.1%
                        unexplained count      
                        delta                  

  Throughput            sustained local                  \>= 1,000 events/s
                        pipeline benchmark     

  Throughput            burst benchmark        \>= 5,000 events/s for 5 min

  Scale                 replay benchmark               \>= 5 million events
                        corpus                 

  Warehouse             representative                               \< 5 s
                        dashboard query p95    

  Warehouse             partition-pruned bytes            \>= 70% reduction
                        vs unpartitioned       
                        baseline               

  Orchestration         scheduled DAG success    \>= 99% over measured runs

  Observability         critical failure alert                     \< 2 min
                        detection              

  Maintainability       unit + contract test                        \>= 80%
                        coverage on core       
                        ingestion/transforms   

  CI                    PR validation runtime              \< 10 min target
  -------------------------------------------------------------------------

### Metric definitions

**Ingestion lag**

``` text
ingested_at - event_time
```

**End-to-end Silver latency**

``` text
silver_available_at - event_time
```

**Duplicate rate**

``` text
duplicate_events / total_processed_events
```

**DLQ rate**

``` text
dlq_events / total_consumed_events
```

**Reconciliation delta**

``` text
abs(expected_events - canonical_events) / expected_events
```

Only compare compatible grains and explicitly account for deletes,
filtered event types and intentionally quarantined records.

**Recovery time**

``` text
first_successful_event_after_reconnect - disconnect_detected_at
```

**Throughput**

``` text
processed_events / elapsed_seconds
```

Report p50/p95/p99 latency, not only averages.

------------------------------------------------------------------------

## 14. Benchmark suite

Create reproducible scripts that test:

### Benchmark 1 --- sustained ingestion

-   replay fixture or captured compliant sample at 1k events/s;
-   run 30 minutes;
-   report throughput, lag, CPU, memory, errors.

### Benchmark 2 --- burst

-   5k events/s for 5 minutes;
-   measure backlog growth and drain time.

### Benchmark 3 --- duplicate delivery

-   intentionally redeliver 5--10% of events;
-   prove canonical Silver/Gold remains duplicate-free.

### Benchmark 4 --- disconnect/recovery

-   kill WebSocket/network;
-   keep downstream alive;
-   reconnect from persisted cursor;
-   show zero observed gaps within the test source and quantify
    duplicates before dedup.

### Benchmark 5 --- consumer outage

-   stop Spark consumer;
-   continue broker ingestion;
-   restart consumer;
-   measure catch-up time.

### Benchmark 6 --- schema drift

-   add unknown fields;
-   omit optional fields;
-   inject malformed required fields;
-   prove forward-compatible records survive and invalid records enter
    DLQ.

### Benchmark 7 --- BigQuery optimization

Run the same analytical workload: 1. naive/unpartitioned test table; 2.
partitioned + clustered table; 3. optimized dbt mart.

Record bytes scanned and execution time.

Write results to `docs/benchmark.md`. Never hard-code aspirational
targets as achieved results.

------------------------------------------------------------------------

## 15. Data-quality rules

Required invariants: - event logical key is deterministic; - canonical
post URI is unique; - DID is non-null for repo-scoped records; -
`created_at` is not implausibly in the future beyond configured
tolerance; - delete events never create a live record; - replaying the
same Bronze partition twice does not change canonical row counts; -
Silver schema is stable and versioned; - unknown optional JSON fields do
not crash ingestion; - malformed events are observable, not silently
discarded; - warehouse freshness stays within SLO or alerts.

------------------------------------------------------------------------

## 16. Observability

Expose metrics such as:

``` text
bluesky_events_received_total
bluesky_events_published_total
bluesky_events_dlq_total
bluesky_ingestion_lag_seconds
bluesky_cursor_checkpoint_age_seconds
bluesky_websocket_reconnects_total
kafka_consumer_lag
spark_batch_duration_seconds
spark_rows_processed_total
bronze_files_written_total
bq_rows_loaded_total
dbt_model_duration_seconds
reconciliation_delta_ratio
```

Dashboard panels: - event throughput; - p50/p95/p99 lag; - consumer
lag; - reconnects; - DLQ rate; - data freshness; - row reconciliation; -
Spark micro-batch duration; - DAG failures; - warehouse query cost.

Alerts: - no source events for N minutes when stream is expected
active; - p95 ingestion lag \> threshold; - cursor/checkpoint stale; -
DLQ rate \> threshold; - consumer lag continuously rising; - dbt source
freshness failure; - reconciliation delta \> 0.1%; - DAG failure.

------------------------------------------------------------------------

## 17. Failure behavior

### WebSocket disconnect

-   reconnect with exponential backoff + jitter;
-   resume from durable cursor;
-   never reset to "now" silently;
-   increment reconnect metric.

### Broker unavailable

-   retry with bounded local buffering;
-   do not advance durable source checkpoint until publish succeeds;
-   fail loudly if safe buffer limits are exceeded.

### Spark failure

-   restart from checkpoint;
-   maintain idempotent sink behavior;
-   never manually skip a failed micro-batch without recording it.

### BigQuery failure

-   Airflow retry;
-   deterministic staging batch IDs;
-   idempotent merge.

### Bad record

-   preserve raw payload;
-   send to DLQ with reason;
-   continue processing when safe.

### Schema change

-   tolerate additive unknown fields in Bronze;
-   version normalized schemas;
-   add migration/backfill if a breaking Silver change is necessary.

------------------------------------------------------------------------

## 18. Security and governance

-   no passwords/API secrets committed;
-   `.env.example` contains names only;
-   use workload identity/service accounts in cloud;
-   least-privilege IAM;
-   separate dev/prod datasets if cloud deployment is demonstrated;
-   encrypt cloud storage with provider defaults or stronger
    configuration;
-   do not ingest DMs/private endpoints;
-   document that public social data can still contain sensitive
    personal information;
-   provide retention/deletion policy for portfolio deployment;
-   avoid redistributing unnecessary full-text datasets in the Git
    repository.

------------------------------------------------------------------------

## 19. CI/CD acceptance gates

Every PR: 1. Ruff lint; 2. Black/check formatting; 3. mypy/pyright; 4.
pytest unit tests; 5. contract tests against recorded fixtures; 6. Spark
transformation test; 7. dbt compile; 8. dbt unit/data tests where
runnable; 9. Terraform fmt/validate; 10. Docker build.

Main branch: - build tagged image; - run integration test; - optionally
deploy dev infrastructure; - run smoke test; - record deployment SHA.

No merge when a critical data contract or idempotency test fails.

------------------------------------------------------------------------

## 20. Definition of done

The project is complete only when all of these exist:

-   [ ] live Jetstream ingestion;
-   [ ] durable cursor/checkpoint;
-   [ ] reconnect test;
-   [ ] Kafka/Redpanda buffer;
-   [ ] immutable Bronze storage;
-   [ ] Spark streaming normalization;
-   [ ] Spark batch replay;
-   [ ] deduplicated canonical Silver;
-   [ ] BigQuery warehouse;
-   [ ] dbt facts/dimensions/marts;
-   [ ] Airflow orchestration;
-   [ ] data-quality tests;
-   [ ] observability dashboard;
-   [ ] Terraform;
-   [ ] CI;
-   [ ] benchmark harness;
-   [ ] measured benchmark report;
-   [ ] architecture diagram;
-   [ ] runbook;
-   [ ] dashboard/screenshots;
-   [ ] README with measured, reproducible metrics.

------------------------------------------------------------------------

## 21. Resume-proof evidence

The README must include an **Evidence** section linking each resume
claim to a reproducible artifact.

Example format:

``` text
Claim: sustained 1,420 events/s with p95 Bronze latency 7.8s
Evidence: docs/benchmark.md#sustained-ingestion
Command: make benchmark-ingestion
Commit: <sha>
Environment: <machine/cloud config>
```

Never claim: - "processed billions of events" because Bluesky itself
contains billions of records; - "99.99% uptime" without measuring it; -
"zero data loss" globally when only a sample/test window was verified; -
cost savings without a measured baseline.

Prefer: - "benchmarked on 5M replayed events"; - "maintained zero
duplicate canonical rows under 10% forced redelivery"; - "reduced
BigQuery bytes scanned by X% against an unpartitioned baseline"; -
"recovered from forced WebSocket disconnects in p95 X seconds."

------------------------------------------------------------------------

## 22. Implementation order

### Phase 1 --- local vertical slice

Jetstream → Python → Redpanda → raw JSON → local analytics.

### Phase 2 --- lake

Add GCS-compatible abstraction/MinIO locally and partitioned Bronze.

### Phase 3 --- Spark

Add canonical Silver and idempotency.

### Phase 4 --- warehouse

BigQuery + dbt staging/facts/dims/marts.

### Phase 5 --- orchestration

Airflow batch jobs, reconciliation, backfill.

### Phase 6 --- reliability

Failure injection, DLQ, cursor recovery, schema evolution.

### Phase 7 --- observability

Metrics, dashboard, alerts.

### Phase 8 --- infrastructure

Terraform + CI/CD.

### Phase 9 --- benchmark

Run reproducible tests and replace target values in README with measured
values.

### Phase 10 --- presentation

Architecture diagram, dashboard, dbt lineage, benchmark charts, concise
resume bullets.

------------------------------------------------------------------------

## 23. Coding-agent operating rules

When modifying this repository:

1.  Read this file and `SKILLS.md`.
2.  Identify which data-layer invariant the change affects.
3.  Prefer the smallest architecture-consistent change.
4.  Add/modify tests before considering the task complete.
5.  Preserve raw data.
6.  Never silently drop an event.
7.  Never advance a cursor before durability.
8.  Never introduce a non-idempotent replay path.
9.  Never mix source-version cursor semantics.
10. Keep streaming transformations reusable in batch.
11. Add metrics for new failure modes.
12. Update data contracts and dbt docs when schema changes.
13. For performance claims, run a benchmark; do not infer.
14. For cost claims, capture a before/after query or billing metric.
15. Document architectural decisions in `docs/adr/`.
