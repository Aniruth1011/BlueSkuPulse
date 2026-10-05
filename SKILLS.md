# SKILLS.md --- AtmosPulse Implementation Skills and Work Packages

This file defines the implementation skills an engineering agent must
use to build the project. Each skill is a bounded work package with
inputs, outputs, acceptance tests and target metrics.

------------------------------------------------------------------------

# Skill 1 --- Bluesky / AT Protocol source integration

## Goal

Consume Bluesky public network activity reliably from Jetstream.

## Build

-   asynchronous WebSocket source client;
-   v1 and v2 source adapters;
-   collection filters for posts, likes, reposts and follows;
-   event envelope parser;
-   cursor persistence;
-   reconnect with exponential backoff and jitter;
-   heartbeat/idle detection;
-   fixture source for deterministic tests.

## Key source facts to respect

-   Jetstream exists to make AT Protocol streaming easier to consume as
    JSON.
-   Firehose itself is more complex and uses CBOR/CAR structures.
-   Jetstream consumers must account for replay/resume and duplicate
    delivery.
-   Public AppView GET endpoints are useful for enrichment, but are not
    the primary streaming source.
-   cursor semantics differ by Jetstream version.

## Output

`ingestion/sources/*`

## Acceptance tests

-   consume live records for 15+ minutes;
-   forced socket disconnect reconnects automatically;
-   cursor survives process restart;
-   repeated events do not create duplicate canonical records;
-   invalid JSON enters DLQ;
-   unknown additive fields do not crash parser.

## Metrics

-   received events/sec;
-   p50/p95/p99 ingestion lag;
-   reconnect count;
-   reconnect duration;
-   parse failure rate;
-   cursor checkpoint age.

------------------------------------------------------------------------

# Skill 2 --- Durable stream buffering with Kafka/Redpanda

## Goal

Decouple an unreliable external WebSocket from downstream processing.

## Build

-   local Redpanda/Kafka service;
-   topic definitions;
-   producer configuration;
-   stable message keys;
-   DLQ;
-   retention configuration;
-   broker health check;
-   integration test.

## Design

Primary raw topic: `bluesky.raw.events`

Derived topics: `bluesky.posts`, `bluesky.engagements`, `bluesky.graph`,
`bluesky.identity`.

Use DID as a useful partition key unless a stronger ordering requirement
is documented.

## Acceptance tests

-   broker restart does not corrupt messages;
-   consumer can catch up after outage;
-   producer failure prevents source checkpoint advancement;
-   duplicate source deliveries are observable and downstream-safe.

## Metrics

-   producer throughput;
-   producer error rate;
-   broker/consumer lag;
-   backlog drain time;
-   DLQ rate.

------------------------------------------------------------------------

# Skill 3 --- Bronze data lake

## Goal

Maintain an immutable, replayable copy of ingested source events.

## Build

-   GCS bucket;
-   local MinIO/file abstraction for development if desired;
-   compressed JSONL raw sink;
-   hourly/date/collection partitions;
-   manifest/batch metadata;
-   lifecycle rules.

## Partition layout

`event_date`, `event_hour`, `collection`.

Avoid tiny-object explosion. Buffer records into sensible files rather
than one object per event.

## Acceptance tests

-   every successfully landed batch has manifest/count;
-   rerunning downstream jobs does not mutate Bronze;
-   a day/hour can be replayed independently;
-   malformed payload is preserved in raw/DLQ path.

## Metrics

-   raw bytes/day;
-   compression ratio;
-   files/hour;
-   median file size;
-   Bronze event count;
-   Bronze freshness.

------------------------------------------------------------------------

# Skill 4 --- Spark Structured Streaming normalization

## Goal

Turn heterogeneous nested events into canonical analytics-ready records.

## Build

PySpark transformations for: - posts; - likes; - reposts; - follows; -
identity/account events; - deletes/updates; - nested embeds; - external
links; - reply/quote references; - language arrays.

## Required engineering

-   explicit schemas;
-   event-time handling;
-   checkpointing;
-   deduplication;
-   watermarking only where semantically correct;
-   bad-record quarantine;
-   partitioned Parquet output;
-   reusable pure transformation functions.

## Acceptance tests

-   same fixture transformed by streaming and batch gives equivalent
    canonical rows;
-   10% duplicate injection yields zero duplicate canonical keys;
-   optional new JSON field does not fail;
-   invalid required field is quarantined;
-   restart from Spark checkpoint is correct.

## Metrics

-   rows/sec;
-   micro-batch duration;
-   processing lag;
-   state-store size where relevant;
-   duplicate count;
-   malformed count.

------------------------------------------------------------------------

# Skill 5 --- Backfill and replay

## Goal

Rebuild downstream state without special-case code.

## Build

-   source replay adapter;
-   Bronze replay;
-   time-range parameters;
-   collection filters;
-   idempotent batch IDs;
-   Airflow backfill DAG;
-   resume support.

## Compatibility

Because Jetstream deployment/version capabilities can differ,
support: 1. Jetstream replay/snapshot when the selected service exposes
it; 2. recent cursor replay where supported; 3. replay from the
platform's own Bronze archive.

The project's own Bronze layer is the long-term reproducible backfill
source.

## Acceptance tests

-   rebuild one hour/day from Bronze;
-   run same backfill twice with unchanged canonical counts;
-   interrupted job resumes or safely restarts;
-   backfill and live paths converge.

## Metrics

-   events/sec;
-   GB/min processed;
-   completion time;
-   duplicate count;
-   reconciliation delta.

------------------------------------------------------------------------

# Skill 6 --- BigQuery warehouse engineering

## Goal

Serve low-latency analytical queries with controlled scan cost.

## Build

Datasets: - `raw_external` if needed; - `staging`; - `analytics`; -
`monitoring`.

Tables: - staging events; - dimensions; - fact tables; - marts.

## Physical optimization

-   date partitioning;
-   clustering based on measured access patterns;
-   incremental loads;
-   MERGE current-state tables;
-   append immutable event history.

## Acceptance tests

-   partition filters are enforced where appropriate;
-   representative queries demonstrate pruning;
-   duplicate primary grain = zero;
-   incremental build matches full-refresh result on fixture dataset.

## Metrics

-   bytes scanned/query;
-   query p50/p95;
-   slot time if available;
-   storage size;
-   load duration;
-   incremental vs full-refresh runtime.

------------------------------------------------------------------------

# Skill 7 --- dbt analytics engineering

## Goal

Make warehouse transformations tested, documented and reproducible.

## Build

Layers: - `stg_*`; - `int_*`; - `dim_*`; - `fct_*`; - `mart_*`.

## Required marts

1.  `mart_network_activity_hourly`
2.  `mart_topic_velocity`
3.  `mart_domain_velocity`
4.  `mart_creator_growth`
5.  `mart_post_engagement`
6.  `mart_conversation_health`
7.  `mart_pipeline_health`

## Required tests

-   uniqueness;
-   not-null;
-   accepted values;
-   relationships where valid;
-   source freshness;
-   custom duplicate-current-state test;
-   timestamp sanity;
-   reconciliation test.

## Acceptance tests

-   `dbt build` succeeds;
-   critical tests 100% pass;
-   docs generate;
-   model grains are documented;
-   large models are incremental.

## Metrics

-   model runtime;
-   test pass rate;
-   source freshness;
-   incremental rows processed;
-   full refresh vs incremental duration.

------------------------------------------------------------------------

# Skill 8 --- Topic velocity analytics

## Goal

Build a defensible "what is rising now?" data product.

## Build

For tokens/hashtags/topics: - 5-minute count; - 15-minute count; -
1-hour count; - unique authors; - engagement; - prior-window baseline; -
velocity; - acceleration; - z-score or robust anomaly score.

Example:

`velocity = current_window_count / max(previous_window_count, 1)`

Better production ranking: combine: - volume; - growth; - unique
authors; - engagement; - spam/concentration guardrail.

Do not rank a topic highly solely because one account posted repeatedly.

## Acceptance tests

-   synthetic spike ranks above stable baseline;
-   single-author spam is penalized;
-   zero previous count is handled;
-   late events do not create impossible negative counts.

## Metrics

-   mart freshness;
-   ranking computation time;
-   number of unique authors/topic;
-   concentration ratio;
-   anomaly score.

------------------------------------------------------------------------

# Skill 9 --- Domain/link intelligence

## Goal

Show which external information sources are spreading through Bluesky.

## Build

Extract domains from external embeds/links and calculate: -
shares/hour; - unique sharers; - engagement; - velocity; -
acceleration; - domain share of total outbound links.

Normalize: - scheme; - `www`; - hostname case; - tracking parameters
when full URLs are analyzed.

## Acceptance tests

-   equivalent URL forms map to same domain;
-   malformed URLs are quarantined/null-safe;
-   domain aggregation is deterministic.

------------------------------------------------------------------------

# Skill 10 --- Conversation graph analytics

## Goal

Model replies as a graph-like analytical dataset without requiring a
graph database.

## Build

Fields: - root URI; - parent URI; - child URI; - author DID; - depth; -
created time.

Metrics: - thread depth; - branching factor; - unique participants; -
time-to-first-reply; - conversation duration; - replies per root.

Use BigQuery recursive logic only if justified; otherwise precompute
parent/root fields during normalization.

## Acceptance tests

-   synthetic thread reconstructs correctly;
-   orphan parent is handled;
-   deleted node does not corrupt the entire thread.

------------------------------------------------------------------------

# Skill 11 --- Airflow orchestration

## Goal

Coordinate bounded jobs and data dependencies.

## Build

DAGs: - hourly pipeline; - daily reconciliation; - historical
backfill; - optional daily compaction.

## Rules

-   no permanent WebSocket operator;
-   tasks are idempotent;
-   retries are bounded;
-   backfill parameters are explicit;
-   SLA/freshness failure is observable.

## Acceptance tests

-   DAG can rerun same interval;
-   failed BigQuery/dbt task retries safely;
-   backfill date range works;
-   task logs include batch/run IDs.

## Metrics

-   DAG success rate;
-   task duration;
-   retry count;
-   SLA misses.

------------------------------------------------------------------------

# Skill 12 --- Data reconciliation

## Goal

Prove the platform has not silently lost or multiplied data.

## Build

For each hour/day: - raw event count by collection/operation; - Silver
canonical count; - duplicate count; - quarantine count; - delete
count; - warehouse fact count; - explained differences.

Persist reconciliation output to `mart_pipeline_health`.

## Important

A raw event count and current-state row count are not automatically
equal. Deletes, updates, duplicates, filtering and quarantine must be
accounted for.

## Acceptance test

For deterministic fixtures: `raw = accepted + quarantined`, and
canonical-state reconciliation matches expected folding semantics.

## Target

Unexplained delta \< 0.1%; deterministic integration fixture = 0.

------------------------------------------------------------------------

# Skill 13 --- Observability and SLOs

## Goal

Make pipeline failures visible before a dashboard user notices them.

## Build

Prometheus/Grafana locally or Cloud Monitoring in GCP.

Track: - source throughput; - ingestion lag; - reconnects; - checkpoint
age; - Kafka lag; - Spark duration; - DLQ rate; - Bronze freshness; -
BigQuery load status; - dbt freshness; - reconciliation delta.

## SLO targets

-   p95 event→Bronze \< 15 sec;
-   p95 event→Silver \< 60 sec;
-   Gold freshness \< 10 min;
-   reconnect recovery \< 60 sec p95;
-   critical dbt tests = 100% pass;
-   unexplained reconciliation delta \< 0.1%.

These are targets until benchmarked.

------------------------------------------------------------------------

# Skill 14 --- Failure injection

## Goal

Demonstrate reliability instead of merely describing it.

## Scenarios

1.  kill WebSocket connection;
2.  restart ingestion service;
3.  stop broker temporarily;
4.  stop Spark consumer;
5.  redeliver duplicate events;
6.  inject malformed JSON;
7.  add unknown schema fields;
8.  fail BigQuery load;
9.  fail dbt test.

## Output

`docs/failure-testing.md`

For each scenario record: - setup; - expected behavior; - observed
behavior; - recovery time; - row-count impact; - alert triggered; -
follow-up.

------------------------------------------------------------------------

# Skill 15 --- Performance benchmarking

## Goal

Generate resume-grade metrics that are reproducible.

## Harness

Use generated/recorded compliant fixtures and configurable rates.

### Test A

1,000 events/sec × 30 min.

### Test B

5,000 events/sec × 5 min burst.

### Test C

> = 5 million event replay.

### Test D

10% duplicate injection.

### Test E

BigQuery optimized vs naive layout.

## Record

-   hardware/cloud configuration;
-   git SHA;
-   input count;
-   input size;
-   throughput;
-   p50/p95/p99 latency;
-   CPU;
-   memory;
-   errors;
-   duplicates;
-   cost/bytes scanned where applicable.

Never publish only the best run. Report method and sample count.

------------------------------------------------------------------------

# Skill 16 --- Terraform / cloud deployment

## Goal

Make the cloud environment reproducible.

## Provision

-   GCS buckets;
-   BigQuery datasets;
-   service accounts;
-   IAM bindings;
-   secrets references;
-   monitoring;
-   optional compute.

Do not provision expensive always-on resources by default for a
portfolio project.

Use variables for: - project ID; - region; - environment; - retention; -
labels.

## Acceptance tests

-   `terraform fmt -check`;
-   `terraform validate`;
-   plan is reviewable;
-   no credentials in state/config committed to Git.

------------------------------------------------------------------------

# Skill 17 --- CI/CD

## Goal

Make correctness automatic.

## PR workflow

-   Python lint;
-   formatting;
-   type checking;
-   pytest;
-   fixture/contract test;
-   Spark transformation test;
-   dbt compile;
-   Terraform validate;
-   Docker build.

## Main workflow

-   integration test;
-   image tag;
-   optional dev deploy;
-   smoke test.

## Target

PR CI \< 10 minutes where practical.

------------------------------------------------------------------------

# Skill 18 --- Cost engineering

## Goal

Show that warehouse/data-lake design considers cost.

## Measure

-   compressed Bronze size;
-   Parquet compression;
-   BigQuery bytes scanned;
-   partition pruning;
-   incremental model bytes processed;
-   storage lifecycle impact.

## Experiment

Compare representative analytical query: 1. unpartitioned; 2.
partitioned; 3. partitioned + clustered / mart.

Resume claim must use the measured percentage reduction, not an
estimate.

------------------------------------------------------------------------

# Skill 19 --- Dashboard / demo

## Goal

Provide a visual consumer that proves the data products work.

## Pages

### Network pulse

-   events/minute;
-   active authors;
-   collection mix;
-   language mix.

### Trending

-   topic velocity;
-   rising domains;
-   engagement velocity.

### Conversations

-   thread depth;
-   participants;
-   response time.

### Platform health

-   ingestion lag;
-   throughput;
-   Kafka lag;
-   DLQ;
-   reconciliation;
-   freshness.

Keep BI logic thin. Important definitions belong in dbt, not
dashboard-specific SQL.

------------------------------------------------------------------------

# Skill 20 --- Documentation and resume evidence

## Required docs

-   `README.md`;
-   `docs/architecture.md`;
-   `docs/data_contract.md`;
-   `docs/runbook.md`;
-   `docs/benchmark.md`;
-   `docs/failure-testing.md`;
-   ADRs.

## README must show

-   architecture diagram;
-   why each technology is present;
-   local quick start;
-   cloud deployment path;
-   screenshots;
-   dbt lineage;
-   measured benchmarks;
-   failure-recovery demonstration;
-   cost optimization result.

## Resume bullet template

Do not copy this with fake numbers. Replace brackets only after
measurement:

> Built a real-time Bluesky/AT Protocol data platform using Python,
> Kafka, PySpark, GCS, BigQuery, dbt and Airflow, benchmarking **\[X\]
> events/sec** across **\[N\] replayed events** with **\[Y\]s p95**
> ingestion latency.

> Implemented cursor-based recovery, idempotent Spark transformations
> and Bronze→Silver→Gold reconciliation, maintaining **0 duplicate
> canonical rows** under **\[D\]% forced redelivery** and recovering
> forced disconnects in **\[R\]s p95**.

> Partitioned/clustered BigQuery marts and incremental dbt models,
> reducing bytes scanned by **\[C\]%** versus an unpartitioned baseline
> while keeping dashboard queries below **\[Q\]s p95**.

These are evidence templates, not claims.

------------------------------------------------------------------------

# Final project scorecard

A strong completion should demonstrate all four dimensions:

  -----------------------------------------------------------------------
  Dimension                           Evidence
  ----------------------------------- -----------------------------------
  Streaming DE                        Jetstream, Kafka, Spark Structured
                                      Streaming, checkpoints

  Batch DE                            Bronze replay, PySpark backfill,
                                      Airflow

  Analytics engineering               BigQuery, dbt
                                      facts/dims/marts/tests

  Production engineering              IaC, CI/CD, observability, failure
                                      injection, benchmarks
  -----------------------------------------------------------------------

If one of these is absent, the project is materially weaker as a
senior-quality DE portfolio project.
