# Benchmark policy and runbook

## Current evidence status

The local profiles below were measured on 2026-10-05. Results apply only to the described machine,
corpus, commits, and methods. Unlisted engineering SLOs remain **TARGET**.

| Scenario | Exact workload | Required transport | Current status |
|---|---:|---|---|
| Sustained ingestion | 1,800,000 events at 1,000 events/s for 30 min | Kafka/Redpanda | MEASURED |
| Burst ingestion | 1,500,000 events at 5,000 events/s for 5 min | Kafka/Redpanda | MEASURED |
| Historical replay | at least 5,000,000 events | Kafka plus Bronze/Spark replay | MEASURED |
| Duplicate delivery | 1,000,000 events, 10% forced duplicates | Kafka plus canonical Silver check | MEASURED |
| Live Jetstream window | 505,195 deliveries over 15.57 min | public v2 Jetstream through Bronze | MEASURED; latency SLO FAILED |
| Live-date Bronze/Silver replay | 604,266 deliveries | local Bronze and batch Spark | MEASURED |
| Malformed/additive drift | 10,000 frames per case | isolated Kafka topics | MEASURED |
| Alert detection | injected load-failure metric | Prometheus and Alertmanager | MEASURED |
| BigQuery layout | same bounded query, naive vs partitioned/clustered | BigQuery, cache disabled | BLOCKED: GCP login expired |

## Measured environment

- Host: MacBook Air, macOS 26.6, arm64, 10 logical CPUs reported by Python.
- Host runtime: Python 3.14.8.
- Containers: Colima, 2 vCPUs; 4 GiB for producer profiles and 8 GiB for the corrected large-state
  Spark replay; 60 GiB Docker disk.
- Broker: Redpanda v26.2.3, one node, six raw-topic partitions, `acks=all`, idempotent producer,
  gzip compression.
- Producer benchmark commit: `044a4461fe88105db69b3add1a55ca08b6fb8df7`.
- Corrected Spark pipeline commit: `32b2684371e1e118ac10bc68c491026e42e3051d`.
- Batch replay/verifier commit: `d4d41636905db68380f9720b7d5ad936827ad3b6`.
- Live measurement and batching commits: `1cd80a3`, `8c9d35c`, and `4c5c811`.

## Results

| Profile | Events | Elapsed | Throughput | p50 / p95 / p99 broker-ack latency | Errors |
|---|---:|---:|---:|---:|---:|
| sustained | 1,800,000 | 1,800.020s | 999.989/s | 524.339 / 974.176 / 1,000.371ms | 0 |
| burst | 1,500,000 | 300.023s | 4,999.613/s | 124.380 / 199.272 / 203.011ms | 0 |
| replay | 5,000,000 | 247.773s | 20,179.722/s | 37.015 / 48.661 / 53.330ms | 0 |
| forced duplicates | 1,000,000, including 100,000 duplicates | 49.567s | 20,174.525/s | 36.915 / 48.753 / 54.658ms | 0 |

Latency above is contract-parse start to Kafka acknowledgement in 1,000-record producer batches.
It is not source-event-to-Bronze latency and must not be used to claim that SLO.

The four profiles plus an earlier 20,000-delivery smoke run left 9,320,000 raw broker deliveries.
Bronze reached `TOTAL-LAG=0` and stored the corpus in 28,066 compressed immutable data objects,
each paired with a manifest (56,132 files total, 1.1 GiB). The first Spark configuration used the
default heap-backed state store and restarted 22 times with
`java.lang.OutOfMemoryError`; that run is a failed result, not discarded noise. ADR 0004 records
the decision to use bounded RocksDB state and capped input micro-batches.

The corrected stream consumed all 9,320,000 deliveries through each of six independently
checkpointed output queries with zero process restarts. Spark retained 5,000,000 unique event IDs
and reported 4,320,000 dropped duplicate deliveries. Final snapshots were 3.439 GiB Spark memory,
5.7 GiB checkpoint state, and 1.4 GiB Silver output. Direct Parquet verification found:

- `events`: 5,000,000 rows, 5,000,000 distinct event IDs, 0 duplicates;
- `posts`: 5,000,000 rows, 5,000,000 distinct event IDs, 0 duplicates;
- `quarantine`: 0 rows;
- engagement, graph, and identity outputs: 0 rows because this generated corpus contains posts.

An independent batch rebuild from Bronze completed in approximately 319 seconds and produced the
same 5,000,000-row, duplicate-free event and post counts. Direct verification took 11.737 seconds.

Controlled service drills measured:

| Drill | Configured outage | Service-running recovery | Scope |
|---|---:|---:|---|
| Redpanda outage | 10s | 12.022s | broker health restored |
| Bronze consumer outage | 30s | 31.401s | consumer process restored |
| Spark restart | restart only | 0.155s | process running; not full state catch-up |

The failure harness deliberately does not infer row impact from process health. Canonical row
impact is checked separately by the batch/stream Silver verifier.

## Live Jetstream result

The corrected live window ran from `2026-10-05T18:34:13.348292Z` through
`2026-10-05T18:49:47.409699Z` against the public Jetstream v2 endpoint. It accepted all four
configured record collections plus account/identity lifecycle messages. Bronze drained to zero
Kafka lag before measurement ended.

| Metric | Measured result | Target/status |
|---|---:|---|
| Deliveries / distinct IDs | 505,195 / 505,180 | 15 expected inclusive-cursor redeliveries |
| Window throughput | 540.858 events/s | observation, not a load-generator result |
| Source to ingest p50 / p95 / p99 | 174.496 / 288.063 / 315.244s | FAILED |
| Ingest to Bronze p50 / p95 / p99 | 5.423 / 10.190 / 10.677s | p95 below 15s, but not the full SLO |
| Source to Bronze p50 / p95 / p99 | 179.859 / 294.116 / 320.521s | FAILED: targets are p50 <5s, p95 <15s |
| Forced outage to reconnect p50 / p95 | 31.804 / 32.837s across 5 trials | PASS: p95 <60s |
| Detected socket failure to reconnect p50 / p95 | 0.685 / 0.763s across 15 samples | diagnostic |
| Raw duplicate rate | 0.002969% | expected at-least-once behavior |
| Bronze objects / median compressed size | 458 / 130,818.5 bytes | 82 committed batches |
| Compression ratio | 5.497:1 | 757.8 MB raw to 137.8 MB compressed |

The latency failure is real. Serial per-event Kafka acknowledgements let the public stream outrun
the two-vCPU local ingestion process, and the backlog caused repeated WebSocket ping timeouts. It
must not be represented as meeting the source-to-Bronze SLO. The separate ingest-to-Bronze metric
shows that the new size-or-deadline Bronze batching itself remained under 15 seconds at p95.

The live Structured Streaming attempt was also a failed result: a checkpoint created under the
old 200-partition HDFS state-store configuration overrode the current RocksDB settings. The local
Spark container restarted nine times with state-store/heap OOM errors and produced no valid live
Silver latency sample. Source-to-Silver is therefore `FAILED / NOT MEASURED`, not inferred from
batch output.

An independent batch rebuild of the exact `event_date=2026-10-05` Bronze partition completed in
165.803 seconds. It reconciled 604,266 raw deliveries containing 604,243 distinct IDs and 23
redeliveries to 604,243 duplicate-free Silver event rows, zero quarantine rows, and a 0.000%
unexplained delta. Direct verification took 6.472 seconds. Derived row counts were 69,343 posts,
484,016 engagements, 49,919 graph edges, and 965 identity/account events.

The earlier pre-fix live attempt created tens of thousands of tiny immutable objects. Those files
were preserved, as Bronze policy requires, and made the rebuild scan 1,886 input partitions. ADR
0005 documents the deadline-batching correction; the result demonstrates why compaction remains
an operational requirement.

## Reliability, quality, and local warehouse measurements

- Malformed input: 10,000 frames with exactly 100 malformed frames produced 9,900 raw records and
  100 DLQ records in 8.047 seconds of measured ingestion time. The DLQ rate was exactly 1.0%, and
  no input disappeared.
- Additive schema drift: 10,000 events carrying an unknown field produced 10,000 raw records and
  zero DLQ records in 8.032 seconds. Unknown optional fields survived ingestion.
- Alert detection: an injected `bq_load_failures_total` change fired in Prometheus after 17.605
  seconds and reached Alertmanager after 31.385 seconds. This passes the <2 minute detection target;
  no external email/chat receiver was configured.
- dbt local full refresh: all 116 checks passed in 2.69 seconds wall time (1.63 seconds reported by
  dbt). The immediate incremental rerun again passed 116/116 in 2.80 seconds wall time (1.73 seconds
  reported by dbt). This tiny fixture demonstrates correctness, not an incremental speedup.
- Seven local DuckDB dashboard queries ran 100 warm-cache samples each. Aggregate latency was
  p50 0.099ms, p95 0.238ms, and p99 0.257ms. This is explicitly not BigQuery evidence.
- The complete local CI-equivalent validation passed 63 Python/Spark tests, achieved 84.89% core
  ingestion coverage, and passed three 116-check dbt stages in 24.96 seconds. It is not a measured
  GitHub Actions PR duration.

Ten-minute resource sampling collected 60 observations per service. Bronze used p95 26.78% CPU
and 85.47 MiB memory; ingestion 28.27% and 131.8 MiB; Redpanda 10.24% and 905.6 MiB. Spark showed
p95 192.02% CPU and 2,596.864 MiB while failing/restarting, so its resource figures describe the
failed state rather than healthy steady-state capacity.

## SLO accounting after this run

| Engineering SLO | Result | Status |
|---|---:|---|
| p50 / p95 source to Bronze | 179.859s / 294.116s | FAILED |
| p95 source to queryable Silver | no valid live output | FAILED / NOT MEASURED |
| Gold freshness <10 min | requires BigQuery/dbt production target | BLOCKED |
| >=1,000/s sustained | 999.989/s | FAILED strictly by 0.011/s |
| >=5,000/s for five minutes | 4,999.613/s | FAILED strictly by 0.387/s |
| >=5 million replay corpus | 5,000,000 events | PASS |
| zero canonical Silver duplicates | 0 across 604,243 live-date IDs; also 0 across 5M replay | PASS |
| reconnect p95 <60s | 32.837s including forced 30s outage | PASS |
| Bronze to Silver unexplained delta <0.1% | 0.000% on compatible live-date grains | PASS |
| critical dbt tests passing | 116/116 in both local runs | PASS locally |
| critical alert detection <2 min | 17.605s Prometheus; 31.385s Alertmanager | PASS locally |
| core ingestion/transformation coverage >=80% | 84.89% ingestion coverage | PASS for measured scope |
| BigQuery query p95 and bytes reduction | no authenticated cloud run | BLOCKED |
| scheduled Airflow success >=99% | no history of scheduled production runs | NOT MEASURED |
| PR validation <10 min | local equivalent 24.96s only | NOT MEASURED in GitHub Actions |

`gcloud auth print-access-token` failed with a non-interactive reauthentication error for the
configured account and project. Consequently BigQuery scan bytes/runtime, optimized-vs-naive cost,
cloud Gold freshness, deployed dashboard latency, Terraform plan/apply evidence, and true cloud
Airflow success remain blocked. Local DuckDB numbers are not substituted for them.

## Evidence artifacts

- `benchmark/results/sustained-kafka-20261005T165601634306Z.json`
- `benchmark/results/burst-kafka-20261005T170102428187Z.json`
- `benchmark/results/replay-kafka-20261005T170512385216Z.json`
- `benchmark/results/duplicates-kafka-20261005T170602568992Z.json`
- `benchmark/results/silver-verification-32b2684.json`
- `benchmark/results/silver-batch-verification-d4d4163.json`
- `benchmark/results/failure-broker-d4d41636905db68380f9720b7d5ad936827ad3b6.json`
- `benchmark/results/failure-consumer-d4d41636905db68380f9720b7d5ad936827ad3b6.json`
- `benchmark/results/failure-spark-d4d41636905db68380f9720b7d5ad936827ad3b6.json`
- `benchmark/results/live-pipeline-8c9d35c.json`
- `benchmark/results/live-recovery-8c9d35c.json`
- `benchmark/results/live-forced-disconnect-8c9d35c.json`
- `benchmark/results/live-resource-summary-8c9d35c.json`
- `benchmark/results/live-date-reconciliation-4c5c811.json`
- `benchmark/results/live-batch-silver-verification-4c5c811.json`
- `benchmark/results/live-alert-detection-4c5c811.json`
- `benchmark/results/duckdb-dashboard-4c5c811.json`
- `benchmark/results/local-ci-validation-4c5c811-escalated.log`

These generated artifacts are ignored by Git because they can become large and machine-specific;
`./scripts/metrics/run.sh archive-stop` packages them for durable attachment to a release or CI run.

## Event benchmark

The generator emits deterministic, valid v2-shaped Jetstream messages. Each message crosses the
contract parser and deterministic event-ID logic. `--transport kafka` additionally waits for
broker acknowledgement in bounded batches. Results are atomically written under
`benchmark/results/` and contain parameters, method, event and byte counts, throughput,
p50/p95/p99 processing latency, process CPU, peak RSS, errors, duplicates, environment, and Git
commit.

```bash
docker compose up -d --wait redpanda topic-init
uv run python -m benchmarking.run sustained --transport kafka
uv run python -m benchmarking.run burst --transport kafka
uv run python -m benchmarking.run replay --transport kafka
uv run python -m benchmarking.run duplicates --transport kafka
```

The memory transport is a generator/parser capacity diagnostic, not an end-to-end pipeline
benchmark. The smoke scenario is deliberately non-publishable.

## Canonical replay and duplicate proof

After the Kafka profiles, allow Bronze to drain and run bounded Spark replay. Record Bronze
manifest counts, Silver distinct `event_id` counts, elapsed time, Spark CPU/memory, and the daily
reconciliation result. A duplicate test passes only when canonical Silver/Gold contain zero
duplicate identities; the producer's observed input duplicate rate alone is insufficient.

## BigQuery layout comparison

Prepare two tables containing the same event rows: one naive/unpartitioned and one using the
production `event_date` partition plus workload clustering. The comparator never creates or
deletes tables and uses the same parameterized query with cache disabled.

```bash
uv run python -m benchmarking.bigquery_layout \
  --project PROJECT --naive-table PROJECT.BENCHMARK.naive_events \
  --optimized-table PROJECT.BENCHMARK.optimized_events \
  --start-date 2026-01-01 --end-date 2026-02-01
```

The default is a dry run, which measures bytes processed but not execution runtime. Add
`--execute` only after reviewing cost; only an executed run may make a runtime claim. The target
is at least 70% fewer bytes scanned, not a predeclared result.

## Publishing a result

A result can be cited in the README only when `publishable=true`, error count is zero, the JSON
artifact is retained by CI or durable storage, and reconciliation accompanies end-to-end runs.
The claim must name the scenario, event count, environment, Git commit, and measurement method.
Never hand-edit a generated result or infer a production SLO from smoke validation.
