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
| BigQuery layout | same bounded query, naive vs partitioned/clustered | BigQuery, cache disabled | TARGET |

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
