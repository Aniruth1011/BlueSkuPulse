# Benchmark policy and runbook

## Current evidence status

All engineering SLOs remain **TARGET** until the exact workload completes in a versioned,
described environment. The workspace used to implement this harness is not a Git repository, so
its smoke output is `VALIDATION` evidence and cannot be promoted to an achieved result.

| Scenario | Exact workload | Required transport | Current status |
|---|---:|---|---|
| Sustained ingestion | 1,800,000 events at 1,000 events/s for 30 min | Kafka/Redpanda | TARGET |
| Burst ingestion | 1,500,000 events at 5,000 events/s for 5 min | Kafka/Redpanda | TARGET |
| Historical replay | at least 5,000,000 events | Kafka plus Bronze/Spark replay | TARGET |
| Duplicate delivery | 1,000,000 events, 10% forced duplicates | Kafka plus canonical Silver check | TARGET |
| BigQuery layout | same bounded query, naive vs partitioned/clustered | BigQuery, cache disabled | TARGET |

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

