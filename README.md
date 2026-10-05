# AtmosPulse

AtmosPulse is a production-style real-time data platform for public Bluesky / AT Protocol
activity. The architecture and acceptance criteria are defined in `AGENTS.md` and `SKILLS.md`.

Current status: **Phases 1–10 implemented** under the requested phase plan: ingestion through
warehouse, analytics products, Airflow orchestration, observability, infrastructure/CI, and
reproducible reliability/benchmark tooling. The implemented paths are:

```text
FixtureSource or Jetstream v1/v2 -> async Python ingestion -> Redpanda/Kafka
                                      |-> immutable Bronze JSONL + manifests
                                      |-> Spark -> canonical Silver Parquet
                                                     |-> BigQuery landing
                                                     |-> dbt facts/dims/marts
                                                            |-> Grafana analytics

Airflow -> bounded hourly/reconciliation/backfill commands
Services + batch outcomes -> Prometheus -> Alertmanager + Grafana health
```

All SLO and throughput values are **TARGETS** until the benchmark harness produces measured
results. No performance result is claimed in this repository yet.

## Local quick start

Prerequisites: Docker, Python 3.12–3.14, `uv`, and Java 17 for Spark.

```bash
cp .env.example .env
make install
make up
make ingest-fixture
make bronze-once
```

Redpanda Console is available at <http://localhost:8080>. The fixture command publishes a
deterministic set of v2-shaped events to `bluesky.raw.events`. A source cursor is persisted only
after Kafka acknowledges each event.

Run bounded replay from local Bronze (the glob deliberately excludes manifests):

```bash
make spark-replay \
  BRONZE_PATH='data/bronze/bluesky/event_date=*/event_hour=*/collection=*/*.jsonl.gz' \
  OUTPUT_PATH='data/silver-rebuild/run-001'
```

On macOS with Homebrew's keg-only JDK, set
`JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home` before Spark commands.
Live Spark also resolves the official Spark Kafka connector package configured by
`ATMOSPULSE_SPARK_KAFKA_PACKAGE`; production images should pre-bake that artifact.

Run checks with:

```bash
make check
```

Run the deterministic local Phase 5 warehouse build with:

```bash
make dbt-deps
make dbt-seed
make dbt-build
make dbt-build
make dbt-docs
```

The repeated build validates incremental rerun behavior. Production BigQuery loading requires a
GCP project and Application Default Credentials; see
[`docs/phase-5-warehouse.md`](docs/phase-5-warehouse.md).

Run the Phase 6–8 acceptance suite with:

```bash
make phase6-8-check
```

Validate Phase 9–10 code and run the safe local failure/benchmark checks with:

```bash
make terraform-validate      # requires Terraform >=1.8
make docker-build
make phase9-10-check
```

The full target profiles are intentionally explicit and write provenance-rich JSON artifacts:

```bash
uv run python -m benchmarking.run sustained --transport kafka
uv run python -m benchmarking.run burst --transport kafka
uv run python -m benchmarking.run replay --transport kafka
uv run python -m benchmarking.run duplicates --transport kafka
```

See [`docs/phase-9-10-implementation.md`](docs/phase-9-10-implementation.md),
[`docs/failure-testing.md`](docs/failure-testing.md), and
[`docs/benchmark.md`](docs/benchmark.md). No target is marked achieved merely because the harness
exists.

For a shorter operator workflow, use the packaged command runner instead of entering each command
manually:

```bash
./scripts/metrics/run.sh help
./scripts/metrics/run.sh validate
./scripts/metrics/run.sh smoke
```

See [`scripts/metrics/README.md`](scripts/metrics/README.md) for the terminal layout and protected
cloud commands.

Start orchestration and observability after setting non-empty local passwords:

```bash
make up-orchestration AIRFLOW_DB_PASSWORD='<local password>'
make up-observability GRAFANA_ADMIN_PASSWORD='<local password>'
```

Airflow is served at <http://localhost:8081>, Grafana at <http://localhost:3000>, Prometheus at
<http://localhost:9090>, and Alertmanager at <http://localhost:9093>. The provisioned business
dashboard requires BigQuery credentials supplied outside the repository. See
[`docs/phase-6-8-implementation.md`](docs/phase-6-8-implementation.md) and
[`docs/runbook.md`](docs/runbook.md).

## Source compatibility

- `JetstreamV1Source` speaks the legacy `/subscribe` JSON wire and persists Unix-microsecond
  `time_us` cursors.
- `JetstreamV2Source` speaks `network.bsky.jetstream.subscribeEvents`, negotiates
  `xrpc.v1.json`, and persists inclusive monotonic `seq` cursors.
- `FixtureSource` replays deterministic JSONL without a network dependency.

Cursor stores include the source version and host. The process fails closed on a mismatch; it
never silently resets to the live tip.

## Evidence

Measured evidence will be added only after the corresponding benchmark has run. See
`docs/benchmark.md` for the required reporting format.

Phase 6 functional validation currently executes 28 dbt models and 88 dbt data tests against the
deterministic local warehouse fixture. Incremental reruns and a separate full refresh produce the
same canonical fact/current-state counts. These are correctness checks, not performance results.

The Airflow 3 image builds and all three DAGs import without errors. Prometheus validates the
scrape configuration and all 12 alert rules; Alertmanager validates its routing configuration.
These checks prove configuration integrity, not production availability or SLO achievement.

## Reliability and data contracts

- Jetstream reconnect resumes from the last Kafka-acknowledged, versioned cursor. V1 rewinds two
  seconds; v2 intentionally replays its inclusive last sequence.
- Kafka/DLQ acknowledgement is the ingestion durability boundary. Cursor files never advance
  before it.
- Bronze files are deterministic, compressed, create-only micro-batches. Exact malformed bytes
  are retained under `$malformed`; Kafka offsets commit only after data and manifest durability.
- Silver retains immutable create/update/delete mutations. Live event-ID deduplication uses a
  48-hour watermark; bounded Bronze replay performs unbounded deterministic deduplication.
- Invalid contracts are written to Silver quarantine rather than discarded.

The full decision/tradeoff matrix is in
[`docs/phase-2-4-decisions.md`](docs/phase-2-4-decisions.md), with detailed ADRs in `docs/adr/`.
For a complete problem statement and Phase 1–4 implementation walkthrough, see
[`docs/phase-1-4-implementation.md`](docs/phase-1-4-implementation.md).
