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

Unmeasured SLOs remain **TARGETS**. The local ingestion, replay, deduplication, and failure results
in the Evidence section are measured results from the named commits and environment; they are not
claims about production or GCP performance.

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

Measured on 2026-10-05 on an Apple-silicon MacBook Air. Kafka/Redpanda, Bronze, and Spark ran in a
2-vCPU Colima VM. Producer profiles used 4 GiB VM memory; the corrected large-state Spark replay
used 8 GiB. The host reported macOS 26.6, arm64, 10 logical CPUs, and Python 3.14.8. Full methods,
limitations, and artifact names are in [`docs/benchmark.md`](docs/benchmark.md).

| Claim | Measured result | Reproduce | Commit |
|---|---:|---|---|
| Sustained broker-acknowledged ingestion | 1,800,000 events in 1,800.020s; 999.989 events/s; p95 acknowledgement latency 974.176ms; 0 errors | `./scripts/metrics/run.sh full` | `044a4461fe88105db69b3add1a55ca08b6fb8df7` |
| Five-minute burst | 1,500,000 events in 300.023s; 4,999.613 events/s; p95 acknowledgement latency 199.272ms; 0 errors | same command | `044a4461fe88105db69b3add1a55ca08b6fb8df7` |
| Five-million-event unthrottled publish replay | 5,000,000 events in 247.773s; 20,179.722 events/s; p95 acknowledgement latency 48.661ms; 0 errors | same command | `044a4461fe88105db69b3add1a55ca08b6fb8df7` |
| Canonical duplicate handling | 9,320,000 broker deliveries folded to 5,000,000 distinct Silver event IDs; 0 canonical duplicates and 0 quarantine rows | `./scripts/metrics/run.sh replay-failures` plus `benchmarking.verify_silver` | pipeline `32b2684371e1e118ac10bc68c491026e42e3051d`; verifier `d4d41636905db68380f9720b7d5ad936827ad3b6` |
| Immutable Bronze batch replay | 28,066 Bronze data objects rebuilt to 5,000,000 duplicate-free canonical events in approximately 319s | same replay command | `d4d41636905db68380f9720b7d5ad936827ad3b6` |
| Controlled service recovery | broker 12.022s including a 10s outage; Bronze consumer 31.401s including a 30s outage; Spark process restart 0.155s | same replay command | `d4d41636905db68380f9720b7d5ad936827ad3b6` |
| Live Jetstream v2 to Bronze | 505,195 deliveries in 15.57 min; ingest-to-Bronze p95 10.190s, but source-to-Bronze p95 294.116s (SLO failed) | bounded live run described in `docs/benchmark.md` | `8c9d35c` |
| Forced live reconnect | five 30s outages; reconnect p50 31.804s and p95 32.837s | same live run | `8c9d35c` |
| Live-date Bronze/Silver reconciliation | 604,266 raw deliveries, 604,243 distinct IDs, 604,243 Silver rows, 0 duplicates, 0 quarantine, 0.000% unexplained delta | batch replay plus `benchmarking.verify_silver` | `4c5c811` |
| Malformed and additive-drift handling | 100/100 malformed frames reached DLQ; 10,000/10,000 additive-field events survived with 0 DLQ | isolated-topic injection described in `docs/benchmark.md` | `4c5c811` |
| Critical alert detection | Prometheus fired in 17.605s; Alertmanager received in 31.385s | injected failure counter | `4c5c811` |

The service-recovery harness proves that each service returned to `running`; it does not infer row
impact. The separate Silver verifier proves duplicate-free canonical outputs for the retained test
corpus. The live run passed reconnect and Bronze/Silver reconciliation targets but failed the
source-to-Bronze latency target and failed to produce a valid streaming-Silver latency sample.
BigQuery bytes scanned, BigQuery dashboard latency, Gold freshness, deployed Airflow success, and
Terraform apply evidence remain **BLOCKED** because the configured GCP credential requires
interactive reauthentication. Local DuckDB results are not used as substitutes.

Phase 6 functional validation executes 28 dbt models and 88 dbt data tests against the
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
