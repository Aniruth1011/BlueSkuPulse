# Phases 6–8: analytics products, Airflow, and observability

This document uses the phase numbering from the implementation request: Phase 6 is analytics,
Phase 7 is Airflow, and Phase 8 is observability.

## Problem being solved

Phases 1–5 can collect, preserve, normalize, and warehouse Bluesky events. That is not enough for
an operated data product. We also need consistent answers to analytical questions, a safe way to
run bounded work repeatedly, and immediate evidence when freshness or correctness degrades.

```text
BigQuery Silver -> dbt analytical marts -> Grafana analytics dashboard
                         ^
                         | bounded commands
                       Airflow

Services + Airflow + batch outcomes -> Prometheus -> alerts + health dashboard
```

## Phase 6: analytics products

The dbt layer now exposes:

- hourly network activity and event mix;
- hourly language distribution;
- topic volume, engagement, velocity, acceleration, and anomaly score at 5, 15, and 60 minutes;
- unique-author and maximum-author-concentration topic safeguards;
- domain shares, unique sharers, engagement per share, outbound-link share, velocity, and
  concentration;
- daily and rolling seven-day creator activity;
- post engagement;
- conversation reply count, participants, reconstructable depth, branching factor,
  time-to-first-reply, duration, and orphan count;
- pipeline reconciliation health.

The provisioned analytics dashboard queries these marts directly. It performs only filtering,
ordering, and display. Rankings and definitions stay in dbt so SQL analysis and dashboards agree.

### Topic decision

A topic receives a zero ranking score when fewer than two authors contributed or when one author
created more than half its posts. This is deliberately conservative: it may suppress a genuine
small-community topic, but it prevents a single account from manufacturing a trend.

### Conversation decision

Reply edges are recursively walked from the declared root. The walk stops at depth 100 to contain
cycles or corrupt records. Replies with an unavailable parent chain remain visible as orphans.

## Phase 7: Airflow orchestration

Three Airflow 3 DAGs use the stable `airflow.sdk` authoring interface.

### `hourly_pipeline`

1. Verify Redpanda and the raw topic.
2. Verify Bronze freshness.
3. Verify Silver freshness.
4. Load all six Silver datasets to BigQuery with deterministic merges.
5. Run the hourly dbt graph, including upstream dependencies and tests.
6. Publish dbt outcome metrics.

### `daily_reconciliation`

1. Run dbt source freshness.
2. Run contract, timestamp, duplicate-current-state, folding, and spam-guardrail tests.
3. Rebuild `mart_pipeline_health`.
4. Validate the requested day's unexplained delta is at most 0.1%.
5. Persist a JSON report and publish reconciliation/dbt metrics.

### `historical_backfill`

The manual DAG requires start, end, collections, source mode, target layer, and rebuild choice.
The long-term source is immutable Bronze. Its output path and batch identity are deterministic.
Repeated runs either reuse an existing output or overwrite it only when `rebuild=true`; BigQuery
and dbt remain merge/idempotency protected.

Airflow never starts the permanent Jetstream WebSocket. Every task logs its task name, Airflow run
ID, duration, outcome, and the component-specific batch ID where relevant.

## Phase 8: observability

The local stack contains Prometheus, Alertmanager, Grafana, Airflow's StatsD exporter, and a
Pushgateway for the two service-level batch outcomes. Prometheus scrapes:

- ingestion at port 9108;
- Bronze at 9109;
- Spark at 9110;
- BigQuery loading at 9111;
- Redpanda `/public_metrics`;
- Airflow StatsD-exported metrics;
- dbt and reconciliation batch metrics from Pushgateway.

The platform-health dashboard covers throughput, p50/p95/p99 lag, cursor age, Kafka lag, DLQ rate,
Spark rates, reconnects, load failures, Airflow failures, and reconciliation. Twelve alert rules
cover source silence, freshness, lag, checkpoint age, DLQ, Spark duration, BigQuery failure, dbt
failure/freshness, reconciliation, and Airflow task failure.

Alertmanager's repository configuration deliberately has a local receiver with no external
destination. Production must add an approved Slack, PagerDuty, email, or cloud-notification
receiver; the repository does not invent a destination or commit a webhook secret.

## Running locally

Set non-empty local passwords in `.env`, then:

```bash
make up
make up-orchestration AIRFLOW_DB_PASSWORD='<local password>'
make up-observability GRAFANA_ADMIN_PASSWORD='<local password>'
```

- Redpanda Console: <http://localhost:8080>
- Airflow API/UI: <http://localhost:8081>
- Grafana: <http://localhost:3000>
- Prometheus: <http://localhost:9090>
- Alertmanager: <http://localhost:9093>

Run local acceptance checks with `make phase6-8-check`. Run container-level validation with
`make airflow-validate` and `make monitoring-validate` after the images are available.

## Validation status

The deterministic local dbt build executes 28 models and 88 data tests. Python tests cover interval
identity, backfill validation, freshness, warehouse URI selection, reconciliation fail-closed
behavior, dashboards, alerts, and DAG contracts. The Airflow image and DAG imports, Prometheus
configuration and 12 rules, Alertmanager configuration, Grafana JSON, and Compose file have been
validated.

This is functional correctness evidence, not a production availability benchmark. The 2026-10-05
local drill subsequently measured Prometheus detection at 17.605 seconds and Alertmanager routing
at 31.385 seconds. Live reconnect, Bronze latency, resource use, and local dashboard-query metrics
are recorded in `docs/benchmark.md`. Live GCP execution, BigQuery dashboard latency, external
notification delivery, and scheduled Airflow success remain blocked or unmeasured.

## Official implementation references

- [Airflow 3 public interface](https://airflow.apache.org/docs/apache-airflow/stable/public-airflow-interface.html)
- [Airflow best practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [Airflow metrics](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/metrics.html)
- [Prometheus Pushgateway guidance](https://prometheus.io/docs/practices/pushing/)
- [Grafana provisioning](https://grafana.com/docs/grafana/latest/administration/provisioning/)
- [Grafana BigQuery data source](https://grafana.com/docs/plugins/grafana-bigquery-datasource/latest/configure/)
- [Redpanda monitoring](https://docs.redpanda.com/streaming/current/manage/monitoring/)
