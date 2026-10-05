# AtmosPulse architecture

`AGENTS.md` is the architectural source of truth. The system is delivered in independently
testable vertical slices.

## Implemented Phase 1–8 data path

```text
Jetstream v1 / Jetstream v2 / deterministic fixture
                         |
                         v
            async source adapter + parser
                         |
              valid -----+----- invalid
                |                  |
                v                  v
      bluesky.raw.events      bluesky.dlq
                |                 |
                +--- Kafka acknowledgement ---> versioned cursor checkpoint
                |
                +---> immutable Bronze JSONL + manifest
                |
                +---> Spark Structured Streaming / bounded replay
                              |
                              +-- canonical Silver Parquet mutation history
                              +-- Silver quarantine
                                          |
                                          v
                               BigQuery partitioned landing
                                          |
                                          v
                              dbt dimensions / facts / marts
                                          |
                                          v
                            Grafana analytical products
```

Kafka acknowledgement is the first platform durability boundary. The source cursor advances only
after the corresponding raw or DLQ message is acknowledged. A crash between Kafka acknowledgement
and checkpoint persistence causes safe redelivery, not loss. Later phases deduplicate deterministic
`event_id` values in canonical Silver. Bronze offsets commit only after create-only data objects
and their deterministic manifest are durable. Spark live state is checkpointed; replay reuses the
same transformations without a live watermark.

## Implemented control and observability planes

```text
Airflow 3
  |-- hourly freshness -> BigQuery merges -> dbt build -> quality metrics
  |-- daily contracts -> reconciliation -> persisted report
  `-- explicit Bronze backfill -> Silver -> BigQuery -> affected Gold

Long-running service metrics ----\
Redpanda public metrics ----------+--> Prometheus --> Alertmanager
Airflow StatsD -------------------+         |
dbt/BQ/reconciliation Pushgateway-/         `--> Grafana platform health
```

Airflow orchestrates bounded hourly, reconciliation, and backfill workflows. It never owns the
permanent WebSocket connection. dbt remains the owner of analytics definitions; Grafana performs
only filtering and presentation. See `docs/phase-6-8-implementation.md` for the task graph, metrics,
alerts, and trade-offs.
