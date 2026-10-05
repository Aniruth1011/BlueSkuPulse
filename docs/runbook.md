# AtmosPulse operations runbook

## First checks

1. Open the platform-health Grafana dashboard and identify the first stale layer.
2. Confirm the corresponding Prometheus target is `UP`; missing telemetry is not evidence that the
   service is healthy.
3. Use the Airflow run ID, deterministic load batch ID, or Kafka coordinates from structured logs
   to correlate the failure.
4. Do not reset a Jetstream cursor, delete a Spark checkpoint, mutate Bronze, or skip a failed
   batch merely to restore a green dashboard.

## Source silence or stale cursor

- Check ingestion process health and WebSocket reconnect logs.
- Verify the persisted cursor source version and host match the configured adapter.
- Restart ingestion normally; it resumes from the durable cursor and may redeliver safe overlap.
- Escalate before moving or deleting the cursor file.

## Kafka lag

- Confirm Redpanda is healthy and disk is available.
- Identify the lagging consumer group and inspect its consumer logs.
- Restart the consumer from its committed offset. Never advance offsets manually without a
  recorded loss decision.

## Bronze or Silver stale

- Bronze: inspect the newest manifest and its object digests before restarting the sink.
- Silver: inspect Spark query termination, checkpoint, quarantine, and processing-rate metrics.
- Replay the bounded Bronze interval through `historical_backfill`; do not edit raw objects.

## BigQuery or dbt failure

- Retry the same Airflow interval. Load job IDs and event merges are deterministic.
- For schema failures, confirm the change is additive; breaking Silver changes require a migration
  and backfill.
- Do not bypass a critical dbt test. Investigate its returned rows and quarantine or correct logic.

## Reconciliation delta

- Compare distinct Silver events to modeled mutations, then account separately for quarantine,
  duplicates, deletes, updates, and unsupported lifecycle records.
- A raw count and current-state count are not expected to match without folding semantics.
- Rebuild the affected interval twice and confirm canonical counts are stable.

## Airflow failure

- Retry only the failed task for the same data interval.
- Every task is bounded and idempotent; never change its interval to make a retry pass.
- Airflow is not responsible for restarting the permanent ingestion WebSocket.
