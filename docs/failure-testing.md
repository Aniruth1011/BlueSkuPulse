# Failure-injection runbook

Failure testing is split into deterministic CI tests and operator-controlled Docker drills. The
tests prove invariants. The drills measure recovery behavior in the integrated local stack. Neither
is silently presented as a production result.

## Deterministic CI suite

Run:

```bash
uv run python scripts/run_failure_tests.py --include-dbt-failure
```

| Injection | Expected invariant | Automated evidence |
|---|---|---|
| Ingestion process restart | inclusive cursor replays the boundary event; deterministic ID makes it deduplicable | restart test |
| Broker publish failure | cursor does not advance before acknowledgement; retry republishes the event | broker failure test |
| Malformed JSON/contract | original bytes and best-effort cursor reach DLQ; following valid data proceeds | malformed test |
| Additive source field | parser accepts it and immutable payload retains it | schema-drift test |
| 10% duplicate delivery | duplicates retain identical event IDs; canonical distinct count is stable | duplicate test plus Spark integration |
| BigQuery load failure | exception is visible, failure metric path runs, temporary cleanup is attempted | loader failure test |
| dbt test failure | dbt exits non-zero and the harness treats detection as success | opt-in forced dbt test |
| Spark restart | checkpoint restart rejects replayed event IDs | Bronze-to-Silver integration test |

The forced dbt test is disabled during normal builds and can only be enabled with
`--vars '{inject_failure: true}'`.

## Integrated Docker drills

Start the relevant profiles, establish a baseline topic/manifest/reconciliation count, and invoke
one explicit drill:

```bash
docker compose --profile pipeline up -d --build
uv run python -m reliability.compose_scenarios ingestion-restart
uv run python -m reliability.compose_scenarios broker-outage --outage-seconds 10
uv run python -m reliability.compose_scenarios consumer-outage --outage-seconds 30
uv run python -m reliability.compose_scenarios spark-restart
```

The command touches only a named service in the current `atmospulse` Compose project. It logs the
injection and observed service-running recovery time to JSON. Service recovery alone does not prove
row correctness: run daily reconciliation afterward and attach its result.

For each drill record setup, Git SHA, event interval, expected behavior, observed behavior,
recovery time, Bronze/Silver/Gold row impact, alert fired, and follow-up. If any field is unknown,
write `NOT MEASURED`; never substitute an estimate.

## WebSocket disconnect

The deterministic socket test forces a connection failure and confirms reconnect with the last
inclusive v2 sequence. For a live drill, interrupt only the ingestion container's outbound network,
restore it, confirm reconnect metrics, and reconcile the cursor interval. Do not reset the cursor
to “now.”

## Alert expectations

- source disconnect/restart: reconnect counter and checkpoint-age alerts;
- broker/consumer outage: Kafka consumer lag and Bronze freshness alerts;
- Spark outage: Silver freshness and processing-failure alerts;
- BigQuery/dbt failure: load failure, Gold freshness, and dbt test alerts;
- malformed burst: DLQ-rate alert.

If an expected alert does not fire, the scenario is incomplete even if data later recovers.

## Measured 2026-10-05 results

The public-v2 live run measured five deliberate 30-second network interruptions. Consumption
reconnected from the persisted inclusive cursor in 31.804 seconds p50 and 32.837 seconds p95.
Fifteen detected socket failures reconnected in 0.685 seconds p50 and 0.763 seconds p95 after
detection. At-least-once overlap produced 15 duplicate deliveries in the bounded window; the
subsequent exact-date batch replay folded all redeliveries to zero duplicate Silver IDs.

The malformed-event drill sent 10,000 frames with every 100th frame invalid. Exactly 100 reached
the DLQ, 9,900 reached the raw topic, and none disappeared. The additive-drift drill sent 10,000
events with a previously unknown optional field; all 10,000 reached raw and none reached DLQ.

An injected BigQuery-load-failure metric was detected by Prometheus in 17.605 seconds and routed
to Alertmanager in 31.385 seconds. This validates local rule evaluation and routing, but not an
external notification channel.

The live Spark failure is retained as evidence: an old checkpoint forced the legacy heap-backed
200-partition state store, causing nine restarts and no valid live Silver output. The independent
batch rebuild recovered all 604,243 distinct date-partition IDs with zero duplicates and a 0.000%
reconciliation delta. Operators must migrate or explicitly retire incompatible checkpoints during
state-store configuration changes; silently reusing them is unsafe.
