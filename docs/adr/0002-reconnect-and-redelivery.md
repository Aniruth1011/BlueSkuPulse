# ADR 0002: Prefer redelivery over cursor gaps

Status: accepted

## Decision

Jetstream adapters reconnect indefinitely with capped exponential backoff, jitter, heartbeat/idle
detection, and their last durably handled cursor. V1 rewinds two seconds because its public cursor
is a witnessed-time value; v2 resumes its inclusive sequence number. Kafka publishing uses
idempotent producer mode with `acks=all`. The ingestion loop processes one source item across the
durability boundary before requesting the next item.

## Tradeoffs

- Sequential acknowledgement limits peak producer throughput but makes cursor ownership and
  failure behavior obvious. Producer batching will be introduced only with a batch-level cursor
  invariant and benchmarks.
- V1 rewind and v2 inclusive replay intentionally create duplicates. Deterministic IDs make this
  safer than risking a gap.
- Retries are capped in delay but not count: a permanent outage remains visible through logs,
  metrics, and stale-checkpoint alerts rather than silently switching source or jumping to now.
- The Python process is the long-running ingestion service. Airflow must not supervise individual
  WebSocket messages.

