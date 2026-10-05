# Phase 2–4 operations runbook

## Log contract

All Python services emit one-line JSON with UTC timestamp, severity, logger, message, and stable
context fields. Use `INFO` for lifecycle and durable batch boundaries, `DEBUG` for acknowledged
events and cursor movement, `WARNING` for reconnects, advisories, malformed data and DLQ writes,
and `ERROR` (with stack trace) for failed Kafka, Bronze, or Spark operations.

Never log the entire event payload at INFO. Payloads can be large and may contain user content;
the immutable Bronze/DLQ record is the forensic source. Correlate with `event_id`, cursor, Kafka
partition/offset, or Bronze `batch_id`.

## Recovery sequence

1. Check `bluesky_cursor_checkpoint_age_seconds`, reconnect count, and the latest structured error.
2. Restore Kafka/Redpanda availability before restarting ingestion. The saved cursor is still the
   last acknowledged item; never delete it or reset it to now.
3. A crash after Kafka acknowledgement but before cursor persistence causes safe redelivery.
   Deterministic `event_id` removes that duplicate in canonical Silver.
4. Restart Bronze with the same consumer group. Its offsets only advance after create-only objects
   and the deterministic manifest are durable, so a retry completes or reuses the same keys.
5. For data older than the live watermark, run bounded Spark replay from Bronze to a new versioned
   output prefix. Validate counts and digests before promoting that prefix.

## Data failure handling

- Source contract failures go to `bluesky.dlq`, including raw text, error, source version, and a
  best-effort cursor. A durable DLQ acknowledgement may advance that cursor because the bytes are
  now recoverable.
- Non-envelope Kafka values are preserved byte-for-byte in Bronze as base64 and are not hidden.
- Valid envelopes that fail canonical required-field checks go to Silver `quarantine` with the
  original envelope and `quality_errors`.
- Additive source fields remain in `payload_json`; they do not crash the stable Silver envelope.

## Verification

Run `make check`. Spark tests need Java 17 and permission to bind a loopback JVM gateway. The suite
covers cursor-after-ack behavior, forced disconnect and inclusive replay, Bronze retry
idempotency, malformed-byte preservation, duplicate canonicalization, additive schema drift, and
create/update/delete folding. Throughput and latency remain `TARGET` until the benchmark phase is
executed and recorded with environment and Git commit.
