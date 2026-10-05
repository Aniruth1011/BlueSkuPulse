# ADR 0005: Bronze uses size-or-deadline batching

- Status: Accepted
- Date: 2026-10-05

## Context

The first live Jetstream measurement showed that `AIOKafkaConsumer.getmany()` can return as soon
as records are available. Writing every non-empty poll produced many tiny immutable objects even
though the configured maximum batch size was 10,000. The old behavior treated the poll timeout as
a minimum accumulation window, which Kafka does not guarantee.

## Decision

The Bronze consumer accumulates uncommitted Kafka records in memory until either:

- the configured record limit is reached; or
- the configured monotonic flush deadline expires.

Offsets are committed only after the accumulated batch and its manifest are durably written. A
crash before the flush leaves offsets uncommitted, so Kafka safely redelivers those records.

## Consequences

This bounds normal landing latency by the flush interval while preventing one-object-per-poll
behavior. A batch may still create several objects because the immutable layout partitions by
event date, hour, and collection. Lower flush intervals trade larger object counts for lower
source-to-Bronze latency; production values must be selected from measured traffic and storage
costs.
