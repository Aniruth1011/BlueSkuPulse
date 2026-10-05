# ADR 0001: Kafka acknowledgement is the ingestion durability boundary

Status: accepted

## Decision

The ingestion process persists a source cursor only after Redpanda/Kafka acknowledges the raw
event, or acknowledges a DLQ record for an invalid event. Cursor records are namespaced by source
wire version and source host.

## Consequences

A crash after Kafka acknowledgement but before cursor persistence can redeliver an event. This is
intentional at-least-once behavior and requires deterministic event IDs and downstream
deduplication. A broker failure cannot advance the cursor. V1 `time_us` and v2 `seq` cursors cannot
be loaded by the wrong adapter.

## Sources

- https://github.com/bluesky-social/jetstream-legacy
- https://github.com/bluesky-social/jetstream/blob/main/docs/README.md
- https://github.com/bluesky-social/jetstream/blob/main/lexicons/network/bsky/jetstream/subscribeEvents.json
