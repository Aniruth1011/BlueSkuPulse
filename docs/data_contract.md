# Source event contract

AtmosPulse retains the original JSON in `payload_json` and adds a stable platform envelope.

| Field | Meaning |
|---|---|
| `event_id` | SHA-256 deterministic identity from stable source mutation fields |
| `source_version` | `jetstream_v1` or `jetstream_v2` |
| `source_host` | Deployment host used for this delivery |
| `cursor` | Version-specific resume position |
| `event_kind` | `commit`, `identity`, `account`, or `sync` |
| `operation` | Commit mutation or lifecycle kind |
| `did` | Repository/account DID |
| `collection`, `rkey`, `at_uri`, `cid` | Record identity when the event is a commit |
| `event_time` | Source/Jetstream display timestamp |
| `ingested_at` | Time AtmosPulse first parsed this delivery |
| `payload_json` | Canonical JSON representation of the complete source frame |
| `schema_version` | AtmosPulse envelope version |

## Deterministic identity

Commit identity material is:

```text
(source_version, did, collection, rkey, operation, cid-or-record-revision)
```

This distinguishes creates, updates, and deletes while making redelivery of the same source
mutation stable. Non-commit lifecycle events use source version, kind, DID, cursor, and operation.

## Cursor compatibility

| Adapter | Endpoint | Persisted cursor | Resume behavior |
|---|---|---|---|
| `JetstreamV1Source` | legacy `/subscribe` | Unix microseconds from `time_us` | reconnect with a two-second safety rewind |
| `JetstreamV2Source` | `/xrpc/network.bsky.jetstream.subscribeEvents` | monotonic `seq` | inclusive; last event is redelivered |
| `FixtureSource` | local JSONL emulating v1 or v2 | matching emulated cursor | matching inclusive fixture replay |

The cursor file also stores its source host and kind. A mismatch fails closed. There is no fallback
to “now.”

V2 `collections` limits commit events only. AtmosPulse deliberately omits the `kinds` filter so
identity, account, and sync lifecycle events remain available.

## Evolution and bad data

Unknown additive source fields remain in `payload_json` and do not affect event identity. Missing
required fields, invalid JSON, and unsupported event kinds are published to `bluesky.dlq` with the
original payload and failure reason. A DLQ acknowledgement is durable handling and may advance a
known source cursor; a record never disappears silently.

## Authoritative protocol references

- https://github.com/bluesky-social/jetstream-legacy
- https://github.com/bluesky-social/jetstream/blob/main/docs/README.md
- https://github.com/bluesky-social/jetstream/blob/main/lexicons/network/bsky/jetstream/subscribeEvents.json
