from __future__ import annotations

import json

from ingestion.sources.parsing import parse_v1, parse_v2


def _v2_frame(*, extra: bool = False) -> bytes:
    payload = {
        "$type": "network.bsky.jetstream.subscribeEvents#commit",
        "seq": 500,
        "did": "did:plc:testactor",
        "time": "2026-10-04T12:00:00.123456Z",
        "rev": "3mtestrev",
        "operation": "create",
        "collection": "app.bsky.feed.post",
        "rkey": "3mtestrkey",
        "cid": "bafyreitestcid",
        "record": {
            "$type": "app.bsky.feed.post",
            "text": "hello",
            "createdAt": "2026-10-04T12:00:00.000Z",
        },
    }
    if extra:
        payload["unknownFutureField"] = {"nested": True}
    return json.dumps({"$type": "message", "payload": payload}).encode()


def test_v2_parser_builds_canonical_envelope() -> None:
    event, cursor = parse_v2(_v2_frame(), "v2.example")

    assert cursor.kind == "jetstream_v2_seq"
    assert cursor.value == 500
    assert event.at_uri == "at://did:plc:testactor/app.bsky.feed.post/3mtestrkey"
    assert event.operation == "create"
    assert event.cursor == 500


def test_additive_v2_field_does_not_change_identity() -> None:
    original, _ = parse_v2(_v2_frame(), "v2.example")
    evolved, _ = parse_v2(_v2_frame(extra=True), "v2.example")

    assert evolved.event_id == original.event_id
    assert "unknownFutureField" in evolved.payload_json


def test_duplicate_delivery_has_deterministic_event_id() -> None:
    first, _ = parse_v2(_v2_frame(), "v2.example")
    second, _ = parse_v2(_v2_frame(), "v2.example")

    assert first.event_id == second.event_id


def test_v1_uses_time_us_cursor_namespace() -> None:
    raw = json.dumps(
        {
            "did": "did:plc:testactor",
            "time_us": 1_760_000_000_000_000,
            "kind": "commit",
            "commit": {
                "rev": "3mlegacyrev",
                "operation": "delete",
                "collection": "app.bsky.feed.post",
                "rkey": "3mlegacykey",
            },
        }
    ).encode()

    event, cursor = parse_v1(raw, "legacy.example")

    assert cursor.kind == "jetstream_v1_time_us"
    assert cursor.value == 1_760_000_000_000_000
    assert event.operation == "delete"
    assert event.cid is None
