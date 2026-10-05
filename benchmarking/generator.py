from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta


def v2_frame(seq: int, *, logical_seq: int | None = None, additive_field: bool = False) -> bytes:
    """Build a deterministic, valid Jetstream v2-shaped frame.

    logical_seq allows forced redelivery: the transport sequence can advance while the record
    identity remains identical. This mirrors at-least-once delivery without random fixtures.
    """
    identity = seq if logical_seq is None else logical_seq
    payload: dict[str, object] = {
        "$type": "network.bsky.jetstream.subscribeEvents#commit",
        "seq": seq,
        "did": f"did:plc:benchmark{identity % 100_000:05d}",
        "time": (datetime(2026, 1, 1, tzinfo=UTC) + timedelta(microseconds=seq)).isoformat(),
        "rev": f"rev-{identity}",
        "operation": "create",
        "collection": "app.bsky.feed.post",
        "rkey": f"post-{identity}",
        "cid": f"bafybenchmark{identity}",
        "record": {
            "$type": "app.bsky.feed.post",
            "createdAt": "2026-01-01T00:00:00Z",
            "text": f"benchmark event {identity}",
            "langs": ["en"],
        },
    }
    if additive_field:
        payload["future_additive_field"] = {"version": 2, "safe": True}
    return json.dumps(
        {"$type": "message", "payload": payload},
        sort_keys=True,
        separators=(",", ":"),
    ).encode()


def generated_frames(count: int, *, duplicate_fraction: float = 0.0) -> Iterator[bytes]:
    if count < 1:
        raise ValueError("count must be positive")
    if not 0.0 <= duplicate_fraction < 1.0:
        raise ValueError("duplicate_fraction must be in [0, 1)")
    duplicate_every = round(1 / duplicate_fraction) if duplicate_fraction else 0
    previous_unique = 0
    for seq in range(1, count + 1):
        if duplicate_every and seq % duplicate_every == 0:
            yield v2_frame(seq, logical_seq=previous_unique)
        else:
            previous_unique = seq
            yield v2_frame(seq)
