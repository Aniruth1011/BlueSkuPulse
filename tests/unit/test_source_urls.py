from __future__ import annotations

from urllib.parse import parse_qs, urlsplit

from ingestion.cursor_store import SourceCursor
from ingestion.sources.jetstream_v1 import JetstreamV1Source
from ingestion.sources.jetstream_v2 import JetstreamV2Source


def test_v1_uses_legacy_parameter_names_and_safety_rewind() -> None:
    source = JetstreamV1Source(
        "wss://legacy.example/subscribe", ("app.bsky.feed.post", "app.bsky.feed.like")
    )
    cursor = SourceCursor("jetstream_v1_time_us", "legacy.example", 9_000_000)

    query = parse_qs(urlsplit(source._subscription_url(cursor)).query)

    assert query["wantedCollections"] == ["app.bsky.feed.post", "app.bsky.feed.like"]
    assert query["cursor"] == ["7000000"]
    assert "collections" not in query


def test_v2_uses_v2_filters_and_inclusive_seq() -> None:
    source = JetstreamV2Source(
        "wss://v2.example/xrpc/network.bsky.jetstream.subscribeEvents",
        ("app.bsky.feed.post",),
    )
    cursor = SourceCursor("jetstream_v2_seq", "v2.example", 1234)

    query = parse_qs(urlsplit(source._subscription_url(cursor)).query)

    assert query == {
        "collections": ["app.bsky.feed.post"],
        "cursor": ["1234"],
    }
    assert source._subprotocols() == ["xrpc.v1.json"]
