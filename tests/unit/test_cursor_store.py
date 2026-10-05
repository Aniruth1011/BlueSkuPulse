from __future__ import annotations

import json

import pytest

from ingestion.cursor_store import CursorMismatchError, FileCursorStore, SourceCursor


def test_cursor_round_trip_is_namespaced(tmp_path) -> None:
    path = tmp_path / "cursor.json"
    store = FileCursorStore(path)
    cursor = SourceCursor("jetstream_v2_seq", "jetstream.example", 42)

    store.save(cursor)

    assert store.load(kind="jetstream_v2_seq", source_host="jetstream.example") == cursor
    assert json.loads(path.read_text()) == {
        "kind": "jetstream_v2_seq",
        "source_host": "jetstream.example",
        "value": 42,
    }


def test_cursor_version_mismatch_fails_closed(tmp_path) -> None:
    store = FileCursorStore(tmp_path / "cursor.json")
    store.save(SourceCursor("jetstream_v1_time_us", "legacy.example", 1_700_000_000_000_000))

    with pytest.raises(CursorMismatchError, match="explicit migration"):
        store.load(kind="jetstream_v2_seq", source_host="legacy.example")


def test_cursor_host_mismatch_fails_closed(tmp_path) -> None:
    store = FileCursorStore(tmp_path / "cursor.json")
    store.save(SourceCursor("jetstream_v2_seq", "east.example", 100))

    with pytest.raises(CursorMismatchError):
        store.load(kind="jetstream_v2_seq", source_host="west.example")
