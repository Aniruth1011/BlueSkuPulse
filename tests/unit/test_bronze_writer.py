from __future__ import annotations

import gzip
import json
from pathlib import Path

from bronze.storage import LocalObjectStore
from bronze.writer import BronzeBatchWriter, KafkaRawRecord


def event(event_id: str, collection: str = "app.bsky.feed.post") -> bytes:
    return json.dumps(
        {
            "event_id": event_id,
            "source_version": "jetstream_v2",
            "source_host": "fixture",
            "cursor": int(event_id),
            "event_kind": "commit",
            "operation": "create",
            "did": "did:plc:test",
            "collection": collection,
            "rkey": f"key-{event_id}",
            "at_uri": f"at://did:plc:test/{collection}/key-{event_id}",
            "cid": f"cid-{event_id}",
            "event_time": "2026-10-04T18:15:00Z",
            "ingested_at": "2026-10-04T18:15:01Z",
            "payload_json": "{}",
            "schema_version": 1,
        }
    ).encode()


def records() -> list[KafkaRawRecord]:
    return [
        KafkaRawRecord("bluesky.raw.events", 0, 10, "did:plc:test", event("10")),
        KafkaRawRecord("bluesky.raw.events", 0, 11, "did:plc:test", event("11")),
    ]


def test_bronze_batch_is_partitioned_and_manifested(tmp_path: Path) -> None:
    result = BronzeBatchWriter(LocalObjectStore(tmp_path)).write(records())

    assert result.record_count == 2
    assert len(result.object_keys) == 1
    assert "event_date=2026-10-04/event_hour=18" in result.object_keys[0]
    manifest = json.loads((tmp_path / result.manifest_key).read_text())
    assert manifest["record_count"] == 2
    assert manifest["offsets"]["bluesky.raw.events:0"] == {"first": 10, "last": 11}
    with gzip.open(tmp_path / result.object_keys[0], "rt") as handle:
        assert len(handle.readlines()) == 2


def test_replaying_same_offsets_does_not_create_new_objects(tmp_path: Path) -> None:
    writer = BronzeBatchWriter(LocalObjectStore(tmp_path))
    first = writer.write(records())
    second = writer.write(list(reversed(records())))

    assert second.batch_id == first.batch_id
    assert second.object_keys == first.object_keys
    assert second.replayed is True
    assert len(list(tmp_path.rglob("*.jsonl.gz"))) == 1


def test_malformed_kafka_value_is_preserved(tmp_path: Path) -> None:
    bad = KafkaRawRecord(
        "bluesky.raw.events", 1, 20, None, b"\x00not-json", kafka_timestamp_ms=1_759_600_000_000
    )

    result = BronzeBatchWriter(LocalObjectStore(tmp_path)).write([bad])

    assert "collection=$malformed" in result.object_keys[0]
    with gzip.open(tmp_path / result.object_keys[0], "rt") as handle:
        row = json.loads(handle.readline())
    assert row["event"]["raw_value_base64"] == "AG5vdC1qc29u"
    assert row["event"]["bronze_parse_error"]


def test_local_store_never_overwrites_existing_object(tmp_path: Path) -> None:
    store = LocalObjectStore(tmp_path)
    assert store.put_if_absent("x/object", b"first", content_type="text/plain") is True
    assert store.put_if_absent("x/object", b"second", content_type="text/plain") is False
    assert (tmp_path / "x/object").read_bytes() == b"first"
