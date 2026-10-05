from __future__ import annotations

import json
from pathlib import Path

import pytest

from ingestion.cursor_store import FileCursorStore
from ingestion.models.events import DeadLetterRecord, EventEnvelope
from ingestion.producer import IngestionRunner
from ingestion.sources.fixture import FixtureSource


class RecordingPublisher:
    def __init__(self, *, fail_on_event: bool = False) -> None:
        self.events: list[EventEnvelope] = []
        self.dead_letters: list[DeadLetterRecord] = []
        self.fail_on_event = fail_on_event
        self.started = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.started = False

    async def publish_event(self, event: EventEnvelope) -> None:
        if self.fail_on_event:
            raise RuntimeError("broker unavailable")
        self.events.append(event)

    async def publish_dlq(self, record: DeadLetterRecord) -> None:
        self.dead_letters.append(record)


def write_fixture(path: Path, lines: list[str]) -> None:
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def valid_frame(seq: int) -> str:
    return json.dumps(
        {
            "$type": "message",
            "payload": {
                "$type": "network.bsky.jetstream.subscribeEvents#commit",
                "seq": seq,
                "did": "did:plc:integration",
                "time": "2026-10-04T12:00:00.000001Z",
                "rev": f"rev-{seq}",
                "operation": "create",
                "collection": "app.bsky.feed.post",
                "rkey": f"key-{seq}",
                "cid": f"cid-{seq}",
                "record": {"$type": "app.bsky.feed.post", "text": "test"},
            },
        }
    )


@pytest.mark.asyncio
async def test_fixture_to_publisher_advances_cursor_after_ack(tmp_path) -> None:
    fixture = tmp_path / "events.jsonl"
    write_fixture(fixture, [valid_frame(10), valid_frame(11)])
    source = FixtureSource(fixture)
    publisher = RecordingPublisher()
    cursor_path = tmp_path / "cursor.json"

    count = await IngestionRunner(
        source=source,
        publisher=publisher,
        cursor_store=FileCursorStore(cursor_path),
    ).run()

    assert count == 2
    assert [event.cursor for event in publisher.events] == [10, 11]
    assert json.loads(cursor_path.read_text())["value"] == 11


@pytest.mark.asyncio
async def test_broker_failure_does_not_advance_cursor(tmp_path) -> None:
    fixture = tmp_path / "events.jsonl"
    write_fixture(fixture, [valid_frame(10)])
    cursor_path = tmp_path / "cursor.json"

    with pytest.raises(RuntimeError, match="broker unavailable"):
        await IngestionRunner(
            source=FixtureSource(fixture),
            publisher=RecordingPublisher(fail_on_event=True),
            cursor_store=FileCursorStore(cursor_path),
        ).run()

    assert not cursor_path.exists()


@pytest.mark.asyncio
async def test_malformed_event_is_durable_in_dlq(tmp_path) -> None:
    fixture = tmp_path / "events.jsonl"
    malformed_with_cursor = json.dumps(
        {
            "$type": "message",
            "payload": {
                "$type": "network.bsky.jetstream.subscribeEvents#commit",
                "seq": 10,
                "did": "did:plc:integration",
            },
        }
    )
    write_fixture(fixture, [malformed_with_cursor, valid_frame(11)])
    publisher = RecordingPublisher()
    cursor_path = tmp_path / "cursor.json"

    await IngestionRunner(
        source=FixtureSource(fixture),
        publisher=publisher,
        cursor_store=FileCursorStore(cursor_path),
    ).run()

    assert len(publisher.dead_letters) == 1
    assert publisher.dead_letters[0].cursor == 10
    assert len(publisher.events) == 1
    assert json.loads(cursor_path.read_text())["value"] == 11
