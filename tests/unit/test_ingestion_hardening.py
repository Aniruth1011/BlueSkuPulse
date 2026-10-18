from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from ingestion.config import Settings
from ingestion.cursor_store import CursorCorruptError, FileCursorStore, SourceCursor
from ingestion.models.events import DeadLetterRecord, EventEnvelope
from ingestion.producer import IngestionRunner, run_until_stopped
from ingestion.sources import FixtureSource
from ingestion.sources.base import SourceItem

FIXTURE = Path("tests/fixtures/jetstream_v2.jsonl")


class RecordingPublisher:
    def __init__(self) -> None:
        self.events: list[EventEnvelope] = []
        self.stopped = False

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        self.stopped = True

    async def publish_event(self, event: EventEnvelope) -> None:
        self.events.append(event)

    async def publish_dlq(self, record: DeadLetterRecord) -> None:
        pass


class IdleAfterFirstSource(FixtureSource):
    """Yields one fixture event, then blocks like a quiet WebSocket."""

    def __init__(self, path: Path) -> None:
        super().__init__(path)
        self.idle = asyncio.Event()

    async def stream(self, cursor: SourceCursor | None) -> AsyncIterator[SourceItem]:
        async for item in super().stream(cursor):
            yield item
            break
        self.idle.set()
        await asyncio.Event().wait()


@pytest.mark.parametrize(
    "content",
    [
        "{not json",
        "[]",
        '{"kind": "jetstream_v2_seq", "source_host": "h"}',
        '{"kind": "bogus", "source_host": "h", "value": 1}',
        '{"kind": "jetstream_v2_seq", "source_host": "h", "value": "12"}',
        '{"kind": "jetstream_v2_seq", "source_host": "h", "value": true}',
        '{"kind": "jetstream_v2_seq", "source_host": "h", "value": -1}',
        '{"kind": "jetstream_v2_seq", "source_host": "", "value": 1}',
    ],
)
def test_corrupt_cursor_fails_closed_instead_of_resetting(tmp_path, content: str) -> None:
    path = tmp_path / "cursor.json"
    path.write_text(content)

    with pytest.raises(CursorCorruptError, match="Refusing to reset"):
        FileCursorStore(path).load(kind="jetstream_v2_seq", source_host="h")


def test_undecodable_cursor_fails_closed(tmp_path) -> None:
    path = tmp_path / "cursor.json"
    path.write_bytes(b"\xff\xfe\x00")

    with pytest.raises(CursorCorruptError):
        FileCursorStore(path).load(kind="jetstream_v2_seq", source_host="h")


@pytest.mark.parametrize(
    ("name", "value", "message"),
    [
        ("ATMOSPULSE_METRICS_PORT", "abc", "must be an integer port"),
        ("ATMOSPULSE_METRICS_PORT", "70000", "between 1 and 65535"),
        ("ATMOSPULSE_METRICS_PORT", "0", "between 1 and 65535"),
        ("ATMOSPULSE_LOG_LEVEL", "verbose", "Unsupported ATMOSPULSE_LOG_LEVEL"),
        ("ATMOSPULSE_KAFKA_BOOTSTRAP_SERVERS", "  ", "must not be empty"),
        ("ATMOSPULSE_COLLECTIONS", "app.bsky.feed.post,chat.bsky.convo", "public app.bsky"),
    ],
)
def test_settings_reject_invalid_configuration(monkeypatch, name, value, message) -> None:
    monkeypatch.setenv(name, value)

    with pytest.raises(ValueError, match=message):
        Settings.from_env()


@pytest.mark.asyncio
async def test_stop_request_ends_at_event_boundary_with_cursor_checkpointed(tmp_path) -> None:
    publisher = RecordingPublisher()
    store = FileCursorStore(tmp_path / "cursor.json")
    runner = IngestionRunner(source=FixtureSource(FIXTURE), publisher=publisher, cursor_store=store)
    stop = asyncio.Event()
    stop.set()

    handled = await run_until_stopped(runner, stop=stop)

    assert handled == 1
    assert publisher.stopped
    saved = store.load(kind="jetstream_v2_seq", source_host=FixtureSource(FIXTURE).source_host)
    assert saved is not None
    assert publisher.events[0].cursor == saved.value


@pytest.mark.asyncio
async def test_stop_cancels_idle_source_after_grace_period(tmp_path) -> None:
    publisher = RecordingPublisher()
    source = IdleAfterFirstSource(FIXTURE)
    runner = IngestionRunner(
        source=source,
        publisher=publisher,
        cursor_store=FileCursorStore(tmp_path / "cursor.json"),
    )
    stop = asyncio.Event()

    async def request_stop() -> None:
        await source.idle.wait()
        stop.set()

    stopper = asyncio.create_task(request_stop())
    handled = await asyncio.wait_for(
        run_until_stopped(runner, stop=stop, grace_seconds=0.05), timeout=5
    )
    await stopper

    assert handled == 1
    assert publisher.stopped
