from __future__ import annotations

import json
from itertools import pairwise
from pathlib import Path

import pytest

from benchmarking.generator import generated_frames, v2_frame
from ingestion.cursor_store import FileCursorStore
from ingestion.models.events import DeadLetterRecord, EventEnvelope
from ingestion.producer import IngestionRunner
from ingestion.sources.fixture import FixtureSource
from ingestion.sources.parsing import parse_v2
from warehouse.config import WarehouseSettings
from warehouse.loader import BigQuerySilverLoader


class RecordingPublisher:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.events: list[EventEnvelope] = []
        self.dead_letters: list[DeadLetterRecord] = []

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def publish_event(self, event: EventEnvelope) -> None:
        if self.fail:
            raise RuntimeError("injected broker outage")
        self.events.append(event)

    async def publish_dlq(self, record: DeadLetterRecord) -> None:
        self.dead_letters.append(record)


def write_frames(path: Path, frames: list[bytes]) -> None:
    path.write_bytes(b"\n".join(frames) + b"\n")


@pytest.mark.asyncio
async def test_ingestion_restart_replays_inclusive_cursor_without_loss(tmp_path: Path) -> None:
    fixture = tmp_path / "restart.jsonl"
    write_frames(fixture, [v2_frame(10), v2_frame(11), v2_frame(12)])
    cursor = tmp_path / "cursor.json"
    first = RecordingPublisher()
    await IngestionRunner(
        source=FixtureSource(fixture), publisher=first, cursor_store=FileCursorStore(cursor)
    ).run(max_events=2)
    second = RecordingPublisher()
    await IngestionRunner(
        source=FixtureSource(fixture), publisher=second, cursor_store=FileCursorStore(cursor)
    ).run()
    combined = [event.event_id for event in [*first.events, *second.events]]
    assert len(combined) == 4  # cursor 11 is intentionally replayed
    assert len(set(combined)) == 3
    assert json.loads(cursor.read_text())["value"] == 12


@pytest.mark.asyncio
async def test_broker_outage_cannot_advance_cursor_and_recovery_replays(tmp_path: Path) -> None:
    fixture = tmp_path / "broker.jsonl"
    write_frames(fixture, [v2_frame(20)])
    cursor = tmp_path / "cursor.json"
    with pytest.raises(RuntimeError, match="injected broker outage"):
        await IngestionRunner(
            source=FixtureSource(fixture),
            publisher=RecordingPublisher(fail=True),
            cursor_store=FileCursorStore(cursor),
        ).run()
    assert not cursor.exists()
    recovered = RecordingPublisher()
    await IngestionRunner(
        source=FixtureSource(fixture), publisher=recovered, cursor_store=FileCursorStore(cursor)
    ).run()
    assert [event.cursor for event in recovered.events] == [20]


@pytest.mark.asyncio
async def test_malformed_payload_is_durable_before_cursor_advances(tmp_path: Path) -> None:
    fixture = tmp_path / "malformed.jsonl"
    malformed = b'{"$type":"message","payload":{"seq":30,"did":"did:plc:bad"}}'
    write_frames(fixture, [malformed, v2_frame(31)])
    publisher = RecordingPublisher()
    cursor = tmp_path / "cursor.json"
    await IngestionRunner(
        source=FixtureSource(fixture), publisher=publisher, cursor_store=FileCursorStore(cursor)
    ).run()
    assert publisher.dead_letters[0].raw_payload == malformed.decode()
    assert publisher.dead_letters[0].cursor == 30
    assert json.loads(cursor.read_text())["value"] == 31


def test_additive_source_schema_is_preserved_without_breaking_contract() -> None:
    raw = v2_frame(40, additive_field=True)
    event, cursor = parse_v2(raw, "fixture://schema-drift")
    assert cursor.value == 40
    assert "future_additive_field" in event.payload_json


def test_ten_percent_forced_redelivery_has_stable_event_identity() -> None:
    events = [
        parse_v2(raw, "fixture://duplicates")[0]
        for raw in generated_frames(100, duplicate_fraction=0.10)
    ]
    duplicate_count = sum(
        current.event_id == previous.event_id for previous, current in pairwise(events)
    )
    assert duplicate_count == 10
    assert len({event.event_id for event in events}) == 90


class FailingBigQueryClient:
    def load_table_from_uri(self, *_: object, **__: object) -> object:
        raise RuntimeError("injected BigQuery outage")

    def delete_table(self, table: str, *, not_found_ok: bool) -> None:
        assert "._load_events_" in table
        assert not_found_ok


def test_bigquery_failure_is_raised_and_temporary_table_cleanup_is_attempted() -> None:
    settings = WarehouseSettings(
        project_id="test-project",
        location="US",
        staging_dataset="staging",
        analytics_dataset="analytics",
        monitoring_dataset="monitoring",
    )
    loader = BigQuerySilverLoader(settings, client=FailingBigQueryClient())
    with pytest.raises(RuntimeError, match="injected BigQuery outage"):
        loader.load("events", ["gs://test/silver/events/*.parquet"])
