from __future__ import annotations

import json
import logging
from dataclasses import replace
from pathlib import Path

import pytest

import ingestion.producer as producer_module
from ingestion.config import DEFAULT_COLLECTIONS, Settings
from ingestion.logging import JsonFormatter, configure_logging
from ingestion.models.events import DeadLetterRecord
from ingestion.producer import KafkaPublisher, build_source
from ingestion.sources import FixtureSource, JetstreamV1Source, JetstreamV2Source
from ingestion.sources.parsing import parse_v2

FIRST_FRAME = Path("tests/fixtures/jetstream_v2.jsonl").read_bytes().splitlines()[0]


class FakeKafkaProducer:
    def __init__(self) -> None:
        self.started = False
        self.fail = False
        self.calls: list[tuple[str, dict[str, object]]] = []

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.started = False

    async def send_and_wait(self, topic: str, **kwargs: object) -> None:
        if self.fail:
            raise RuntimeError("forced broker failure")
        self.calls.append((topic, kwargs))


def test_settings_parses_environment_and_rejects_unknown_source(monkeypatch) -> None:
    monkeypatch.setenv("ATMOSPULSE_SOURCE", "JETSTREAM_V2")
    monkeypatch.setenv("ATMOSPULSE_COLLECTIONS", "app.bsky.feed.post, app.bsky.feed.like, ,")
    settings = Settings.from_env()
    assert settings.source == "jetstream_v2"
    assert settings.collections == ("app.bsky.feed.post", "app.bsky.feed.like")

    monkeypatch.setenv("ATMOSPULSE_SOURCE", "unknown")
    with pytest.raises(ValueError, match="Unsupported"):
        Settings.from_env()

    monkeypatch.delenv("ATMOSPULSE_COLLECTIONS")
    monkeypatch.setenv("ATMOSPULSE_SOURCE", "fixture")
    assert Settings.from_env().collections == DEFAULT_COLLECTIONS


def test_json_logging_includes_fields_and_exception() -> None:
    formatter = JsonFormatter()
    try:
        raise ValueError("structured failure")
    except ValueError:
        record = logging.LogRecord(
            "test",
            logging.ERROR,
            __file__,
            1,
            "operation failed",
            (),
            exc_info=__import__("sys").exc_info(),
        )
    record.fields = {"event_id": "event-1"}  # type: ignore[attr-defined]
    payload = json.loads(formatter.format(record))
    assert payload["event_id"] == "event-1"
    assert "structured failure" in payload["exception"]
    configure_logging("WARNING")
    assert logging.getLogger().level == logging.WARNING


@pytest.mark.asyncio
async def test_kafka_publisher_event_dlq_and_failure_paths(monkeypatch) -> None:
    fake = FakeKafkaProducer()
    monkeypatch.setattr(producer_module, "AIOKafkaProducer", lambda **_: fake)
    settings = Settings.from_env()
    publisher = KafkaPublisher(settings)
    event, _ = parse_v2(FIRST_FRAME, "fixture.local")
    dlq = DeadLetterRecord(
        source_version="jetstream_v2",
        source_host="fixture.local",
        cursor=10,
        reason="bad frame",
        raw_payload="{}",
    )

    await publisher.start()
    await publisher.publish_event(event)
    await publisher.publish_dlq(dlq)
    await publisher.stop()
    assert [topic for topic, _ in fake.calls] == [settings.raw_topic, settings.dlq_topic]
    assert isinstance(fake.calls[0][1]["headers"], list)

    fake.fail = True
    with pytest.raises(RuntimeError, match="forced broker failure"):
        await publisher.publish_event(event)
    with pytest.raises(RuntimeError, match="forced broker failure"):
        await publisher.publish_dlq(dlq)


def test_build_source_selects_each_adapter() -> None:
    settings = Settings.from_env()
    assert isinstance(build_source(replace(settings, source="fixture")), FixtureSource)
    assert isinstance(build_source(replace(settings, source="jetstream_v1")), JetstreamV1Source)
    assert isinstance(build_source(replace(settings, source="jetstream_v2")), JetstreamV2Source)
