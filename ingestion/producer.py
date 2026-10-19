from __future__ import annotations

import argparse
import asyncio
import logging
import signal
from datetime import UTC, datetime
from typing import Protocol

from aiokafka import AIOKafkaProducer
from prometheus_client import start_http_server

from ingestion.config import Settings
from ingestion.cursor_store import FileCursorStore
from ingestion.logging import configure_logging
from ingestion.metrics import (
    EVENTS_DLQ,
    EVENTS_PUBLISHED,
    EVENTS_RECEIVED,
    INGESTION_LAG,
    PRODUCER_ERRORS,
    mark_cursor_checkpoint,
)
from ingestion.models.events import DeadLetterRecord, EventEnvelope
from ingestion.sources import EventSource, FixtureSource, JetstreamV1Source, JetstreamV2Source


class DurablePublisher(Protocol):
    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def publish_event(self, event: EventEnvelope) -> None: ...

    async def publish_dlq(self, record: DeadLetterRecord) -> None: ...


class KafkaPublisher:
    def __init__(self, settings: Settings) -> None:
        self._raw_topic = settings.raw_topic
        self._dlq_topic = settings.dlq_topic
        self._logger = logging.getLogger(self.__class__.__name__)
        self._producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            acks="all",
            enable_idempotence=True,
            compression_type="gzip",
            client_id="atmospulse-ingestion",
            request_timeout_ms=30_000,
        )

    async def start(self) -> None:
        await self._producer.start()

    async def stop(self) -> None:
        await self._producer.stop()

    async def publish_event(self, event: EventEnvelope) -> None:
        try:
            await self._producer.send_and_wait(
                self._raw_topic,
                value=event.kafka_value(),
                key=event.did.encode(),
                headers=[
                    ("event_id", event.event_id.encode()),
                    ("source_version", event.source_version.encode()),
                ],
            )
            EVENTS_PUBLISHED.labels(self._raw_topic).inc()
            self._logger.debug(
                "kafka_event_acknowledged",
                extra={"fields": {"event_id": event.event_id, "cursor": event.cursor}},
            )
        except Exception:
            PRODUCER_ERRORS.labels(self._raw_topic).inc()
            self._logger.exception(
                "kafka_event_publish_failed", extra={"fields": {"event_id": event.event_id}}
            )
            raise

    async def publish_dlq(self, record: DeadLetterRecord) -> None:
        try:
            key = str(record.cursor).encode() if record.cursor is not None else None
            await self._producer.send_and_wait(
                self._dlq_topic,
                value=record.kafka_value(),
                key=key,
                headers=[("source_version", record.source_version.encode())],
            )
            EVENTS_PUBLISHED.labels(self._dlq_topic).inc()
            EVENTS_DLQ.labels("parse_or_contract_error").inc()
            self._logger.warning(
                "dlq_event_acknowledged",
                extra={"fields": {"cursor": record.cursor, "reason": record.reason}},
            )
        except Exception:
            PRODUCER_ERRORS.labels(self._dlq_topic).inc()
            self._logger.exception(
                "dlq_publish_failed", extra={"fields": {"cursor": record.cursor}}
            )
            raise


class IngestionRunner:
    def __init__(
        self,
        *,
        source: EventSource,
        publisher: DurablePublisher,
        cursor_store: FileCursorStore,
    ) -> None:
        self._source = source
        self._publisher = publisher
        self._cursor_store = cursor_store
        self._logger = logging.getLogger(self.__class__.__name__)
        self.handled = 0

    async def run(self, *, max_events: int | None = None, stop: asyncio.Event | None = None) -> int:
        cursor = self._cursor_store.load(
            kind=self._source.cursor_kind, source_host=self._source.source_host
        )
        handled = self.handled = 0
        await self._publisher.start()
        self._logger.info(
            "ingestion_started",
            extra={
                "fields": {
                    "source_version": self._source.source_version,
                    "source_host": self._source.source_host,
                    "resume_cursor": cursor.value if cursor else None,
                }
            },
        )
        try:
            async for item in self._source.stream(cursor):
                EVENTS_RECEIVED.labels(self._source.source_version).inc()
                if item.error is not None:
                    dlq = DeadLetterRecord(
                        source_version=self._source.source_version,
                        source_host=self._source.source_host,
                        cursor=item.cursor.value if item.cursor else None,
                        reason=item.error,
                        raw_payload=item.raw.decode("utf-8", errors="replace"),
                    )
                    await self._publisher.publish_dlq(dlq)
                else:
                    assert item.event is not None
                    await self._publisher.publish_event(item.event)
                    lag = (item.event.ingested_at - item.event.event_time).total_seconds()
                    INGESTION_LAG.observe(max(0.0, lag))

                # Kafka acknowledgement above is the durability boundary.
                if item.cursor is not None:
                    self._cursor_store.save(item.cursor)
                    mark_cursor_checkpoint()
                    self._logger.debug(
                        "cursor_checkpoint_committed",
                        extra={"fields": {"cursor": item.cursor.value}},
                    )
                handled = self.handled = handled + 1
                if max_events is not None and handled >= max_events:
                    break
                if stop is not None and stop.is_set():
                    self._logger.info(
                        "ingestion_stop_requested", extra={"fields": {"handled_events": handled}}
                    )
                    break
        finally:
            await self._publisher.stop()
        self._logger.info("ingestion_stopped", extra={"fields": {"handled_events": handled}})
        return handled


def build_source(settings: Settings) -> EventSource:
    if settings.source == "fixture":
        return FixtureSource(settings.fixture_path, wire_version="jetstream_v2")
    if settings.source == "jetstream_v1":
        return JetstreamV1Source(settings.jetstream_v1_url, settings.collections)
    return JetstreamV2Source(settings.jetstream_v2_url, settings.collections)


async def run_until_stopped(
    runner: IngestionRunner,
    *,
    max_events: int | None = None,
    stop: asyncio.Event,
    grace_seconds: float = 10,
) -> int:
    """Run ingestion, stopping at an event boundary once ``stop`` is set.

    An idle source never reaches an event boundary, so after ``grace_seconds`` the run is
    cancelled. That is safe: the cursor only ever reflects Kafka-acknowledged events, so any
    in-flight event is redelivered on restart and deduplicated downstream.
    """
    task = asyncio.create_task(runner.run(max_events=max_events, stop=stop))
    stop_waiter = asyncio.create_task(stop.wait())
    try:
        await asyncio.wait({task, stop_waiter}, return_when=asyncio.FIRST_COMPLETED)
        if not task.done():
            try:
                return await asyncio.wait_for(asyncio.shield(task), timeout=grace_seconds)
            except TimeoutError:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                logging.getLogger(__name__).warning(
                    "ingestion_cancelled_after_grace",
                    extra={"fields": {"grace_seconds": grace_seconds, "handled": runner.handled}},
                )
                return runner.handled
        return task.result()
    finally:
        stop_waiter.cancel()


def install_stop_handlers(stop: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):  # pragma: no cover - non-Unix event loops
            pass


async def async_main(max_events: int | None = None) -> int:
    settings = Settings.from_env()
    configure_logging(settings.log_level)
    start_http_server(settings.metrics_port)
    runner = IngestionRunner(
        source=build_source(settings),
        publisher=KafkaPublisher(settings),
        cursor_store=FileCursorStore(settings.cursor_path),
    )
    stop = asyncio.Event()
    install_stop_handlers(stop)
    started = datetime.now(UTC)
    count = await run_until_stopped(runner, max_events=max_events, stop=stop)
    logging.getLogger(__name__).info(
        "ingestion_summary",
        extra={
            "fields": {
                "count": count,
                "elapsed_seconds": (datetime.now(UTC) - started).total_seconds(),
            }
        },
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Publish Jetstream events to Kafka/Redpanda")
    parser.add_argument("--max-events", type=int, default=None)
    args = parser.parse_args()
    return asyncio.run(async_main(max_events=args.max_events))


if __name__ == "__main__":
    raise SystemExit(main())
