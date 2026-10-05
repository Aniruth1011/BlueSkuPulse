from __future__ import annotations

import argparse
import asyncio
import logging
import os
import time
from pathlib import Path

from aiokafka import AIOKafkaConsumer
from prometheus_client import start_http_server

from bronze.storage import GCSObjectStore, LocalObjectStore
from bronze.writer import BronzeBatchWriter, KafkaRawRecord
from ingestion.config import Settings
from ingestion.logging import configure_logging


def should_flush(
    *, pending_count: int, batch_size: int, now: float, deadline: float, once: bool
) -> bool:
    return pending_count > 0 and (pending_count >= batch_size or now >= deadline or once)


def build_store() -> LocalObjectStore | GCSObjectStore:
    backend = os.getenv("ATMOSPULSE_BRONZE_BACKEND", "local")
    if backend == "local":
        return LocalObjectStore(Path(os.getenv("ATMOSPULSE_BRONZE_ROOT", "data/bronze")))
    if backend == "gcs":
        return GCSObjectStore(os.environ["ATMOSPULSE_BRONZE_GCS_BUCKET"], prefix="bronze")
    raise ValueError(f"unsupported Bronze backend: {backend}")


async def run_consumer(*, once: bool = False) -> None:
    settings = Settings.from_env()
    logger = logging.getLogger("BronzeConsumer")
    consumer = AIOKafkaConsumer(
        settings.raw_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id="atmospulse-bronze-v1",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    writer = BronzeBatchWriter(build_store())
    batch_size = int(os.getenv("ATMOSPULSE_BRONZE_BATCH_SIZE", "10000"))
    flush_seconds = float(os.getenv("ATMOSPULSE_BRONZE_FLUSH_SECONDS", "10"))
    if batch_size <= 0:
        raise ValueError("ATMOSPULSE_BRONZE_BATCH_SIZE must be positive")
    if flush_seconds <= 0:
        raise ValueError("ATMOSPULSE_BRONZE_FLUSH_SECONDS must be positive")
    pending: list[KafkaRawRecord] = []
    flush_deadline = time.monotonic() + flush_seconds
    await consumer.start()
    logger.info(
        "bronze_consumer_started",
        extra={
            "fields": {
                "batch_size": batch_size,
                "flush_seconds": flush_seconds,
            }
        },
    )
    try:
        while True:
            remaining = max(1, batch_size - len(pending))
            timeout_ms = max(1, int((flush_deadline - time.monotonic()) * 1000))
            batches = await consumer.getmany(timeout_ms=timeout_ms, max_records=remaining)
            pending.extend(
                KafkaRawRecord(
                    topic=message.topic,
                    partition=message.partition,
                    offset=message.offset,
                    key=message.key.decode(errors="replace") if message.key else None,
                    value=message.value,
                    kafka_timestamp_ms=message.timestamp,
                )
                for messages in batches.values()
                for message in messages
            )
            if should_flush(
                pending_count=len(pending),
                batch_size=batch_size,
                now=time.monotonic(),
                deadline=flush_deadline,
                once=once,
            ):
                result = await asyncio.to_thread(writer.write, pending)
                await consumer.commit()
                logger.info(
                    "bronze_offsets_committed",
                    extra={"fields": {"batch_id": result.batch_id, "count": len(pending)}},
                )
                pending.clear()
                flush_deadline = time.monotonic() + flush_seconds
            if once:
                return
    except Exception:
        logger.exception("bronze_consumer_failed")
        raise
    finally:
        await consumer.stop()
        logger.info("bronze_consumer_stopped")


def main() -> int:
    parser = argparse.ArgumentParser(description="Land raw Kafka events in immutable Bronze")
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    configure_logging(os.getenv("ATMOSPULSE_LOG_LEVEL", "INFO"))
    start_http_server(int(os.getenv("ATMOSPULSE_BRONZE_METRICS_PORT", "9109")))
    asyncio.run(run_consumer(once=args.once))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
