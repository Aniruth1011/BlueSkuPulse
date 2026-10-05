from __future__ import annotations

import argparse
import asyncio
import logging
import os

from aiokafka import AIOKafkaConsumer

from ingestion.logging import configure_logging


async def verify_broker(bootstrap_servers: str, topic: str, timeout_seconds: float) -> None:
    consumer = AIOKafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        request_timeout_ms=int(timeout_seconds * 1000),
    )
    try:
        await asyncio.wait_for(consumer.start(), timeout=timeout_seconds)
        topics = await asyncio.wait_for(consumer.topics(), timeout=timeout_seconds)
        if topic not in topics:
            raise RuntimeError(f"required Kafka topic is missing: {topic}")
        logging.getLogger(__name__).info(
            "broker_heartbeat_ok",
            extra={"fields": {"bootstrap_servers": bootstrap_servers, "topic": topic}},
        )
    finally:
        await consumer.stop()


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify the AtmosPulse broker and raw topic")
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("ATMOSPULSE_KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092"),
    )
    parser.add_argument("--topic", default=os.getenv("ATMOSPULSE_RAW_TOPIC", "bluesky.raw.events"))
    parser.add_argument("--timeout-seconds", type=float, default=15)
    args = parser.parse_args()
    configure_logging()
    asyncio.run(verify_broker(args.bootstrap_servers, args.topic, args.timeout_seconds))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
