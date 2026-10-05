from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

DEFAULT_COLLECTIONS = (
    "app.bsky.feed.post",
    "app.bsky.feed.like",
    "app.bsky.feed.repost",
    "app.bsky.graph.follow",
)


def _csv(name: str, default: tuple[str, ...]) -> tuple[str, ...]:
    value = os.getenv(name)
    if not value:
        return default
    return tuple(part.strip() for part in value.split(",") if part.strip())


@dataclass(frozen=True, slots=True)
class Settings:
    source: str
    jetstream_v1_url: str
    jetstream_v2_url: str
    collections: tuple[str, ...]
    kafka_bootstrap_servers: str
    raw_topic: str
    dlq_topic: str
    cursor_path: Path
    fixture_path: Path
    log_level: str
    metrics_port: int

    @classmethod
    def from_env(cls) -> Settings:
        source = os.getenv("ATMOSPULSE_SOURCE", "fixture").lower()
        if source not in {"fixture", "jetstream_v1", "jetstream_v2"}:
            raise ValueError(f"Unsupported ATMOSPULSE_SOURCE: {source}")
        return cls(
            source=source,
            jetstream_v1_url=os.getenv(
                "ATMOSPULSE_JETSTREAM_V1_URL",
                "wss://jetstream2.us-west.bsky.network/subscribe",
            ),
            jetstream_v2_url=os.getenv(
                "ATMOSPULSE_JETSTREAM_V2_URL",
                "wss://jetstream.us-east.bsky.network/xrpc/network.bsky.jetstream.subscribeEvents",
            ),
            collections=_csv("ATMOSPULSE_COLLECTIONS", DEFAULT_COLLECTIONS),
            kafka_bootstrap_servers=os.getenv(
                "ATMOSPULSE_KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"
            ),
            raw_topic=os.getenv("ATMOSPULSE_RAW_TOPIC", "bluesky.raw.events"),
            dlq_topic=os.getenv("ATMOSPULSE_DLQ_TOPIC", "bluesky.dlq"),
            cursor_path=Path(os.getenv("ATMOSPULSE_CURSOR_PATH", "state/cursor.json")),
            fixture_path=Path(
                os.getenv("ATMOSPULSE_FIXTURE_PATH", "tests/fixtures/jetstream_v2.jsonl")
            ),
            log_level=os.getenv("ATMOSPULSE_LOG_LEVEL", "INFO").upper(),
            metrics_port=int(os.getenv("ATMOSPULSE_METRICS_PORT", "9108")),
        )
