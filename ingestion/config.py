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


LOG_LEVELS = frozenset({"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})


def _port(name: str, default: int) -> int:
    value = os.getenv(name, str(default))
    try:
        port = int(value)
    except ValueError:
        raise ValueError(f"{name} must be an integer port, got {value!r}") from None
    if not 1 <= port <= 65535:
        raise ValueError(f"{name} must be between 1 and 65535, got {port}")
    return port


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
        collections = _csv("ATMOSPULSE_COLLECTIONS", DEFAULT_COLLECTIONS)
        # Only public app.bsky records are in scope; chat.bsky (DMs) must never be ingested.
        invalid = [c for c in collections if not c.startswith("app.bsky.")]
        if invalid:
            raise ValueError(f"ATMOSPULSE_COLLECTIONS must be public app.bsky NSIDs: {invalid}")
        log_level = os.getenv("ATMOSPULSE_LOG_LEVEL", "INFO").upper()
        if log_level not in LOG_LEVELS:
            raise ValueError(f"Unsupported ATMOSPULSE_LOG_LEVEL: {log_level}")
        bootstrap = os.getenv("ATMOSPULSE_KAFKA_BOOTSTRAP_SERVERS", "localhost:19092").strip()
        if not bootstrap:
            raise ValueError("ATMOSPULSE_KAFKA_BOOTSTRAP_SERVERS must not be empty")
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
            collections=collections,
            kafka_bootstrap_servers=bootstrap,
            raw_topic=os.getenv("ATMOSPULSE_RAW_TOPIC", "bluesky.raw.events"),
            dlq_topic=os.getenv("ATMOSPULSE_DLQ_TOPIC", "bluesky.dlq"),
            cursor_path=Path(os.getenv("ATMOSPULSE_CURSOR_PATH", "state/cursor.json")),
            fixture_path=Path(
                os.getenv("ATMOSPULSE_FIXTURE_PATH", "tests/fixtures/jetstream_v2.jsonl")
            ),
            log_level=log_level,
            metrics_port=_port("ATMOSPULSE_METRICS_PORT", 9108),
        )
