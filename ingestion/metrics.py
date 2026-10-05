from __future__ import annotations

import math
import time

from prometheus_client import Counter, Gauge, Histogram

EVENTS_RECEIVED = Counter(
    "bluesky_events_received_total", "Source messages received", ["source_version"]
)
EVENTS_PUBLISHED = Counter(
    "bluesky_events_published_total", "Events acknowledged by Kafka", ["topic"]
)
EVENTS_DLQ = Counter("bluesky_events_dlq_total", "Events acknowledged by the DLQ", ["reason"])
PRODUCER_ERRORS = Counter("bluesky_producer_errors_total", "Kafka publish errors", ["topic"])
RECONNECTS = Counter(
    "bluesky_websocket_reconnects_total", "WebSocket reconnect attempts", ["source_version"]
)
INGESTION_LAG = Histogram(
    "bluesky_ingestion_lag_seconds",
    "Difference between platform observation time and source event time",
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 15, 30, 60, 300),
)
CURSOR_CHECKPOINT_AGE = Gauge(
    "bluesky_cursor_checkpoint_age_seconds", "Age of the latest durable cursor checkpoint"
)
_last_cursor_checkpoint_at: float | None = None


def mark_cursor_checkpoint() -> None:
    global _last_cursor_checkpoint_at
    _last_cursor_checkpoint_at = time.time()


def _cursor_checkpoint_age() -> float:
    if _last_cursor_checkpoint_at is None:
        return math.nan
    return max(0.0, time.time() - _last_cursor_checkpoint_at)


CURSOR_CHECKPOINT_AGE.set_function(_cursor_checkpoint_age)
