from __future__ import annotations

import base64
import gzip
import hashlib
import json
import logging
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from bronze.metrics import (
    BRONZE_BATCH_DURATION,
    BRONZE_BYTES,
    BRONZE_EVENTS,
    BRONZE_FILES,
    BRONZE_FRESHNESS,
)
from bronze.storage import ImmutableObjectStore


@dataclass(frozen=True, slots=True)
class KafkaRawRecord:
    topic: str
    partition: int
    offset: int
    key: str | None
    value: bytes
    kafka_timestamp_ms: int | None = None


@dataclass(frozen=True, slots=True)
class BronzeBatchResult:
    batch_id: str
    object_keys: tuple[str, ...]
    manifest_key: str
    record_count: int
    compressed_bytes: int
    replayed: bool


class BronzeBatchWriter:
    def __init__(self, store: ImmutableObjectStore) -> None:
        self._store = store
        self._logger = logging.getLogger(self.__class__.__name__)

    def write(self, records: list[KafkaRawRecord]) -> BronzeBatchResult:
        started = time.monotonic()
        if not records:
            raise ValueError("cannot write an empty Bronze batch")
        ordered = sorted(records, key=lambda row: (row.topic, row.partition, row.offset))
        batch_id = self._batch_id(ordered)
        grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
        offsets: dict[str, dict[str, int]] = {}
        malformed_count = 0

        for record in ordered:
            try:
                envelope = json.loads(record.value)
                if not isinstance(envelope, dict):
                    raise ValueError("Kafka value is not a JSON object")
                event_time = datetime.fromisoformat(
                    str(envelope["event_time"]).replace("Z", "+00:00")
                )
                collection = str(envelope.get("collection") or f"${envelope['event_kind']}")
            except (ValueError, TypeError, KeyError) as exc:
                malformed_count += 1
                event_time = (
                    datetime.fromtimestamp(record.kafka_timestamp_ms / 1000, tz=UTC)
                    if record.kafka_timestamp_ms is not None
                    else datetime.now(UTC)
                )
                collection = "$malformed"
                envelope = {
                    "event_id": hashlib.sha256(record.value).hexdigest(),
                    "event_time": event_time.isoformat(),
                    "ingested_at": event_time.isoformat(),
                    "event_kind": "unknown",
                    "operation": "unknown",
                    "payload_json": None,
                    "bronze_parse_error": str(exc),
                    "raw_value_base64": base64.b64encode(record.value).decode(),
                    "schema_version": 0,
                }
                self._logger.warning(
                    "bronze_malformed_value_preserved",
                    extra={
                        "fields": {
                            "topic": record.topic,
                            "partition": record.partition,
                            "offset": record.offset,
                            "error": str(exc),
                        }
                    },
                )
            date = event_time.astimezone(UTC).strftime("%Y-%m-%d")
            hour = event_time.astimezone(UTC).strftime("%H")
            grouped[(date, hour, collection)].append(
                {
                    "kafka": {
                        "topic": record.topic,
                        "partition": record.partition,
                        "offset": record.offset,
                        "timestamp_ms": record.kafka_timestamp_ms,
                        "key": record.key,
                    },
                    "event": envelope,
                }
            )
            offset_key = f"{record.topic}:{record.partition}"
            bounds = offsets.setdefault(offset_key, {"first": record.offset, "last": record.offset})
            bounds["first"] = min(bounds["first"], record.offset)
            bounds["last"] = max(bounds["last"], record.offset)

        object_keys: list[str] = []
        compressed_bytes = 0
        replayed = True
        object_digests: dict[str, str] = {}
        for (date, hour, collection), rows in sorted(grouped.items()):
            payload = b"".join(
                json.dumps(row, sort_keys=True, separators=(",", ":")).encode() + b"\n"
                for row in rows
            )
            compressed = gzip.compress(payload, compresslevel=6, mtime=0)
            key = (
                f"bluesky/event_date={date}/event_hour={hour}/collection={collection}/"
                f"batch-{batch_id}.jsonl.gz"
            )
            created = self._store.put_if_absent(key, compressed, content_type="application/gzip")
            replayed = replayed and not created
            object_keys.append(key)
            compressed_bytes += len(compressed)
            object_digests[key] = hashlib.sha256(compressed).hexdigest()

        manifest = {
            "schema_version": 1,
            "batch_id": batch_id,
            "created_at": datetime.now(UTC).isoformat(),
            "record_count": len(ordered),
            "compressed_bytes": compressed_bytes,
            "offsets": offsets,
            "objects": object_digests,
        }
        manifest_key = f"bluesky/_manifests/batch-{batch_id}.json"
        self._store.put_if_absent(
            manifest_key,
            json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode(),
            content_type="application/json",
        )
        BRONZE_EVENTS.inc(len(ordered))
        BRONZE_FILES.inc(len(object_keys))
        BRONZE_BYTES.inc(compressed_bytes)
        BRONZE_BATCH_DURATION.observe(time.monotonic() - started)
        BRONZE_FRESHNESS.set_to_current_time()
        self._logger.info(
            "bronze_batch_committed",
            extra={
                "fields": {
                    "batch_id": batch_id,
                    "record_count": len(ordered),
                    "object_count": len(object_keys),
                    "compressed_bytes": compressed_bytes,
                    "replayed": replayed,
                    "malformed_count": malformed_count,
                    "offsets": offsets,
                }
            },
        )
        return BronzeBatchResult(
            batch_id=batch_id,
            object_keys=tuple(object_keys),
            manifest_key=manifest_key,
            record_count=len(ordered),
            compressed_bytes=compressed_bytes,
            replayed=replayed,
        )

    @staticmethod
    def _batch_id(records: list[KafkaRawRecord]) -> str:
        coordinates = [
            {"topic": row.topic, "partition": row.partition, "offset": row.offset}
            for row in records
        ]
        return hashlib.sha256(
            json.dumps(coordinates, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()[:24]
