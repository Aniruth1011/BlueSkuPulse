from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

EventKind = Literal["commit", "identity", "account", "sync"]


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def deterministic_event_id(parts: list[str | int | None]) -> str:
    material = canonical_json(parts).encode()
    return hashlib.sha256(material).hexdigest()


class EventEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    event_id: str = Field(min_length=64, max_length=64)
    source_version: Literal["jetstream_v1", "jetstream_v2"]
    source_host: str
    cursor: int = Field(ge=0)
    event_kind: EventKind
    operation: str
    did: str = Field(pattern=r"^did:")
    collection: str | None = None
    rkey: str | None = None
    at_uri: str | None = None
    cid: str | None = None
    event_time: datetime
    ingested_at: datetime
    payload_json: str
    schema_version: int = 1

    @field_validator("event_time", "ingested_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("timestamps must be timezone-aware")
        return value.astimezone(UTC)

    def kafka_value(self) -> bytes:
        return self.model_dump_json().encode()


class DeadLetterRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    source_version: str
    source_host: str
    cursor: int | None
    reason: str
    raw_payload: str
    failed_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    def kafka_value(self) -> bytes:
        return self.model_dump_json().encode()


def make_envelope(
    *,
    source_version: Literal["jetstream_v1", "jetstream_v2"],
    source_host: str,
    cursor: int,
    event_kind: EventKind,
    operation: str,
    did: str,
    event_time: datetime,
    raw_payload: dict[str, Any],
    collection: str | None = None,
    rkey: str | None = None,
    cid: str | None = None,
    record_version: str | None = None,
) -> EventEnvelope:
    at_uri = f"at://{did}/{collection}/{rkey}" if collection and rkey else None
    if event_kind == "commit":
        identity_parts: list[str | int | None] = [
            source_version,
            did,
            collection,
            rkey,
            operation,
            cid or record_version,
        ]
    else:
        identity_parts = [source_version, event_kind, did, cursor, operation]
    return EventEnvelope(
        event_id=deterministic_event_id(identity_parts),
        source_version=source_version,
        source_host=source_host,
        cursor=cursor,
        event_kind=event_kind,
        operation=operation,
        did=did,
        collection=collection,
        rkey=rkey,
        at_uri=at_uri,
        cid=cid,
        event_time=event_time,
        ingested_at=datetime.now(UTC),
        payload_json=canonical_json(raw_payload),
    )
