from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any, cast

from ingestion.cursor_store import SourceCursor
from ingestion.models.events import EventEnvelope, EventKind, make_envelope


class AdvisoryFrame(Exception):
    """Non-data v2 information frame which must not advance a cursor."""


def _object(raw: bytes) -> dict[str, Any]:
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError("event must be a JSON object")
    return value


def parse_v1(raw: bytes, source_host: str) -> tuple[EventEnvelope, SourceCursor]:
    data = _object(raw)
    did = str(data["did"])
    time_us = int(data["time_us"])
    kind = str(data["kind"])
    event_time = datetime.fromtimestamp(time_us / 1_000_000, tz=UTC)
    cursor = SourceCursor("jetstream_v1_time_us", source_host, time_us)

    if kind == "commit":
        commit = data["commit"]
        if not isinstance(commit, dict):
            raise ValueError("v1 commit must be an object")
        operation = str(commit["operation"])
        collection = str(commit["collection"])
        rkey = str(commit["rkey"])
        cid = str(commit["cid"]) if commit.get("cid") is not None else None
        event = make_envelope(
            source_version="jetstream_v1",
            source_host=source_host,
            cursor=time_us,
            event_kind="commit",
            operation=operation,
            did=did,
            event_time=event_time,
            raw_payload=data,
            collection=collection,
            rkey=rkey,
            cid=cid,
            record_version=str(commit.get("rev", "")),
        )
    elif kind in {"identity", "account"}:
        event = make_envelope(
            source_version="jetstream_v1",
            source_host=source_host,
            cursor=time_us,
            event_kind=cast(EventKind, kind),
            operation=kind,
            did=did,
            event_time=event_time,
            raw_payload=data,
        )
    else:
        raise ValueError(f"unsupported v1 event kind: {kind}")
    return event, cursor


def parse_v2(raw: bytes, source_host: str) -> tuple[EventEnvelope, SourceCursor]:
    frame = _object(raw)
    if frame.get("$type") != "message":
        raise ValueError("v2 frame must have $type=message")
    payload = frame.get("payload")
    if not isinstance(payload, dict):
        raise ValueError("v2 message payload must be an object")
    type_name = str(payload.get("$type", ""))
    kind = type_name.rsplit("#", 1)[-1]
    if kind == "info":
        raise AdvisoryFrame(str(payload.get("message", payload.get("name", "info"))))
    if kind not in {"commit", "identity", "account", "sync"}:
        raise ValueError(f"unsupported v2 event type: {type_name}")

    seq = int(payload["seq"])
    did = str(payload["did"])
    event_time = datetime.fromisoformat(str(payload["time"]).replace("Z", "+00:00"))
    cursor = SourceCursor("jetstream_v2_seq", source_host, seq)

    if kind == "commit":
        operation = str(payload["operation"])
        collection = str(payload["collection"])
        rkey = str(payload["rkey"])
        cid = str(payload["cid"]) if payload.get("cid") is not None else None
        event = make_envelope(
            source_version="jetstream_v2",
            source_host=source_host,
            cursor=seq,
            event_kind="commit",
            operation=operation,
            did=did,
            event_time=event_time,
            raw_payload=frame,
            collection=collection,
            rkey=rkey,
            cid=cid,
            record_version=str(payload.get("rev", "")),
        )
    else:
        event = make_envelope(
            source_version="jetstream_v2",
            source_host=source_host,
            cursor=seq,
            event_kind=cast(EventKind, kind),
            operation=kind,
            did=did,
            event_time=event_time,
            raw_payload=frame,
        )
    return event, cursor


def best_effort_v2_cursor(raw: bytes, source_host: str) -> SourceCursor | None:
    try:
        frame = _object(raw)
        payload = frame.get("payload")
        if isinstance(payload, dict) and "seq" in payload:
            return SourceCursor("jetstream_v2_seq", source_host, int(payload["seq"]))
    except (ValueError, TypeError, KeyError):
        pass
    return None


def best_effort_v1_cursor(raw: bytes, source_host: str) -> SourceCursor | None:
    try:
        data = _object(raw)
        if "time_us" in data:
            return SourceCursor("jetstream_v1_time_us", source_host, int(data["time_us"]))
    except (ValueError, TypeError, KeyError):
        pass
    return None
