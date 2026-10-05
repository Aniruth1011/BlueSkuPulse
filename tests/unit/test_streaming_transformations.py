from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from ingestion.sources.parsing import parse_v2
from streaming.jobs.transformations import (
    canonical_events,
    engagements,
    fold_current_state,
    graph_edges,
    identity_events,
    posts,
)
from streaming.schemas.events import EVENT_ENVELOPE_SCHEMA


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("atmospulse-transform-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _fixture_events() -> list[dict[str, object]]:
    path = Path("tests/fixtures/jetstream_v2.jsonl")
    events = []
    for line in path.read_bytes().splitlines():
        event, _ = parse_v2(line, "fixture.local")
        events.append(event.model_dump())
    return events


def test_deduplicates_and_quarantines_invalid_rows(spark: SparkSession) -> None:
    source = _fixture_events()
    duplicate = dict(source[0])
    invalid = dict(source[0])
    invalid["event_id"] = "f" * 64
    invalid["did"] = "not-a-did"
    frame = spark.createDataFrame([*source, duplicate, invalid], EVENT_ENVELOPE_SCHEMA)

    valid, quarantine = canonical_events(frame, streaming=False)

    assert valid.count() == len(source)
    assert quarantine.count() == 1
    assert quarantine.first().quality_errors == ["did"]


def test_extracts_post_and_tolerates_additive_payload_fields(spark: SparkSession) -> None:
    frame = spark.createDataFrame(_fixture_events(), EVENT_ENVELOPE_SCHEMA)
    valid, _ = canonical_events(frame, streaming=False)
    rows = {row.operation: row for row in posts(valid).collect()}

    assert rows["create"].text == "AtmosPulse fixture post"
    assert rows["create"].languages == ["en"]
    assert rows["update"].text == "AtmosPulse fixture post edited"
    raw = next(event for event in _fixture_events() if event["operation"] == "update")
    assert json.loads(str(raw["payload_json"]))["payload"]["record"]["futureAdditiveField"] == {
        "safe": True
    }


def test_latest_mutation_models_update_and_delete(spark: SparkSession) -> None:
    now = datetime(2026, 10, 4, tzinfo=UTC)
    base = _fixture_events()[0]
    create = {**base, "cursor": 1, "operation": "create", "event_time": now}
    update = {
        **base,
        "event_id": "a" * 64,
        "cursor": 2,
        "operation": "update",
        "event_time": now + timedelta(seconds=1),
    }
    delete = {
        **base,
        "event_id": "b" * 64,
        "cursor": 3,
        "operation": "delete",
        "event_time": now + timedelta(seconds=2),
    }
    frame = spark.createDataFrame([create, update, delete], EVENT_ENVELOPE_SCHEMA)

    current = fold_current_state(frame).first()

    assert current.operation == "delete"
    assert current.is_deleted is True
    assert current.cursor == 3


def test_extracts_engagement_graph_and_identity_shapes(spark: SparkSession) -> None:
    rows = _fixture_events()
    like = spark.createDataFrame(rows, EVENT_ENVELOPE_SCHEMA)
    engagement = engagements(like).first()
    assert engagement.engagement_type == "like"
    assert engagement.subject_uri.endswith("/3mabcpost001")

    follow = dict(rows[0])
    follow["event_id"] = "c" * 64
    follow["collection"] = "app.bsky.graph.follow"
    follow["at_uri"] = "at://did:plc:aliceexample/app.bsky.graph.follow/follow-1"
    follow["payload_json"] = json.dumps(
        {
            "payload": {
                "record": {
                    "$type": "app.bsky.graph.follow",
                    "createdAt": "2026-10-04T18:00:00Z",
                    "subject": "did:plc:bobexample",
                }
            }
        }
    )
    edge = graph_edges(spark.createDataFrame([follow], EVENT_ENVELOPE_SCHEMA)).first()
    assert edge.subject_did == "did:plc:bobexample"

    identity = dict(rows[0])
    identity.update(
        event_id="d" * 64,
        event_kind="identity",
        operation="identity",
        collection=None,
        rkey=None,
        at_uri=None,
    )
    lifecycle = identity_events(spark.createDataFrame([identity], EVENT_ENVELOPE_SCHEMA)).first()
    assert lifecycle.event_kind == "identity"


def test_extracts_reply_external_quote_and_media_flags(spark: SparkSession) -> None:
    base = _fixture_events()[0]
    record = {
        "$type": "app.bsky.feed.post",
        "text": "rich post",
        "createdAt": "2026-10-04T18:00:00Z",
        "reply": {
            "root": {"uri": "at://root", "cid": "root-cid"},
            "parent": {"uri": "at://parent", "cid": "parent-cid"},
        },
        "embed": {
            "$type": "app.bsky.embed.recordWithMedia",
            "record": {"record": {"uri": "at://quoted", "cid": "quote-cid"}},
            "media": {"$type": "app.bsky.embed.images", "images": []},
        },
    }
    rich = {
        **base,
        "event_id": "e" * 64,
        "payload_json": json.dumps({"payload": {"record": record}}),
    }
    row = posts(spark.createDataFrame([rich], EVENT_ENVELOPE_SCHEMA)).first()
    assert row.reply_root_uri == "at://root"
    assert row.reply_parent_uri == "at://parent"
    assert row.quoted_post_uri == "at://quoted"
    assert row.has_quote is True
    assert row.has_media is True

    external = {
        **rich,
        "event_id": "9" * 64,
        "payload_json": json.dumps(
            {
                "payload": {
                    "record": {
                        **record,
                        "embed": {
                            "$type": "app.bsky.embed.external",
                            "external": {
                                "uri": "https://example.com/story",
                                "title": "Story",
                                "description": "Example",
                            },
                        },
                    }
                }
            }
        ),
    }
    external_row = posts(spark.createDataFrame([external], EVENT_ENVELOPE_SCHEMA)).first()
    assert external_row.external_uri == "https://example.com/story"
    assert external_row.has_external_link is True
