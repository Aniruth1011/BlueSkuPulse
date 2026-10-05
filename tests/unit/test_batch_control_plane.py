from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from batch.backfill import silver_output, validate_collections
from batch.freshness import assert_fresh, newest_local_manifest
from batch.intervals import DataInterval
from batch.load_interval import silver_uris
from batch.publish_dbt_metrics import artifact_metrics
from batch.reconcile import ReconciliationResult, validate


def test_interval_is_stable_and_enumerates_cross_day_hours() -> None:
    interval = DataInterval.parse("2026-10-04T23:30:00Z", "2026-10-05T01:01:00Z")
    assert (
        interval.batch_id
        == DataInterval.parse("2026-10-04T23:30:00+00:00", "2026-10-05T01:01:00+00:00").batch_id
    )
    assert interval.hourly_partitions() == (
        ("2026-10-04", "23"),
        ("2026-10-05", "00"),
        ("2026-10-05", "01"),
    )


def test_backfill_output_and_collection_validation_are_deterministic() -> None:
    interval = DataInterval.parse("2026-10-04T00:00:00Z", "2026-10-05T00:00:00Z")
    collections = validate_collections("app.bsky.feed.post,app.bsky.feed.like")
    assert collections == ("app.bsky.feed.like", "app.bsky.feed.post")
    assert silver_output("gs://bucket/silver", interval, collections) == silver_output(
        "gs://bucket/silver/", interval, tuple(reversed(collections))
    )
    with pytest.raises(ValueError):
        validate_collections("app.bsky.feed.post; rm -rf x")


def test_silver_uri_layout_handles_nested_partitions() -> None:
    interval = DataInterval.parse("2026-10-04T23:00:00Z", "2026-10-05T01:00:00Z")
    assert silver_uris("gs://bucket/silver", "posts", interval) == [
        "gs://bucket/silver/posts/event_date=2026-10-04/*.parquet",
        "gs://bucket/silver/posts/event_date=2026-10-05/*.parquet",
    ]
    assert silver_uris("gs://bucket/silver", "events", interval)[0].endswith("*/*.parquet")


def test_manifest_freshness_uses_manifest_timestamp(tmp_path: Path) -> None:
    directory = tmp_path / "bluesky" / "_manifests"
    directory.mkdir(parents=True)
    (directory / "batch-a.json").write_text('{"created_at":"2026-10-04T12:00:00Z"}')
    newest = newest_local_manifest(tmp_path)
    assert assert_fresh(newest, 301, datetime(2026, 10, 4, 12, 5, tzinfo=UTC)) == 300
    with pytest.raises(RuntimeError, match="stale"):
        assert_fresh(newest, 299, datetime(2026, 10, 4, 12, 5, tzinfo=UTC))


def test_reconciliation_fails_closed_above_threshold() -> None:
    result = ReconciliationResult("2026-10-04", 100, 100, 99, 0, 0, 1, 0.01)
    with pytest.raises(RuntimeError, match="exceeds"):
        validate(result, 0.001)


def test_dbt_artifact_metrics_count_failures(tmp_path: Path) -> None:
    artifact = tmp_path / "run_results.json"
    artifact.write_text(
        json.dumps(
            {
                "results": [
                    {"unique_id": "test.a", "status": "pass", "execution_time": 1.5},
                    {"unique_id": "model.b", "status": "error", "execution_time": 2.0},
                ]
            }
        )
    )
    assert artifact_metrics(artifact) == (1, 1, 3.5)
