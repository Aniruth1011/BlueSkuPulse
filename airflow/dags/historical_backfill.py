from __future__ import annotations

from datetime import UTC, datetime

from _common import bounded_bash
from airflow.sdk import DAG, Param

with DAG(
    dag_id="historical_backfill",
    description="Explicit, deterministic Bronze replay through Silver, BigQuery, and Gold",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    params={
        "start": Param("2026-01-01T00:00:00+00:00", type="string", format="date-time"),
        "end": Param("2026-01-01T01:00:00+00:00", type="string", format="date-time"),
        "collections": Param(
            "app.bsky.feed.post,app.bsky.feed.like,app.bsky.feed.repost,app.bsky.graph.follow",
            type="string",
        ),
        "target": Param("gold", enum=["silver", "warehouse", "gold"]),
        "source_mode": Param("bronze", enum=["bronze"]),
        "rebuild": Param(False, type="boolean"),
    },
    tags=["atmospulse", "backfill"],
) as dag:
    common = (
        '--start "$ATMOSPULSE_BACKFILL_START" --end "$ATMOSPULSE_BACKFILL_END" '
        '--collections "$ATMOSPULSE_BACKFILL_COLLECTIONS"'
    )
    env = {
        "ATMOSPULSE_BACKFILL_START": "{{ params.start }}",
        "ATMOSPULSE_BACKFILL_END": "{{ params.end }}",
        "ATMOSPULSE_BACKFILL_COLLECTIONS": "{{ params.collections }}",
        "ATMOSPULSE_BACKFILL_TARGET": "{{ params.target }}",
        "ATMOSPULSE_BACKFILL_SOURCE_MODE": "{{ params.source_mode }}",
        "ATMOSPULSE_BACKFILL_REBUILD": "{{ params.rebuild | lower }}",
    }
    silver = bounded_bash(
        "rebuild_silver_interval",
        f'python -m batch.backfill {common} --target "$ATMOSPULSE_BACKFILL_TARGET" '
        '--source-mode "$ATMOSPULSE_BACKFILL_SOURCE_MODE" '
        '--rebuild "$ATMOSPULSE_BACKFILL_REBUILD"',
    )
    warehouse = bounded_bash(
        "merge_backfill_to_bigquery", f"python -m batch.load_backfill {common}"
    )
    gold = bounded_bash("rebuild_affected_gold", "python -m batch.build_backfill_gold")
    for task in (silver, warehouse, gold):
        task.env.update(env)
    silver >> warehouse >> gold
