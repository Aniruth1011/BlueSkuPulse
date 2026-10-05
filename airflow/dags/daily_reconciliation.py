from __future__ import annotations

from datetime import UTC, datetime

from _common import DBT_FLAGS, bounded_bash
from airflow.sdk import DAG

with DAG(
    dag_id="daily_reconciliation",
    description="Daily freshness, contract, folding, duplicate, and cross-layer reconciliation",
    schedule="30 2 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    tags=["atmospulse", "quality", "daily"],
) as dag:
    freshness = bounded_bash("dbt_source_freshness", f"dbt source freshness {DBT_FLAGS}")
    quality = bounded_bash(
        "validate_contracts_and_folding",
        "dbt test --select assert_current_state_has_no_duplicate_uris "
        "assert_event_timestamps_are_possible assert_silver_to_staging_reconciles "
        f"assert_topic_spam_is_penalized {DBT_FLAGS}",
    )
    health = bounded_bash(
        "build_pipeline_health", f"dbt build --select +mart_pipeline_health {DBT_FLAGS}"
    )
    reconcile = bounded_bash(
        "validate_reconciliation_delta",
        "python -m batch.reconcile "
        "--date \"{{ data_interval_start.strftime('%Y-%m-%d') }}\" "
        "--maximum-delta-ratio 0.001 "
        '--report-path "state/reconciliation/'
        "{{ data_interval_start.strftime('%Y-%m-%d') }}.json\"",
    )
    publish = bounded_bash(
        "publish_daily_quality_metrics",
        "python -m batch.publish_dbt_metrics --artifact dbt/target/run_results.json",
        retries=1,
    )
    freshness >> quality >> health >> reconcile >> publish
