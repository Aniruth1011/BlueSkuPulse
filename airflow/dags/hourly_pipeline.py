from __future__ import annotations

from datetime import UTC, datetime

from _common import DBT_FLAGS, bounded_bash
from airflow.sdk import DAG

with DAG(
    dag_id="hourly_pipeline",
    description="Bounded Silver-to-Gold interval processing; ingestion remains a separate service",
    schedule="0 * * * *",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    tags=["atmospulse", "hourly"],
) as dag:
    heartbeat = bounded_bash("verify_broker_heartbeat", "python -m batch.heartbeat")
    bronze = bounded_bash(
        "verify_bronze_freshness",
        "python -m batch.freshness --layer bronze --max-age-seconds 900",
    )
    silver = bounded_bash(
        "verify_silver_freshness",
        "python -m batch.freshness --layer silver --max-age-seconds 3600",
    )
    loads = []
    for dataset in ("events", "posts", "engagements", "graph_edges", "identity", "quarantine"):
        loads.append(
            bounded_bash(
                f"load_{dataset}",
                "python -m batch.load_interval "
                '--start "$ATMOSPULSE_DATA_INTERVAL_START" '
                '--end "$ATMOSPULSE_DATA_INTERVAL_END" '
                f"--dataset {dataset}",
            )
        )
    build = bounded_bash("dbt_hourly_build", f"dbt build --select +tag:hourly {DBT_FLAGS}")
    metrics = bounded_bash(
        "publish_quality_metrics",
        "python -m batch.publish_dbt_metrics --artifact dbt/target/run_results.json",
        retries=1,
    )
    heartbeat >> bronze >> silver >> loads >> build >> metrics
