from __future__ import annotations

import ast
import json
from pathlib import Path

import yaml

ROOT = Path(__file__).parents[2]


def test_airflow_dags_are_syntax_valid_and_never_run_ingestion() -> None:
    expected = {"hourly_pipeline.py", "daily_reconciliation.py", "historical_backfill.py"}
    files = {path.name: path for path in (ROOT / "airflow" / "dags").glob("*.py")}
    assert expected <= files.keys()
    for name in expected:
        source = files[name].read_text()
        ast.parse(source)
        assert "ingestion.producer" not in source
        assert "websocket" not in source.lower()
        assert "bounded_bash" in source


def test_grafana_dashboards_are_valid_and_cover_products_and_health() -> None:
    dashboard_dir = ROOT / "monitoring" / "grafana" / "dashboards"
    dashboards = [json.loads(path.read_text()) for path in dashboard_dir.glob("*.json")]
    assert {item["uid"] for item in dashboards} == {"atmospulse-health", "atmospulse-analytics"}
    titles = {panel["title"] for item in dashboards for panel in item["panels"]}
    assert {"Network activity", "Rising topics (5 minute)", "Conversation health"} <= titles
    assert {"Ingestion lag percentiles", "Max Kafka lag", "Reconciliation delta"} <= titles


def test_alert_rules_cover_each_critical_failure_mode() -> None:
    rules = yaml.safe_load((ROOT / "monitoring" / "alerts" / "pipeline.yml").read_text())
    names = {rule["alert"] for group in rules["groups"] for rule in group["rules"]}
    assert {
        "SourceEventsSilent",
        "CursorCheckpointStale",
        "KafkaConsumerLagHigh",
        "DLQRateHigh",
        "BronzeStale",
        "BigQueryLoadFailure",
        "DbtCriticalTestFailure",
        "ReconciliationDeltaAboveSLO",
        "AirflowTaskFailure",
    } <= names
