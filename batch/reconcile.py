from __future__ import annotations

import argparse
import json
import logging
import os
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any

from ingestion.logging import configure_logging
from warehouse.config import WarehouseSettings


@dataclass(frozen=True, slots=True)
class ReconciliationResult:
    event_date: str
    silver_events: int
    distinct_silver_events: int
    modeled_mutations: int
    quarantine_events: int
    duplicate_events: int
    unexplained_delta: int
    reconciliation_delta_ratio: float


def fetch_reconciliation(
    client: Any, settings: WarehouseSettings, event_date: date
) -> ReconciliationResult:
    table = f"{settings.project_id}.{settings.analytics_dataset}.mart_pipeline_health"
    sql = f"""
SELECT event_date, silver_events, distinct_silver_events, modeled_mutations,
       quarantine_events, duplicate_events, unexplained_delta,
       COALESCE(reconciliation_delta_ratio, 0) AS reconciliation_delta_ratio
FROM `{table}`
WHERE event_date = @event_date
""".strip()
    from google.cloud import bigquery

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("event_date", "DATE", event_date)]
    )
    rows = list(client.query(sql, job_config=job_config, location=settings.location).result())
    if len(rows) != 1:
        raise RuntimeError(f"expected one reconciliation row for {event_date}, found {len(rows)}")
    row = rows[0]
    return ReconciliationResult(
        event_date=str(row.event_date),
        silver_events=int(row.silver_events),
        distinct_silver_events=int(row.distinct_silver_events),
        modeled_mutations=int(row.modeled_mutations),
        quarantine_events=int(row.quarantine_events),
        duplicate_events=int(row.duplicate_events),
        unexplained_delta=int(row.unexplained_delta),
        reconciliation_delta_ratio=float(row.reconciliation_delta_ratio),
    )


def validate(result: ReconciliationResult, maximum_delta_ratio: float) -> None:
    if result.reconciliation_delta_ratio > maximum_delta_ratio:
        raise RuntimeError(
            "reconciliation delta exceeds limit: "
            f"actual={result.reconciliation_delta_ratio:.6f} limit={maximum_delta_ratio:.6f}"
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate Bronze/Silver/Gold accounting")
    parser.add_argument("--date", required=True, type=date.fromisoformat)
    parser.add_argument("--maximum-delta-ratio", type=float, default=0.001)
    parser.add_argument("--report-path")
    args = parser.parse_args()
    configure_logging()
    settings = WarehouseSettings.from_env()
    from google.cloud import bigquery

    result = fetch_reconciliation(bigquery.Client(project=settings.project_id), settings, args.date)
    if args.report_path:
        destination = Path(args.report_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(json.dumps(asdict(result), sort_keys=True, indent=2) + "\n")
    validate(result, args.maximum_delta_ratio)
    logging.getLogger(__name__).info(
        "daily_reconciliation_passed", extra={"fields": asdict(result)}
    )
    if gateway := os.getenv("ATMOSPULSE_PUSHGATEWAY_URL"):
        from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

        registry = CollectorRegistry()
        delta = Gauge(
            "reconciliation_delta_ratio",
            "Unexplained canonical accounting delta",
            registry=registry,
        )
        delta.set(result.reconciliation_delta_ratio)
        push_to_gateway(gateway, job="atmospulse_reconciliation", registry=registry)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
