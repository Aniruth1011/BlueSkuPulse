from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path

from ingestion.logging import configure_logging


def artifact_metrics(path: Path) -> tuple[int, int, float]:
    artifact = json.loads(path.read_text())
    results = artifact.get("results", [])
    failures = sum(1 for row in results if row.get("status") not in {"success", "pass"})
    tests = sum(1 for row in results if str(row.get("unique_id", "")).startswith("test."))
    duration = sum(float(row.get("execution_time", 0)) for row in results)
    return tests, failures, duration


def main() -> int:
    parser = argparse.ArgumentParser(description="Publish dbt batch outcome metrics")
    parser.add_argument("--artifact", default="dbt/target/run_results.json")
    args = parser.parse_args()
    configure_logging()
    tests, failures, duration = artifact_metrics(Path(args.artifact))
    gateway = os.environ["ATMOSPULSE_PUSHGATEWAY_URL"]
    from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

    registry = CollectorRegistry()
    Gauge("dbt_tests_total", "dbt tests executed", registry=registry).set(tests)
    Gauge("dbt_test_failures", "dbt tests that did not pass", registry=registry).set(failures)
    Gauge("dbt_build_duration_seconds", "dbt build execution time", registry=registry).set(duration)
    Gauge(
        "dbt_last_success_timestamp_seconds", "Last successful dbt build", registry=registry
    ).set_to_current_time()
    push_to_gateway(gateway, job="atmospulse_dbt", registry=registry)
    logging.getLogger(__name__).info(
        "dbt_metrics_published",
        extra={"fields": {"tests": tests, "failures": failures, "duration_seconds": duration}},
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
