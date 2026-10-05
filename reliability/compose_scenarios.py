from __future__ import annotations

import argparse
import json
import logging
import shutil
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ingestion.logging import configure_logging

SERVICE_BY_SCENARIO = {
    "broker-outage": "redpanda",
    "ingestion-restart": "ingestion",
    "consumer-outage": "bronze",
    "spark-restart": "spark",
}


def compose(*arguments: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    standalone = shutil.which("docker-compose")
    command = [standalone] if standalone else ["docker", "compose"]
    return subprocess.run(
        [*command, *arguments],
        check=check,
        capture_output=True,
        text=True,
        timeout=180,
    )


def wait_running(service: str, timeout_seconds: float) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        result = compose("ps", "--status", "running", "--services", check=False)
        if result.returncode == 0 and service in result.stdout.splitlines():
            return
        time.sleep(1)
    raise TimeoutError(f"{service} did not return to running state within {timeout_seconds}s")


def run_scenario(name: str, outage_seconds: float, timeout_seconds: float) -> dict[str, Any]:
    service = SERVICE_BY_SCENARIO[name]
    logger = logging.getLogger(__name__)
    started = time.monotonic()
    logger.warning(
        "failure_injection_started", extra={"fields": {"scenario": name, "service": service}}
    )
    if name.endswith("restart"):
        compose("restart", service)
    else:
        compose("stop", service)
        time.sleep(outage_seconds)
        compose("start", service)
    wait_running(service, timeout_seconds)
    if service == "redpanda":
        compose("exec", "-T", "redpanda", "rpk", "cluster", "health", "--exit-when-healthy")
    recovery = time.monotonic() - started
    logger.info(
        "failure_injection_recovered",
        extra={"fields": {"scenario": name, "service": service, "recovery_seconds": recovery}},
    )
    return {
        "run_at": datetime.now(UTC).isoformat(),
        "scenario": name,
        "service": service,
        "outage_seconds": outage_seconds,
        "recovery_seconds": recovery,
        "status": "service_running",
        "row_impact": "requires reconciliation job; not inferred by this harness",
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Inject a scoped failure into the current AtmosPulse Docker Compose project"
    )
    parser.add_argument("scenario", choices=sorted(SERVICE_BY_SCENARIO))
    parser.add_argument("--outage-seconds", type=float, default=10.0)
    parser.add_argument("--timeout-seconds", type=float, default=60.0)
    parser.add_argument(
        "--output", type=Path, default=Path("benchmark/results/failure-latest.json")
    )
    args = parser.parse_args()
    configure_logging()
    result = run_scenario(args.scenario, args.outage_seconds, args.timeout_seconds)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
