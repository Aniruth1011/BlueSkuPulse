from __future__ import annotations

import argparse
import logging
import subprocess
import time

from ingestion.logging import configure_logging


def main() -> int:
    parser = argparse.ArgumentParser(description="Log and execute one bounded Airflow task")
    parser.add_argument("--task", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        parser.error("a command is required after --")
    configure_logging()
    logger = logging.getLogger(__name__)
    started = time.monotonic()
    fields = {"task": args.task, "run_id": args.run_id, "command": command[0]}
    logger.info("bounded_task_started", extra={"fields": fields})
    try:
        subprocess.run(command, check=True)
    except Exception:
        logger.exception("bounded_task_failed", extra={"fields": fields})
        raise
    logger.info(
        "bounded_task_succeeded",
        extra={"fields": {**fields, "duration_seconds": time.monotonic() - started}},
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
