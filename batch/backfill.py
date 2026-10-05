from __future__ import annotations

import argparse
import logging
import os
import re
import subprocess
import sys
from pathlib import Path

from batch.intervals import DataInterval
from ingestion.logging import configure_logging

_COLLECTION = re.compile(r"^[a-z][a-z0-9]*(?:\.[a-z][a-z0-9]*)+$")
VALID_TARGETS = {"silver", "warehouse", "gold"}


def validate_collections(value: str) -> tuple[str, ...]:
    collections = tuple(sorted({item.strip() for item in value.split(",") if item.strip()}))
    if not collections or any(not _COLLECTION.fullmatch(item) for item in collections):
        raise ValueError("collections must be comma-separated AT Protocol NSIDs")
    return collections


def silver_output(root: str, interval: DataInterval, collections: tuple[str, ...]) -> str:
    material = f"{interval.batch_id}:{','.join(sorted(collections))}"
    import hashlib

    suffix = hashlib.sha256(material.encode()).hexdigest()[:16]
    return f"{root.rstrip('/')}/_backfills/{interval.start:%Y%m%dT%H%M%S}-{suffix}"


def run_silver_backfill(
    interval: DataInterval,
    collections: tuple[str, ...],
    bronze_path: str,
    output_path: str,
) -> None:
    command = [
        sys.executable,
        "-m",
        "streaming.jobs.normalize_events",
        "--mode",
        "batch",
        "--bronze-path",
        bronze_path,
        "--output-path",
        output_path,
        "--start",
        interval.start.isoformat(),
        "--end",
        interval.end.isoformat(),
        "--collections",
        ",".join(collections),
    ]
    logging.getLogger(__name__).info(
        "backfill_silver_started",
        extra={"fields": {"batch_id": interval.batch_id, "output_path": output_path}},
    )
    subprocess.run(command, check=True)


def output_exists(path: str) -> bool:
    if path.startswith("gs://"):
        from google.cloud import storage  # type: ignore[attr-defined]

        bucket_and_prefix = path.removeprefix("gs://")
        bucket_name, _, prefix = bucket_and_prefix.partition("/")
        return (
            next(storage.Client().list_blobs(bucket_name, prefix=prefix + "/", max_results=1), None)
            is not None
        )
    return Path(path).exists()


def main() -> int:
    parser = argparse.ArgumentParser(description="Run an idempotent Bronze-to-Silver backfill")
    parser.add_argument("--start", default=os.getenv("ATMOSPULSE_BACKFILL_START"))
    parser.add_argument("--end", default=os.getenv("ATMOSPULSE_BACKFILL_END"))
    parser.add_argument("--collections", default=os.getenv("ATMOSPULSE_BACKFILL_COLLECTIONS", ""))
    parser.add_argument("--target", choices=sorted(VALID_TARGETS), default="gold")
    parser.add_argument("--source-mode", choices=("bronze",), default="bronze")
    parser.add_argument("--rebuild", choices=("true", "false"), default="false")
    parser.add_argument("--bronze-path", default=os.getenv("ATMOSPULSE_BACKFILL_BRONZE_PATH"))
    parser.add_argument("--silver-root", default=os.getenv("ATMOSPULSE_SILVER_ROOT", "data/silver"))
    args = parser.parse_args()
    configure_logging()
    if not args.start or not args.end or not args.bronze_path:
        parser.error("start, end, and bronze-path are required")
    interval = DataInterval.parse(args.start, args.end)
    collections = validate_collections(args.collections)
    output = silver_output(args.silver_root, interval, collections)
    if args.rebuild == "false" and output_exists(output):
        logging.getLogger(__name__).info(
            "backfill_silver_already_complete",
            extra={"fields": {"batch_id": interval.batch_id, "output_path": output}},
        )
        return 0
    run_silver_backfill(interval, collections, args.bronze_path, output)
    logging.getLogger(__name__).info(
        "backfill_silver_committed",
        extra={
            "fields": {
                "batch_id": interval.batch_id,
                "target": args.target,
                "output_path": output,
                "collections": collections,
            }
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
