from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

from ingestion.logging import configure_logging


def newest_local_manifest(root: Path) -> datetime:
    manifests = list((root / "bluesky" / "_manifests").glob("batch-*.json"))
    if not manifests:
        raise RuntimeError(f"no Bronze manifests found below {root}")
    newest: datetime | None = None
    for path in manifests:
        created = datetime.fromisoformat(json.loads(path.read_text())["created_at"])
        newest = created if newest is None or created > newest else newest
    assert newest is not None
    return newest.astimezone(UTC)


def newest_gcs_manifest(bucket_name: str, prefix: str) -> datetime:
    from google.cloud import storage  # type: ignore[attr-defined]

    blobs = storage.Client().list_blobs(
        bucket_name, prefix=f"{prefix.strip('/')}/bluesky/_manifests/".lstrip("/")
    )
    newest = max((blob.updated for blob in blobs if blob.name.endswith(".json")), default=None)
    if newest is None:
        raise RuntimeError(f"no Bronze manifests found in gs://{bucket_name}/{prefix}")
    return cast(datetime, newest).astimezone(UTC)


def assert_fresh(timestamp: datetime, max_age_seconds: float, now: datetime | None = None) -> float:
    reference = now or datetime.now(UTC)
    age = (reference - timestamp).total_seconds()
    if age < -300:
        raise RuntimeError(f"freshness timestamp is {abs(age):.1f}s in the future")
    if age > max_age_seconds:
        raise RuntimeError(f"Bronze is stale: age={age:.1f}s limit={max_age_seconds:.1f}s")
    return max(age, 0.0)


def newest_local_silver(root: Path) -> datetime:
    files = list(root.glob("**/*.parquet"))
    if not files:
        raise RuntimeError(f"no Silver Parquet files found below {root}")
    return datetime.fromtimestamp(max(path.stat().st_mtime for path in files), tz=UTC)


def newest_gcs_silver(root: str) -> datetime:
    from google.cloud import storage  # type: ignore[attr-defined]

    without_scheme = root.removeprefix("gs://")
    bucket_name, _, prefix = without_scheme.partition("/")
    if not bucket_name:
        raise ValueError("a gs:// Silver root is required")
    blobs = storage.Client().list_blobs(bucket_name, prefix=prefix.strip("/") + "/")
    newest = max((blob.updated for blob in blobs if blob.name.endswith(".parquet")), default=None)
    if newest is None:
        raise RuntimeError(f"no Silver Parquet files found below {root}")
    return cast(datetime, newest).astimezone(UTC)


def main() -> int:
    parser = argparse.ArgumentParser(description="Fail when immutable Bronze manifests are stale")
    parser.add_argument("--max-age-seconds", type=float, default=900)
    parser.add_argument("--layer", choices=("bronze", "silver"), default="bronze")
    args = parser.parse_args()
    configure_logging()
    backend = os.getenv("ATMOSPULSE_BRONZE_BACKEND", "local")
    if args.layer == "silver":
        silver_root = os.getenv("ATMOSPULSE_SILVER_ROOT", "data/silver")
        timestamp = (
            newest_gcs_silver(silver_root)
            if silver_root.startswith("gs://")
            else newest_local_silver(Path(silver_root))
        )
    elif backend == "gcs":
        timestamp = newest_gcs_manifest(
            os.environ["ATMOSPULSE_BRONZE_GCS_BUCKET"],
            os.getenv("ATMOSPULSE_BRONZE_GCS_PREFIX", ""),
        )
    elif backend == "local":
        timestamp = newest_local_manifest(Path(os.getenv("ATMOSPULSE_BRONZE_ROOT", "data/bronze")))
    else:
        raise ValueError(f"unsupported Bronze backend: {backend}")
    age = assert_fresh(timestamp, args.max_age_seconds)
    logging.getLogger(__name__).info(
        "layer_freshness_ok",
        extra={"fields": {"layer": args.layer, "backend": backend, "age_seconds": round(age, 3)}},
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
