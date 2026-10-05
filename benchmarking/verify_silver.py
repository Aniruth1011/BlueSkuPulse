from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DATASETS = ("events", "posts", "engagements", "graph_edges", "identity", "quarantine")


def git_sha() -> str:
    return (
        subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=False,
            capture_output=True,
            text=True,
        ).stdout.strip()
        or "unavailable-in-image"
    )


def parquet_files(root: Path, dataset: str) -> list[str]:
    return sorted(str(path) for path in (root / dataset).glob("**/*.parquet"))


def dataset_counts(spark: SparkSession, files: list[str]) -> dict[str, int]:
    if not files:
        return {"rows": 0, "distinct_event_ids": 0, "duplicate_event_ids": 0}
    row = (
        spark.read.parquet(*files)
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.countDistinct("event_id").alias("distinct_event_ids"),
        )
        .first()
    )
    if row is None:
        raise RuntimeError("Silver aggregate unexpectedly returned no row")
    rows = int(row.rows)
    distinct = int(row.distinct_event_ids)
    return {
        "rows": rows,
        "distinct_event_ids": distinct,
        "duplicate_event_ids": rows - distinct,
    }


def verify(
    root: Path, output: Path, pipeline_commit: str, verifier_commit: str | None = None
) -> dict[str, Any]:
    started = time.monotonic()
    spark = (
        SparkSession.builder.master(os.getenv("ATMOSPULSE_VERIFY_SPARK_MASTER", "local[2]"))
        .appName("AtmosPulseSilverVerification")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "12")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        datasets = {name: dataset_counts(spark, parquet_files(root, name)) for name in DATASETS}
    finally:
        spark.stop()
    result = {
        "status": "MEASURED",
        "run_at": datetime.now(UTC).isoformat(),
        "pipeline_commit": pipeline_commit,
        "verifier_commit": verifier_commit or git_sha(),
        "silver_root": str(root),
        "elapsed_seconds": time.monotonic() - started,
        "datasets": datasets,
        "canonical_duplicate_free": datasets["events"]["duplicate_event_ids"] == 0,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Count and reconcile canonical Silver Parquet")
    parser.add_argument("--silver-root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--pipeline-commit", required=True)
    parser.add_argument("--verifier-commit")
    args = parser.parse_args()
    result = verify(
        args.silver_root,
        args.output,
        args.pipeline_commit,
        verifier_commit=args.verifier_commit,
    )
    print(args.output)
    return 0 if result["canonical_duplicate_free"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
