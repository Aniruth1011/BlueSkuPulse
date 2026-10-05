from __future__ import annotations

import argparse
import gzip
import json
import math
import os
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from statistics import median
from typing import Any


def parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def percentile(values: list[float], quantile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(quantile * len(ordered)) - 1)]


def distribution(values: list[float]) -> dict[str, float | int | None]:
    return {
        "samples": len(values),
        "p50_seconds": percentile(values, 0.50),
        "p95_seconds": percentile(values, 0.95),
        "p99_seconds": percentile(values, 0.99),
        "max_seconds": max(values) if values else None,
    }


def analyze_bronze(root: Path, start: datetime, end: datetime) -> dict[str, Any]:
    manifest_root = root / "bluesky" / "_manifests"
    source_lag: list[float] = []
    bronze_lag: list[float] = []
    landing_delay: list[float] = []
    event_ids: Counter[str] = Counter()
    kinds: Counter[str] = Counter()
    collections: Counter[str] = Counter()
    operations: Counter[str] = Counter()
    selected_objects: set[Path] = set()
    selected_batches = 0
    compressed_bytes = 0
    uncompressed_bytes = 0
    max_manifest_time: datetime | None = None

    for manifest_path in manifest_root.glob("batch-*.json"):
        manifest = json.loads(manifest_path.read_text())
        committed_at = parse_timestamp(manifest["created_at"])
        if committed_at < start or committed_at > end:
            continue
        selected_batches += 1
        max_manifest_time = max(max_manifest_time or committed_at, committed_at)
        for object_key in manifest["objects"]:
            object_path = root / object_key
            selected_objects.add(object_path)
            compressed_bytes += object_path.stat().st_size
            with gzip.open(object_path, "rb") as handle:
                for line in handle:
                    uncompressed_bytes += len(line)
                    row = json.loads(line)
                    event = row["event"]
                    ingested_at = parse_timestamp(event["ingested_at"])
                    if ingested_at < start or ingested_at > end:
                        continue
                    event_time = parse_timestamp(event["event_time"])
                    source_lag.append(max(0.0, (ingested_at - event_time).total_seconds()))
                    bronze_lag.append(max(0.0, (committed_at - event_time).total_seconds()))
                    landing_delay.append(max(0.0, (committed_at - ingested_at).total_seconds()))
                    event_ids[str(event["event_id"])] += 1
                    kinds[str(event.get("event_kind"))] += 1
                    collections[str(event.get("collection"))] += 1
                    operations[str(event.get("operation"))] += 1

    sizes = [path.stat().st_size for path in selected_objects]
    duration_hours = max((end - start).total_seconds() / 3600, 1 / 3600)
    duplicate_deliveries = sum(count - 1 for count in event_ids.values())
    return {
        "event_count": sum(event_ids.values()),
        "distinct_event_ids": len(event_ids),
        "duplicate_deliveries": duplicate_deliveries,
        "duplicate_rate": duplicate_deliveries / sum(event_ids.values()) if event_ids else 0.0,
        "batch_count": selected_batches,
        "object_count": len(selected_objects),
        "objects_per_hour": len(selected_objects) / duration_hours,
        "median_object_bytes": median(sizes) if sizes else None,
        "compressed_bytes": compressed_bytes,
        "uncompressed_bytes": uncompressed_bytes,
        "compression_ratio_uncompressed_to_compressed": (
            uncompressed_bytes / compressed_bytes if compressed_bytes else None
        ),
        "source_to_ingest": distribution(source_lag),
        "source_to_bronze": distribution(bronze_lag),
        "ingest_to_bronze": distribution(landing_delay),
        "freshness_at_window_end_seconds": (
            max(0.0, (end - max_manifest_time).total_seconds()) if max_manifest_time else None
        ),
        "event_kinds": dict(kinds),
        "collections": dict(collections),
        "operations": dict(operations),
    }


def analyze_silver(root: Path, start: datetime, end: datetime) -> dict[str, Any]:
    import pyarrow.parquet as pq  # type: ignore[import-untyped]

    latencies: list[float] = []
    processing_delays: list[float] = []
    event_ids: Counter[str] = Counter()
    files = list((root / "events").rglob("*.parquet"))
    for path in files:
        available_at = datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
        table = pq.read_table(path, columns=["event_id", "event_time", "ingested_at"])
        for row in table.to_pylist():
            ingested_at = row["ingested_at"].replace(tzinfo=UTC)
            if ingested_at < start or ingested_at > end:
                continue
            event_time = row["event_time"].replace(tzinfo=UTC)
            event_ids[str(row["event_id"])] += 1
            latencies.append(max(0.0, (available_at - event_time).total_seconds()))
            processing_delays.append(max(0.0, (available_at - ingested_at).total_seconds()))
    duplicate_rows = sum(count - 1 for count in event_ids.values())
    return {
        "event_rows": sum(event_ids.values()),
        "distinct_event_ids": len(event_ids),
        "duplicate_rows": duplicate_rows,
        "parquet_file_count": len(files),
        "source_to_queryable_silver_upper_bound": distribution(latencies),
        "ingest_to_queryable_silver_upper_bound": distribution(processing_delays),
        "availability_method": (
            "Parquet file mtime; an upper-bound proxy, not an instrumented row timestamp"
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Measure a bounded live AtmosPulse window")
    parser.add_argument("--bronze-root", type=Path, required=True)
    parser.add_argument("--silver-root", type=Path)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    start = parse_timestamp(args.start)
    end = parse_timestamp(args.end)
    result: dict[str, Any] = {
        "schema_version": 1,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "git_commit": os.getenv("ATMOSPULSE_BENCHMARK_SHA", "unknown"),
        "bronze": analyze_bronze(args.bronze_root, start, end),
    }
    if args.silver_root:
        result["silver"] = analyze_silver(args.silver_root, start, end)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
