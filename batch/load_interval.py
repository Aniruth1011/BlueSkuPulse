from __future__ import annotations

import argparse
import logging
import os

from batch.intervals import DataInterval
from ingestion.logging import configure_logging
from warehouse.config import WarehouseSettings
from warehouse.loader import TABLE_SPECS, BigQuerySilverLoader


def silver_uris(root: str, dataset: str, interval: DataInterval) -> list[str]:
    if not root.startswith("gs://"):
        raise ValueError("hourly warehouse loading requires a gs:// Silver root")
    dates = sorted({date for date, _ in interval.hourly_partitions()})
    nested = dataset in {"events", "engagements", "identity"}
    suffix = "*/*.parquet" if nested else "*.parquet"
    return [f"{root.rstrip('/')}/{dataset}/event_date={date}/{suffix}" for date in dates]


def main() -> int:
    parser = argparse.ArgumentParser(description="Load one data interval from Silver into BigQuery")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--dataset", choices=sorted(TABLE_SPECS), required=True)
    parser.add_argument("--silver-root", default=os.getenv("ATMOSPULSE_SILVER_ROOT", ""))
    args = parser.parse_args()
    configure_logging()
    interval = DataInterval.parse(args.start, args.end)
    uris = silver_uris(args.silver_root, args.dataset, interval)
    loader = BigQuerySilverLoader(WarehouseSettings.from_env())
    loader.ensure_datasets()
    batch_id = loader.load(args.dataset, uris)
    logging.getLogger(__name__).info(
        "interval_warehouse_load_committed",
        extra={
            "fields": {
                "interval_batch_id": interval.batch_id,
                "load_batch_id": batch_id,
                "dataset": args.dataset,
                "uris": uris,
            }
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
