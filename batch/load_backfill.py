from __future__ import annotations

import argparse
import logging
import os

from batch.backfill import silver_output, validate_collections
from batch.intervals import DataInterval
from ingestion.logging import configure_logging
from warehouse.config import WarehouseSettings
from warehouse.loader import TABLE_SPECS, BigQuerySilverLoader


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Load a deterministic Silver backfill into BigQuery"
    )
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--collections", required=True)
    parser.add_argument("--silver-root", default=os.getenv("ATMOSPULSE_SILVER_ROOT", ""))
    args = parser.parse_args()
    configure_logging()
    if os.getenv("ATMOSPULSE_BACKFILL_TARGET", "gold") == "silver":
        logging.getLogger(__name__).info("backfill_warehouse_skipped_for_silver_target")
        return 0
    interval = DataInterval.parse(args.start, args.end)
    collections = validate_collections(args.collections)
    output = silver_output(args.silver_root, interval, collections)
    if not output.startswith("gs://"):
        raise ValueError("BigQuery backfill loading requires a gs:// Silver output")
    loader = BigQuerySilverLoader(WarehouseSettings.from_env())
    loader.ensure_datasets()
    for dataset in TABLE_SPECS:
        batch_id = loader.load(dataset, [f"{output}/{dataset}/event_date=*/*.parquet"])
        logging.getLogger(__name__).info(
            "backfill_dataset_loaded",
            extra={"fields": {"dataset": dataset, "batch_id": batch_id, "output": output}},
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
