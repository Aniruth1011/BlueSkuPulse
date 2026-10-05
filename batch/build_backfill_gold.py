from __future__ import annotations

import logging
import os
import subprocess

from ingestion.logging import configure_logging


def main() -> int:
    configure_logging()
    if os.getenv("ATMOSPULSE_BACKFILL_TARGET", "gold") != "gold":
        logging.getLogger(__name__).info("backfill_gold_skipped_for_non_gold_target")
        return 0
    target = os.getenv("ATMOSPULSE_DBT_TARGET", "bigquery")
    subprocess.run(
        [
            "dbt",
            "build",
            "--select",
            "+tag:hourly",
            "--project-dir",
            "dbt",
            "--profiles-dir",
            "dbt",
            "--target",
            target,
        ],
        check=True,
    )
    logging.getLogger(__name__).info("backfill_gold_committed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
