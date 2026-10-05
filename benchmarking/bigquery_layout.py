from __future__ import annotations

import argparse
import time
from pathlib import Path

from google.cloud import bigquery

from benchmarking.results import BenchmarkResult


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compare the same bounded query against naive and optimized BigQuery tables"
    )
    parser.add_argument("--project", required=True)
    parser.add_argument("--location", default="US")
    parser.add_argument(
        "--naive-table", required=True, help="Fully qualified project.dataset.table"
    )
    parser.add_argument("--optimized-table", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--execute", action="store_true", help="Bill queries and measure runtime")
    parser.add_argument("--output-dir", type=Path, default=Path("benchmark/results"))
    args = parser.parse_args()
    template = (
        "SELECT collection, COUNT(*) AS events FROM `{table}` "
        "WHERE event_date >= @start_date AND event_date < @end_date GROUP BY collection"
    )
    client = bigquery.Client(project=args.project)
    parameters = [
        bigquery.ScalarQueryParameter("start_date", "DATE", args.start_date),
        bigquery.ScalarQueryParameter("end_date", "DATE", args.end_date),
    ]

    def measure(table: str) -> dict[str, int | float]:
        sql = template.format(table=table)
        config = bigquery.QueryJobConfig(
            dry_run=not args.execute,
            use_query_cache=False,
            query_parameters=parameters,
        )
        started = time.perf_counter()
        job = client.query(sql, job_config=config, location=args.location)
        if args.execute:
            job.result()
        return {
            "bytes_processed": int(job.total_bytes_processed or 0),
            "runtime_seconds": time.perf_counter() - started,
        }

    naive = measure(args.naive_table)
    optimized = measure(args.optimized_table)
    naive_bytes = int(naive["bytes_processed"])
    optimized_bytes = int(optimized["bytes_processed"])
    reduction = 0.0 if naive_bytes == 0 else 1 - (optimized_bytes / naive_bytes)
    result = BenchmarkResult.create(
        benchmark_name="bigquery-layout",
        parameters={
            "naive_table": args.naive_table,
            "optimized_table": args.optimized_table,
            "start_date": args.start_date,
            "end_date": args.end_date,
            "execute": args.execute,
        },
        measurements={
            "naive": naive,
            "optimized": optimized,
            "bytes_scanned_reduction": reduction,
        },
        method="Identical parameterized query with query cache disabled; dry-run unless --execute",
        target_run=args.execute,
    )
    print(result.write(args.output_dir))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
