from prometheus_client import Counter, Histogram

BQ_LOAD_ROWS = Counter(
    "bq_rows_loaded_total", "Rows inserted by successful BigQuery merges", ["table"]
)
BQ_LOAD_DURATION = Histogram(
    "bq_load_duration_seconds", "BigQuery load and merge duration", ["table"]
)
BQ_LOAD_FAILURES = Counter(
    "bq_load_failures_total", "Failed BigQuery load or merge jobs", ["table"]
)
