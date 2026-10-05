from prometheus_client import Counter, Gauge, Histogram

BRONZE_EVENTS = Counter("bronze_events_written_total", "Raw events written to Bronze")
BRONZE_FILES = Counter("bronze_files_written_total", "Immutable Bronze objects written")
BRONZE_BYTES = Counter("bronze_compressed_bytes_written_total", "Compressed Bronze bytes")
BRONZE_BATCH_DURATION = Histogram("bronze_batch_duration_seconds", "Time to create a Bronze batch")
BRONZE_FRESHNESS = Gauge(
    "bronze_freshness_timestamp_seconds", "Newest successfully manifested Bronze batch time"
)
