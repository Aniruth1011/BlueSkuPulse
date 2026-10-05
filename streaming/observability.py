from __future__ import annotations

import json
import logging
from datetime import UTC, datetime

from prometheus_client import Counter, Gauge, Histogram
from pyspark.sql.streaming.listener import (
    QueryIdleEvent,
    QueryProgressEvent,
    QueryStartedEvent,
    QueryTerminatedEvent,
    StreamingQueryListener,
)

SPARK_ROWS = Counter("spark_rows_processed_total", "Rows processed by Spark", ["query_name"])
SPARK_BATCH_DURATION = Histogram(
    "spark_batch_duration_seconds", "Spark micro-batch duration", ["query_name"]
)
SPARK_INPUT_RATE = Gauge("spark_input_rows_per_second", "Spark input rate", ["query_name"])
SPARK_PROCESSING_RATE = Gauge(
    "spark_processed_rows_per_second", "Spark processing rate", ["query_name"]
)
SPARK_QUERY_FAILURES = Counter(
    "spark_query_failures_total", "Terminated Spark queries with an exception", ["query_name"]
)
SPARK_DUPLICATES = Counter(
    "spark_duplicate_rows_dropped_total",
    "Duplicate rows dropped by stateful operators",
    ["query_name"],
)
SPARK_QUARANTINED = Counter("spark_quarantined_rows_total", "Rows written to Silver quarantine")
SPARK_STATE_ROWS = Gauge(
    "spark_state_store_rows", "Rows retained in Spark state operators", ["query_name"]
)
SPARK_EVENT_TIME_LAG = Gauge(
    "spark_event_time_lag_seconds",
    "Wall-clock lag behind maximum observed event time",
    ["query_name"],
)


class StructuredLoggingQueryListener(StreamingQueryListener):
    def __init__(self) -> None:
        super().__init__()
        self._logger = logging.getLogger(self.__class__.__name__)
        self._names: dict[str, str] = {}

    def onQueryStarted(self, event: QueryStartedEvent) -> None:
        query_id = str(event.id)
        name = str(event.name or query_id)
        self._names[query_id] = name
        self._logger.info(
            "spark_query_started", extra={"fields": {"query_id": query_id, "name": name}}
        )

    def onQueryProgress(self, event: QueryProgressEvent) -> None:
        progress = event.progress
        query_id = str(progress.id)
        name = self._names.get(query_id, str(progress.name or query_id))
        rows = int(progress.numInputRows)
        duration = dict(progress.durationMs)
        SPARK_ROWS.labels(name).inc(rows)
        SPARK_BATCH_DURATION.labels(name).observe(duration.get("triggerExecution", 0) / 1000)
        SPARK_INPUT_RATE.labels(name).set(float(progress.inputRowsPerSecond))
        SPARK_PROCESSING_RATE.labels(name).set(float(progress.processedRowsPerSecond))
        progress_json = json.loads(progress.json)
        state_operators = progress_json.get("stateOperators", [])
        SPARK_STATE_ROWS.labels(name).set(
            sum(int(operator.get("numRowsTotal", 0)) for operator in state_operators)
        )
        duplicates = sum(
            int(operator.get("customMetrics", {}).get("numDroppedDuplicateRows", 0))
            for operator in state_operators
        )
        if duplicates:
            SPARK_DUPLICATES.labels(name).inc(duplicates)
        if name == "quarantine" and rows:
            SPARK_QUARANTINED.inc(rows)
        maximum_event_time = progress_json.get("eventTime", {}).get("max")
        if maximum_event_time:
            maximum = datetime.fromisoformat(maximum_event_time.replace("Z", "+00:00"))
            SPARK_EVENT_TIME_LAG.labels(name).set(
                max(0.0, (datetime.now(UTC) - maximum).total_seconds())
            )
        self._logger.info(
            "spark_query_progress",
            extra={
                "fields": {
                    "query_id": query_id,
                    "name": name,
                    "batch_id": int(progress.batchId),
                    "input_rows": rows,
                    "duration_ms": duration,
                    "state_operators": state_operators,
                    "duplicates_dropped": duplicates,
                }
            },
        )

    def onQueryIdle(self, event: QueryIdleEvent) -> None:
        self._logger.debug("spark_query_idle", extra={"fields": {"query_id": str(event.id)}})

    def onQueryTerminated(self, event: QueryTerminatedEvent) -> None:
        query_id = str(event.id)
        name = self._names.pop(query_id, query_id)
        exception = event.exception
        if exception:
            SPARK_QUERY_FAILURES.labels(name).inc()
            self._logger.error(
                "spark_query_failed",
                extra={"fields": {"query_id": query_id, "name": name, "error": exception}},
            )
        else:
            self._logger.info(
                "spark_query_terminated",
                extra={"fields": {"query_id": query_id, "name": name}},
            )
