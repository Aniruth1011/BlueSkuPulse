from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from typing import Any

from prometheus_client import start_http_server

from ingestion.logging import configure_logging
from warehouse.config import WarehouseSettings
from warehouse.metrics import BQ_LOAD_DURATION, BQ_LOAD_FAILURES, BQ_LOAD_ROWS

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True, slots=True)
class TableSpec:
    name: str
    partition_field: str
    cluster_fields: tuple[str, ...]


TABLE_SPECS = {
    "events": TableSpec("silver_events", "event_date", ("collection", "did", "operation")),
    "posts": TableSpec("silver_posts", "event_date", ("author_did", "at_uri", "operation")),
    "engagements": TableSpec(
        "silver_engagements",
        "event_date",
        ("subject_uri", "actor_did", "engagement_type", "operation"),
    ),
    "graph_edges": TableSpec(
        "silver_graph_edges", "event_date", ("actor_did", "subject_did", "operation")
    ),
    "identity": TableSpec("silver_identity", "event_date", ("did", "event_kind")),
    "quarantine": TableSpec("silver_quarantine", "event_date", ("event_kind", "collection")),
}


def deterministic_batch_id(table: str, uris: list[str]) -> str:
    material = json.dumps([table, *sorted(uris)], separators=(",", ":")).encode()
    return hashlib.sha256(material).hexdigest()[:24]


def _identifier(value: str) -> str:
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"invalid BigQuery identifier: {value}")
    return value


def create_target_sql(target: str, temporary: str, spec: TableSpec) -> str:
    clusters = ", ".join(_identifier(field) for field in spec.cluster_fields)
    return f"""
CREATE TABLE IF NOT EXISTS `{target}`
PARTITION BY {_identifier(spec.partition_field)}
CLUSTER BY {clusters}
OPTIONS (
  require_partition_filter = TRUE,
  description = 'AtmosPulse canonical Silver landing table; managed by warehouse.loader'
)
AS SELECT * FROM `{temporary}` WHERE FALSE
""".strip()


def merge_sql(target: str, temporary: str) -> str:
    return f"""
DECLARE min_event_date DATE DEFAULT (SELECT MIN(event_date) FROM `{temporary}`);
DECLARE max_event_date DATE DEFAULT (SELECT MAX(event_date) FROM `{temporary}`);

MERGE `{target}` AS T
USING (
  SELECT * EXCEPT (_atmospulse_rank)
  FROM (
    SELECT *, ROW_NUMBER() OVER (
      PARTITION BY event_id ORDER BY ingested_at DESC
    ) AS _atmospulse_rank
    FROM `{temporary}`
  )
  WHERE _atmospulse_rank = 1
) AS S
ON T.event_id = S.event_id
AND T.event_date BETWEEN min_event_date AND max_event_date
WHEN NOT MATCHED THEN INSERT ROW
""".strip()


class BigQuerySilverLoader:
    def __init__(self, settings: WarehouseSettings, client: Any | None = None) -> None:
        from google.cloud import bigquery

        self.settings = settings
        self.client = client or bigquery.Client(project=settings.project_id)
        self._bq = bigquery
        self._logger = logging.getLogger(self.__class__.__name__)

    def ensure_datasets(self) -> None:
        for dataset_name in (
            self.settings.staging_dataset,
            self.settings.analytics_dataset,
            self.settings.monitoring_dataset,
        ):
            dataset_id = f"{self.settings.project_id}.{_identifier(dataset_name)}"
            dataset = self._bq.Dataset(dataset_id)
            dataset.location = self.settings.location
            dataset.description = "AtmosPulse managed dataset"
            self.client.create_dataset(dataset, exists_ok=True)
            self._logger.info(
                "bigquery_dataset_ready", extra={"fields": {"dataset_id": dataset_id}}
            )

    def _sync_additive_schema(self, temporary: str, target: str) -> None:
        source_table = self.client.get_table(temporary)
        target_table = self.client.get_table(target)
        target_names = {field.name for field in target_table.schema}
        additions = [field for field in source_table.schema if field.name not in target_names]
        if additions:
            target_table.schema = [*target_table.schema, *additions]
            self.client.update_table(target_table, ["schema"])
            self._logger.info(
                "bigquery_schema_extended",
                extra={
                    "fields": {
                        "table": target,
                        "columns": [field.name for field in additions],
                    }
                },
            )

    def load(self, dataset: str, uris: list[str]) -> str:
        if dataset not in TABLE_SPECS:
            raise ValueError(f"unsupported Silver dataset: {dataset}")
        if not uris or any(not uri.startswith("gs://") for uri in uris):
            raise ValueError("one or more gs:// Silver Parquet URIs are required")
        spec = TABLE_SPECS[dataset]
        batch_id = deterministic_batch_id(dataset, uris)
        project = self.settings.project_id
        staging = _identifier(self.settings.staging_dataset)
        target = f"{project}.{staging}.{spec.name}"
        temporary = f"{project}.{staging}._load_{dataset}_{batch_id}"
        started = time.monotonic()
        self._logger.info(
            "bigquery_load_started",
            extra={
                "fields": {
                    "dataset": dataset,
                    "batch_id": batch_id,
                    "uri_count": len(uris),
                    "target": target,
                }
            },
        )
        try:
            load_config = self._bq.LoadJobConfig(
                source_format=self._bq.SourceFormat.PARQUET,
                write_disposition=self._bq.WriteDisposition.WRITE_TRUNCATE,
                labels={"system": "atmospulse", "layer": "silver", "dataset": dataset},
            )
            load_job = self.client.load_table_from_uri(
                sorted(uris),
                temporary,
                job_id=f"atmospulse_{dataset}_{batch_id}",
                location=self.settings.location,
                job_config=load_config,
            )
            load_job.result()
            self.client.query(
                create_target_sql(target, temporary, spec), location=self.settings.location
            ).result()
            self._sync_additive_schema(temporary, target)
            merge_job = self.client.query(
                merge_sql(target, temporary), location=self.settings.location
            )
            merge_job.result()
            presented_rows = int(self.client.get_table(temporary).num_rows or 0)
            inserted_rows = int(merge_job.num_dml_affected_rows or 0)
            BQ_LOAD_ROWS.labels(spec.name).inc(inserted_rows)
            BQ_LOAD_DURATION.labels(spec.name).observe(time.monotonic() - started)
            self._logger.info(
                "bigquery_load_committed",
                extra={
                    "fields": {
                        "dataset": dataset,
                        "batch_id": batch_id,
                        "presented_rows": presented_rows,
                        "inserted_rows": inserted_rows,
                    }
                },
            )
            return batch_id
        except Exception:
            BQ_LOAD_FAILURES.labels(spec.name).inc()
            self._logger.exception(
                "bigquery_load_failed",
                extra={"fields": {"dataset": dataset, "batch_id": batch_id}},
            )
            raise
        finally:
            self.client.delete_table(temporary, not_found_ok=True)
            self._push_batch_metrics(spec.name)

    def _push_batch_metrics(self, dataset: str) -> None:
        gateway = os.getenv("ATMOSPULSE_PUSHGATEWAY_URL")
        if not gateway:
            return
        try:
            from prometheus_client import REGISTRY, push_to_gateway

            push_to_gateway(
                gateway,
                job="atmospulse_bigquery",
                grouping_key={"dataset": dataset},
                registry=REGISTRY,
            )
        except Exception as exc:
            self._logger.warning(
                "bigquery_metrics_push_failed",
                extra={"fields": {"dataset": dataset, "error": str(exc)}},
            )


def main() -> int:
    parser = argparse.ArgumentParser(description="Idempotently load Silver Parquet into BigQuery")
    parser.add_argument("dataset", choices=sorted(TABLE_SPECS))
    parser.add_argument("uris", nargs="+")
    args = parser.parse_args()
    configure_logging()
    if metrics_port := os.getenv("ATMOSPULSE_BQ_METRICS_PORT"):
        start_http_server(int(metrics_port))
    loader = BigQuerySilverLoader(WarehouseSettings.from_env())
    loader.ensure_datasets()
    loader.load(args.dataset, args.uris)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
