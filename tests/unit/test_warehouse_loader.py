from __future__ import annotations

from types import SimpleNamespace

import pytest
from google.cloud import bigquery

from warehouse.config import WarehouseSettings
from warehouse.loader import (
    TABLE_SPECS,
    BigQuerySilverLoader,
    create_target_sql,
    deterministic_batch_id,
    merge_sql,
)


class CompletedJob:
    def __init__(self, affected: int | None = None) -> None:
        self.num_dml_affected_rows = affected

    def result(self) -> CompletedJob:
        return self


class FakeBigQueryClient:
    def __init__(self) -> None:
        schema = [
            bigquery.SchemaField("event_id", "STRING"),
            bigquery.SchemaField("event_date", "DATE"),
            bigquery.SchemaField("ingested_at", "TIMESTAMP"),
        ]
        self.temporary = SimpleNamespace(schema=schema, num_rows=3)
        self.target = SimpleNamespace(schema=schema[:2], num_rows=0)
        self.loads: list[tuple[object, ...]] = []
        self.queries: list[str] = []
        self.deleted: list[str] = []
        self.datasets: list[str] = []
        self.updated = False

    def create_dataset(self, dataset: object, *, exists_ok: bool) -> object:
        assert exists_ok
        self.datasets.append(dataset.dataset_id)
        return dataset

    def load_table_from_uri(self, *args: object, **kwargs: object) -> CompletedJob:
        self.loads.append((*args, kwargs))
        return CompletedJob()

    def query(self, sql: str, **_: object) -> CompletedJob:
        self.queries.append(sql)
        return CompletedJob(affected=3 if sql.startswith("MERGE") else None)

    def get_table(self, table: str) -> object:
        return self.temporary if "._load_" in table else self.target

    def update_table(self, table: object, fields: list[str]) -> object:
        assert fields == ["schema"]
        self.updated = True
        return table

    def delete_table(self, table: str, *, not_found_ok: bool) -> None:
        assert not_found_ok
        self.deleted.append(table)


def _settings() -> WarehouseSettings:
    return WarehouseSettings(
        project_id="test-project",
        location="US",
        staging_dataset="atmospulse_staging",
        analytics_dataset="atmospulse_analytics",
        monitoring_dataset="atmospulse_monitoring",
    )


def test_deterministic_batch_id_is_uri_order_independent() -> None:
    first = deterministic_batch_id("posts", ["gs://bucket/b", "gs://bucket/a"])
    second = deterministic_batch_id("posts", ["gs://bucket/a", "gs://bucket/b"])
    assert first == second
    assert len(first) == 24


def test_generated_sql_enforces_physical_layout_and_event_id_merge() -> None:
    create = create_target_sql("p.d.silver_events", "p.d.temp", TABLE_SPECS["events"])
    merge = merge_sql("p.d.silver_events", "p.d.temp")
    assert "PARTITION BY event_date" in create
    assert "CLUSTER BY collection, did, operation" in create
    assert "require_partition_filter = TRUE" in create
    assert "PARTITION BY event_id" in merge
    assert "ON T.event_id = S.event_id" in merge
    assert "T.event_date BETWEEN min_event_date AND max_event_date" in merge
    assert "WHEN NOT MATCHED THEN INSERT ROW" in merge


def test_loader_uses_deterministic_job_merges_and_syncs_additive_schema() -> None:
    client = FakeBigQueryClient()
    loader = BigQuerySilverLoader(_settings(), client=client)

    batch_id = loader.load("events", ["gs://bucket/silver/events/*.parquet"])

    assert batch_id == deterministic_batch_id("events", ["gs://bucket/silver/events/*.parquet"])
    assert client.loads
    assert client.loads[0][-1]["job_id"] == f"atmospulse_events_{batch_id}"
    assert len(client.queries) == 2
    assert client.updated
    assert client.deleted == [f"test-project.atmospulse_staging._load_events_{batch_id}"]


def test_loader_creates_all_managed_datasets() -> None:
    client = FakeBigQueryClient()
    loader = BigQuerySilverLoader(_settings(), client=client)
    loader.ensure_datasets()
    assert client.datasets == [
        "atmospulse_staging",
        "atmospulse_analytics",
        "atmospulse_monitoring",
    ]


@pytest.mark.parametrize(
    ("dataset", "uris"),
    [("unknown", ["gs://bucket/a"]), ("events", []), ("events", ["https://bucket/a"])],
)
def test_loader_rejects_unsupported_or_non_gcs_input(dataset: str, uris: list[str]) -> None:
    loader = BigQuerySilverLoader(_settings(), client=FakeBigQueryClient())
    with pytest.raises(ValueError):
        loader.load(dataset, uris)
