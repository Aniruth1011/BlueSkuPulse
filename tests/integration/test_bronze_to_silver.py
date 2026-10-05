from __future__ import annotations

from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from bronze.storage import LocalObjectStore
from bronze.writer import BronzeBatchWriter, KafkaRawRecord
from ingestion.sources.parsing import parse_v2
from streaming.jobs.normalize_events import run_batch
from streaming.jobs.transformations import canonical_events
from streaming.schemas.events import EVENT_ENVELOPE_SCHEMA


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("atmospulse-bronze-silver-integration")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_immutable_bronze_replays_to_deduplicated_silver(
    spark: SparkSession, tmp_path: Path
) -> None:
    frames = Path("tests/fixtures/jetstream_v2.jsonl").read_bytes().splitlines()
    envelopes = [parse_v2(frame, "fixture.local")[0].kafka_value() for frame in frames]
    records = [
        KafkaRawRecord("bluesky.raw.events", 0, offset, None, value, 0)
        for offset, value in enumerate([*envelopes, envelopes[0]])
    ]
    bronze_root = tmp_path / "bronze"
    result = BronzeBatchWriter(LocalObjectStore(bronze_root)).write(records)
    output = tmp_path / "silver-rebuild"

    run_batch(
        spark,
        str(bronze_root / "bluesky/event_date=*/event_hour=*/collection=*/*.jsonl.gz"),
        str(output),
    )

    events = spark.read.parquet(str(output / "events"))
    posts = spark.read.parquet(str(output / "posts"))
    assert result.record_count == 5
    assert events.count() == 4
    assert posts.count() == 2
    assert events.select("event_id").distinct().count() == events.count()


def test_stream_checkpoint_restart_matches_batch_and_rejects_redelivery(
    spark: SparkSession, tmp_path: Path
) -> None:
    frames = Path("tests/fixtures/jetstream_v2.jsonl").read_bytes().splitlines()
    envelopes = [parse_v2(frame, "fixture.local")[0] for frame in frames]
    input_path = tmp_path / "stream-input"
    input_path.mkdir()
    (input_path / "batch-1.jsonl").write_text(
        "\n".join(event.model_dump_json() for event in [*envelopes, envelopes[0]]) + "\n"
    )
    output_path = tmp_path / "stream-output"
    checkpoint_path = tmp_path / "stream-checkpoint"

    def run_available_now() -> None:
        incoming = spark.readStream.schema(EVENT_ENVELOPE_SCHEMA).json(str(input_path))
        valid, _ = canonical_events(incoming, streaming=True)
        query = (
            valid.writeStream.format("parquet")
            .option("path", str(output_path))
            .option("checkpointLocation", str(checkpoint_path))
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination()

    run_available_now()
    first = spark.read.parquet(str(output_path))
    batch, _ = canonical_events(
        spark.createDataFrame([event.model_dump() for event in envelopes], EVENT_ENVELOPE_SCHEMA),
        streaming=False,
    )
    assert {row.event_id for row in first.select("event_id").collect()} == {
        row.event_id for row in batch.select("event_id").collect()
    }

    (input_path / "batch-2-redelivery.jsonl").write_text(envelopes[0].model_dump_json() + "\n")
    run_available_now()
    after_restart = spark.read.parquet(str(output_path))
    assert after_restart.count() == len(envelopes)
    assert after_restart.select("event_id").distinct().count() == len(envelopes)
