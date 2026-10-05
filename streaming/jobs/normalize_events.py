from __future__ import annotations

import argparse
import logging
import os

from prometheus_client import start_http_server
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ingestion.logging import configure_logging
from streaming.jobs.transformations import (
    canonical_events,
    engagements,
    graph_edges,
    identity_events,
    posts,
)
from streaming.observability import StructuredLoggingQueryListener
from streaming.schemas.events import EVENT_ENVELOPE_SCHEMA


def positive_int_env(name: str, default: int) -> int:
    value = int(os.getenv(name, str(default)))
    if value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def streaming_spark_configs() -> dict[str, str]:
    return {
        "spark.sql.shuffle.partitions": os.getenv("ATMOSPULSE_SPARK_SHUFFLE_PARTITIONS", "12"),
        "spark.sql.streaming.stateStore.providerClass": os.getenv(
            "ATMOSPULSE_SPARK_STATE_STORE_PROVIDER",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
        ),
        "spark.sql.streaming.stateStore.rocksdb.boundedMemoryUsage": "true",
        "spark.sql.streaming.stateStore.rocksdb.maxMemoryUsageMB": str(
            positive_int_env("ATMOSPULSE_SPARK_ROCKSDB_MEMORY_MB", 512)
        ),
        "spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled": "true",
    }


def kafka_events(spark: SparkSession, bootstrap_servers: str, topic: str) -> DataFrame:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .option(
            "maxOffsetsPerTrigger",
            positive_int_env("ATMOSPULSE_SPARK_MAX_OFFSETS_PER_TRIGGER", 150_000),
        )
        .load()
    )
    return (
        raw.withColumn("json", F.col("value").cast("string"))
        .withColumn("event", F.from_json("json", EVENT_ENVELOPE_SCHEMA))
        .select("event.*", "topic", "partition", "offset", F.col("timestamp").alias("kafka_time"))
    )


def bronze_events(spark: SparkSession, path: str) -> DataFrame:
    raw = spark.read.json(path)
    return raw.select("event.*", "kafka.*")


def write_stream(dataset: DataFrame, path: str, checkpoint: str, partitions: list[str]) -> None:
    writer = (
        dataset.writeStream.format("parquet")
        .outputMode("append")
        .option("path", path)
        .option("checkpointLocation", checkpoint)
    )
    if partitions:
        writer = writer.partitionBy(*partitions)
    writer.queryName(path.rsplit("/", 1)[-1]).trigger(processingTime="30 seconds").start()


def run_stream(spark: SparkSession) -> None:
    silver_root = os.getenv("ATMOSPULSE_SILVER_ROOT", "data/silver")
    checkpoints = os.getenv("ATMOSPULSE_SPARK_CHECKPOINT_ROOT", "state/spark-checkpoints")
    events = kafka_events(
        spark,
        os.getenv("ATMOSPULSE_KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"),
        os.getenv("ATMOSPULSE_RAW_TOPIC", "bluesky.raw.events"),
    )
    valid, quarantine = canonical_events(events, streaming=True)
    outputs = {
        "events": (
            valid.withColumn("event_date", F.to_date("event_time")),
            ["event_date", "collection"],
        ),
        "posts": (posts(valid), ["event_date"]),
        "engagements": (engagements(valid), ["event_date", "engagement_type"]),
        "graph_edges": (graph_edges(valid), ["event_date"]),
        "identity": (identity_events(valid), ["event_date", "event_kind"]),
        "quarantine": (
            quarantine.withColumn("event_date", F.to_date("ingested_at")),
            ["event_date"],
        ),
    }
    for name, (dataset, partitions) in outputs.items():
        write_stream(dataset, f"{silver_root}/{name}", f"{checkpoints}/{name}", partitions)
    logging.getLogger(__name__).info(
        "silver_streams_started", extra={"fields": {"outputs": sorted(outputs)}}
    )
    spark.streams.awaitAnyTermination()


def run_batch(
    spark: SparkSession,
    bronze_path: str,
    output_path: str,
    *,
    start: str | None = None,
    end: str | None = None,
    collections: tuple[str, ...] = (),
) -> None:
    events = bronze_events(spark, bronze_path)
    if start:
        events = events.where(F.col("event_time") >= F.to_timestamp(F.lit(start)))
    if end:
        events = events.where(F.col("event_time") < F.to_timestamp(F.lit(end)))
    if collections:
        events = events.where(F.col("collection").isin(*collections))
    valid, quarantine = canonical_events(events, streaming=False)
    datasets = {
        "events": valid.withColumn("event_date", F.to_date("event_time")),
        "posts": posts(valid),
        "engagements": engagements(valid),
        "graph_edges": graph_edges(valid),
        "identity": identity_events(valid),
        "quarantine": quarantine.withColumn("event_date", F.to_date("ingested_at")),
    }
    for name, dataset in datasets.items():
        dataset.write.mode("overwrite").partitionBy("event_date").parquet(f"{output_path}/{name}")
        logging.getLogger(__name__).info(
            "silver_batch_dataset_written", extra={"fields": {"dataset": name}}
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Normalize AtmosPulse events with Spark")
    parser.add_argument("--mode", choices=("stream", "batch"), default="stream")
    parser.add_argument("--bronze-path")
    parser.add_argument("--output-path")
    parser.add_argument("--start", help="Inclusive ISO-8601 event-time bound")
    parser.add_argument("--end", help="Exclusive ISO-8601 event-time bound")
    parser.add_argument("--collections", help="Comma-separated collection NSIDs")
    args = parser.parse_args()
    configure_logging(os.getenv("ATMOSPULSE_LOG_LEVEL", "INFO"))
    builder = SparkSession.builder.appName("AtmosPulseSilver")
    if args.mode == "stream":
        for key, value in streaming_spark_configs().items():
            builder = builder.config(key, value)
    packages = [
        item.strip()
        for item in os.getenv("ATMOSPULSE_SPARK_EXTRA_PACKAGES", "").split(",")
        if item.strip()
    ]
    if args.mode == "stream":
        packages.append(
            os.getenv(
                "ATMOSPULSE_SPARK_KAFKA_PACKAGE",
                "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.4",
            )
        )
    if packages:
        builder = builder.config("spark.jars.packages", ",".join(packages))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("ATMOSPULSE_SPARK_LOG_LEVEL", "WARN"))
    spark.streams.addListener(StructuredLoggingQueryListener())
    start_http_server(int(os.getenv("ATMOSPULSE_SPARK_METRICS_PORT", "9110")))
    try:
        if args.mode == "stream":
            run_stream(spark)
        else:
            if not args.bronze_path or not args.output_path:
                parser.error("batch mode requires --bronze-path and --output-path")
            selected_collections = tuple(
                item.strip() for item in (args.collections or "").split(",") if item.strip()
            )
            run_batch(
                spark,
                args.bronze_path,
                args.output_path,
                start=args.start,
                end=args.end,
                collections=selected_collections,
            )
    except Exception:
        logging.getLogger(__name__).exception(
            "silver_job_failed", extra={"fields": {"mode": args.mode}}
        )
        raise
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
