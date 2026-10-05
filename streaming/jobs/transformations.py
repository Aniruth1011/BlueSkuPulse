from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from streaming.schemas.events import FOLLOW_RECORD_SCHEMA, POST_RECORD_SCHEMA, SUBJECT_RECORD_SCHEMA

REQUIRED_EXPRESSIONS = {
    "event_id": "event_id IS NOT NULL",
    "did": "did IS NOT NULL AND did LIKE 'did:%'",
    "event_time": "event_time IS NOT NULL",
    "known_kind": "event_kind IN ('commit', 'identity', 'account', 'sync')",
    "known_operation": ("event_kind <> 'commit' OR operation IN ('create', 'update', 'delete')"),
    "commit_identity": (
        "event_kind <> 'commit' OR "
        "(collection IS NOT NULL AND rkey IS NOT NULL AND at_uri IS NOT NULL)"
    ),
}


def record_json() -> Column:
    return F.coalesce(
        F.get_json_object("payload_json", "$.commit.record"),
        F.get_json_object("payload_json", "$.payload.record"),
    )


def with_quality_status(events: DataFrame) -> DataFrame:
    failures = F.array_compact(
        F.array(
            *[
                F.when(~F.expr(expression), F.lit(name))
                for name, expression in REQUIRED_EXPRESSIONS.items()
            ]
        )
    )
    return events.withColumn("quality_errors", failures).withColumn(
        "is_valid", F.size("quality_errors") == 0
    )


def canonical_events(events: DataFrame, *, streaming: bool) -> tuple[DataFrame, DataFrame]:
    checked = with_quality_status(events)
    quarantine = checked.filter(~F.col("is_valid")).select(
        "*", F.current_timestamp().alias("quarantined_at")
    )
    valid = checked.filter(F.col("is_valid")).drop("quality_errors", "is_valid")
    if streaming:
        valid = valid.withWatermark("event_time", "48 hours").dropDuplicatesWithinWatermark(
            ["event_id"]
        )
    else:
        valid = valid.dropDuplicates(["event_id"])
    return valid, quarantine


def posts(events: DataFrame) -> DataFrame:
    parsed = events.filter(F.col("collection") == "app.bsky.feed.post").withColumn(
        "record", F.from_json(record_json(), POST_RECORD_SCHEMA)
    )
    return parsed.select(
        "event_id",
        "at_uri",
        F.col("did").alias("author_did"),
        "operation",
        "cid",
        "event_time",
        "ingested_at",
        F.to_timestamp("record.createdAt").alias("record_created_at"),
        F.col("record.text").alias("text"),
        F.col("record.langs").alias("languages"),
        F.col("record.reply.root.uri").alias("reply_root_uri"),
        F.col("record.reply.parent.uri").alias("reply_parent_uri"),
        F.col("record.embed.external.uri").alias("external_uri"),
        F.coalesce(F.col("record.embed.record.uri"), F.col("record.embed.record.record.uri")).alias(
            "quoted_post_uri"
        ),
        F.col("record.`$type`").alias("record_type"),
        F.col("record.embed.`$type`").alias("embed_type"),
        F.when(F.col("record.embed").isNotNull(), F.lit(True))
        .otherwise(F.lit(False))
        .alias("has_embed"),
        F.col("record.embed.external.uri").isNotNull().alias("has_external_link"),
        F.coalesce(F.col("record.embed.record.uri"), F.col("record.embed.record.record.uri"))
        .isNotNull()
        .alias("has_quote"),
        F.col("record.embed.`$type`")
        .isin(
            "app.bsky.embed.images",
            "app.bsky.embed.video",
            "app.bsky.embed.recordWithMedia",
        )
        .alias("has_media"),
        F.to_date("event_time").alias("event_date"),
    )


def engagements(events: DataFrame) -> DataFrame:
    filtered = events.filter(
        F.col("collection").isin("app.bsky.feed.like", "app.bsky.feed.repost")
    ).withColumn("record", F.from_json(record_json(), SUBJECT_RECORD_SCHEMA))
    return filtered.select(
        "event_id",
        "at_uri",
        F.col("did").alias("actor_did"),
        F.when(F.col("collection") == "app.bsky.feed.like", F.lit("like"))
        .otherwise(F.lit("repost"))
        .alias("engagement_type"),
        "operation",
        F.col("record.subject.uri").alias("subject_uri"),
        F.col("record.subject.cid").alias("subject_cid"),
        "event_time",
        "ingested_at",
        F.to_timestamp("record.createdAt").alias("record_created_at"),
        F.to_date("event_time").alias("event_date"),
    )


def graph_edges(events: DataFrame) -> DataFrame:
    parsed = events.filter(F.col("collection") == "app.bsky.graph.follow").withColumn(
        "record", F.from_json(record_json(), FOLLOW_RECORD_SCHEMA)
    )
    return parsed.select(
        "event_id",
        "at_uri",
        F.col("did").alias("actor_did"),
        F.col("record.subject").alias("subject_did"),
        "operation",
        "event_time",
        "ingested_at",
        F.to_timestamp("record.createdAt").alias("record_created_at"),
        F.to_date("event_time").alias("event_date"),
    )


def identity_events(events: DataFrame) -> DataFrame:
    return events.filter(F.col("event_kind").isin("identity", "account", "sync")).select(
        "event_id",
        "did",
        "event_kind",
        "operation",
        "event_time",
        "ingested_at",
        "payload_json",
        F.to_date("event_time").alias("event_date"),
    )


def fold_current_state(mutations: DataFrame, key: str = "at_uri") -> DataFrame:
    ordering = Window.partitionBy(key).orderBy(
        F.col("event_time").desc(), F.col("cursor").cast("long").desc()
    )
    return (
        mutations.withColumn("_rank", F.row_number().over(ordering))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
        .withColumn("is_deleted", F.col("operation") == "delete")
    )
