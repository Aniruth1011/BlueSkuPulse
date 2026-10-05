from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

EVENT_ENVELOPE_SCHEMA: StructType = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("source_version", StringType(), False),
        StructField("source_host", StringType(), False),
        StructField("cursor", LongType(), False),
        StructField("event_kind", StringType(), False),
        StructField("operation", StringType(), False),
        StructField("did", StringType(), False),
        StructField("collection", StringType(), True),
        StructField("rkey", StringType(), True),
        StructField("at_uri", StringType(), True),
        StructField("cid", StringType(), True),
        StructField("event_time", TimestampType(), False),
        StructField("ingested_at", TimestampType(), False),
        StructField("payload_json", StringType(), False),
        StructField("schema_version", IntegerType(), False),
    ]
)

REFERENCE_SCHEMA = StructType([StructField("uri", StringType()), StructField("cid", StringType())])

POST_RECORD_SCHEMA = StructType(
    [
        StructField("$type", StringType()),
        StructField("text", StringType()),
        StructField("createdAt", StringType()),
        StructField("langs", ArrayType(StringType())),
        StructField(
            "reply",
            StructType(
                [
                    StructField("root", REFERENCE_SCHEMA),
                    StructField("parent", REFERENCE_SCHEMA),
                ]
            ),
        ),
        StructField(
            "embed",
            StructType(
                [
                    StructField("$type", StringType()),
                    StructField(
                        "external",
                        StructType(
                            [
                                StructField("uri", StringType()),
                                StructField("title", StringType()),
                                StructField("description", StringType()),
                            ]
                        ),
                    ),
                    StructField(
                        "record",
                        StructType(
                            [
                                StructField("uri", StringType()),
                                StructField("cid", StringType()),
                                StructField("record", REFERENCE_SCHEMA),
                            ]
                        ),
                    ),
                ]
            ),
        ),
    ]
)

SUBJECT_RECORD_SCHEMA = StructType(
    [
        StructField("$type", StringType()),
        StructField("createdAt", StringType()),
        StructField("subject", REFERENCE_SCHEMA),
    ]
)

FOLLOW_RECORD_SCHEMA = StructType(
    [
        StructField("$type", StringType()),
        StructField("createdAt", StringType()),
        StructField("subject", StringType()),
    ]
)
