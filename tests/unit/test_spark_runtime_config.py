from __future__ import annotations

import pytest

from streaming.jobs.normalize_events import positive_int_env, streaming_spark_configs


def test_streaming_runtime_defaults_use_bounded_rocksdb(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "ATMOSPULSE_SPARK_SHUFFLE_PARTITIONS",
        "ATMOSPULSE_SPARK_STATE_STORE_PROVIDER",
        "ATMOSPULSE_SPARK_ROCKSDB_MEMORY_MB",
    ):
        monkeypatch.delenv(name, raising=False)

    configs = streaming_spark_configs()

    assert configs["spark.sql.shuffle.partitions"] == "12"
    assert configs["spark.sql.streaming.stateStore.providerClass"].endswith(
        ".RocksDBStateStoreProvider"
    )
    assert configs["spark.sql.streaming.stateStore.rocksdb.boundedMemoryUsage"] == "true"
    assert configs["spark.sql.streaming.stateStore.rocksdb.maxMemoryUsageMB"] == "512"


def test_positive_integer_environment_rejects_invalid_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ATMOSPULSE_SPARK_MAX_OFFSETS_PER_TRIGGER", "0")

    with pytest.raises(ValueError, match="must be a positive integer"):
        positive_int_env("ATMOSPULSE_SPARK_MAX_OFFSETS_PER_TRIGGER", 150_000)
