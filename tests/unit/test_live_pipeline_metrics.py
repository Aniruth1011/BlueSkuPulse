from benchmarking.live_pipeline import distribution, percentile


def test_percentiles_use_nearest_rank() -> None:
    values = [4.0, 1.0, 3.0, 2.0, 5.0]
    assert percentile(values, 0.50) == 3.0
    assert percentile(values, 0.95) == 5.0


def test_empty_distribution_is_explicit() -> None:
    assert distribution([]) == {
        "samples": 0,
        "p50_seconds": None,
        "p95_seconds": None,
        "p99_seconds": None,
        "max_seconds": None,
    }
