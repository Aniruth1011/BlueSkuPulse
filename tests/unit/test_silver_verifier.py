from __future__ import annotations

from pathlib import Path

from benchmarking.verify_silver import DATASETS, parquet_files


def test_parquet_files_are_recursive_and_sorted(tmp_path: Path) -> None:
    first = tmp_path / "events" / "event_date=2026-10-05" / "b.parquet"
    second = tmp_path / "events" / "event_date=2026-10-04" / "a.parquet"
    first.parent.mkdir(parents=True)
    second.parent.mkdir(parents=True)
    first.touch()
    second.touch()

    assert parquet_files(tmp_path, "events") == sorted((str(first), str(second)))
    assert "quarantine" in DATASETS
