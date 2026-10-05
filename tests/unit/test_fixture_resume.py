from __future__ import annotations

from pathlib import Path

import pytest

from ingestion.cursor_store import SourceCursor
from ingestion.sources.fixture import FixtureSource


@pytest.mark.asyncio
async def test_fixture_resume_is_inclusive() -> None:
    path = Path("tests/fixtures/jetstream_v2.jsonl")
    source = FixtureSource(path)
    cursor = SourceCursor("jetstream_v2_seq", source.source_host, 103)

    observed = [item.cursor.value async for item in source.stream(cursor) if item.cursor]

    assert observed == [103, 104]


@pytest.mark.asyncio
async def test_fixture_rejects_incompatible_cursor() -> None:
    source = FixtureSource(Path("tests/fixtures/jetstream_v2.jsonl"))
    cursor = SourceCursor("jetstream_v2_seq", "another-host", 103)

    with pytest.raises(ValueError, match="incompatible"):
        _ = [item async for item in source.stream(cursor)]
