from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

from ingestion.cursor_store import CursorKind, SourceCursor
from ingestion.sources.base import EventSource, SourceItem
from ingestion.sources.parsing import (
    best_effort_v1_cursor,
    best_effort_v2_cursor,
    parse_v1,
    parse_v2,
)


class FixtureSource(EventSource):
    def __init__(self, path: Path, *, wire_version: str = "jetstream_v2") -> None:
        if wire_version not in {"jetstream_v1", "jetstream_v2"}:
            raise ValueError(f"unsupported fixture wire version: {wire_version}")
        self._path = path
        self.source_version = wire_version
        self.source_host = f"fixture://{path.resolve()}"
        self.cursor_kind: CursorKind = (
            "jetstream_v1_time_us" if wire_version == "jetstream_v1" else "jetstream_v2_seq"
        )

    async def stream(self, cursor: SourceCursor | None) -> AsyncIterator[SourceItem]:
        if cursor and (cursor.kind != self.cursor_kind or cursor.source_host != self.source_host):
            raise ValueError("incompatible fixture cursor")
        resume = cursor.value if cursor else None
        parser = parse_v1 if self.source_version == "jetstream_v1" else parse_v2
        cursor_parser = (
            best_effort_v1_cursor
            if self.source_version == "jetstream_v1"
            else best_effort_v2_cursor
        )
        with self._path.open("rb") as handle:
            for raw_line in handle:
                raw = raw_line.strip()
                if not raw:
                    continue
                item_cursor = cursor_parser(raw, self.source_host)
                if resume is not None and item_cursor is not None and item_cursor.value < resume:
                    continue
                try:
                    event, item_cursor = parser(raw, self.source_host)
                    yield SourceItem(raw=raw, cursor=item_cursor, event=event)
                except Exception as exc:  # parsing failures are data, not source termination
                    yield SourceItem(raw=raw, cursor=item_cursor, error=str(exc))
