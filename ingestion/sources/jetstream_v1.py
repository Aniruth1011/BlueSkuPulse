from __future__ import annotations

from urllib.parse import urlencode, urlsplit, urlunsplit

from ingestion.cursor_store import CursorKind, SourceCursor
from ingestion.models.events import EventEnvelope
from ingestion.sources.parsing import best_effort_v1_cursor, parse_v1
from ingestion.sources.websocket_base import ReconnectingWebSocketSource


class JetstreamV1Source(ReconnectingWebSocketSource):
    source_version = "jetstream_v1"
    cursor_kind: CursorKind = "jetstream_v1_time_us"

    def __init__(
        self,
        url: str,
        collections: tuple[str, ...],
        *,
        idle_timeout_seconds: float = 75,
        max_backoff_seconds: float = 30,
    ) -> None:
        super().__init__(
            url=url,
            collections=collections,
            idle_timeout_seconds=idle_timeout_seconds,
            max_backoff_seconds=max_backoff_seconds,
        )
        self.source_host = urlsplit(url).netloc

    def _subscription_url(self, cursor: SourceCursor | None) -> str:
        split = urlsplit(self._url)
        params: list[tuple[str, str | int]] = [
            ("wantedCollections", collection) for collection in self._collections
        ]
        if cursor is not None:
            # Official v1 guidance recommends a small rewind for gapless at-least-once replay.
            params.append(("cursor", max(0, cursor.value - 2_000_000)))
        return urlunsplit((split.scheme, split.netloc, split.path, urlencode(params), ""))

    def _parse(self, raw: bytes) -> tuple[EventEnvelope, SourceCursor]:
        return parse_v1(raw, self.source_host)

    def _best_effort_cursor(self, raw: bytes) -> SourceCursor | None:
        return best_effort_v1_cursor(raw, self.source_host)
