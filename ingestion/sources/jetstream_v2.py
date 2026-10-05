from __future__ import annotations

from urllib.parse import urlencode, urlsplit, urlunsplit

from websockets.typing import Subprotocol

from ingestion.cursor_store import CursorKind, SourceCursor
from ingestion.models.events import EventEnvelope
from ingestion.sources.parsing import best_effort_v2_cursor, parse_v2
from ingestion.sources.websocket_base import ReconnectingWebSocketSource


class JetstreamV2Source(ReconnectingWebSocketSource):
    source_version = "jetstream_v2"
    cursor_kind: CursorKind = "jetstream_v2_seq"

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
        # Collection filters constrain commit events only. Omitting `kinds` retains
        # identity, account, and sync lifecycle events as required by the platform.
        params: list[tuple[str, str | int]] = [
            ("collections", collection) for collection in self._collections
        ]
        if cursor is not None:
            # v2 is inclusive. Re-sending the last durable seq is intentional.
            params.append(("cursor", cursor.value))
        return urlunsplit((split.scheme, split.netloc, split.path, urlencode(params), ""))

    def _subprotocols(self) -> list[Subprotocol]:
        return [Subprotocol("xrpc.v1.json")]

    def _parse(self, raw: bytes) -> tuple[EventEnvelope, SourceCursor]:
        return parse_v2(raw, self.source_host)

    def _best_effort_cursor(self, raw: bytes) -> SourceCursor | None:
        return best_effort_v2_cursor(raw, self.source_host)
