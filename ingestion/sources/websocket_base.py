from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import AsyncIterator, Callable

from websockets.asyncio.client import connect
from websockets.typing import Subprotocol

from ingestion.cursor_store import SourceCursor
from ingestion.metrics import RECONNECTS
from ingestion.models.events import EventEnvelope
from ingestion.sources.base import EventSource, SourceItem
from ingestion.sources.parsing import AdvisoryFrame

Parser = Callable[[bytes, str], tuple[EventEnvelope, SourceCursor]]
CursorExtractor = Callable[[bytes, str], SourceCursor | None]


class ReconnectingWebSocketSource(EventSource):
    def __init__(
        self,
        *,
        url: str,
        collections: tuple[str, ...],
        idle_timeout_seconds: float = 75,
        max_backoff_seconds: float = 30,
    ) -> None:
        self._url = url
        self._collections = collections
        self._idle_timeout = idle_timeout_seconds
        self._max_backoff = max_backoff_seconds
        self._logger = logging.getLogger(self.__class__.__name__)

    def _subscription_url(self, cursor: SourceCursor | None) -> str:
        raise NotImplementedError

    def _subprotocols(self) -> list[Subprotocol] | None:
        return None

    def _parse(self, raw: bytes) -> tuple[EventEnvelope, SourceCursor]:
        raise NotImplementedError

    def _best_effort_cursor(self, raw: bytes) -> SourceCursor | None:
        raise NotImplementedError

    async def stream(self, cursor: SourceCursor | None) -> AsyncIterator[SourceItem]:
        if cursor and (cursor.kind != self.cursor_kind or cursor.source_host != self.source_host):
            raise ValueError("incompatible source cursor")
        resume = cursor
        attempt = 0
        while True:
            try:
                async with connect(
                    self._subscription_url(resume),
                    subprotocols=self._subprotocols(),
                    open_timeout=15,
                    ping_interval=20,
                    ping_timeout=20,
                    max_size=4 * 1024 * 1024,
                ) as websocket:
                    self._logger.info(
                        "websocket_connected",
                        extra={
                            "fields": {
                                "source_version": self.source_version,
                                "resume_cursor": resume.value if resume else None,
                            }
                        },
                    )
                    attempt = 0
                    while True:
                        raw_message = await asyncio.wait_for(
                            websocket.recv(), timeout=self._idle_timeout
                        )
                        raw = raw_message.encode() if isinstance(raw_message, str) else raw_message
                        try:
                            parsed, item_cursor = self._parse(raw)
                            self._logger.debug(
                                "source_event_parsed",
                                extra={
                                    "fields": {
                                        "event_id": parsed.event_id,
                                        "cursor": item_cursor.value,
                                        "event_kind": parsed.event_kind,
                                    }
                                },
                            )
                            yield SourceItem(raw=raw, cursor=item_cursor, event=parsed)
                            # The generator resumes only after the runner durably handles the item.
                            resume = item_cursor
                        except AdvisoryFrame as advisory:
                            self._logger.warning(
                                "jetstream_advisory", extra={"fields": {"message": str(advisory)}}
                            )
                        except Exception as exc:
                            fallback_cursor = self._best_effort_cursor(raw)
                            self._logger.warning(
                                "source_event_rejected",
                                extra={
                                    "fields": {
                                        "cursor": fallback_cursor.value
                                        if fallback_cursor
                                        else None,
                                        "error": str(exc),
                                    }
                                },
                            )
                            yield SourceItem(raw=raw, cursor=fallback_cursor, error=str(exc))
                            if fallback_cursor is not None:
                                resume = fallback_cursor
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                attempt += 1
                RECONNECTS.labels(self.source_version).inc()
                delay = min(self._max_backoff, 0.5 * (2 ** min(attempt - 1, 8)))
                delay += random.uniform(0, delay * 0.25)
                self._logger.warning(
                    "websocket_reconnect",
                    extra={
                        "fields": {
                            "attempt": attempt,
                            "delay_seconds": round(delay, 3),
                            "error": str(exc),
                        }
                    },
                )
                await asyncio.sleep(delay)
