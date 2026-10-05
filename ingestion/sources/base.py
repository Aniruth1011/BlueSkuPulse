from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from dataclasses import dataclass

from ingestion.cursor_store import CursorKind, SourceCursor
from ingestion.models.events import EventEnvelope


@dataclass(frozen=True, slots=True)
class SourceItem:
    raw: bytes
    cursor: SourceCursor | None
    event: EventEnvelope | None = None
    error: str | None = None

    def __post_init__(self) -> None:
        if (self.event is None) == (self.error is None):
            raise ValueError("SourceItem must contain exactly one of event or error")


class EventSource(ABC):
    source_version: str
    source_host: str
    cursor_kind: CursorKind

    @abstractmethod
    def stream(self, cursor: SourceCursor | None) -> AsyncIterator[SourceItem]:
        """Yield one source item at a time, starting with inclusive resume semantics."""
        raise NotImplementedError
