from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal

CursorKind = Literal["jetstream_v1_time_us", "jetstream_v2_seq"]


class CursorMismatchError(RuntimeError):
    """Raised rather than silently applying an incompatible cursor."""


@dataclass(frozen=True, slots=True)
class SourceCursor:
    kind: CursorKind
    source_host: str
    value: int

    def __post_init__(self) -> None:
        if self.value < 0:
            raise ValueError("cursor must be non-negative")


class FileCursorStore:
    def __init__(self, path: Path) -> None:
        self._path = path

    def load(self, *, kind: CursorKind, source_host: str) -> SourceCursor | None:
        if not self._path.exists():
            return None
        data = json.loads(self._path.read_text(encoding="utf-8"))
        cursor = SourceCursor(**data)
        if cursor.kind != kind or cursor.source_host != source_host:
            raise CursorMismatchError(
                "Stored cursor belongs to "
                f"{cursor.kind}@{cursor.source_host}; requested {kind}@{source_host}. "
                "Use a separate cursor path or perform an explicit migration."
            )
        return cursor

    def save(self, cursor: SourceCursor) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary = tempfile.mkstemp(prefix=".cursor-", dir=self._path.parent, text=True)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(asdict(cursor), handle, sort_keys=True, separators=(",", ":"))
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self._path)
            directory_fd = os.open(self._path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
