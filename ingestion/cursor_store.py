from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal, get_args

CursorKind = Literal["jetstream_v1_time_us", "jetstream_v2_seq"]


class CursorMismatchError(RuntimeError):
    """Raised rather than silently applying an incompatible cursor."""


class CursorCorruptError(RuntimeError):
    """Raised rather than silently resetting when the stored cursor cannot be trusted."""


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
        cursor = self._read()
        if cursor.kind != kind or cursor.source_host != source_host:
            raise CursorMismatchError(
                "Stored cursor belongs to "
                f"{cursor.kind}@{cursor.source_host}; requested {kind}@{source_host}. "
                "Use a separate cursor path or perform an explicit migration."
            )
        return cursor

    def _read(self) -> SourceCursor:
        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
            if not isinstance(data, dict) or set(data) != {"kind", "source_host", "value"}:
                raise ValueError(f"unexpected cursor fields: {data!r}")
            if data["kind"] not in get_args(CursorKind):
                raise ValueError(f"unknown cursor kind: {data['kind']!r}")
            if not isinstance(data["source_host"], str) or not data["source_host"]:
                raise ValueError("cursor source_host must be a non-empty string")
            if type(data["value"]) is not int:
                raise ValueError("cursor value must be an integer")
            return SourceCursor(**data)
        except (UnicodeDecodeError, ValueError) as exc:
            raise CursorCorruptError(
                f"Stored cursor at {self._path} is unreadable ({exc}). Refusing to reset to "
                "'now'; restore the file or remove it deliberately to start a new capture."
            ) from exc

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
