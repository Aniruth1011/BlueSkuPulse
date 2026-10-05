from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime


@dataclass(frozen=True, slots=True)
class DataInterval:
    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        if self.start.tzinfo is None or self.end.tzinfo is None:
            raise ValueError("data interval timestamps must be timezone-aware")
        if self.start >= self.end:
            raise ValueError("data interval start must precede end")

    @classmethod
    def parse(cls, start: str, end: str) -> DataInterval:
        return cls(_parse_timestamp(start), _parse_timestamp(end))

    @property
    def batch_id(self) -> str:
        material = json.dumps(
            [self.start.isoformat(), self.end.isoformat()], separators=(",", ":")
        ).encode()
        return hashlib.sha256(material).hexdigest()[:24]

    def hourly_partitions(self) -> tuple[tuple[str, str], ...]:
        cursor = self.start.replace(minute=0, second=0, microsecond=0)
        result: list[tuple[str, str]] = []
        while cursor < self.end:
            result.append((cursor.strftime("%Y-%m-%d"), cursor.strftime("%H")))
            cursor = datetime.fromtimestamp(cursor.timestamp() + 3600, tz=UTC)
        return tuple(result)


def _parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamps must include a timezone")
    return parsed.astimezone(UTC)
