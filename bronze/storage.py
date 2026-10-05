from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Protocol


class ImmutableObjectStore(Protocol):
    def put_if_absent(self, key: str, data: bytes, *, content_type: str) -> bool:
        """Create an object once; return False when the exact key already exists."""
        ...


class LocalObjectStore:
    def __init__(self, root: Path) -> None:
        self.root = root

    def put_if_absent(self, key: str, data: bytes, *, content_type: str) -> bool:
        del content_type
        destination = self.root / key
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists():
            return False
        fd, temporary = tempfile.mkstemp(prefix=".bronze-", dir=destination.parent)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            try:
                os.link(temporary, destination)
            except FileExistsError:
                return False
            directory_fd = os.open(destination.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
            return True
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)


class GCSObjectStore:
    def __init__(self, bucket_name: str, prefix: str = "") -> None:
        if not bucket_name:
            raise ValueError("GCS bucket name is required")
        from google.cloud import storage  # type: ignore[attr-defined]

        self._bucket = storage.Client().bucket(bucket_name)
        self._prefix = prefix.strip("/")

    def put_if_absent(self, key: str, data: bytes, *, content_type: str) -> bool:
        from google.api_core.exceptions import PreconditionFailed

        object_name = "/".join(part for part in (self._prefix, key) if part)
        blob = self._bucket.blob(object_name)
        try:
            blob.upload_from_string(
                data,
                content_type=content_type,
                if_generation_match=0,
                checksum="crc32c",
            )
            return True
        except PreconditionFailed:
            return False
