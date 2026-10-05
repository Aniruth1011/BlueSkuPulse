from __future__ import annotations

import json
from types import TracebackType

import pytest

from ingestion.sources.jetstream_v2 import JetstreamV2Source


def _frame(seq: int) -> bytes:
    return json.dumps(
        {
            "$type": "message",
            "payload": {
                "$type": "network.bsky.jetstream.subscribeEvents#commit",
                "seq": seq,
                "did": "did:plc:recovery",
                "time": "2026-10-04T12:00:00.000001Z",
                "rev": f"rev-{seq}",
                "operation": "create",
                "collection": "app.bsky.feed.post",
                "rkey": f"key-{seq}",
                "cid": f"cid-{seq}",
                "record": {"$type": "app.bsky.feed.post", "text": "recovery"},
            },
        }
    ).encode()


class FakeSocket:
    def __init__(self, messages: list[bytes]) -> None:
        self.messages = messages

    async def recv(self) -> bytes:
        if not self.messages:
            raise ConnectionError("forced disconnect")
        return self.messages.pop(0)


class FakeConnection:
    def __init__(self, socket: FakeSocket) -> None:
        self.socket = socket

    async def __aenter__(self) -> FakeSocket:
        return self.socket

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        return None


@pytest.mark.asyncio
async def test_disconnect_reconnects_from_last_handled_inclusive_cursor(monkeypatch) -> None:
    sockets = [FakeSocket([_frame(10)]), FakeSocket([_frame(10), _frame(11)])]
    urls: list[str] = []

    def fake_connect(url: str, **_: object) -> FakeConnection:
        urls.append(url)
        return FakeConnection(sockets.pop(0))

    async def no_sleep(_: float) -> None:
        return None

    monkeypatch.setattr("ingestion.sources.websocket_base.connect", fake_connect)
    monkeypatch.setattr("ingestion.sources.websocket_base.asyncio.sleep", no_sleep)
    monkeypatch.setattr("ingestion.sources.websocket_base.random.uniform", lambda *_: 0.0)
    source = JetstreamV2Source(
        "wss://jetstream.example/xrpc/network.bsky.jetstream.subscribeEvents",
        ("app.bsky.feed.post",),
    )
    stream = source.stream(None)

    first = await anext(stream)
    replay = await anext(stream)
    await stream.aclose()

    assert first.event is not None and first.event.event_id == replay.event.event_id
    assert first.cursor is not None and first.cursor.value == 10
    assert len(urls) == 2
    assert "cursor=10" in urls[1]
