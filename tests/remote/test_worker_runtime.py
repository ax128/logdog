from __future__ import annotations

import asyncio
import queue
import threading
from typing import Any

import pytest

from logdog.remote.worker_protocol import FrameReader, encode_frame
from logdog.remote.worker_runtime import WorkerSession


class _FakeChannel:
    def __init__(self) -> None:
        self._reads: queue.Queue[bytes | None] = queue.Queue()
        self.writes: list[bytes] = []
        self.closed = False

    def push_read(self, payload: dict[str, Any]) -> None:
        frame = encode_frame(payload)
        midpoint = max(1, len(frame) // 2)
        self._reads.put(frame[:midpoint])
        self._reads.put(frame[midpoint:])

    def finish(self) -> None:
        self._reads.put(None)

    def read(self, _size: int) -> bytes:
        item = self._reads.get(timeout=1.0)
        if item is None:
            return b""
        return item

    def write(self, data: bytes) -> int:
        self.writes.append(bytes(data))
        return len(data)

    def flush(self) -> None:
        return None

    def close(self) -> None:
        self.closed = True


class _FailingReadChannel(_FakeChannel):
    def __init__(self) -> None:
        super().__init__()
        self._fail_now = threading.Event()

    def trigger_failure(self) -> None:
        self._fail_now.set()

    def read(self, _size: int) -> bytes:
        if self._fail_now.wait(timeout=1.0):
            raise EOFError("reader failed")
        return super().read(_size)


def _decode_written_messages(channel: _FakeChannel) -> list[dict[str, Any]]:
    reader = FrameReader()
    messages: list[dict[str, Any]] = []
    for chunk in channel.writes:
        messages.extend(reader.feed(chunk))
    messages.extend(reader.close())
    return messages


@pytest.mark.asyncio
async def test_worker_session_routes_response_by_request_id() -> None:
    channel = _FakeChannel()
    session = WorkerSession(channel=channel)
    await session.start()

    task = asyncio.create_task(
        session.request(
            action="list_containers",
            payload={"host": "prod"},
            timeout_seconds=1.0,
        )
    )
    await asyncio.sleep(0)
    channel.push_read({"type": "heartbeat", "ts": "2026-04-20T12:00:00Z"})
    channel.push_read(
        {
            "type": "response",
            "request_id": "req-1",
            "ok": True,
            "result": [{"id": "c1"}],
        }
    )

    response = await task

    assert response["ok"] is True
    assert response["result"] == [{"id": "c1"}]
    assert channel.writes
    await session.close()


@pytest.mark.asyncio
async def test_worker_session_closes_stream_when_channel_eof() -> None:
    channel = _FakeChannel()
    session = WorkerSession(channel=channel)
    await session.start()

    stream = await session.open_stream(
        action="stream_logs",
        payload={"host": "prod", "container_id": "c1"},
        request_id="req-stream-eof",
        stream_id="stream-eof",
    )
    channel.finish()

    with pytest.raises(StopAsyncIteration):
        await anext(stream)

    await session.close()


@pytest.mark.asyncio
async def test_worker_session_dispatches_stream_messages_to_subscription_queue() -> None:
    channel = _FakeChannel()
    session = WorkerSession(channel=channel)
    await session.start()

    stream = await session.open_stream(
        action="stream_logs",
        payload={"host": "prod", "container_id": "c1"},
        request_id="req-stream-1",
        stream_id="stream-1",
    )
    channel.push_read(
        {
            "type": "response",
            "request_id": "req-stream-1",
            "ok": True,
            "stream_id": "stream-1",
        }
    )
    channel.push_read(
        {
            "type": "log",
            "stream_id": "stream-1",
            "timestamp": "2026-04-20T12:00:00Z",
            "line": "ERROR boom",
        }
    )

    item = await asyncio.wait_for(anext(stream), timeout=1.0)

    assert item["line"] == "ERROR boom"
    await stream.aclose()
    await session.close()


@pytest.mark.asyncio
async def test_worker_session_stream_close_sends_cancel_request() -> None:
    channel = _FakeChannel()
    session = WorkerSession(channel=channel)
    await session.start()

    stream = await session.open_stream(
        action="stream_logs",
        payload={"host": "prod", "container_id": "c1"},
        request_id="req-stream-cancel",
        stream_id="stream-cancel",
    )

    await stream.aclose()
    await session.close()

    written = _decode_written_messages(channel)
    assert len(written) >= 2
    assert written[0]["type"] == "request"
    assert written[0]["action"] == "stream_logs"
    assert written[1]["type"] == "request"
    assert written[1]["action"] == "cancel_stream"
    assert written[1]["payload"] == {"stream_id": "stream-cancel"}


@pytest.mark.asyncio
async def test_worker_session_closes_pending_requests_when_channel_ends() -> None:
    channel = _FakeChannel()
    session = WorkerSession(channel=channel)
    await session.start()

    task = asyncio.create_task(
        session.request(
            action="get_system_metrics",
            payload={"host": "prod"},
            request_id="req-close-1",
            timeout_seconds=1.0,
        )
    )
    await asyncio.sleep(0)
    channel.finish()

    with pytest.raises(RuntimeError, match="worker session closed"):
        await task

    await session.close()


@pytest.mark.asyncio
async def test_worker_session_closes_pending_requests_and_streams_on_reader_failure() -> None:
    channel = _FailingReadChannel()
    session = WorkerSession(channel=channel)
    await session.start()

    request_task = asyncio.create_task(
        session.request(
            action="get_system_metrics",
            payload={"host": "prod"},
            request_id="req-reader-failure",
            timeout_seconds=1.0,
        )
    )
    stream = await session.open_stream(
        action="stream_logs",
        payload={"host": "prod", "container_id": "c1"},
        request_id="req-reader-failure-stream",
        stream_id="stream-reader-failure",
    )
    await asyncio.sleep(0)
    channel.trigger_failure()

    with pytest.raises(RuntimeError, match="worker session closed"):
        await request_task

    with pytest.raises(StopAsyncIteration):
        await anext(stream)

    await session.close()


@pytest.mark.asyncio
async def test_worker_session_can_send_heartbeat_frames() -> None:
    channel = _FakeChannel()
    session = WorkerSession(channel=channel)

    await session.send_heartbeat()

    assert channel.writes
    payload = channel.writes[-1]
    assert b'"type":"heartbeat"' in payload
    await session.close()
