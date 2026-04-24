from __future__ import annotations

import asyncio
import inspect
import queue
import threading
from dataclasses import dataclass
from itertools import count
from typing import Any

from logdog.remote.worker_protocol import FrameReader, encode_frame


_STREAM_END = object()
_QUEUE_POLL_INTERVAL_SECONDS = 0.01


async def _to_thread(fn: Any, /, *args: Any, **kwargs: Any) -> Any:
    runner = getattr(asyncio, "to_thread", None)
    if callable(runner):
        return await runner(fn, *args, **kwargs)
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, lambda: fn(*args, **kwargs))


async def _poll_queue_item(queue_obj: queue.Queue[Any]) -> Any:
    while True:
        try:
            return queue_obj.get_nowait()
        except queue.Empty:
            await asyncio.sleep(_QUEUE_POLL_INTERVAL_SECONDS)


@dataclass(slots=True)
class _StreamFailure:
    error: BaseException


def _error_from_message(message: dict[str, Any]) -> RuntimeError:
    error_payload = message.get("error")
    if isinstance(error_payload, dict):
        message_text = str(error_payload.get("message") or "worker error")
    else:
        message_text = str(message.get("message") or "worker error")
    return RuntimeError(message_text)


class _WorkerStream:
    def __init__(
        self,
        *,
        stream_id: str,
        queue: queue.Queue[Any],
        on_close: Any,
    ) -> None:
        self._stream_id = stream_id
        self._queue = queue
        self._on_close = on_close
        self._closed = False

    def __aiter__(self) -> _WorkerStream:
        return self

    async def __anext__(self) -> dict[str, Any]:
        item = await _poll_queue_item(self._queue)
        if item is _STREAM_END:
            self._closed = True
            raise StopAsyncIteration
        if isinstance(item, _StreamFailure):
            self._closed = True
            raise item.error
        return dict(item)

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        maybe_awaitable = self._on_close(self._stream_id)
        if inspect.isawaitable(maybe_awaitable):
            await maybe_awaitable


class WorkerSession:
    def __init__(
        self,
        *,
        channel: Any,
        read_size: int = 65536,
        stream_queue_maxsize: int = 256,
    ) -> None:
        if int(read_size) <= 0:
            raise ValueError("read_size must be > 0")
        if int(stream_queue_maxsize) <= 0:
            raise ValueError("stream_queue_maxsize must be > 0")
        self._channel = channel
        self._read_size = int(read_size)
        self._stream_queue_maxsize = int(stream_queue_maxsize)
        self._loop: asyncio.AbstractEventLoop | None = None
        self._reader = FrameReader()
        self._reader_thread: threading.Thread | None = None
        self._state_lock = asyncio.Lock()
        self._write_lock = asyncio.Lock()
        self._closed = False
        self._started = False
        self._close_reason = RuntimeError("worker session closed")
        self._pending_requests: dict[str, queue.Queue[Any]] = {}
        self._streams: dict[str, queue.Queue[Any]] = {}
        self._stream_request_ids: dict[str, str] = {}
        self._request_sequence = count(1)

    async def start(self) -> None:
        async with self._state_lock:
            if self._started:
                return
            self._loop = asyncio.get_running_loop()
            self._reader_thread = threading.Thread(
                target=self._reader_loop,
                name="worker-session-reader",
                daemon=True,
            )
            self._started = True
            self._reader_thread.start()

    async def request(
        self,
        *,
        action: str,
        payload: dict[str, Any] | None = None,
        request_id: str | None = None,
        timeout_seconds: float = 30.0,
    ) -> dict[str, Any]:
        if not self._started:
            await self.start()
        normalized_request_id = request_id or f"req-{next(self._request_sequence)}"
        response_queue: queue.Queue[Any] = queue.Queue(maxsize=1)

        async with self._state_lock:
            self._ensure_open()
            if (
                normalized_request_id in self._pending_requests
                or normalized_request_id in self._stream_request_ids
            ):
                raise ValueError(f"duplicate request_id: {normalized_request_id}")
            self._pending_requests[normalized_request_id] = response_queue

        try:
            await self._send_message(
                {
                    "type": "request",
                    "request_id": normalized_request_id,
                    "action": str(action),
                    "payload": dict(payload or {}),
                }
            )
            item = await asyncio.wait_for(
                _poll_queue_item(response_queue),
                timeout=float(timeout_seconds),
            )
        except Exception:
            async with self._state_lock:
                self._pending_requests.pop(normalized_request_id, None)
            raise

        if isinstance(item, BaseException):
            raise item
        return dict(item)

    async def send_heartbeat(self) -> None:
        if not self._started:
            await self.start()
        async with self._state_lock:
            self._ensure_open()
        await self._send_message({"type": "heartbeat"})

    async def open_stream(
        self,
        *,
        action: str,
        payload: dict[str, Any] | None = None,
        request_id: str,
        stream_id: str,
    ) -> _WorkerStream:
        if not self._started:
            await self.start()
        normalized_stream_id = str(stream_id or "").strip()
        if normalized_stream_id == "":
            raise ValueError("stream_id must not be empty")

        queue_obj: queue.Queue[Any] = queue.Queue(maxsize=self._stream_queue_maxsize)
        normalized_request_id = str(request_id or "").strip()
        if normalized_request_id == "":
            raise ValueError("request_id must not be empty")
        async with self._state_lock:
            self._ensure_open()
            if normalized_stream_id in self._streams:
                raise ValueError(f"duplicate stream_id: {normalized_stream_id}")
            if (
                normalized_request_id in self._pending_requests
                or normalized_request_id in self._stream_request_ids
            ):
                raise ValueError(f"duplicate request_id: {normalized_request_id}")
            self._streams[normalized_stream_id] = queue_obj
            self._stream_request_ids[normalized_request_id] = normalized_stream_id

        try:
            await self._send_message(
                {
                    "type": "request",
                    "request_id": normalized_request_id,
                    "action": str(action),
                    "payload": dict(payload or {}),
                    "stream_id": normalized_stream_id,
                }
            )
        except Exception:
            await self._remove_stream(normalized_stream_id)
            raise

        return _WorkerStream(
            stream_id=normalized_stream_id,
            queue=queue_obj,
            on_close=self._remove_stream,
        )

    async def close(self) -> None:
        async with self._state_lock:
            if not self._closed:
                self._closed = True
                self._fail_pending_locked(self._close_reason)
                self._close_streams_locked()

        close_fn = getattr(self._channel, "close", None)
        if callable(close_fn):
            close_fn()

    def _reader_loop(self) -> None:
        try:
            while True:
                chunk = self._channel.read(self._read_size)
                if not chunk:
                    break
                for message in self._reader.feed(chunk):
                    self._dispatch_message(message)
            self._reader.close()
        except Exception as exc:  # noqa: BLE001
            self._dispatch_reader_failure(exc)
            return

        self._dispatch_reader_failure(self._close_reason)

    def _dispatch_message(self, message: dict[str, Any]) -> None:
        loop = self._loop
        if loop is None:
            return
        try:
            loop.call_soon_threadsafe(self._handle_message, dict(message))
        except RuntimeError:
            return

    def _dispatch_reader_failure(self, exc: BaseException) -> None:
        loop = self._loop
        if loop is None:
            return
        try:
            loop.call_soon_threadsafe(self._handle_reader_failure, exc)
        except RuntimeError:
            return

    def _handle_message(self, message: dict[str, Any]) -> None:
        message_type = str(message.get("type") or "")
        if message_type == "response":
            request_id = str(message.get("request_id") or "").strip()
            queue = self._pending_requests.pop(request_id, None)
            if queue is not None:
                self._push_to_queue(queue, dict(message))
                return
            stream_id = self._stream_request_ids.pop(request_id, None)
            if stream_id is not None and message.get("ok") is False:
                error = _error_from_message(message)
                self._finish_stream(stream_id, _StreamFailure(error))
            return

        if message_type == "error":
            request_id = str(message.get("request_id") or "").strip()
            error = RuntimeError(str(message.get("message") or "worker error"))
            if request_id != "":
                queue = self._pending_requests.pop(request_id, None)
                if queue is not None:
                    self._push_to_queue(queue, error)
                    return
            stream_id = str(message.get("stream_id") or "").strip()
            if stream_id != "":
                self._finish_stream(stream_id, _StreamFailure(error))
            return

        if message_type == "stream_end":
            stream_id = str(message.get("stream_id") or "").strip()
            if stream_id != "":
                self._finish_stream(stream_id, _STREAM_END)
            return

        if message_type in {"log", "event", "metrics"}:
            stream_id = str(message.get("stream_id") or "").strip()
            if stream_id != "":
                self._push_stream_item(stream_id, dict(message))

    def _handle_reader_failure(self, _exc: BaseException) -> None:
        if self._closed:
            return
        self._closed = True
        self._close_reason = RuntimeError("worker session closed")
        self._fail_pending_locked(self._close_reason)
        self._close_streams_locked()

    async def _send_message(self, message: dict[str, Any]) -> None:
        encoded = encode_frame(message)
        async with self._write_lock:
            self._ensure_open()
            self._channel.write(encoded)
            flush = getattr(self._channel, "flush", None)
            if callable(flush):
                flush()

    async def _remove_stream(self, stream_id: str) -> None:
        should_send_cancel = False
        async with self._state_lock:
            queue = self._streams.pop(stream_id, None)
            if queue is not None and not self._closed:
                should_send_cancel = True
        if queue is not None:
            self._discard_stream_request_ids(stream_id)
            self._push_to_queue(queue, _STREAM_END)
        if should_send_cancel:
            await self._send_cancel_stream(stream_id)

    def _push_stream_item(self, stream_id: str, item: Any) -> None:
        queue_obj = self._streams.get(stream_id)
        if queue_obj is None:
            return
        self._push_to_queue(queue_obj, item)

    def _finish_stream(self, stream_id: str, item: Any) -> None:
        queue_obj = self._streams.pop(stream_id, None)
        self._discard_stream_request_ids(stream_id)
        if queue_obj is not None:
            self._push_to_queue(queue_obj, item)

    def _push_to_queue(self, queue_obj: queue.Queue[Any], item: Any) -> None:
        try:
            queue_obj.put_nowait(item)
        except queue.Full:
            try:
                queue_obj.get_nowait()
            except queue.Empty:
                return
            try:
                queue_obj.put_nowait(item)
            except queue.Full:
                return

    def _fail_pending_locked(self, exc: BaseException) -> None:
        pending = list(self._pending_requests.values())
        self._pending_requests.clear()
        for queue_obj in pending:
            self._push_to_queue(queue_obj, exc)

    def _close_streams_locked(self) -> None:
        queues = list(self._streams.values())
        self._streams.clear()
        self._stream_request_ids.clear()
        for queue_obj in queues:
            self._push_to_queue(queue_obj, _STREAM_END)

    def _discard_stream_request_ids(self, stream_id: str) -> None:
        stale_request_ids = [
            request_id
            for request_id, mapped_stream_id in self._stream_request_ids.items()
            if mapped_stream_id == stream_id
        ]
        for request_id in stale_request_ids:
            self._stream_request_ids.pop(request_id, None)

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("worker session closed")

    async def _send_cancel_stream(self, stream_id: str) -> None:
        message = {
            "type": "request",
            "request_id": f"cancel-{next(self._request_sequence)}",
            "action": "cancel_stream",
            "payload": {"stream_id": str(stream_id)},
        }
        try:
            await self._send_message(message)
        except Exception:
            return
