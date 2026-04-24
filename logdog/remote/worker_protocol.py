from __future__ import annotations

import json
import struct
from typing import Any

MAX_FRAME_SIZE = 1024 * 1024
_FRAME_HEADER_SIZE = 4
_SUPPORTED_MESSAGE_TYPES = {
    "hello",
    "heartbeat",
    "request",
    "response",
    "log",
    "event",
    "metrics",
    "stream_end",
    "error",
    "shutdown_ack",
}
_STREAM_MESSAGE_TYPES = {"log", "event", "metrics", "stream_end"}
_REQUEST_MESSAGE_TYPES = {"request", "response"}


class WorkerProtocolError(ValueError):
    pass


class FrameTooLargeError(WorkerProtocolError):
    pass


class MalformedFrameError(WorkerProtocolError):
    pass


class TruncatedFrameError(WorkerProtocolError):
    pass


def encode_frame(message: dict[str, Any]) -> bytes:
    _validate_message(message)
    payload = json.dumps(
        message,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")
    if len(payload) > MAX_FRAME_SIZE:
        raise FrameTooLargeError(
            f"frame payload exceeds maximum size of {MAX_FRAME_SIZE} bytes"
        )
    return struct.pack(">I", len(payload)) + payload


def decode_frame(frame: bytes | bytearray | memoryview) -> dict[str, Any]:
    raw = bytes(frame)
    if len(raw) < _FRAME_HEADER_SIZE:
        raise TruncatedFrameError("frame header is truncated")

    (payload_size,) = struct.unpack(">I", raw[:_FRAME_HEADER_SIZE])
    if payload_size > MAX_FRAME_SIZE:
        raise FrameTooLargeError(
            f"frame payload exceeds maximum size of {MAX_FRAME_SIZE} bytes"
        )

    expected_size = _FRAME_HEADER_SIZE + payload_size
    if len(raw) < expected_size:
        raise TruncatedFrameError("frame payload is truncated")
    if len(raw) > expected_size:
        raise MalformedFrameError("frame contains trailing bytes")

    return _decode_payload(raw[_FRAME_HEADER_SIZE:expected_size])


class FrameReader:
    def __init__(self) -> None:
        self._buffer = bytearray()

    def feed(
        self, data: bytes | bytearray | memoryview
    ) -> list[dict[str, Any]]:
        if data:
            self._buffer.extend(data)
        messages: list[dict[str, Any]] = []

        while True:
            if len(self._buffer) < _FRAME_HEADER_SIZE:
                break

            (payload_size,) = struct.unpack(">I", self._buffer[:_FRAME_HEADER_SIZE])
            if payload_size > MAX_FRAME_SIZE:
                raise FrameTooLargeError(
                    f"frame payload exceeds maximum size of {MAX_FRAME_SIZE} bytes"
                )

            frame_size = _FRAME_HEADER_SIZE + payload_size
            if len(self._buffer) < frame_size:
                break

            payload = bytes(self._buffer[_FRAME_HEADER_SIZE:frame_size])
            del self._buffer[:frame_size]
            messages.append(_decode_payload(payload))

        return messages

    def close(self) -> list[dict[str, Any]]:
        if self._buffer:
            raise TruncatedFrameError("stream ended with a truncated frame")
        return []


def _decode_payload(payload: bytes) -> dict[str, Any]:
    try:
        message = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MalformedFrameError("frame payload is not valid JSON") from exc

    _validate_message(message)
    return message


def _validate_message(message: Any) -> None:
    if not isinstance(message, dict):
        raise MalformedFrameError("frame payload must decode to a JSON object")

    message_type = message.get("type")
    if not isinstance(message_type, str) or message_type not in _SUPPORTED_MESSAGE_TYPES:
        raise MalformedFrameError("unsupported or missing message type")

    if message_type in _REQUEST_MESSAGE_TYPES:
        request_id = message.get("request_id")
        if not isinstance(request_id, str) or not request_id:
            raise MalformedFrameError("request and response messages require request_id")

    if message_type in _STREAM_MESSAGE_TYPES:
        stream_id = message.get("stream_id")
        if not isinstance(stream_id, str) or not stream_id:
            raise MalformedFrameError("stream messages require stream_id")
