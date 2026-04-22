from __future__ import annotations

import struct

import pytest

from logdog.remote.worker_protocol import (
    FrameReader,
    FrameTooLargeError,
    MalformedFrameError,
    TruncatedFrameError,
    decode_frame,
    encode_frame,
)


def test_encode_frame_round_trips_supported_message_types() -> None:
    messages = [
        {"type": "hello", "node_id": "node-1", "version": "1"},
        {"type": "heartbeat", "ts": "2026-04-20T10:00:00Z"},
        {"type": "request", "request_id": "req-1", "action": "ping"},
        {"type": "response", "request_id": "req-1", "ok": True},
        {"type": "log", "stream_id": "stream-1", "line": "line one"},
        {"type": "event", "stream_id": "stream-1", "kind": "started"},
        {"type": "metrics", "stream_id": "stream-1", "cpu": 0.5},
        {"type": "error", "code": "bad_request"},
        {"type": "shutdown_ack", "reason": "done"},
    ]

    reader = FrameReader()
    chunks: list[bytes] = []

    for message in messages:
        frame = encode_frame(message)
        payload_len = struct.unpack(">I", frame[:4])[0]
        assert payload_len == len(frame) - 4
        assert decode_frame(frame) == message
        chunks.extend([frame[:2], frame[2:5], frame[5:]])

    parsed: list[dict[str, object]] = []
    for chunk in chunks:
        parsed.extend(reader.feed(chunk))
    parsed.extend(reader.close())

    assert parsed == messages


@pytest.mark.parametrize(
    ("payload", "expected_error"),
    [
        (struct.pack(">I", 6) + b"{oops}", MalformedFrameError),
        (struct.pack(">I", 1024 * 1024 + 1) + b"{}", FrameTooLargeError),
    ],
)
def test_decode_frame_rejects_invalid_frames(
    payload: bytes, expected_error: type[Exception]
) -> None:
    with pytest.raises(expected_error):
        decode_frame(payload)


def test_frame_reader_rejects_truncated_frame_on_close() -> None:
    frame = encode_frame({"type": "request", "request_id": "req-2", "action": "ping"})
    reader = FrameReader()

    assert reader.feed(frame[:-3]) == []

    with pytest.raises(TruncatedFrameError):
        reader.close()
