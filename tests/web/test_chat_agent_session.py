from __future__ import annotations

import asyncio

import pytest
from logdog.llm.agent_runtime import DEFAULT_CHAT_FALLBACK_MESSAGE
from logdog.web.chat import create_chat_app
from starlette.exceptions import WebSocketException
from starlette.websockets import WebSocketDisconnect


class FakeWebSocket:
    def __init__(
        self, query_params: dict[str, str], headers: dict[str, str] | None = None
    ):
        self.query_params = query_params
        self.headers = headers or {}
        self.accepted = False
        self.sent: list[str] = []
        self.received: list[str] = []

    async def accept(self) -> None:
        self.accepted = True

    async def receive_text(self) -> str:
        if not self.received:
            raise WebSocketDisconnect(code=1000)
        return self.received.pop(0)

    async def send_text(self, text: str) -> None:
        self.sent.append(text)


class _RecordingRuntime:
    def __init__(self) -> None:
        self.calls: list[dict[str, str]] = []

    def invoke_text(
        self,
        prompt: str,
        *,
        user_id: str | None = None,
        session_key: str | None = None,
        fallback: str,
    ) -> str:
        self.calls.append(
            {
                "prompt": prompt,
                "user_id": str(user_id or ""),
                "session_key": str(session_key or ""),
                "fallback": fallback,
            }
        )
        return f"agent::{prompt}"


def test_ws_chat_returns_agent_response_and_reuses_session_identity() -> None:
    runtime = _RecordingRuntime()
    app = create_chat_app(web_auth_token="secret", chat_runtime=runtime)
    ws_handler = app.state.ws_handler

    fake_ws = FakeWebSocket(
        query_params={},
        headers={
            "authorization": "Bearer secret",
            "x-session-id": "browser-tab-1",
        },
    )
    fake_ws.received.extend(["ping", "pong"])

    asyncio.run(ws_handler(fake_ws))

    assert fake_ws.accepted is True
    assert fake_ws.sent == ["agent::ping", "agent::pong"]
    assert runtime.calls == [
        {
            "prompt": "ping",
            "user_id": runtime.calls[0]["user_id"],
            "session_key": "browser-tab-1",
            "fallback": DEFAULT_CHAT_FALLBACK_MESSAGE,
        },
        {
            "prompt": "pong",
            "user_id": runtime.calls[0]["user_id"],
            "session_key": "browser-tab-1",
            "fallback": DEFAULT_CHAT_FALLBACK_MESSAGE,
        },
    ]
    assert runtime.calls[0]["user_id"].startswith("auth:")
    assert runtime.calls[0]["user_id"] != "secret"


def test_ws_chat_sends_fallback_message_when_runtime_raises() -> None:
    class _RaisingRuntime:
        def invoke_text(
            self,
            prompt: str,
            *,
            user_id: str | None = None,
            session_key: str | None = None,
            fallback: str,
        ) -> str:
            raise RuntimeError("boom")

    app = create_chat_app(web_auth_token="secret", chat_runtime=_RaisingRuntime())
    ws_handler = app.state.ws_handler

    fake_ws = FakeWebSocket(
        query_params={},
        headers={"authorization": "Bearer secret"},
    )
    fake_ws.received.append("ping")

    asyncio.run(ws_handler(fake_ws))

    assert fake_ws.accepted is True
    assert fake_ws.sent == [DEFAULT_CHAT_FALLBACK_MESSAGE]


def test_ws_chat_default_session_key_does_not_reuse_auth_token() -> None:
    runtime = _RecordingRuntime()
    app = create_chat_app(web_auth_token="secret", chat_runtime=runtime)
    ws_handler = app.state.ws_handler

    fake_ws = FakeWebSocket(
        query_params={},
        headers={"authorization": "Bearer secret"},
    )
    fake_ws.received.append("ping")

    asyncio.run(ws_handler(fake_ws))

    assert fake_ws.accepted is True
    assert runtime.calls[0]["session_key"] != "secret"


def test_ws_chat_rejects_oversized_session_key() -> None:
    runtime = _RecordingRuntime()
    app = create_chat_app(web_auth_token="secret", chat_runtime=runtime)
    ws_handler = app.state.ws_handler

    fake_ws = FakeWebSocket(
        query_params={},
        headers={
            "authorization": "Bearer secret",
            "x-session-id": "a" * 257,
        },
    )
    fake_ws.received.append("ping")

    with pytest.raises(WebSocketException) as exc_info:
        asyncio.run(ws_handler(fake_ws))

    assert exc_info.value.code == 1008
    assert fake_ws.accepted is False
    assert runtime.calls == []
