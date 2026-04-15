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


class _BlockingWebSocket(FakeWebSocket):
    def __init__(self, query_params: dict[str, str], headers: dict[str, str] | None = None):
        super().__init__(query_params=query_params, headers=headers)
        self.accept_event = asyncio.Event()
        self.release_event = asyncio.Event()
        self.closed_code: int | None = None

    async def accept(self) -> None:
        await super().accept()
        self.accept_event.set()

    async def receive_text(self) -> str:
        await self.release_event.wait()
        raise WebSocketDisconnect(code=1000)

    async def close(self, code: int = 1000) -> None:
        self.closed_code = code


class _SlowWebSocket(FakeWebSocket):
    def __init__(self, query_params: dict[str, str], headers: dict[str, str] | None = None):
        super().__init__(query_params=query_params, headers=headers)
        self.closed_code: int | None = None

    async def receive_text(self) -> str:
        await asyncio.sleep(0.05)
        return "late-message"

    async def close(self, code: int = 1000) -> None:
        self.closed_code = code


def test_ws_rejects_query_token() -> None:
    app = create_chat_app(web_auth_token="secret")
    ws_handler = app.state.ws_handler

    fake_ws = FakeWebSocket(query_params={"token": "abc"})

    with pytest.raises(WebSocketException) as exc:
        asyncio.run(ws_handler(fake_ws))

    assert exc.value.code == 1008


def test_ws_rejects_query_token_case_insensitive() -> None:
    app = create_chat_app(web_auth_token="secret")
    ws_handler = app.state.ws_handler

    fake_ws = FakeWebSocket(query_params={"Token": "abc"})

    with pytest.raises(WebSocketException) as exc:
        asyncio.run(ws_handler(fake_ws))

    assert exc.value.code == 1008


def test_ws_allows_authorization_header_token_and_returns_fallback_response() -> None:
    app = create_chat_app(web_auth_token="secret")
    ws_handler = app.state.ws_handler

    fake_ws = FakeWebSocket(query_params={}, headers={"authorization": "Bearer secret"})
    fake_ws.received.append("ping")

    asyncio.run(ws_handler(fake_ws))

    assert fake_ws.accepted
    assert fake_ws.sent == [DEFAULT_CHAT_FALLBACK_MESSAGE]


def test_ws_rejects_subprotocol_bearer_token() -> None:
    app = create_chat_app(web_auth_token="secret")
    ws_handler = app.state.ws_handler

    fake_ws = FakeWebSocket(query_params={}, headers={"sec-websocket-protocol": "Bearer,secret"})

    with pytest.raises(WebSocketException) as exc:
        asyncio.run(ws_handler(fake_ws))

    assert exc.value.code == 1008


def test_ws_accepts_one_time_ticket_query_param() -> None:
    def consume_ticket(ticket: str) -> str | None:
        if ticket == "ticket-1":
            return "secret"
        return None

    app = create_chat_app(web_auth_token="secret", ws_ticket_consumer=consume_ticket)
    ws_handler = app.state.ws_handler

    fake_ws = FakeWebSocket(query_params={"ticket": "ticket-1"})
    fake_ws.received.append("ping")

    asyncio.run(ws_handler(fake_ws))

    assert fake_ws.accepted
    assert fake_ws.sent == [DEFAULT_CHAT_FALLBACK_MESSAGE]


def test_ws_enforces_max_messages_per_connection() -> None:
    app = create_chat_app(web_auth_token="secret", max_messages_per_connection=1)
    ws_handler = app.state.ws_handler

    fake_ws = FakeWebSocket(query_params={}, headers={"authorization": "Bearer secret"})
    fake_ws.received.extend(["ping1", "ping2"])

    asyncio.run(ws_handler(fake_ws))

    assert fake_ws.accepted
    assert fake_ws.sent == [DEFAULT_CHAT_FALLBACK_MESSAGE]


@pytest.mark.asyncio
async def test_ws_rejects_when_total_connection_limit_reached() -> None:
    app = create_chat_app(web_auth_token="secret", max_connections=1)
    ws_handler = app.state.ws_handler

    ws1 = _BlockingWebSocket(query_params={}, headers={"authorization": "Bearer secret"})
    ws2 = FakeWebSocket(query_params={}, headers={"authorization": "Bearer secret"})

    task1 = asyncio.create_task(ws_handler(ws1))
    await asyncio.wait_for(ws1.accept_event.wait(), timeout=0.5)
    try:
        with pytest.raises(WebSocketException) as exc:
            await ws_handler(ws2)
        assert exc.value.code == 1008
    finally:
        ws1.release_event.set()
        await asyncio.wait_for(task1, timeout=0.5)


@pytest.mark.asyncio
async def test_ws_closes_idle_connection_when_receive_timeout_exceeded() -> None:
    app = create_chat_app(
        web_auth_token="secret",
        max_messages_per_connection=5,
        receive_timeout_seconds=0.01,
    )
    ws_handler = app.state.ws_handler
    ws = _SlowWebSocket(query_params={}, headers={"authorization": "Bearer secret"})

    await ws_handler(ws)

    assert ws.accepted is True
    assert ws.closed_code == 1008
