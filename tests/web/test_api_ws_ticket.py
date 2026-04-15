import asyncio

import httpx
import pytest

from logwatch.llm.agent_runtime import DEFAULT_CHAT_FALLBACK_MESSAGE
from logwatch.main import create_app
from starlette.exceptions import WebSocketException
from starlette.routing import WebSocketRoute
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


def test_ws_ticket_requires_web_token() -> None:
    async def run_case() -> httpx.Response:
        app = create_app(enable_metrics_scheduler=False)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as client:
            return await client.post("/api/ws-ticket")

    resp = asyncio.run(run_case())
    assert resp.status_code == 403


def test_ws_ticket_is_one_time_and_allows_ws_chat() -> None:
    async def run_case() -> tuple[httpx.Response, FakeWebSocket, int]:
        app = create_app(enable_metrics_scheduler=False)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as client:
            ticket_resp = await client.post(
                "/api/ws-ticket",
                headers={"Authorization": "Bearer web-token"},
            )

        ws_route = next(
            route
            for route in app.router.routes
            if isinstance(route, WebSocketRoute) and route.path == "/ws/chat"
        )

        payload = ticket_resp.json()
        ticket = str(payload["ticket"])

        ws_ok = FakeWebSocket(query_params={"ticket": ticket})
        ws_ok.received.append("ping")
        await ws_route.endpoint(ws_ok)

        ws_replay = FakeWebSocket(query_params={"ticket": ticket})
        replay_code = -1
        try:
            await ws_route.endpoint(ws_replay)
        except WebSocketException as exc:
            replay_code = exc.code

        return ticket_resp, ws_ok, replay_code

    ticket_resp, ws_ok, replay_code = asyncio.run(run_case())

    assert ticket_resp.status_code == 200
    payload = ticket_resp.json()
    assert isinstance(payload["ticket"], str)
    assert payload["ticket"] != ""
    assert int(payload["ttl_seconds"]) > 0
    assert isinstance(payload["expires_at"], str)

    assert ws_ok.accepted is True
    assert ws_ok.sent == [DEFAULT_CHAT_FALLBACK_MESSAGE]
    assert replay_code == 1008


def test_ws_ticket_rejects_admin_token() -> None:
    async def run_case() -> httpx.Response:
        app = create_app(enable_metrics_scheduler=False)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as client:
            return await client.post(
                "/api/ws-ticket",
                headers={"Authorization": "Bearer admin-token"},
            )

    resp = asyncio.run(run_case())
    assert resp.status_code == 403
