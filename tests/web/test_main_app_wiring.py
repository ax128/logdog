import asyncio

import httpx
from logdog.llm.agent_runtime import DEFAULT_CHAT_FALLBACK_MESSAGE
from logdog.main import create_app
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


def test_main_app_exposes_reload_route_and_admin_guard() -> None:
    app = create_app(web_auth_token="user-token", web_admin_token="admin-token")

    async def run_case() -> tuple[httpx.Response, httpx.Response]:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as client:
            denied = await client.post(
                "/api/reload",
                headers={"Authorization": "Bearer user-token"},
            )
            ok = await client.post(
                "/api/reload",
                headers={"Authorization": "Bearer admin-token"},
            )
        return denied, ok

    denied, ok = asyncio.run(run_case())
    assert denied.status_code == 403
    assert ok.status_code == 200
    payload = ok.json()
    assert payload.get("ok") is True
    if "added" in payload:
        assert isinstance(payload["added"], list)
    if "updated" in payload:
        assert isinstance(payload["updated"], list)
    if "removed_requires_restart" in payload:
        assert isinstance(payload["removed_requires_restart"], list)


def test_main_app_registers_ws_chat_route_and_returns_fallback_response() -> None:
    app = create_app(web_auth_token="ws-token", web_admin_token="admin-token")

    ws_route = next(
        route
        for route in app.router.routes
        if isinstance(route, WebSocketRoute) and route.path == "/ws/chat"
    )
    fake_ws = FakeWebSocket(
        query_params={}, headers={"authorization": "Bearer ws-token"}
    )
    fake_ws.received.append("ping")

    asyncio.run(ws_route.endpoint(fake_ws))

    assert fake_ws.accepted is True
    assert fake_ws.sent == [DEFAULT_CHAT_FALLBACK_MESSAGE]


def test_main_app_exposes_tool_registry_on_app_state() -> None:
    app = create_app(
        enable_metrics_scheduler=False,
        enable_retention_cleanup=False,
        enable_watch_manager=False,
    )

    assert set(app.state.tool_registry) == {
        "list_hosts",
        "list_containers",
        "query_logs",
        "get_metrics",
        "get_alerts",
        "mute_alert",
        "unmute_alert",
        "restart_container",
        "get_system_metrics",
        "list_alert_mutes",
        "get_storm_events",
        "exec_container",
    }
