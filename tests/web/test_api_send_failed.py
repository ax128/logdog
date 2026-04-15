import asyncio
import sqlite3

import httpx

from logdog.main import create_app
from logdog.web.api import create_api_app


class _SyncCursor:
    def __init__(self, cur: sqlite3.Cursor):
        self._cur = cur

    async def fetchone(self):
        return self._cur.fetchone()


class _SyncAioSqliteConn:
    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    async def execute(self, sql: str, params=()):
        return _SyncCursor(self._conn.execute(sql, params))

    async def commit(self) -> None:
        self._conn.commit()

    async def close(self) -> None:
        self._conn.close()


def test_send_failed_requires_admin_token_user_forbidden() -> None:
    async def run_case() -> httpx.Response:
        app = create_api_app(
            web_auth_token="user-token",
            web_admin_token="admin-token",
            list_send_failed_action=lambda _limit: [],
        )
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            return await client.get(
                "/api/send-failed",
                headers={"Authorization": "Bearer user-token"},
            )

    resp = asyncio.run(run_case())
    assert resp.status_code == 403


def test_send_failed_admin_ok_and_passes_limit() -> None:
    seen_limits: list[int] = []

    async def list_send_failed(limit: int) -> list[dict]:
        seen_limits.append(limit)
        return [{"id": 1, "created_at": "2026-04-11T00:00:00+00:00", "payload": {"host": "h1"}}]

    async def run_case() -> httpx.Response:
        app = create_api_app(
            web_auth_token="user-token",
            web_admin_token="admin-token",
            list_send_failed_action=list_send_failed,
        )
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            return await client.get(
                "/api/send-failed",
                params={"limit": 7},
                headers={"Authorization": "Bearer admin-token"},
            )

    resp = asyncio.run(run_case())

    assert resp.status_code == 200
    assert resp.json() == {
        "items": [{"id": 1, "created_at": "2026-04-11T00:00:00+00:00", "payload": {"host": "h1"}}]
    }
    assert seen_limits == [7]


def test_main_app_send_failed_endpoint_returns_persisted_failures() -> None:
    state = {"up": False}
    sync_conn = sqlite3.connect(":memory:")
    wrapped_conn = _SyncAioSqliteConn(sync_conn)

    async def connector(_host: dict) -> dict:
        if not state["up"]:
            raise RuntimeError("docker unavailable")
        return {"server_version": "24.0.7"}

    async def telegram_send(_target: str, _message: str) -> None:
        raise RuntimeError("telegram down")

    def connect_db(_db_path: str) -> _SyncAioSqliteConn:
        return wrapped_conn

    async def run_case() -> httpx.Response:
        app = create_app(
            hosts=[{"name": "local", "url": "unix:///var/run/docker.sock"}],
            host_connector=connector,
            enable_metrics_scheduler=False,
            enable_retention_cleanup=False,
            metrics_db_path=":memory:",
            metrics_db_connect=connect_db,
            app_config={
                "notify": {
                    "telegram": {
                        "enabled": True,
                        "chat_ids": ["tg-1"],
                    }
                }
            },
            telegram_send_func=telegram_send,
            web_admin_token="admin-token",
        )

        await app.state.host_manager.startup_check()
        state["up"] = True
        await app.state.host_manager.startup_check()

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            return await client.get(
                "/api/send-failed",
                headers={"Authorization": "Bearer admin-token"},
            )

    resp = asyncio.run(run_case())
    try:
        assert resp.status_code == 200
        payload = resp.json()
        assert len(payload["items"]) == 1
        assert payload["items"][0]["payload"]["host"] == "local"
        assert payload["items"][0]["payload"]["channel"] == "telegram"
    finally:
        sync_conn.close()
