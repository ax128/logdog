import asyncio

import httpx

from logdog.web.api import create_api_app


def test_reload_requires_admin_token_user_forbidden() -> None:
    async def run_case() -> httpx.Response:
        app = create_api_app(web_auth_token="user-token", web_admin_token="admin-token")
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as client:
            return await client.post(
                "/api/reload",
                headers={"Authorization": "Bearer user-token"},
            )

    resp = asyncio.run(run_case())

    assert resp.status_code == 403


def test_reload_admin_token_ok() -> None:
    async def run_case() -> httpx.Response:
        app = create_api_app(web_auth_token="user-token", web_admin_token="admin-token")
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as client:
            return await client.post(
                "/api/reload",
                headers={"Authorization": "Bearer admin-token"},
            )

    resp = asyncio.run(run_case())

    assert resp.status_code == 200
    assert resp.json() == {"ok": True}


def test_reload_calls_reload_action_once() -> None:
    async def run_case() -> tuple[httpx.Response, int]:
        calls = {"count": 0}

        async def reload_action() -> None:
            calls["count"] += 1

        app = create_api_app(
            web_auth_token="user-token",
            web_admin_token="admin-token",
            reload_action=reload_action,
        )
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as client:
            resp = await client.post(
                "/api/reload",
                headers={"Authorization": "Bearer admin-token"},
            )
        return resp, calls["count"]

    resp, count = asyncio.run(run_case())

    assert resp.status_code == 200
    assert resp.json() == {"ok": True}
    assert count == 1


def test_health_endpoint_is_public_and_returns_ok() -> None:
    async def run_case() -> httpx.Response:
        app = create_api_app(web_auth_token="user-token", web_admin_token="admin-token")
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as client:
            return await client.get("/health")

    resp = asyncio.run(run_case())

    assert resp.status_code == 200
    assert resp.json() == {"ok": True}
