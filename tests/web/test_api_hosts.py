import asyncio

import httpx

from logdog.main import create_app


def test_api_hosts_returns_host_statuses() -> None:
    async def host_connector(_host: dict) -> dict:
        return {"server_version": "24.0.7"}

    async def run_case() -> httpx.Response:
        app = create_app(
            hosts=[{"name": "local", "url": "unix:///var/run/docker.sock"}],
            host_connector=host_connector,
            enable_metrics_scheduler=False,
        )
        await app.state.host_manager.startup_check()
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as client:
            return await client.get(
                "/api/hosts",
                headers={"Authorization": "Bearer web-token"},
            )

    resp = asyncio.run(run_case())
    assert resp.status_code == 200
    payload = resp.json()
    assert list(payload.keys()) == ["hosts"]
    assert len(payload["hosts"]) == 1
    assert payload["hosts"][0]["name"] == "local"
    assert payload["hosts"][0]["status"] == "connected"


def test_api_hosts_requires_web_auth_token() -> None:
    async def run_case() -> httpx.Response:
        app = create_app(enable_metrics_scheduler=False)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as client:
            return await client.get("/api/hosts")

    resp = asyncio.run(run_case())
    assert resp.status_code == 403


def test_api_hosts_redacts_sensitive_last_error() -> None:
    async def host_connector(_host: dict) -> dict:
        raise RuntimeError("Bearer SECRET token=abc password=xyz")

    async def run_case() -> httpx.Response:
        app = create_app(
            hosts=[{"name": "local", "url": "unix:///var/run/docker.sock"}],
            host_connector=host_connector,
            enable_metrics_scheduler=False,
        )
        await app.state.host_manager.startup_check()
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as client:
            return await client.get(
                "/api/hosts",
                headers={"Authorization": "Bearer web-token"},
            )

    resp = asyncio.run(run_case())
    assert resp.status_code == 200
    last_error = str(resp.json()["hosts"][0]["last_error"] or "")
    assert "SECRET" not in last_error
    assert "token=abc" not in last_error
    assert "password=xyz" not in last_error
    assert "***REDACTED***" in last_error


def test_api_hosts_redacts_sensitive_url_parts() -> None:
    async def host_connector(_host: dict) -> dict:
        return {"server_version": "24.0.7"}

    async def run_case() -> httpx.Response:
        app = create_app(
            hosts=[
                {
                    "name": "prod",
                    "url": "ssh://deploy:secret@10.0.1.10:22/var/run/docker.sock?token=abc#frag",
                }
            ],
            host_connector=host_connector,
            enable_metrics_scheduler=False,
        )
        await app.state.host_manager.startup_check()
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as client:
            return await client.get(
                "/api/hosts",
                headers={"Authorization": "Bearer web-token"},
            )

    resp = asyncio.run(run_case())
    assert resp.status_code == 200
    host_url = str(resp.json()["hosts"][0]["url"] or "")
    assert "secret" not in host_url
    assert "deploy" not in host_url
    assert "token=abc" not in host_url
    assert "?" not in host_url
    assert "#" not in host_url
