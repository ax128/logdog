import httpx
import pytest

from logwatch.main import create_app


@pytest.mark.asyncio
async def test_main_app_serves_static_frontend_entrypoint() -> None:
    app = create_app(
        enable_metrics_scheduler=False,
        enable_retention_cleanup=False,
        enable_watch_manager=False,
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://testserver"
    ) as client:
        entrypoint = await client.get("/")
        script = await client.get("/static/app.js")
        stylesheet = await client.get("/static/app.css")
        health = await client.get("/health")

    assert entrypoint.status_code == 200
    assert entrypoint.headers["content-type"].startswith("text/html")
    assert "LogWatch" in entrypoint.text
    assert "host-overview" in entrypoint.text
    assert "chat-panel" in entrypoint.text

    assert script.status_code == 200
    assert "javascript" in script.headers["content-type"]
    assert "/api/hosts" in script.text
    assert "/api/ws-ticket" in script.text
    assert "/ws/chat" in script.text
    assert "Authorization" in script.text

    assert stylesheet.status_code == 200
    assert stylesheet.headers["content-type"].startswith("text/css")
    assert ":root" in stylesheet.text

    assert health.status_code == 200
    assert health.json() == {"ok": True}
