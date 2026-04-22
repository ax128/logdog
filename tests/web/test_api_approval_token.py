from __future__ import annotations

import asyncio
import time
from typing import Any

import httpx

from logdog.llm.permissions import has_valid_approval_token, load_permission_policy
from logdog.main import create_app
from logdog.web.api import create_api_app


def test_issue_approval_token_requires_admin_token() -> None:
    async def issue_action(
        _tool_name: str,
        _arguments: dict[str, Any],
    ) -> dict[str, Any]:
        return {"approval_token": "token"}

    async def run_case() -> httpx.Response:
        app = create_api_app(
            web_auth_token="user-token",
            web_admin_token="admin-token",
            issue_approval_token_action=issue_action,
        )
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://testserver",
        ) as client:
            return await client.post(
                "/api/approval-token",
                headers={"Authorization": "Bearer user-token"},
                json={
                    "tool": "restart_container",
                    "arguments": {"host": "prod", "container_id": "c1"},
                },
            )

    response = asyncio.run(run_case())

    assert response.status_code == 403


def test_issue_approval_token_returns_signed_token_for_allowlisted_tool() -> None:
    app = create_app(
        app_config={
            "llm": {
                "permissions": {
                    "dangerous_tools": ["restart_container", "exec_container"],
                    "dangerous_host_allowlist": ["prod"],
                    "approval_secret": "test-secret",
                    "approval_token_ttl_seconds": 120,
                }
            },
            "hosts": [{"name": "prod", "url": "unix:///var/run/docker.sock"}],
        },
        enable_metrics_scheduler=False,
        enable_retention_cleanup=False,
        enable_watch_manager=False,
    )
    arguments = {"host": "prod", "container_id": "c1", "timeout": 10}

    async def run_case() -> httpx.Response:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://testserver",
        ) as client:
            return await client.post(
                "/api/approval-token",
                headers={"Authorization": "Bearer admin-token"},
                json={
                    "tool": "restart_container",
                    "arguments": arguments,
                },
            )

    response = asyncio.run(run_case())
    payload = response.json()

    assert response.status_code == 200
    assert payload["tool"] == "restart_container"
    assert payload["host"] == "prod"
    assert payload["ttl_seconds"] == 120
    assert payload["expires_at"] >= int(time.time())

    policy = load_permission_policy(app.state.app_config)
    assert has_valid_approval_token(
        "restart_container",
        {**arguments, "approval_token": payload["approval_token"]},
        policy=policy,
        now=int(time.time()),
    )


def test_issue_approval_token_rejects_non_dangerous_tool() -> None:
    app = create_app(
        app_config={
            "llm": {
                "permissions": {
                    "dangerous_tools": ["restart_container", "exec_container"],
                    "dangerous_host_allowlist": ["prod"],
                    "approval_secret": "test-secret",
                }
            },
            "hosts": [{"name": "prod", "url": "unix:///var/run/docker.sock"}],
        },
        enable_metrics_scheduler=False,
        enable_retention_cleanup=False,
        enable_watch_manager=False,
    )

    async def run_case() -> httpx.Response:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://testserver",
        ) as client:
            return await client.post(
                "/api/approval-token",
                headers={"Authorization": "Bearer admin-token"},
                json={
                    "tool": "query_logs",
                    "arguments": {"host": "prod", "container_id": "c1"},
                },
            )

    response = asyncio.run(run_case())

    assert response.status_code == 400
    assert "dangerous" in str(response.json().get("detail", "")).lower()


def test_issue_approval_token_requires_approval_secret() -> None:
    app = create_app(
        app_config={
            "llm": {
                "permissions": {
                    "dangerous_tools": ["restart_container", "exec_container"],
                    "dangerous_host_allowlist": ["prod"],
                    "approval_secret": "",
                }
            },
            "hosts": [{"name": "prod", "url": "unix:///var/run/docker.sock"}],
        },
        enable_metrics_scheduler=False,
        enable_retention_cleanup=False,
        enable_watch_manager=False,
    )

    async def run_case() -> httpx.Response:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://testserver",
        ) as client:
            return await client.post(
                "/api/approval-token",
                headers={"Authorization": "Bearer admin-token"},
                json={
                    "tool": "restart_container",
                    "arguments": {"host": "prod", "container_id": "c1"},
                },
            )

    response = asyncio.run(run_case())

    assert response.status_code == 503
    assert "approval_secret" in str(response.json().get("detail", ""))
