from __future__ import annotations

import asyncio

import httpx

from logwatch.web.api import create_api_app


def test_metrics_endpoint_maps_value_error_to_bad_request() -> None:
    async def query_metrics(
        _host: str,
        _container: str,
        _start: str | None,
        _end: str | None,
        _limit: int,
    ) -> list[dict]:
        raise ValueError("invalid timestamp")

    async def run_case() -> httpx.Response:
        app = create_api_app(
            web_auth_token="web-token",
            web_admin_token="admin-token",
            query_metrics_action=query_metrics,
        )
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://testserver",
        ) as client:
            return await client.get(
                "/api/hosts/local/metrics/c1",
                params={"start": "bad-ts"},
                headers={"Authorization": "Bearer web-token"},
            )

    resp = asyncio.run(run_case())
    assert resp.status_code == 400
    assert resp.json()["detail"] == "invalid request"


def test_host_system_metrics_endpoint_maps_value_error_to_bad_request() -> None:
    async def query_host_metrics(
        _host: str,
        _start: str | None,
        _end: str | None,
        _limit: int,
    ) -> list[dict]:
        raise ValueError("invalid timestamp")

    async def run_case() -> httpx.Response:
        app = create_api_app(
            web_auth_token="web-token",
            web_admin_token="admin-token",
            query_host_system_metrics_action=query_host_metrics,
        )
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://testserver",
        ) as client:
            return await client.get(
                "/api/hosts/local/system-metrics",
                params={"start": "bad-ts"},
                headers={"Authorization": "Bearer web-token"},
            )

    resp = asyncio.run(run_case())
    assert resp.status_code == 400
    assert resp.json()["detail"] == "invalid request"
