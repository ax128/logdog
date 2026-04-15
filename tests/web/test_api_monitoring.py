from __future__ import annotations

import asyncio
import sqlite3

import httpx

from logdog.core.db import (
    init_db,
    insert_alert,
    insert_host_metric_sample,
    insert_metric_sample,
    insert_mute,
    insert_storm_event,
)
from logdog.main import create_app


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


def test_api_host_containers_returns_injected_container_list() -> None:
    async def host_connector(_host: dict) -> dict:
        return {"server_version": "24.0.7"}

    async def list_containers(_host_name: str) -> list[dict]:
        return [{"id": "c1", "name": "api", "status": "running"}]

    async def run_case() -> httpx.Response:
        app = create_app(
            hosts=[{"name": "local", "url": "unix:///var/run/docker.sock"}],
            host_connector=host_connector,
            list_containers_fn=list_containers,
            enable_metrics_scheduler=False,
            enable_watch_manager=False,
        )
        await app.state.host_manager.startup_check()
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as client:
            return await client.get(
                "/api/hosts/local/containers",
                headers={"Authorization": "Bearer web-token"},
            )

    resp = asyncio.run(run_case())
    assert resp.status_code == 200
    assert resp.json() == {
        "host": "local",
        "containers": [{"id": "c1", "name": "api", "status": "running"}],
    }


def test_api_alerts_metrics_and_mutes_surface_stored_data() -> None:
    sync_conn = sqlite3.connect(":memory:")
    wrapped_conn = _SyncAioSqliteConn(sync_conn)

    def connect_db(_db_path: str) -> _SyncAioSqliteConn:
        return wrapped_conn

    async def run_case() -> tuple[
        httpx.Response,
        httpx.Response,
        httpx.Response,
        httpx.Response,
        httpx.Response,
        httpx.Response,
    ]:
        app = create_app(
            hosts=[{"name": "host-a", "url": "unix:///var/run/docker.sock"}],
            metrics_db_path=":memory:",
            metrics_db_connect=connect_db,
            enable_metrics_scheduler=False,
            enable_retention_cleanup=False,
            enable_watch_manager=False,
        )

        await init_db(wrapped_conn)
        await insert_alert(
            wrapped_conn,
            {
                "host": "host-a",
                "container_id": "c1",
                "container_name": "api",
                "category": "ERROR",
                "line": "boom",
                "analysis": "rendered",
                "pushed": True,
            },
        )
        await insert_alert(
            wrapped_conn,
            {
                "host": "host-a",
                "container_id": "c1",
                "container_name": "api",
                "category": "WARN",
                "line": "warn-line",
                "analysis": "warn-analysis",
                "pushed": False,
            },
        )
        await insert_metric_sample(
            wrapped_conn,
            {
                "host_name": "host-a",
                "container_id": "c1",
                "container_name": "api",
                "timestamp": "2026-04-11T00:00:01+00:00",
                "cpu": 12.5,
                "mem_used": 128,
                "mem_limit": 1024,
                "net_rx": 1,
                "net_tx": 2,
                "disk_read": 3,
                "disk_write": 4,
                "status": "running",
                "restart_count": 0,
            },
        )
        await insert_host_metric_sample(
            wrapped_conn,
            {
                "host_name": "host-a",
                "timestamp": "2026-04-11T00:00:02+00:00",
                "cpu_percent": 90.0,
                "load_1": 1.2,
                "load_5": 1.1,
                "load_15": 1.0,
                "mem_total": 1024,
                "mem_used": 900,
                "mem_available": 124,
                "disk_root_total": 4096,
                "disk_root_used": 3000,
                "disk_root_free": 1096,
                "net_rx": 100,
                "net_tx": 200,
                "source": "local",
            },
        )
        await insert_mute(
            wrapped_conn,
            {
                "host": "host-a",
                "container_id": "c1",
                "category": "ERROR",
                "reason": "maintenance",
                "expires_at": "2999-01-01T00:00:00+00:00",
            },
        )
        await insert_storm_event(
            wrapped_conn,
            {
                "host": "host-a",
                "category": "ERROR",
                "storm_phase": "start",
                "storm_total": 4,
            },
        )

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://testserver"
        ) as client:
            alerts = await client.get(
                "/api/alerts",
                params={"host": "host-a", "container": "api", "category": "ERROR"},
                headers={"Authorization": "Bearer web-token"},
            )
            metrics = await client.get(
                "/api/hosts/host-a/metrics/c1",
                params={"start": "2026-04-11 00:00:01", "end": "2026-04-11 00:00:01"},
                headers={"Authorization": "Bearer web-token"},
            )
            mutes = await client.get(
                "/api/mutes",
                headers={"Authorization": "Bearer web-token"},
            )
            storms = await client.get(
                "/api/storm-events",
                params={"phase": "start", "category": "ERROR"},
                headers={"Authorization": "Bearer web-token"},
            )
            storm_stats = await client.get(
                "/api/storm-events/stats",
                headers={"Authorization": "Bearer web-token"},
            )
            host_system_metrics = await client.get(
                "/api/hosts/host-a/system-metrics",
                params={"start": "2026-04-11 00:00:00", "end": "2026-04-11 00:00:05"},
                headers={"Authorization": "Bearer web-token"},
            )
        return alerts, metrics, mutes, storms, storm_stats, host_system_metrics

    alerts, metrics, mutes, storms, storm_stats, host_system_metrics = asyncio.run(
        run_case()
    )
    try:
        assert alerts.status_code == 200
        assert alerts.json()["alerts"][0]["payload"]["container_name"] == "api"
        assert alerts.json()["alerts"][0]["payload"]["host"] == "host-a"
        assert len(alerts.json()["alerts"]) == 1
        assert alerts.json()["alerts"][0]["payload"]["category"] == "ERROR"

        assert metrics.status_code == 200
        assert metrics.json() == {
            "host": "host-a",
            "container": "c1",
            "points": [
                {
                    "host_name": "host-a",
                    "container_id": "c1",
                    "container_name": "api",
                    "timestamp": "2026-04-11 00:00:01",
                    "cpu": 12.5,
                    "mem_used": 128,
                    "mem_limit": 1024,
                    "net_rx": 1,
                    "net_tx": 2,
                    "disk_read": 3,
                    "disk_write": 4,
                    "status": "running",
                    "restart_count": 0,
                }
            ],
        }

        assert mutes.status_code == 200
        assert mutes.json()["mutes"][0]["host"] == "host-a"
        assert mutes.json()["mutes"][0]["reason"] == "maintenance"

        assert storms.status_code == 200
        assert storms.json()["items"][0]["phase"] == "start"
        assert storms.json()["items"][0]["category"] == "ERROR"

        assert storm_stats.status_code == 200
        assert storm_stats.json()["total_events"] == 1
        assert storm_stats.json()["host_count"] == 1
        assert storm_stats.json()["by_category"][0]["category"] == "ERROR"
        assert storm_stats.json()["by_phase"][0]["phase"] == "start"

        assert host_system_metrics.status_code == 200
        assert host_system_metrics.json() == {
            "host": "host-a",
            "points": [
                {
                    "host_name": "host-a",
                    "timestamp": "2026-04-11 00:00:02",
                    "cpu_percent": 90.0,
                    "load_1": 1.2,
                    "load_5": 1.1,
                    "load_15": 1.0,
                    "mem_total": 1024,
                    "mem_used": 900,
                    "mem_available": 124,
                    "disk_root_total": 4096,
                    "disk_root_used": 3000,
                    "disk_root_free": 1096,
                    "net_rx": 100,
                    "net_tx": 200,
                    "source": "local",
                }
            ],
        }
    finally:
        sync_conn.close()
