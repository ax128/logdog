from __future__ import annotations

import asyncio
import sqlite3
from typing import Any, cast

import pytest

from logwatch.main import create_app
from logwatch.core.host_manager import HostManager


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


class _MetricsSchedulerStub:
    def __init__(self, lifecycle: list[str]) -> None:
        self._lifecycle = lifecycle

    async def start(self) -> None:
        self._lifecycle.append("metrics:start")

    async def shutdown(self) -> None:
        self._lifecycle.append("metrics:stop")


class _RetentionSchedulerStub:
    def __init__(self, lifecycle: list[str]) -> None:
        self._lifecycle = lifecycle

    async def start(self) -> None:
        self._lifecycle.append("retention:start")

    async def shutdown(self) -> None:
        self._lifecycle.append("retention:stop")


class _WatchManagerStub:
    def __init__(self, lifecycle: list[str]) -> None:
        self._lifecycle = lifecycle

    async def start(self) -> None:
        self._lifecycle.append("watch:start")

    async def shutdown(self) -> None:
        self._lifecycle.append("watch:stop")


class _DockerPoolStub:
    def __init__(self, lifecycle: list[str]) -> None:
        self._lifecycle = lifecycle

    async def connect_host(self, _host: dict) -> dict:
        self._lifecycle.append("host:connect")
        return {"server_version": "24.0.7"}

    async def close_all(self) -> None:
        self._lifecycle.append("docker:close")


class _StreamingDockerPoolStub:
    def __init__(self) -> None:
        self.closed = 0

    async def connect_host(self, _host: dict) -> dict:
        return {"server_version": "24.0.7"}

    async def close_all(self) -> None:
        self.closed += 1

    async def list_containers_for_host(self, _host: dict) -> list[dict]:
        return []

    async def stream_container_logs(self, _host: dict, _container: dict, **_kwargs):
        if False:
            yield None

    async def stream_docker_events(self, _host: dict, **_kwargs):
        yield {
            "time": "2026-04-11T10:00:02Z",
            "action": "oom",
            "container_id": "c1",
            "container_name": "svc-api",
        }


class _NotifyRouterStub:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str]] = []

    async def send(self, host: str, message: str, category: str) -> bool:
        self.calls.append((host, message, category))
        return True


class _TelegramSenderStub:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    async def __call__(self, target: str, message: str, parse_mode: str = "") -> None:
        self.calls.append((target, message))


class _WriterStub:
    def __init__(self) -> None:
        self.alerts: list[dict] = []
        self.mute_checks: list[dict] = []
        self.closed = 0

    async def write_alert(self, payload: dict) -> None:
        self.alerts.append(payload)

    async def find_active_mute(self, **kwargs) -> None:
        self.mute_checks.append(dict(kwargs))
        return None

    async def close(self) -> None:
        self.closed += 1


@pytest.mark.asyncio
async def test_app_lifespan_wires_watch_manager_without_breaking_existing_jobs() -> (
    None
):
    lifecycle: list[str] = []
    app = create_app(
        hosts=[{"name": "local", "url": "unix:///var/run/docker.sock"}],
        docker_client_pool=cast(Any, _DockerPoolStub(lifecycle)),
        metrics_scheduler=cast(Any, _MetricsSchedulerStub(lifecycle)),
        retention_scheduler=cast(Any, _RetentionSchedulerStub(lifecycle)),
        watch_manager=cast(Any, _WatchManagerStub(lifecycle)),
    )

    async with app.router.lifespan_context(app):
        assert lifecycle == [
            "host:connect",
            "metrics:start",
            "retention:start",
            "watch:start",
        ]

    assert lifecycle == [
        "host:connect",
        "metrics:start",
        "retention:start",
        "watch:start",
        "watch:stop",
        "retention:stop",
        "metrics:stop",
        "docker:close",
    ]


@pytest.mark.asyncio
async def test_create_app_event_wiring_uses_notify_and_persistence_dependencies() -> (
    None
):
    notify_router = _NotifyRouterStub()
    pool = _StreamingDockerPoolStub()
    sync_conn = sqlite3.connect(":memory:")
    wrapped_conn = _SyncAioSqliteConn(sync_conn)

    def connect_db(_db_path: str) -> _SyncAioSqliteConn:
        return wrapped_conn

    app = create_app(
        hosts=[
            {
                "name": "host-a",
                "url": "unix:///var/run/docker.sock",
                "prompt_template": "default_alert",
                "rules": {"custom_alerts": [{"pattern": "OOM", "category": "OOM"}]},
            }
        ],
        docker_client_pool=cast(Any, pool),
        notify_router=cast(Any, notify_router),
        metrics_db_path=":memory:",
        metrics_db_connect=connect_db,
        enable_metrics_scheduler=False,
        enable_retention_cleanup=False,
    )

    async with app.router.lifespan_context(app):
        payload: dict[str, Any] | None = None
        for _ in range(100):
            alerts = await app.state.metrics_writer.list_alerts(limit=10)
            payload = next(
                (
                    row["payload"]
                    for row in alerts
                    if row["payload"]["category"] == "OOM"
                ),
                None,
            )
            if notify_router.calls and payload is not None:
                break
            await asyncio.sleep(0.05)

        assert notify_router.calls
        assert any(
            host == "host-a" and category == "OOM"
            for host, _message, category in notify_router.calls
        ), notify_router.calls
        assert payload is not None
        assert payload["host"] == "host-a"
        assert payload["container_id"] == "c1"
        assert payload["category"] == "OOM"
        assert "svc-api exceeded memory limits" in payload["line"]


@pytest.mark.asyncio
async def test_create_app_renders_output_template_and_routes_to_host_targets() -> None:
    telegram_sender = _TelegramSenderStub()
    pool = _StreamingDockerPoolStub()
    sync_conn = sqlite3.connect(":memory:")
    wrapped_conn = _SyncAioSqliteConn(sync_conn)

    def connect_db(_db_path: str) -> _SyncAioSqliteConn:
        return wrapped_conn

    app = create_app(
        app_config={
            "defaults": {
                "notify": {
                    "telegram": {"enabled": True, "chat_ids": ["default-tg"]},
                    "wechat": {"enabled": False},
                },
                "output_template": "standard",
            },
            "hosts": [
                {
                    "name": "host-a",
                    "url": "unix:///var/run/docker.sock",
                    "prompt_template": "default_alert",
                    "output_template": "business",
                    "notify": {"telegram": {"chat_ids": ["host-tg"]}},
                    "rules": {"custom_alerts": [{"pattern": "OOM", "category": "OOM"}]},
                }
            ],
        },
        docker_client_pool=cast(Any, pool),
        telegram_send_func=telegram_sender,
        metrics_db_path=":memory:",
        metrics_db_connect=connect_db,
        enable_metrics_scheduler=False,
        enable_retention_cleanup=False,
    )

    async with app.router.lifespan_context(app):
        for _ in range(200):
            if any("svc-api" in message for _target, message in telegram_sender.calls):
                break
            await asyncio.sleep(0.05)

        assert telegram_sender.calls
        oom_calls = [
            (target, message)
            for target, message in telegram_sender.calls
            if "svc-api" in message
        ]
        assert oom_calls
        assert {target for target, _message in oom_calls} == {"host-tg"}
        assert all(target != "default-tg" for target, _message in telegram_sender.calls)
        assert any(
            "📊 业务告警：svc-api" in message
            for _target, message in oom_calls
        )
        assert any(
            "技术摘要：" in message for _target, message in oom_calls
        )


@pytest.mark.asyncio
async def test_create_app_host_status_handler_drives_watch_manager_dynamically() -> (
    None
):
    calls: list[tuple[str, str]] = []

    class _WatchManagerDynamicStub:
        async def start(self) -> None:
            calls.append(("watch", "start"))

        async def shutdown(self) -> None:
            calls.append(("watch", "stop"))

        async def handle_host_status_change(self, payload: dict) -> None:
            calls.append(
                (
                    str(payload["name"]),
                    f"{payload['old_status']}->{payload['new_status']}",
                )
            )

    state = {"up": False}

    async def connector(_host: dict) -> dict:
        if not state["up"]:
            raise RuntimeError("down")
        return {"server_version": "24.0.7"}

    manager = HostManager(
        [{"name": "host-a", "url": "unix:///var/run/docker.sock"}],
        connector=connector,
        max_retries=1,
    )
    app = create_app(
        host_manager=manager,
        watch_manager=cast(Any, _WatchManagerDynamicStub()),
        enable_metrics_scheduler=False,
        enable_retention_cleanup=False,
    )

    state["up"] = True
    await app.state.host_manager.startup_check()

    assert ("host-a", "disconnected->connected") in calls
