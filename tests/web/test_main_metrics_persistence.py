from __future__ import annotations

import asyncio
import json
import sqlite3

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


class _NotifyRouterStub:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str]] = []

    async def send(self, host: str, message: str, category: str) -> bool:
        self.calls.append((host, message, category))
        return True


def test_main_metrics_scheduler_persists_samples_to_default_sqlite_db() -> None:
    sync_conn = sqlite3.connect(":memory:")
    wrapped_conn = _SyncAioSqliteConn(sync_conn)
    connect_calls: list[str] = []

    async def connector(_host: dict) -> dict:
        return {"server_version": "24.0.7"}

    async def list_containers(_host_name: str) -> list[dict]:
        return [{"id": "c1", "name": "svc", "status": "running", "restart_count": 0}]

    async def fetch_stats(_host_name: str, _container: dict) -> dict:
        return {
            "cpu_stats": {
                "cpu_usage": {"total_usage": 300},
                "system_cpu_usage": 1400,
                "online_cpus": 2,
            },
            "memory_stats": {"usage": 256, "limit": 1024},
            "networks": {"eth0": {"rx_bytes": 10, "tx_bytes": 20}},
            "blkio_stats": {"io_service_bytes_recursive": []},
        }

    def connect_db(db_path: str) -> _SyncAioSqliteConn:
        connect_calls.append(db_path)
        return wrapped_conn

    async def run_case() -> None:
        app = create_app(
            hosts=[{"name": "h1", "url": "unix:///var/run/docker.sock"}],
            host_connector=connector,
            list_containers_fn=list_containers,
            fetch_stats_fn=fetch_stats,
            metrics_db_path=":memory:",
            metrics_db_connect=connect_db,
        )
        await app.state.host_manager.startup_check()
        written = await app.state.metrics_scheduler.run_cycle_once()
        assert written == 1

    asyncio.run(run_case())

    assert connect_calls == [":memory:"]

    try:
        (count,) = sync_conn.execute("SELECT COUNT(*) FROM metrics;").fetchone()
    finally:
        sync_conn.close()
    assert count == 1


def test_main_metrics_scheduler_uses_env_db_path_when_not_explicit(monkeypatch) -> None:
    sync_conn = sqlite3.connect(":memory:")
    wrapped_conn = _SyncAioSqliteConn(sync_conn)
    connect_calls: list[str] = []
    monkeypatch.setenv("LOGDOG_METRICS_DB_PATH", "custom-metrics.db")

    async def connector(_host: dict) -> dict:
        return {"server_version": "24.0.7"}

    async def list_containers(_host_name: str) -> list[dict]:
        return [{"id": "c1", "name": "svc", "status": "running", "restart_count": 0}]

    async def fetch_stats(_host_name: str, _container: dict) -> dict:
        return {
            "cpu_stats": {
                "cpu_usage": {"total_usage": 300},
                "system_cpu_usage": 1400,
                "online_cpus": 2,
            },
            "memory_stats": {"usage": 256, "limit": 1024},
            "networks": {"eth0": {"rx_bytes": 10, "tx_bytes": 20}},
            "blkio_stats": {"io_service_bytes_recursive": []},
        }

    def connect_db(db_path: str) -> _SyncAioSqliteConn:
        connect_calls.append(db_path)
        return wrapped_conn

    async def run_case() -> None:
        app = create_app(
            hosts=[{"name": "h1", "url": "unix:///var/run/docker.sock"}],
            host_connector=connector,
            list_containers_fn=list_containers,
            fetch_stats_fn=fetch_stats,
            metrics_db_connect=connect_db,
            app_config={"storage": {"db_path": "from-config.db"}},
        )
        await app.state.host_manager.startup_check()
        written = await app.state.metrics_scheduler.run_cycle_once()
        assert written == 1

    asyncio.run(run_case())

    try:
        (count,) = sync_conn.execute("SELECT COUNT(*) FROM metrics;").fetchone()
    finally:
        sync_conn.close()

    assert count == 1
    assert connect_calls == ["custom-metrics.db"]


def test_main_metrics_scheduler_uses_app_config_db_path_when_no_env_and_no_explicit(
    monkeypatch,
) -> None:
    sync_conn = sqlite3.connect(":memory:")
    wrapped_conn = _SyncAioSqliteConn(sync_conn)
    connect_calls: list[str] = []
    monkeypatch.delenv("LOGDOG_METRICS_DB_PATH", raising=False)

    async def connector(_host: dict) -> dict:
        return {"server_version": "24.0.7"}

    async def list_containers(_host_name: str) -> list[dict]:
        return [{"id": "c1", "name": "svc", "status": "running", "restart_count": 0}]

    async def fetch_stats(_host_name: str, _container: dict) -> dict:
        return {
            "cpu_stats": {
                "cpu_usage": {"total_usage": 300},
                "system_cpu_usage": 1400,
                "online_cpus": 2,
            },
            "memory_stats": {"usage": 256, "limit": 1024},
            "networks": {"eth0": {"rx_bytes": 10, "tx_bytes": 20}},
            "blkio_stats": {"io_service_bytes_recursive": []},
        }

    def connect_db(db_path: str) -> _SyncAioSqliteConn:
        connect_calls.append(db_path)
        return wrapped_conn

    async def run_case() -> None:
        app = create_app(
            hosts=[{"name": "h1", "url": "unix:///var/run/docker.sock"}],
            host_connector=connector,
            list_containers_fn=list_containers,
            fetch_stats_fn=fetch_stats,
            metrics_db_connect=connect_db,
            app_config={"storage": {"db_path": "from-config.db"}},
        )
        await app.state.host_manager.startup_check()
        written = await app.state.metrics_scheduler.run_cycle_once()
        assert written == 1

    asyncio.run(run_case())

    try:
        (count,) = sync_conn.execute("SELECT COUNT(*) FROM metrics;").fetchone()
    finally:
        sync_conn.close()

    assert count == 1
    assert connect_calls == ["from-config.db"]


def test_main_host_metrics_scheduler_persists_samples_when_enabled() -> None:
    sync_conn = sqlite3.connect(":memory:")
    wrapped_conn = _SyncAioSqliteConn(sync_conn)

    async def connector(_host: dict) -> dict:
        return {"server_version": "24.0.7"}

    def connect_db(_db_path: str) -> _SyncAioSqliteConn:
        return wrapped_conn

    async def collect_host_metrics(
        _host_cfg: dict,
        *,
        collect_load: bool,
        collect_network: bool,
        timeout_seconds: int,
    ) -> dict:
        assert collect_load is True
        assert collect_network is True
        assert timeout_seconds > 0
        return {
            "timestamp": "2026-04-12T10:30:00+00:00",
            "cpu_percent": 70.5,
            "load_1": 0.5,
            "load_5": 0.4,
            "load_15": 0.3,
            "mem_total": 1000,
            "mem_used": 600,
            "mem_available": 400,
            "disk_root_total": 2000,
            "disk_root_used": 1500,
            "disk_root_free": 500,
            "net_rx": 10,
            "net_tx": 20,
            "source": "local",
        }

    async def run_case() -> None:
        app = create_app(
            hosts=[{"name": "h1", "url": "unix:///var/run/docker.sock"}],
            host_connector=connector,
            metrics_db_path=":memory:",
            metrics_db_connect=connect_db,
            enable_metrics_scheduler=False,
            enable_watch_manager=False,
            app_config={"metrics": {"host_system": {"enabled": True}}},
            collect_host_metrics_fn=collect_host_metrics,
        )
        await app.state.host_manager.startup_check()
        assert app.state.host_metrics_scheduler is not None
        written = await app.state.host_metrics_scheduler.run_cycle_once()
        assert written == 1

    asyncio.run(run_case())

    try:
        (count,) = sync_conn.execute("SELECT COUNT(*) FROM host_metrics;").fetchone()
    finally:
        sync_conn.close()
    assert count == 1


def test_main_host_security_check_pushes_and_persists_alert_on_new_issue() -> None:
    sync_conn = sqlite3.connect(":memory:")
    wrapped_conn = _SyncAioSqliteConn(sync_conn)
    router = _NotifyRouterStub()

    async def connector(_host: dict) -> dict:
        return {"server_version": "24.0.7"}

    async def status_sender(_host: str, _message: str, _category: str) -> bool:
        return True

    def connect_db(_db_path: str) -> _SyncAioSqliteConn:
        return wrapped_conn

    async def collect_host_metrics(
        _host_cfg: dict,
        *,
        collect_load: bool,
        collect_network: bool,
        timeout_seconds: int,
    ) -> dict:
        assert collect_load is True
        assert collect_network is True
        assert timeout_seconds > 0
        return {
            "timestamp": "2026-04-12T10:30:00+00:00",
            "cpu_percent": 10.0,
            "load_1": 0.1,
            "load_5": 0.1,
            "load_15": 0.1,
            "mem_total": 1000,
            "mem_used": 990,
            "mem_available": 10,
            "disk_root_total": 2000,
            "disk_root_used": 1990,
            "disk_root_free": 10,
            "net_rx": 10,
            "net_tx": 20,
            "source": "local",
        }

    async def run_case() -> None:
        app = create_app(
            hosts=[{"name": "h1", "url": "unix:///var/run/docker.sock"}],
            host_connector=connector,
            metrics_db_path=":memory:",
            metrics_db_connect=connect_db,
            enable_metrics_scheduler=False,
            enable_watch_manager=False,
            notify_router=router,
            status_notify_sender=status_sender,
            app_config={
                "metrics": {
                    "host_system": {
                        "enabled": True,
                        "security": {"enabled": True, "push_on_issue": True},
                    }
                }
            },
            collect_host_metrics_fn=collect_host_metrics,
        )
        await app.state.host_manager.startup_check()
        assert app.state.host_metrics_scheduler is not None
        written_first = await app.state.host_metrics_scheduler.run_cycle_once()
        written_second = await app.state.host_metrics_scheduler.run_cycle_once()
        assert written_first == 1
        assert written_second == 1

    asyncio.run(run_case())

    try:
        (alert_count,) = sync_conn.execute(
            "SELECT COUNT(*) FROM alerts WHERE category = 'SECURITY_CHECK';"
        ).fetchone()
        row = sync_conn.execute(
            "SELECT payload FROM alerts WHERE category = 'SECURITY_CHECK' ORDER BY id DESC LIMIT 1;"
        ).fetchone()
    finally:
        sync_conn.close()

    assert alert_count == 1
    assert row is not None
    payload = json.loads(row[0])
    assert payload["host"] == "h1"
    assert payload["category"] == "SECURITY_CHECK"
    assert payload["pushed"] is True
    assert payload["security_check"]["ok"] is False
    assert len(router.calls) == 1
    assert router.calls[0][0] == "h1"
    assert router.calls[0][2] == "SECURITY_CHECK"
