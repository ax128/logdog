from __future__ import annotations

import sqlite3

import aiosqlite
import pytest

from logwatch.core.db import (
    apply_sqlite_pragmas,
    init_db,
    insert_alert,
    insert_host_metric_sample,
    insert_metric_sample,
    query_alerts,
    query_host_container_latest_metrics,
    query_host_metrics,
    query_metrics,
    run_retention_cleanup,
)


class _SyncCursor:
    def __init__(self, cur: sqlite3.Cursor):
        self._cur = cur

    async def fetchone(self):
        return self._cur.fetchone()


class _SyncAioSqliteConn:
    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()
        return False

    async def execute(self, sql: str, params=()):
        cur = self._conn.execute(sql, params)
        return _SyncCursor(cur)

    async def commit(self) -> None:
        self._conn.commit()

    async def close(self) -> None:
        self._conn.close()


def _connect_sync(database: str, *args, **kwargs):
    return _SyncAioSqliteConn(sqlite3.connect(database))


@pytest.fixture
def patch_aiosqlite_connect(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(aiosqlite, "connect", _connect_sync)


@pytest.mark.asyncio
async def test_init_db_creates_metrics_table_and_indexes(
    patch_aiosqlite_connect,
) -> None:
    async with aiosqlite.connect(":memory:") as conn:
        await init_db(conn)

        cursor = await conn.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type='index' AND name IN (
                'idx_alerts_category_created',
                'idx_metrics_query',
                'idx_metrics_query_norm',
                'idx_metrics_cleanup',
                'idx_metrics_cleanup_norm',
                'idx_host_metrics_query',
                'idx_host_metrics_query_norm',
                'idx_host_metrics_cleanup',
                'idx_host_metrics_cleanup_norm'
            )
            ORDER BY name;
            """.strip()
        )
        rows = []
        while True:
            row = await cursor.fetchone()
            if row is None:
                break
            rows.append(row[0])

    assert rows == [
        "idx_alerts_category_created",
        "idx_host_metrics_cleanup",
        "idx_host_metrics_cleanup_norm",
        "idx_host_metrics_query",
        "idx_host_metrics_query_norm",
        "idx_metrics_cleanup",
        "idx_metrics_cleanup_norm",
        "idx_metrics_query",
        "idx_metrics_query_norm",
    ]


@pytest.mark.asyncio
async def test_query_metrics_plan_uses_expression_index_without_temp_sort(
    patch_aiosqlite_connect,
) -> None:
    async with aiosqlite.connect(":memory:") as conn:
        await init_db(conn)
        plan_cursor = await conn.execute(
            """
            EXPLAIN QUERY PLAN
            SELECT host_name, container_id, container_name, timestamp
            FROM metrics
            WHERE host_name = ? AND container_id = ?
              AND COALESCE(datetime(timestamp), timestamp) >= ?
              AND COALESCE(datetime(timestamp), timestamp) <= ?
            ORDER BY COALESCE(datetime(timestamp), timestamp) ASC, rowid ASC
            LIMIT ?;
            """.strip(),
            ("h1", "c1", "2026-04-11 00:00:00", "2026-04-11 01:00:00", 100),
        )
        plan_rows = []
        while True:
            row = await plan_cursor.fetchone()
            if row is None:
                break
            plan_rows.append(tuple(str(item) for item in row))

    assert any("idx_metrics_query_norm" in row[-1] for row in plan_rows)
    assert not any("USE TEMP B-TREE FOR ORDER BY" in row[-1] for row in plan_rows)


@pytest.mark.asyncio
async def test_apply_sqlite_pragmas_sets_expected_values(patch_aiosqlite_connect):
    async with aiosqlite.connect(":memory:") as conn:
        await init_db(conn)
        await apply_sqlite_pragmas(
            conn,
            journal_mode="wal",
            synchronous="normal",
            busy_timeout_ms=5000,
        )
        cur = await conn.execute("PRAGMA busy_timeout;")
        row = await cur.fetchone()
    assert row is not None
    assert row[0] == 5000


@pytest.mark.asyncio
async def test_query_alerts_filters_by_host_container_and_category_columns(
    patch_aiosqlite_connect,
):
    async with aiosqlite.connect(":memory:") as conn:
        await init_db(conn)
        await insert_alert(
            conn,
            {
                "host": "host-a",
                "container_id": "c1",
                "container_name": "api",
                "category": "ERROR",
                "line": "a",
            },
        )
        await insert_alert(
            conn,
            {
                "host": "host-a",
                "container_id": "c1",
                "container_name": "api",
                "category": "WARN",
                "line": "b",
            },
        )
        await insert_alert(
            conn,
            {
                "host": "host-b",
                "container_id": "c1",
                "container_name": "api",
                "category": "ERROR",
                "line": "c",
            },
        )

        rows = await query_alerts(
            conn,
            limit=100,
            host="host-a",
            container="api",
            category="ERROR",
        )

    assert len(rows) == 1
    assert rows[0]["payload"]["line"] == "a"


@pytest.mark.asyncio
async def test_insert_and_query_metrics_points(patch_aiosqlite_connect) -> None:
    async with aiosqlite.connect(":memory:") as conn:
        await init_db(conn)
        await insert_metric_sample(
            conn,
            {
                "host_name": "h1",
                "container_id": "c1",
                "container_name": "svc",
                "timestamp": "2026-04-11T00:00:01+00:00",
                "cpu": 12.5,
                "mem_used": 128,
                "mem_limit": 1024,
                "net_rx": 10,
                "net_tx": 20,
                "disk_read": 1,
                "disk_write": 2,
                "status": "running",
                "restart_count": 3,
            },
        )
        await insert_metric_sample(
            conn,
            {
                "host_name": "h1",
                "container_id": "c1",
                "container_name": "svc",
                "timestamp": "2026-04-11T00:00:02+00:00",
                "cpu": 13.5,
                "mem_used": 129,
                "mem_limit": 1024,
                "net_rx": 11,
                "net_tx": 21,
                "disk_read": 2,
                "disk_write": 3,
                "status": "running",
                "restart_count": 3,
            },
        )

        points = await query_metrics(conn, host_name="h1", container_id="c1")

    assert len(points) == 2
    assert points[0]["timestamp"] == "2026-04-11 00:00:01"
    assert points[1]["cpu"] == 13.5


@pytest.mark.asyncio
async def test_query_host_container_latest_metrics_returns_latest_per_container(
    patch_aiosqlite_connect,
) -> None:
    async with aiosqlite.connect(":memory:") as conn:
        await init_db(conn)
        await insert_metric_sample(
            conn,
            {
                "host_name": "h1",
                "container_id": "c1",
                "container_name": "api",
                "timestamp": "2026-04-11T00:00:01+00:00",
                "cpu": 11,
                "mem_used": 100,
            },
        )
        await insert_metric_sample(
            conn,
            {
                "host_name": "h1",
                "container_id": "c1",
                "container_name": "api",
                "timestamp": "2026-04-11T00:00:02+00:00",
                "cpu": 22,
                "mem_used": 200,
            },
        )
        await insert_metric_sample(
            conn,
            {
                "host_name": "h1",
                "container_id": "c2",
                "container_name": "worker",
                "timestamp": "2026-04-11T00:00:03+00:00",
                "cpu": 33,
                "mem_used": 300,
            },
        )

        rows = await query_host_container_latest_metrics(
            conn,
            host_name="h1",
            start_time="2026-04-11 00:00:00",
            end_time="2026-04-11 00:01:00",
            limit=10,
        )

    assert len(rows) == 2
    by_container = {row["container_id"]: row for row in rows}
    assert by_container["c1"]["cpu"] == 22
    assert by_container["c1"]["mem_used"] == 200
    assert by_container["c2"]["cpu"] == 33


@pytest.mark.asyncio
async def test_insert_and_query_host_metrics_points(patch_aiosqlite_connect) -> None:
    async with aiosqlite.connect(":memory:") as conn:
        await init_db(conn)
        await insert_host_metric_sample(
            conn,
            {
                "host_name": "h1",
                "timestamp": "2026-04-11T00:00:01+00:00",
                "cpu_percent": 78.5,
                "load_1": 1.1,
                "load_5": 1.2,
                "load_15": 1.3,
                "mem_total": 1024,
                "mem_used": 768,
                "mem_available": 256,
                "disk_root_total": 10000,
                "disk_root_used": 7000,
                "disk_root_free": 3000,
                "net_rx": 100,
                "net_tx": 200,
                "source": "local",
            },
        )
        await insert_host_metric_sample(
            conn,
            {
                "host_name": "h1",
                "timestamp": "2026-04-11T00:00:02+00:00",
                "cpu_percent": 80.5,
                "load_1": 1.4,
                "load_5": 1.5,
                "load_15": 1.6,
                "mem_total": 1024,
                "mem_used": 800,
                "mem_available": 224,
                "disk_root_total": 10000,
                "disk_root_used": 7100,
                "disk_root_free": 2900,
                "net_rx": 110,
                "net_tx": 210,
                "source": "ssh",
            },
        )

        points = await query_host_metrics(conn, host_name="h1", limit=10)

    assert len(points) == 2
    assert points[0]["timestamp"] == "2026-04-11 00:00:01"
    assert points[1]["cpu_percent"] == 80.5
    assert points[1]["source"] == "ssh"


@pytest.mark.asyncio
async def test_query_metrics_normalizes_equivalent_iso_time_bounds(
    patch_aiosqlite_connect,
) -> None:
    async with aiosqlite.connect(":memory:") as conn:
        await init_db(conn)
        await insert_metric_sample(
            conn,
            {
                "host_name": "h1",
                "container_id": "c1",
                "container_name": "svc",
                "timestamp": "2026-04-11T00:00:01+00:00",
                "cpu": 12.5,
                "mem_used": 128,
                "mem_limit": 1024,
                "net_rx": 10,
                "net_tx": 20,
                "disk_read": 1,
                "disk_write": 2,
                "status": "running",
                "restart_count": 3,
            },
        )

        points = await query_metrics(
            conn,
            host_name="h1",
            container_id="c1",
            start_time="2026-04-11T00:00:01Z",
            end_time="2026-04-11 00:00:01",
        )

    assert len(points) == 1
    assert points[0]["timestamp"] == "2026-04-11 00:00:01"


@pytest.mark.asyncio
async def test_query_metrics_returns_legacy_iso_rows_with_canonical_bounds(
    patch_aiosqlite_connect,
) -> None:
    async with aiosqlite.connect(":memory:") as conn:
        await init_db(conn)
        await conn.execute(
            """
            INSERT INTO metrics(
                host_name, container_id, container_name, timestamp, cpu, mem_used, mem_limit,
                net_rx, net_tx, disk_read, disk_write, status, restart_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """.strip(),
            (
                "h1",
                "c1",
                "svc",
                "2026-04-11T00:00:01+00:00",
                12.5,
                128,
                1024,
                10,
                20,
                1,
                2,
                "running",
                3,
            ),
        )
        await conn.commit()

        points = await query_metrics(
            conn,
            host_name="h1",
            container_id="c1",
            start_time="2026-04-11 00:00:01",
            end_time="2026-04-11 00:00:01",
        )

    assert len(points) == 1
    assert points[0]["timestamp"] == "2026-04-11 00:00:01"


@pytest.mark.asyncio
async def test_retention_cleanup_handles_legacy_iso_metric_timestamps(
    patch_aiosqlite_connect,
) -> None:
    async with aiosqlite.connect(":memory:") as conn:
        await init_db(conn)
        await conn.execute(
            """
            INSERT INTO metrics(
                host_name, container_id, container_name, timestamp, cpu, mem_used, mem_limit,
                net_rx, net_tx, disk_read, disk_write, status, restart_count
            ) VALUES (?, ?, ?, strftime('%Y-%m-%dT%H:%M:%SZ', 'now', '-8 days'), ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """.strip(),
            ("h1", "c1", "svc", 10.0, 1, 2, 3, 4, 5, 6, "running", 0),
        )
        await conn.execute(
            """
            INSERT INTO metrics(
                host_name, container_id, container_name, timestamp, cpu, mem_used, mem_limit,
                net_rx, net_tx, disk_read, disk_write, status, restart_count
            ) VALUES (?, ?, ?, strftime('%Y-%m-%dT%H:%M:%SZ', 'now', '-1 days'), ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """.strip(),
            ("h1", "c1", "svc", 11.0, 1, 2, 3, 4, 5, 6, "running", 0),
        )
        await conn.commit()

        await run_retention_cleanup(
            conn,
            {
                "alerts_days": 30,
                "audit_days": 30,
                "send_failed_days": 14,
                "metrics_days": 7,
            },
        )

        cur = await conn.execute("SELECT COUNT(*) FROM metrics;")
        row = await cur.fetchone()
        assert row is not None
        (count,) = row

    assert count == 1


@pytest.mark.asyncio
async def test_retention_cleanup_removes_expired_metrics_when_configured(
    patch_aiosqlite_connect,
) -> None:
    async with aiosqlite.connect(":memory:") as conn:
        await init_db(conn)
        await conn.execute(
            """
            INSERT INTO metrics(
                host_name, container_id, container_name, timestamp, cpu, mem_used, mem_limit,
                net_rx, net_tx, disk_read, disk_write, status, restart_count
            ) VALUES (?, ?, ?, datetime('now','-8 days'), ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """.strip(),
            ("h1", "c1", "svc", 10.0, 1, 2, 3, 4, 5, 6, "running", 0),
        )
        await conn.execute(
            """
            INSERT INTO metrics(
                host_name, container_id, container_name, timestamp, cpu, mem_used, mem_limit,
                net_rx, net_tx, disk_read, disk_write, status, restart_count
            ) VALUES (?, ?, ?, datetime('now','-1 days'), ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """.strip(),
            ("h1", "c1", "svc", 11.0, 1, 2, 3, 4, 5, 6, "running", 0),
        )
        await conn.commit()

        await run_retention_cleanup(
            conn,
            {
                "alerts_days": 30,
                "audit_days": 30,
                "send_failed_days": 14,
                "metrics_days": 7,
            },
        )

        cur = await conn.execute("SELECT COUNT(*) FROM metrics;")
        row = await cur.fetchone()
        assert row is not None
        (count,) = row

    assert count == 1


@pytest.mark.asyncio
async def test_retention_cleanup_removes_expired_host_metrics_when_configured(
    patch_aiosqlite_connect,
) -> None:
    async with aiosqlite.connect(":memory:") as conn:
        await init_db(conn)
        await conn.execute(
            """
            INSERT INTO host_metrics(
                host_name, timestamp, cpu_percent, load_1, load_5, load_15,
                mem_total, mem_used, mem_available,
                disk_root_total, disk_root_used, disk_root_free,
                net_rx, net_tx, source
            ) VALUES (
                'h1',
                datetime('now','-8 days'),
                10.0, 0.1, 0.1, 0.1,
                1000, 600, 400,
                2000, 1500, 500,
                1, 2, 'local'
            );
            """.strip()
        )
        await conn.execute(
            """
            INSERT INTO host_metrics(
                host_name, timestamp, cpu_percent, load_1, load_5, load_15,
                mem_total, mem_used, mem_available,
                disk_root_total, disk_root_used, disk_root_free,
                net_rx, net_tx, source
            ) VALUES (
                'h1',
                datetime('now','-1 days'),
                11.0, 0.2, 0.2, 0.2,
                1000, 610, 390,
                2000, 1510, 490,
                3, 4, 'local'
            );
            """.strip()
        )
        await conn.commit()

        await run_retention_cleanup(
            conn,
            {
                "alerts_days": 30,
                "audit_days": 30,
                "send_failed_days": 14,
                "metrics_days": 7,
                "host_metrics_days": 7,
            },
        )

        cur = await conn.execute("SELECT COUNT(*) FROM host_metrics;")
        row = await cur.fetchone()
        assert row is not None
        (count,) = row

    assert count == 1
