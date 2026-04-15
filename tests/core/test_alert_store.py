from __future__ import annotations

import sqlite3

import aiosqlite
import pytest

from logdog.core.db import (
    init_db,
    insert_alert,
    insert_storm_event,
    query_alerts,
    query_storm_event_stats,
    query_storm_events,
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
async def test_insert_and_query_alerts_return_structured_payloads(
    patch_aiosqlite_connect,
) -> None:
    async with aiosqlite.connect(":memory:") as conn:
        await init_db(conn)
        await insert_alert(
            conn,
            {
                "host": "host-a",
                "container_id": "c1",
                "category": "error",
                "line": "boom 1",
                "pushed": True,
            },
        )
        await insert_alert(
            conn,
            {
                "host": "host-a",
                "container_id": "c1",
                "category": "error",
                "line": "boom 2",
                "pushed": False,
            },
        )

        rows = await query_alerts(conn, limit=10)

    assert [row["payload"]["line"] for row in rows] == ["boom 2", "boom 1"]
    assert rows[0]["payload"]["pushed"] is False


@pytest.mark.asyncio
async def test_query_alerts_rejects_non_positive_limit(patch_aiosqlite_connect) -> None:
    async with aiosqlite.connect(":memory:") as conn:
        await init_db(conn)
        with pytest.raises(ValueError, match="limit must be > 0"):
            await query_alerts(conn, limit=0)


@pytest.mark.asyncio
async def test_insert_and_query_storm_events_support_phase_and_category_filters(
    patch_aiosqlite_connect,
) -> None:
    async with aiosqlite.connect(":memory:") as conn:
        await init_db(conn)
        await insert_storm_event(
            conn,
            {
                "host": "h1",
                "category": "ERROR",
                "storm_phase": "start",
                "storm_total": 5,
            },
        )
        await insert_storm_event(
            conn,
            {
                "host": "h1",
                "category": "ERROR",
                "storm_phase": "end",
                "storm_total": 8,
            },
        )
        await insert_storm_event(
            conn,
            {
                "host": "h2",
                "category": "OOM",
                "storm_phase": "start",
                "storm_total": 3,
            },
        )

        filtered = await query_storm_events(
            conn, limit=10, phase="start", category="ERROR"
        )

    assert len(filtered) == 1
    assert filtered[0]["phase"] == "start"
    assert filtered[0]["category"] == "ERROR"


@pytest.mark.asyncio
async def test_query_storm_event_stats_returns_aggregations(
    patch_aiosqlite_connect,
) -> None:
    async with aiosqlite.connect(":memory:") as conn:
        await init_db(conn)
        await insert_storm_event(
            conn,
            {
                "host": "h1",
                "category": "ERROR",
                "storm_phase": "start",
            },
        )
        await insert_storm_event(
            conn,
            {
                "host": "h2",
                "category": "ERROR",
                "storm_phase": "end",
            },
        )
        await insert_storm_event(
            conn,
            {
                "host": "h2",
                "category": "OOM",
                "storm_phase": "start",
            },
        )

        stats = await query_storm_event_stats(conn)

    assert stats["total_events"] == 3
    assert stats["host_count"] == 2
    assert stats["by_category"] == [
        {"category": "ERROR", "count": 2},
        {"category": "OOM", "count": 1},
    ]
    assert stats["by_phase"] == [
        {"phase": "start", "count": 2},
        {"phase": "end", "count": 1},
    ]
