from __future__ import annotations

import sqlite3

import aiosqlite
import pytest

from logwatch.core.db import (
    delete_mute,
    find_active_mute,
    init_db,
    insert_mute,
    query_mutes,
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
async def test_insert_query_and_find_active_mute(patch_aiosqlite_connect) -> None:
    async with aiosqlite.connect(":memory:") as conn:
        await init_db(conn)
        await insert_mute(
            conn,
            {
                "host": "host-a",
                "container_id": "c1",
                "category": "error",
                "reason": "maintenance",
                "expires_at": "2999-01-01 00:00:00",
            },
        )

        rows = await query_mutes(conn, limit=10)
        active = await find_active_mute(
            conn,
            host="host-a",
            container_id="c1",
            category="error",
        )

    assert rows[0]["reason"] == "maintenance"
    assert active is not None
    assert active["expires_at"] == "2999-01-01 00:00:00"


@pytest.mark.asyncio
async def test_query_mutes_excludes_expired_rows_before_cleanup(
    patch_aiosqlite_connect,
) -> None:
    async with aiosqlite.connect(":memory:") as conn:
        await init_db(conn)
        await conn.execute(
            """
            INSERT INTO mutes(host, container_id, category, reason, created_at, expires_at)
            VALUES (?, ?, ?, ?, datetime('now','-2 days'), datetime('now','-1 days'));
            """.strip(),
            ("host-a", "c1", "error", "expired"),
        )
        await conn.execute(
            """
            INSERT INTO mutes(host, container_id, category, reason, created_at, expires_at)
            VALUES (?, ?, ?, ?, datetime('now','-1 days'), datetime('now','+2 days'));
            """.strip(),
            ("host-a", "c1", "warn", "active"),
        )
        await conn.commit()

        rows = await query_mutes(conn, limit=10)

    assert len(rows) == 1
    assert rows[0]["category"] == "warn"


@pytest.mark.asyncio
async def test_insert_mute_normalizes_iso_timestamp_for_find_active_mute(
    patch_aiosqlite_connect,
) -> None:
    async with aiosqlite.connect(":memory:") as conn:
        await init_db(conn)
        await insert_mute(
            conn,
            {
                "host": "host-a",
                "container_id": "c1",
                "category": "error",
                "reason": "maintenance",
                "expires_at": "2999-01-01T08:00:00+08:00",
            },
        )

        rows = await query_mutes(conn, limit=10)
        active = await find_active_mute(
            conn,
            host="host-a",
            container_id="c1",
            category="error",
            at_time="2999-01-01T00:00:00Z",
        )

    assert rows[0]["expires_at"] == "2999-01-01 00:00:00"
    assert active is not None
    assert active["expires_at"] == "2999-01-01 00:00:00"


@pytest.mark.asyncio
async def test_find_active_mute_accepts_unix_timestamp_values(
    patch_aiosqlite_connect,
) -> None:
    async with aiosqlite.connect(":memory:") as conn:
        await init_db(conn)
        await insert_mute(
            conn,
            {
                "host": "host-a",
                "container_id": "c1",
                "category": "error",
                "reason": "maintenance",
                "expires_at": "2999-01-01 00:00:00",
            },
        )

        active_epoch_seconds = await find_active_mute(
            conn,
            host="host-a",
            container_id="c1",
            category="error",
            at_time=1712839200,
        )
        active_epoch_seconds_text = await find_active_mute(
            conn,
            host="host-a",
            container_id="c1",
            category="error",
            at_time="1712839200",
        )
        active_epoch_nanoseconds = await find_active_mute(
            conn,
            host="host-a",
            container_id="c1",
            category="error",
            at_time=1712839200000000000,
        )

    assert active_epoch_seconds is not None
    assert active_epoch_seconds_text is not None
    assert active_epoch_nanoseconds is not None


@pytest.mark.asyncio
async def test_legacy_iso_mute_rows_are_classified_with_canonical_comparisons(
    patch_aiosqlite_connect,
) -> None:
    async with aiosqlite.connect(":memory:") as conn:
        await init_db(conn)
        await conn.execute(
            """
            INSERT INTO mutes(host, container_id, category, reason, created_at, expires_at)
            VALUES (?, ?, ?, ?, datetime('now'), ?);
            """.strip(),
            ("host-a", "c1", "error", "legacy-active", "2999-01-01T08:00:00+08:00"),
        )
        await conn.execute(
            """
            INSERT INTO mutes(host, container_id, category, reason, created_at, expires_at)
            VALUES (?, ?, ?, ?, datetime('now'), ?);
            """.strip(),
            ("host-a", "c1", "warn", "legacy-expired", "2000-01-01T00:00:00Z"),
        )
        await conn.commit()

        rows = await query_mutes(conn, limit=10)
        active = await find_active_mute(
            conn,
            host="host-a",
            container_id="c1",
            category="error",
            at_time="2999-01-01 00:00:00",
        )
        expired = await find_active_mute(
            conn,
            host="host-a",
            container_id="c1",
            category="warn",
            at_time="2999-01-01 00:00:00",
        )

    assert len(rows) == 1
    assert rows[0]["reason"] == "legacy-active"
    assert rows[0]["expires_at"] == "2999-01-01 00:00:00"
    assert active is not None
    assert active["expires_at"] == "2999-01-01 00:00:00"
    assert expired is None


@pytest.mark.asyncio
async def test_retention_cleanup_handles_legacy_iso_mute_expirations(
    patch_aiosqlite_connect,
) -> None:
    async with aiosqlite.connect(":memory:") as conn:
        await init_db(conn)
        await conn.execute(
            """
            INSERT INTO mutes(host, container_id, category, reason, created_at, expires_at)
            VALUES (?, ?, ?, ?, datetime('now','-10 days'), strftime('%Y-%m-%dT%H:%M:%SZ', 'now', '-1 days'));
            """.strip(),
            ("host-a", "c1", "error", "legacy-expired"),
        )
        await conn.execute(
            """
            INSERT INTO mutes(host, container_id, category, reason, created_at, expires_at)
            VALUES (?, ?, ?, ?, datetime('now','-1 days'), strftime('%Y-%m-%dT%H:%M:%SZ', 'now', '+2 days'));
            """.strip(),
            ("host-a", "c1", "warn", "legacy-active"),
        )
        await conn.commit()

        await run_retention_cleanup(
            conn,
            {"alerts_days": 30, "audit_days": 30, "send_failed_days": 14},
        )

        rows = await query_mutes(conn, limit=10, active_only=False)

    assert len(rows) == 1
    assert rows[0]["reason"] == "legacy-active"


@pytest.mark.asyncio
async def test_delete_mute_removes_matching_row(patch_aiosqlite_connect) -> None:
    async with aiosqlite.connect(":memory:") as conn:
        await init_db(conn)
        await insert_mute(
            conn,
            {
                "host": "host-a",
                "container_id": "c1",
                "category": "error",
                "reason": "maintenance",
                "expires_at": "2999-01-01 00:00:00",
            },
        )

        deleted = await delete_mute(
            conn,
            host="host-a",
            container_id="c1",
            category="error",
        )
        rows = await query_mutes(conn, limit=10)

    assert deleted == 1
    assert rows == []


@pytest.mark.asyncio
async def test_retention_cleanup_deletes_expired_mutes_but_keeps_active_ones(
    patch_aiosqlite_connect,
) -> None:
    async with aiosqlite.connect(":memory:") as conn:
        await init_db(conn)
        await conn.execute(
            """
            INSERT INTO mutes(host, container_id, category, reason, created_at, expires_at)
            VALUES (?, ?, ?, ?, datetime('now','-10 days'), datetime('now','-1 days'));
            """.strip(),
            ("host-a", "c1", "error", "expired"),
        )
        await conn.execute(
            """
            INSERT INTO mutes(host, container_id, category, reason, created_at, expires_at)
            VALUES (?, ?, ?, ?, datetime('now','-1 days'), datetime('now','+2 days'));
            """.strip(),
            ("host-a", "c1", "warn", "active"),
        )
        await conn.commit()

        await run_retention_cleanup(
            conn,
            {"alerts_days": 30, "audit_days": 30, "send_failed_days": 14},
        )

        rows = await query_mutes(conn, limit=10)

    assert len(rows) == 1
    assert rows[0]["category"] == "warn"
