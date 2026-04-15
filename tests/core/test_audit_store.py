from __future__ import annotations

import sqlite3

import aiosqlite
import pytest

from logdog.core.db import init_db, insert_audit, query_audit


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
async def test_insert_and_query_audit_redacts_string_fields_recursively(
    patch_aiosqlite_connect,
) -> None:
    async with aiosqlite.connect(":memory:") as conn:
        await init_db(conn)
        await insert_audit(
            conn,
            {
                "event": "notify_failed",
                "message": "Authorization: Bearer SECRET_TOKEN",
                "nested": {
                    "detail": "password=supersecret and more",
                },
            },
            redact_patterns=[
                r"(?i)bearer\s+[^\s]+",
                r"(?i)\bpassword\b\s*[:=]\s*[^\s]+",
            ],
            max_chars=18,
        )

        rows = await query_audit(conn, limit=10)

    assert rows[0]["payload"]["event"] == "notify_failed"
    assert rows[0]["payload"]["message"] == "Authorization: ***"
    assert rows[0]["payload"]["nested"]["detail"] == "***REDACTED*** and"


@pytest.mark.asyncio
async def test_query_audit_rejects_non_positive_limit(patch_aiosqlite_connect) -> None:
    async with aiosqlite.connect(":memory:") as conn:
        await init_db(conn)
        with pytest.raises(ValueError, match="limit must be > 0"):
            await query_audit(conn, limit=0)
