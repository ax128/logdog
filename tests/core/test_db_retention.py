import sqlite3

import aiosqlite
import pytest

from logdog.core.db import init_db, redact_audit_payload, run_retention_cleanup


class _SyncCursor:
    def __init__(self, cur: sqlite3.Cursor):
        self._cur = cur

    async def fetchone(self):
        return self._cur.fetchone()


class _SyncAioSqliteConn:
    """
    aiosqlite fallback for this sandbox:
    asyncio cross-thread wakeups are not reliable here, so we run sqlite3 sync
    but expose an async-compatible surface for these tests.
    """

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
    # Match the "aiosqlite.connect(':memory:')" call shape used by the spec.
    return _SyncAioSqliteConn(sqlite3.connect(database))


@pytest.fixture
def patch_aiosqlite_connect(monkeypatch: pytest.MonkeyPatch):
    # aiosqlite relies on thread executor; in this sandbox asyncio cross-thread
    # wakeups are unreliable, so patch connect for this test only.
    monkeypatch.setattr(aiosqlite, "connect", _connect_sync)


@pytest.mark.asyncio
async def test_retention_cleanup_deletes_expired_and_keeps_fresh(
    patch_aiosqlite_connect,
) -> None:
    async with aiosqlite.connect(":memory:") as conn:
        await init_db(conn)

        # alerts: retention 7 days
        await conn.execute(
            "INSERT INTO alerts(created_at, payload) VALUES(datetime('now','-8 days'), ?);",
            ("old",),
        )
        await conn.execute(
            "INSERT INTO alerts(created_at, payload) VALUES(datetime('now','-1 days'), ?);",
            ("new",),
        )

        # audit_log: retention 30 days
        await conn.execute(
            "INSERT INTO audit_log(created_at, payload) VALUES(datetime('now','-31 days'), ?);",
            ("old",),
        )
        await conn.execute(
            "INSERT INTO audit_log(created_at, payload) VALUES(datetime('now','-2 days'), ?);",
            ("new",),
        )

        # send_failed: retention 14 days
        await conn.execute(
            "INSERT INTO send_failed(created_at, payload) VALUES(datetime('now','-15 days'), ?);",
            ("old",),
        )
        await conn.execute(
            "INSERT INTO send_failed(created_at, payload) VALUES(datetime('now','-3 days'), ?);",
            ("new",),
        )
        await conn.commit()

        await run_retention_cleanup(
            conn,
            {"alerts_days": 7, "audit_days": 30, "send_failed_days": 14},
        )

        cur = await conn.execute("SELECT COUNT(*) FROM alerts;")
        row = await cur.fetchone()
        assert row is not None
        (alerts_cnt,) = row
        assert alerts_cnt == 1
        cur = await conn.execute("SELECT payload FROM alerts;")
        row = await cur.fetchone()
        assert row is not None
        (alerts_payload,) = row
        assert alerts_payload == "new"

        cur = await conn.execute("SELECT COUNT(*) FROM audit_log;")
        row = await cur.fetchone()
        assert row is not None
        (audit_cnt,) = row
        assert audit_cnt == 1
        cur = await conn.execute("SELECT payload FROM audit_log;")
        row = await cur.fetchone()
        assert row is not None
        (audit_payload,) = row
        assert audit_payload == "new"

        cur = await conn.execute("SELECT COUNT(*) FROM send_failed;")
        row = await cur.fetchone()
        assert row is not None
        (failed_cnt,) = row
        assert failed_cnt == 1
        cur = await conn.execute("SELECT payload FROM send_failed;")
        row = await cur.fetchone()
        assert row is not None
        (failed_payload,) = row
        assert failed_payload == "new"


@pytest.mark.asyncio
async def test_retention_cleanup_handles_legacy_iso_created_at_rows(
    patch_aiosqlite_connect,
) -> None:
    async with aiosqlite.connect(":memory:") as conn:
        await init_db(conn)

        await conn.execute(
            "INSERT INTO alerts(created_at, payload) VALUES(strftime('%Y-%m-%dT%H:%M:%SZ', 'now', '-7 days', '-1 minute'), ?);",
            ("alerts-old",),
        )
        await conn.execute(
            "INSERT INTO alerts(created_at, payload) VALUES(strftime('%Y-%m-%dT%H:%M:%SZ', 'now', '-7 days', '+1 minute'), ?);",
            ("alerts-new",),
        )

        await conn.execute(
            "INSERT INTO audit_log(created_at, payload) VALUES(strftime('%Y-%m-%dT%H:%M:%SZ', 'now', '-30 days', '-1 minute'), ?);",
            ("audit-old",),
        )
        await conn.execute(
            "INSERT INTO audit_log(created_at, payload) VALUES(strftime('%Y-%m-%dT%H:%M:%SZ', 'now', '-30 days', '+1 minute'), ?);",
            ("audit-new",),
        )

        await conn.execute(
            "INSERT INTO send_failed(created_at, payload) VALUES(strftime('%Y-%m-%dT%H:%M:%SZ', 'now', '-14 days', '-1 minute'), ?);",
            ("failed-old",),
        )
        await conn.execute(
            "INSERT INTO send_failed(created_at, payload) VALUES(strftime('%Y-%m-%dT%H:%M:%SZ', 'now', '-14 days', '+1 minute'), ?);",
            ("failed-new",),
        )
        await conn.commit()

        await run_retention_cleanup(
            conn,
            {"alerts_days": 7, "audit_days": 30, "send_failed_days": 14},
        )

        cur = await conn.execute("SELECT payload FROM alerts ORDER BY id ASC;")
        alerts_rows = []
        while True:
            row = await cur.fetchone()
            if row is None:
                break
            alerts_rows.append(row[0])

        cur = await conn.execute("SELECT payload FROM audit_log ORDER BY id ASC;")
        audit_rows = []
        while True:
            row = await cur.fetchone()
            if row is None:
                break
            audit_rows.append(row[0])

        cur = await conn.execute("SELECT payload FROM send_failed ORDER BY id ASC;")
        failed_rows = []
        while True:
            row = await cur.fetchone()
            if row is None:
                break
            failed_rows.append(row[0])

    assert alerts_rows == ["alerts-new"]
    assert audit_rows == ["audit-new"]
    assert failed_rows == ["failed-new"]


@pytest.mark.asyncio
async def test_retention_cleanup_missing_required_key_raises(
    patch_aiosqlite_connect,
) -> None:
    async with aiosqlite.connect(":memory:") as conn:
        await init_db(conn)
        with pytest.raises(ValueError):
            await run_retention_cleanup(conn, {"alerts_days": 7, "audit_days": 30})


@pytest.mark.asyncio
async def test_retention_cleanup_removes_expired_storm_events_when_configured(
    patch_aiosqlite_connect,
) -> None:
    async with aiosqlite.connect(":memory:") as conn:
        await init_db(conn)
        await conn.execute(
            "INSERT INTO storm_events(created_at, host, category, phase, payload) VALUES(datetime('now','-15 days'), ?, ?, ?, ?);",
            ("h1", "ERROR", "start", "old"),
        )
        await conn.execute(
            "INSERT INTO storm_events(created_at, host, category, phase, payload) VALUES(datetime('now','-1 days'), ?, ?, ?, ?);",
            ("h1", "ERROR", "end", "new"),
        )
        await conn.commit()

        await run_retention_cleanup(
            conn,
            {
                "alerts_days": 30,
                "audit_days": 30,
                "send_failed_days": 14,
                "storm_days": 7,
            },
        )

        cur = await conn.execute("SELECT payload FROM storm_events ORDER BY id ASC;")
        rows = []
        while True:
            row = await cur.fetchone()
            if row is None:
                break
            rows.append(row[0])

    assert rows == ["new"]


@pytest.mark.asyncio
async def test_init_db_creates_created_at_indexes(patch_aiosqlite_connect) -> None:
    async with aiosqlite.connect(":memory:") as conn:
        await init_db(conn)
        cursor = await conn.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type='index' AND name IN (
                'idx_alerts_created_at',
                'idx_audit_log_created_at',
                'idx_send_failed_created_at'
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
        "idx_alerts_created_at",
        "idx_audit_log_created_at",
        "idx_send_failed_created_at",
    ]


def test_redact_audit_payload_masks_sensitive_values() -> None:
    payload = (
        "Authorization: Bearer SECRET_TOKEN_123\n"
        "api_key=AKIA-EXAMPLE-KEY\n"
        "password: supersecret\n"
        "ok=true\n"
    )
    patterns = [
        r"(?i)bearer\s+[^\s]+",
        r"(?i)\bapi[_-]?key\b\s*[:=]\s*[^\s]+",
        r"(?i)\bpassword\b\s*[:=]\s*[^\s]+",
    ]

    out = redact_audit_payload(payload, patterns=patterns, max_chars=10_000)
    assert "SECRET_TOKEN_123" not in out
    assert "AKIA-EXAMPLE-KEY" not in out
    assert "supersecret" not in out
    assert "***REDACTED***" in out


def test_redact_audit_payload_truncates_to_max_chars() -> None:
    payload = "password=supersecret " + ("x" * 200)
    patterns = [r"(?i)\bpassword\b\s*[:=]\s*[^\s]+"]

    out = redact_audit_payload(payload, patterns=patterns, max_chars=20)
    assert len(out) == 20
    assert "***REDACTED***" in out


def test_redact_audit_payload_max_chars_non_positive_returns_empty() -> None:
    out = redact_audit_payload("password=supersecret", patterns=[], max_chars=0)
    assert out == ""
