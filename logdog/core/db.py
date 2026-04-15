import json
import inspect
import re
from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Any


REDACTED = "***REDACTED***"
_NUMERIC_TIMESTAMP_RE = re.compile(r"^[+-]?\d+(?:\.\d+)?$")


_SQLITE_JOURNAL_MODES = {"delete", "truncate", "persist", "memory", "wal", "off"}
_SQLITE_SYNCHRONOUS_MODES = {"off", "normal", "full", "extra"}


async def init_db(conn: Any) -> None:
    """
    Initialize SQLite schema.

    The spec only requires each table to include a TEXT `created_at` column.
    """
    await conn.execute("PRAGMA foreign_keys = ON;")

    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            host TEXT NOT NULL DEFAULT '',
            container_id TEXT NOT NULL DEFAULT '',
            container_name TEXT NOT NULL DEFAULT '',
            category TEXT NOT NULL DEFAULT '',
            pushed INTEGER NOT NULL DEFAULT 0,
            payload TEXT
        );
        """.strip()
    )
    await _ensure_alerts_columns(conn)
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            payload TEXT
        );
        """.strip()
    )
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS send_failed (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            payload TEXT
        );
        """.strip()
    )
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS storm_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            host TEXT NOT NULL DEFAULT '',
            category TEXT NOT NULL DEFAULT '',
            phase TEXT NOT NULL DEFAULT '',
            payload TEXT
        );
        """.strip()
    )
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS mutes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            host TEXT NOT NULL,
            container_id TEXT NOT NULL,
            category TEXT NOT NULL,
            reason TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            expires_at TEXT NOT NULL,
            payload TEXT
        );
        """.strip()
    )
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            host_name TEXT NOT NULL,
            container_id TEXT NOT NULL,
            container_name TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            cpu REAL NOT NULL DEFAULT 0,
            mem_used INTEGER NOT NULL DEFAULT 0,
            mem_limit INTEGER NOT NULL DEFAULT 0,
            net_rx INTEGER NOT NULL DEFAULT 0,
            net_tx INTEGER NOT NULL DEFAULT 0,
            disk_read INTEGER NOT NULL DEFAULT 0,
            disk_write INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'unknown',
            restart_count INTEGER NOT NULL DEFAULT 0
        );
        """.strip()
    )
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS host_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            host_name TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            cpu_percent REAL NOT NULL DEFAULT 0,
            load_1 REAL NOT NULL DEFAULT 0,
            load_5 REAL NOT NULL DEFAULT 0,
            load_15 REAL NOT NULL DEFAULT 0,
            mem_total INTEGER NOT NULL DEFAULT 0,
            mem_used INTEGER NOT NULL DEFAULT 0,
            mem_available INTEGER NOT NULL DEFAULT 0,
            disk_root_total INTEGER NOT NULL DEFAULT 0,
            disk_root_used INTEGER NOT NULL DEFAULT 0,
            disk_root_free INTEGER NOT NULL DEFAULT 0,
            net_rx INTEGER NOT NULL DEFAULT 0,
            net_tx INTEGER NOT NULL DEFAULT 0,
            source TEXT NOT NULL DEFAULT 'unknown'
        );
        """.strip()
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_alerts_created_at ON alerts(created_at);"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_alerts_host_container_created ON alerts(host, container_id, created_at);"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_alerts_category_created ON alerts(category, created_at);"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_alerts_container_name_created ON alerts(container_name, created_at);"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_audit_log_created_at ON audit_log(created_at);"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_send_failed_created_at ON send_failed(created_at);"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_storm_events_created_at ON storm_events(created_at);"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_storm_events_lookup ON storm_events(category, phase, created_at);"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_mutes_expires_at ON mutes(expires_at);"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_mutes_lookup ON mutes(host, container_id, category, expires_at);"
    )
    metrics_ts_expr = _sqlite_normalized_timestamp_expr("timestamp")
    host_metrics_ts_expr = _sqlite_normalized_timestamp_expr("timestamp")
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_metrics_query ON metrics(host_name, container_id, timestamp);"
    )
    await conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_metrics_query_norm ON metrics(host_name, container_id, {metrics_ts_expr}, id);"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_metrics_cleanup ON metrics(timestamp);"
    )
    await conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_metrics_cleanup_norm ON metrics({metrics_ts_expr});"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_host_metrics_query ON host_metrics(host_name, timestamp);"
    )
    await conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_host_metrics_query_norm ON host_metrics(host_name, {host_metrics_ts_expr}, id);"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_host_metrics_cleanup ON host_metrics(timestamp);"
    )
    await conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_host_metrics_cleanup_norm ON host_metrics({host_metrics_ts_expr});"
    )

    await conn.commit()


async def apply_sqlite_pragmas(
    conn: Any,
    *,
    journal_mode: str,
    synchronous: str,
    busy_timeout_ms: int,
) -> None:
    normalized_journal_mode = _normalize_sqlite_enum(
        journal_mode,
        field_name="journal_mode",
        allowed_values=_SQLITE_JOURNAL_MODES,
    )
    normalized_synchronous = _normalize_sqlite_enum(
        synchronous,
        field_name="synchronous",
        allowed_values=_SQLITE_SYNCHRONOUS_MODES,
    )
    busy_timeout = int(busy_timeout_ms)
    if busy_timeout <= 0:
        raise ValueError("busy_timeout_ms must be > 0")

    await conn.execute(f"PRAGMA journal_mode={normalized_journal_mode.upper()};")
    await conn.execute(f"PRAGMA synchronous={normalized_synchronous.upper()};")
    await conn.execute(f"PRAGMA busy_timeout={busy_timeout};")


async def run_retention_cleanup(conn: Any, retention: dict[str, int]) -> None:
    """
    Delete expired rows based on created_at and retention day windows.

    retention:
      {
        "alerts_days": int,
        "audit_days": int,
        "send_failed_days": int,
        "metrics_days": int,  # optional
        "host_metrics_days": int,  # optional
      }
    """
    required = ("alerts_days", "audit_days", "send_failed_days")
    missing = [k for k in required if k not in retention]
    if missing:
        raise ValueError(f"retention missing keys: {missing}")

    alerts_days = int(retention["alerts_days"])
    audit_days = int(retention["audit_days"])
    send_failed_days = int(retention["send_failed_days"])
    if alerts_days < 0 or audit_days < 0 or send_failed_days < 0:
        raise ValueError("retention days must be >= 0")

    await conn.execute(
        f"DELETE FROM alerts WHERE {_sqlite_normalized_timestamp_expr('created_at')} < datetime('now', ?);",
        (f"-{alerts_days} days",),
    )
    await conn.execute(
        f"DELETE FROM audit_log WHERE {_sqlite_normalized_timestamp_expr('created_at')} < datetime('now', ?);",
        (f"-{audit_days} days",),
    )
    await conn.execute(
        f"DELETE FROM send_failed WHERE {_sqlite_normalized_timestamp_expr('created_at')} < datetime('now', ?);",
        (f"-{send_failed_days} days",),
    )
    await conn.execute(
        f"DELETE FROM mutes WHERE {_sqlite_normalized_timestamp_expr('expires_at')} < datetime('now');"
    )
    metrics_days = retention.get("metrics_days")
    if metrics_days is not None:
        metrics_days = int(metrics_days)
        if metrics_days < 0:
            raise ValueError("metrics_days must be >= 0")
        await conn.execute(
            f"DELETE FROM metrics WHERE {_sqlite_normalized_timestamp_expr('timestamp')} < datetime('now', ?);",
            (f"-{metrics_days} days",),
        )
    host_metrics_days = retention.get("host_metrics_days")
    if host_metrics_days is not None:
        host_metrics_days = int(host_metrics_days)
        if host_metrics_days < 0:
            raise ValueError("host_metrics_days must be >= 0")
        await conn.execute(
            f"DELETE FROM host_metrics WHERE {_sqlite_normalized_timestamp_expr('timestamp')} < datetime('now', ?);",
            (f"-{host_metrics_days} days",),
        )
    storm_days = retention.get("storm_days")
    if storm_days is not None:
        storm_days = int(storm_days)
        if storm_days < 0:
            raise ValueError("storm_days must be >= 0")
        await conn.execute(
            f"DELETE FROM storm_events WHERE {_sqlite_normalized_timestamp_expr('created_at')} < datetime('now', ?);",
            (f"-{storm_days} days",),
        )
    await conn.commit()


async def insert_send_failed(conn: Any, payload: dict[str, Any]) -> None:
    await conn.execute(
        "INSERT INTO send_failed(payload) VALUES (?);",
        (_dump_json(payload),),
    )
    await conn.commit()


async def insert_storm_event(conn: Any, payload: dict[str, Any]) -> None:
    await conn.execute(
        """
        INSERT INTO storm_events(host, category, phase, payload)
        VALUES (?, ?, ?, ?);
        """.strip(),
        (
            str(payload.get("host") or ""),
            str(payload.get("category") or ""),
            str(payload.get("storm_phase") or payload.get("phase") or ""),
            _dump_json(payload),
        ),
    )
    await conn.commit()


async def insert_alert(conn: Any, payload: dict[str, Any]) -> None:
    await conn.execute(
        """
        INSERT INTO alerts(host, container_id, container_name, category, pushed, payload)
        VALUES (?, ?, ?, ?, ?, ?);
        """.strip(),
        (
            str(payload.get("host") or ""),
            str(payload.get("container_id") or ""),
            str(payload.get("container_name") or ""),
            str(payload.get("category") or ""),
            1 if bool(payload.get("pushed")) else 0,
            _dump_json(payload),
        ),
    )
    await conn.commit()


async def query_alerts(
    conn: Any,
    *,
    limit: int = 100,
    host: str | None = None,
    container: str | None = None,
    category: str | None = None,
) -> list[dict[str, Any]]:
    normalized_limit = _normalize_limit(limit)
    where_clauses: list[str] = []
    params: list[Any] = []

    if host is not None and str(host).strip() != "":
        where_clauses.append("host = ?")
        params.append(str(host).strip())
    if container is not None and str(container).strip() != "":
        container_text = str(container).strip()
        where_clauses.append("(container_id = ? OR container_name = ?)")
        params.append(container_text)
        params.append(container_text)
    if category is not None and str(category).strip() != "":
        where_clauses.append("category = ?")
        params.append(str(category).strip())

    where_sql = ""
    if where_clauses:
        where_sql = " WHERE " + " AND ".join(where_clauses)

    params.append(normalized_limit)
    cursor = await conn.execute(
        f"SELECT id, created_at, payload FROM alerts{where_sql} ORDER BY id DESC LIMIT ?;",
        tuple(params),
    )

    out: list[dict[str, Any]] = []
    while True:
        row = await cursor.fetchone()
        if row is None:
            break
        out.append(
            {
                "id": int(row[0]),
                "created_at": str(row[1] or ""),
                "payload": _decode_json_payload(row[2]),
            }
        )
    return out


async def insert_audit(
    conn: Any,
    payload: dict[str, Any],
    *,
    redact_patterns: Iterable[Any] = (),
    max_chars: int = 10_000,
) -> None:
    sanitized = _sanitize_audit_value(
        payload, patterns=tuple(redact_patterns), max_chars=int(max_chars)
    )
    await conn.execute(
        "INSERT INTO audit_log(payload) VALUES (?);",
        (_dump_json(sanitized),),
    )
    await conn.commit()


async def query_audit(
    conn: Any,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    return await _query_json_payload_rows(conn, table="audit_log", limit=limit)


async def query_send_failed(
    conn: Any,
    *,
    limit: int = 100,
) -> list[dict[str, Any]]:
    return await _query_json_payload_rows(conn, table="send_failed", limit=limit)


async def query_storm_events(
    conn: Any,
    *,
    limit: int = 100,
    phase: str | None = None,
    category: str | None = None,
) -> list[dict[str, Any]]:
    normalized_limit = _normalize_limit(limit)
    where_clauses: list[str] = []
    params: list[Any] = []
    if phase is not None and str(phase).strip() != "":
        where_clauses.append("phase = ?")
        params.append(str(phase).strip())
    if category is not None and str(category).strip() != "":
        where_clauses.append("category = ?")
        params.append(str(category).strip())

    where_sql = ""
    if where_clauses:
        where_sql = " WHERE " + " AND ".join(where_clauses)

    params.append(normalized_limit)
    cursor = await conn.execute(
        f"""
        SELECT id, created_at, host, category, phase, payload
        FROM storm_events
        {where_sql}
        ORDER BY id DESC
        LIMIT ?;
        """.strip(),
        tuple(params),
    )
    out: list[dict[str, Any]] = []
    while True:
        row = await cursor.fetchone()
        if row is None:
            break
        out.append(
            {
                "id": int(row[0]),
                "created_at": str(row[1] or ""),
                "host": str(row[2] or ""),
                "category": str(row[3] or ""),
                "phase": str(row[4] or ""),
                "payload": _decode_json_payload(row[5]),
            }
        )
    return out


async def query_storm_event_stats(
    conn: Any,
    *,
    start_time: str | None = None,
    end_time: str | None = None,
) -> dict[str, Any]:
    created_expr = _sqlite_normalized_timestamp_expr("created_at")
    where_clauses: list[str] = []
    params: list[Any] = []
    if start_time is not None and str(start_time).strip() != "":
        where_clauses.append(f"{created_expr} >= ?")
        params.append(_normalize_sqlite_utc_timestamp(start_time))
    if end_time is not None and str(end_time).strip() != "":
        where_clauses.append(f"{created_expr} <= ?")
        params.append(_normalize_sqlite_utc_timestamp(end_time))

    where_sql = ""
    if where_clauses:
        where_sql = " WHERE " + " AND ".join(where_clauses)

    total_cursor = await conn.execute(
        f"SELECT COUNT(*), COUNT(DISTINCT host) FROM storm_events{where_sql};",
        tuple(params),
    )
    total_row = await total_cursor.fetchone()
    total_events = int((total_row or (0, 0))[0])
    host_count = int((total_row or (0, 0))[1])

    by_category_cursor = await conn.execute(
        f"""
        SELECT category, COUNT(*)
        FROM storm_events
        {where_sql}
        GROUP BY category
        ORDER BY COUNT(*) DESC, category ASC;
        """.strip(),
        tuple(params),
    )
    by_category: list[dict[str, Any]] = []
    while True:
        row = await by_category_cursor.fetchone()
        if row is None:
            break
        by_category.append(
            {
                "category": str(row[0] or ""),
                "count": int(row[1] or 0),
            }
        )

    by_phase_cursor = await conn.execute(
        f"""
        SELECT phase, COUNT(*)
        FROM storm_events
        {where_sql}
        GROUP BY phase
        ORDER BY COUNT(*) DESC, phase ASC;
        """.strip(),
        tuple(params),
    )
    by_phase: list[dict[str, Any]] = []
    while True:
        row = await by_phase_cursor.fetchone()
        if row is None:
            break
        by_phase.append(
            {
                "phase": str(row[0] or ""),
                "count": int(row[1] or 0),
            }
        )

    return {
        "total_events": total_events,
        "host_count": host_count,
        "by_category": by_category,
        "by_phase": by_phase,
    }


async def insert_mute(conn: Any, mute: dict[str, Any]) -> None:
    required_fields = ("host", "container_id", "category", "expires_at")
    missing = [key for key in required_fields if key not in mute]
    if missing:
        raise ValueError(f"mute missing keys: {missing}")

    base_keys = {"host", "container_id", "category", "reason", "expires_at"}
    extra_payload = {key: value for key, value in mute.items() if key not in base_keys}
    payload_text = _dump_json(extra_payload) if extra_payload else None

    await conn.execute(
        """
        INSERT INTO mutes(host, container_id, category, reason, expires_at, payload)
        VALUES (?, ?, ?, ?, ?, ?);
        """.strip(),
        (
            str(mute["host"]),
            str(mute["container_id"]),
            str(mute["category"]),
            str(mute.get("reason", "")),
            _normalize_sqlite_utc_timestamp(mute["expires_at"]),
            payload_text,
        ),
    )
    await conn.commit()


async def delete_mute(
    conn: Any,
    *,
    host: str,
    container_id: str,
    category: str,
) -> int:
    await conn.execute(
        "DELETE FROM mutes WHERE host = ? AND container_id = ? AND category = ?;",
        (str(host), str(container_id), str(category)),
    )
    cursor = await conn.execute("SELECT changes();")
    row = await cursor.fetchone()
    await conn.commit()
    return int((row or (0,))[0])


async def query_mutes(
    conn: Any,
    *,
    limit: int = 100,
    active_only: bool = True,
) -> list[dict[str, Any]]:
    normalized_limit = _normalize_limit(limit)
    expires_at_expr = _sqlite_normalized_timestamp_expr("expires_at")
    if active_only:
        cursor = await conn.execute(
            """
            SELECT id, host, container_id, category, reason, created_at, expires_at, payload
            FROM mutes
            WHERE {expires_at_expr} >= ?
            ORDER BY {expires_at_expr} DESC, id DESC
            LIMIT ?;
            """.strip().format(expires_at_expr=expires_at_expr),
            (_utc_now_sqlite_timestamp(), normalized_limit),
        )
    else:
        cursor = await conn.execute(
            """
            SELECT id, host, container_id, category, reason, created_at, expires_at, payload
            FROM mutes
            ORDER BY {expires_at_expr} DESC, id DESC
            LIMIT ?;
            """.strip().format(expires_at_expr=expires_at_expr),
            (normalized_limit,),
        )
    return await _collect_mute_rows(cursor)


async def find_active_mute(
    conn: Any,
    *,
    host: str,
    container_id: str,
    category: str,
    at_time: str | None = None,
) -> dict[str, Any] | None:
    effective_at = (
        _normalize_sqlite_utc_timestamp(at_time)
        if at_time is not None
        else _utc_now_sqlite_timestamp()
    )
    expires_at_expr = _sqlite_normalized_timestamp_expr("expires_at")
    cursor = await conn.execute(
        """
        SELECT id, host, container_id, category, reason, created_at, expires_at, payload
        FROM mutes
        WHERE host = ? AND container_id = ? AND category = ?
          AND {expires_at_expr} >= ?
        ORDER BY {expires_at_expr} DESC, id DESC
        LIMIT 1;
        """.strip().format(expires_at_expr=expires_at_expr),
        (str(host), str(container_id), str(category), effective_at),
    )

    row = await cursor.fetchone()
    if row is None:
        return None
    return _format_mute_row(row)


async def insert_metric_sample(conn: Any, sample: dict[str, Any]) -> None:
    await insert_metric_samples(conn, [sample])


async def insert_metric_samples(conn: Any, samples: list[dict[str, Any]]) -> None:
    if not samples:
        return

    rows = [_to_metric_insert_row(sample) for sample in list(samples)]
    sql = """
    INSERT INTO metrics(
        host_name, container_id, container_name, timestamp, cpu, mem_used, mem_limit,
        net_rx, net_tx, disk_read, disk_write, status, restart_count
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """.strip()

    executemany_fn = getattr(conn, "executemany", None)
    if callable(executemany_fn):
        result = executemany_fn(sql, rows)
        if inspect.isawaitable(result):
            await result
    else:
        for row in rows:
            await conn.execute(sql, row)
    await conn.commit()


async def insert_host_metric_sample(conn: Any, sample: dict[str, Any]) -> None:
    required_fields = ("host_name", "timestamp")
    missing = [key for key in required_fields if key not in sample]
    if missing:
        raise ValueError(f"host metric sample missing keys: {missing}")

    await conn.execute(
        """
        INSERT INTO host_metrics(
            host_name, timestamp, cpu_percent, load_1, load_5, load_15,
            mem_total, mem_used, mem_available,
            disk_root_total, disk_root_used, disk_root_free,
            net_rx, net_tx, source
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """.strip(),
        (
            str(sample["host_name"]),
            _normalize_sqlite_utc_timestamp(sample["timestamp"]),
            float(sample.get("cpu_percent", 0.0)),
            float(sample.get("load_1", 0.0)),
            float(sample.get("load_5", 0.0)),
            float(sample.get("load_15", 0.0)),
            int(sample.get("mem_total", 0)),
            int(sample.get("mem_used", 0)),
            int(sample.get("mem_available", 0)),
            int(sample.get("disk_root_total", 0)),
            int(sample.get("disk_root_used", 0)),
            int(sample.get("disk_root_free", 0)),
            int(sample.get("net_rx", 0)),
            int(sample.get("net_tx", 0)),
            str(sample.get("source", "unknown")),
        ),
    )
    await conn.commit()


async def query_metrics(
    conn: Any,
    *,
    host_name: str,
    container_id: str,
    start_time: str | None = None,
    end_time: str | None = None,
    limit: int = 5000,
) -> list[dict[str, Any]]:
    normalized_limit = int(limit)
    if normalized_limit <= 0:
        raise ValueError("limit must be > 0")

    timestamp_expr = _sqlite_normalized_timestamp_expr("timestamp")
    where_clauses = ["host_name = ?", "container_id = ?"]
    params: list[Any] = [str(host_name), str(container_id)]
    if start_time is not None:
        where_clauses.append(f"{timestamp_expr} >= ?")
        params.append(_normalize_sqlite_utc_timestamp(start_time))
    if end_time is not None:
        where_clauses.append(f"{timestamp_expr} <= ?")
        params.append(_normalize_sqlite_utc_timestamp(end_time))
    params.append(normalized_limit)

    cursor = await conn.execute(
        f"""
        SELECT host_name, container_id, container_name, timestamp, cpu, mem_used, mem_limit,
               net_rx, net_tx, disk_read, disk_write, status, restart_count
        FROM metrics
        WHERE {" AND ".join(where_clauses)}
        ORDER BY {timestamp_expr} ASC, rowid ASC
        LIMIT ?;
        """.strip().format(timestamp_expr=timestamp_expr),
        tuple(params),
    )

    points: list[dict[str, Any]] = []
    while True:
        row = await cursor.fetchone()
        if row is None:
            break
        points.append(
            {
                "host_name": row[0],
                "container_id": row[1],
                "container_name": row[2],
                "timestamp": _normalize_timestamp_text(row[3]),
                "cpu": row[4],
                "mem_used": row[5],
                "mem_limit": row[6],
                "net_rx": row[7],
                "net_tx": row[8],
                "disk_read": row[9],
                "disk_write": row[10],
                "status": row[11],
                "restart_count": row[12],
            }
        )
    return points


async def query_host_container_latest_metrics(
    conn: Any,
    *,
    host_name: str,
    start_time: str,
    end_time: str,
    limit: int = 500,
) -> list[dict[str, Any]]:
    normalized_limit = int(limit)
    if normalized_limit <= 0:
        raise ValueError("limit must be > 0")

    timestamp_expr = _sqlite_normalized_timestamp_expr("timestamp")
    cursor = await conn.execute(
        f"""
        SELECT
            container_id,
            container_name,
            timestamp,
            cpu,
            mem_used,
            mem_limit,
            net_rx,
            net_tx,
            disk_read,
            disk_write,
            status,
            restart_count
        FROM (
            SELECT
                container_id,
                container_name,
                timestamp,
                cpu,
                mem_used,
                mem_limit,
                net_rx,
                net_tx,
                disk_read,
                disk_write,
                status,
                restart_count,
                ROW_NUMBER() OVER (
                    PARTITION BY container_id
                    ORDER BY {timestamp_expr} DESC, id DESC
                ) AS rn
            FROM metrics
            WHERE host_name = ?
              AND {timestamp_expr} >= ?
              AND {timestamp_expr} <= ?
        ) latest
        WHERE rn = 1
        ORDER BY container_name ASC, container_id ASC
        LIMIT ?;
        """.strip().format(timestamp_expr=timestamp_expr),
        (
            str(host_name),
            _normalize_sqlite_utc_timestamp(start_time),
            _normalize_sqlite_utc_timestamp(end_time),
            normalized_limit,
        ),
    )

    out: list[dict[str, Any]] = []
    while True:
        row = await cursor.fetchone()
        if row is None:
            break
        out.append(
            {
                "container_id": str(row[0] or ""),
                "container_name": str(row[1] or ""),
                "timestamp": _normalize_timestamp_text(row[2]),
                "cpu": float(row[3] or 0.0),
                "mem_used": int(row[4] or 0),
                "mem_limit": int(row[5] or 0),
                "net_rx": int(row[6] or 0),
                "net_tx": int(row[7] or 0),
                "disk_read": int(row[8] or 0),
                "disk_write": int(row[9] or 0),
                "status": str(row[10] or "unknown"),
                "restart_count": int(row[11] or 0),
            }
        )
    return out


async def query_host_metrics(
    conn: Any,
    *,
    host_name: str,
    start_time: str | None = None,
    end_time: str | None = None,
    limit: int = 5000,
) -> list[dict[str, Any]]:
    normalized_limit = int(limit)
    if normalized_limit <= 0:
        raise ValueError("limit must be > 0")

    timestamp_expr = _sqlite_normalized_timestamp_expr("timestamp")
    where_clauses = ["host_name = ?"]
    params: list[Any] = [str(host_name)]
    if start_time is not None:
        where_clauses.append(f"{timestamp_expr} >= ?")
        params.append(_normalize_sqlite_utc_timestamp(start_time))
    if end_time is not None:
        where_clauses.append(f"{timestamp_expr} <= ?")
        params.append(_normalize_sqlite_utc_timestamp(end_time))
    params.append(normalized_limit)

    cursor = await conn.execute(
        f"""
        SELECT host_name, timestamp, cpu_percent, load_1, load_5, load_15,
               mem_total, mem_used, mem_available,
               disk_root_total, disk_root_used, disk_root_free,
               net_rx, net_tx, source
        FROM host_metrics
        WHERE {" AND ".join(where_clauses)}
        ORDER BY {timestamp_expr} ASC, rowid ASC
        LIMIT ?;
        """.strip().format(timestamp_expr=timestamp_expr),
        tuple(params),
    )

    points: list[dict[str, Any]] = []
    while True:
        row = await cursor.fetchone()
        if row is None:
            break
        points.append(
            {
                "host_name": str(row[0] or ""),
                "timestamp": _normalize_timestamp_text(row[1]),
                "cpu_percent": float(row[2] or 0.0),
                "load_1": float(row[3] or 0.0),
                "load_5": float(row[4] or 0.0),
                "load_15": float(row[5] or 0.0),
                "mem_total": int(row[6] or 0),
                "mem_used": int(row[7] or 0),
                "mem_available": int(row[8] or 0),
                "disk_root_total": int(row[9] or 0),
                "disk_root_used": int(row[10] or 0),
                "disk_root_free": int(row[11] or 0),
                "net_rx": int(row[12] or 0),
                "net_tx": int(row[13] or 0),
                "source": str(row[14] or ""),
            }
        )
    return points


def redact_audit_payload(text: str, patterns: Iterable[Any], max_chars: int) -> str:
    """
    Redact sensitive text by applying regex patterns in order, then truncate.
    """
    if text is None:
        text = ""
    out = str(text)

    if patterns:
        for pat in patterns:
            out = re.sub(pat, REDACTED, out)

    max_chars_i = int(max_chars)
    if max_chars_i <= 0:
        return ""
    if len(out) > max_chars_i:
        return out[:max_chars_i]
    return out


def _dump_json(payload: Any) -> str:
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=True)


def _normalize_limit(limit: int) -> int:
    normalized_limit = int(limit)
    if normalized_limit <= 0:
        raise ValueError("limit must be > 0")
    return normalized_limit


def _decode_json_payload(raw_payload: Any) -> Any:
    if isinstance(raw_payload, str):
        try:
            return json.loads(raw_payload)
        except Exception:  # noqa: BLE001
            return {"raw": raw_payload}
    return raw_payload


async def _query_json_payload_rows(
    conn: Any,
    *,
    table: str,
    limit: int,
) -> list[dict[str, Any]]:
    normalized_limit = _normalize_limit(limit)
    cursor = await conn.execute(
        f"SELECT id, created_at, payload FROM {table} ORDER BY id DESC LIMIT ?;",
        (normalized_limit,),
    )

    out: list[dict[str, Any]] = []
    while True:
        row = await cursor.fetchone()
        if row is None:
            break
        out.append(
            {
                "id": int(row[0]),
                "created_at": str(row[1] or ""),
                "payload": _decode_json_payload(row[2]),
            }
        )
    return out


def _sanitize_audit_value(
    value: Any, *, patterns: tuple[Any, ...], max_chars: int
) -> Any:
    if isinstance(value, str):
        return redact_audit_payload(value, patterns, max_chars)
    if isinstance(value, dict):
        return {
            str(key): _sanitize_audit_value(
                item, patterns=patterns, max_chars=max_chars
            )
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [
            _sanitize_audit_value(item, patterns=patterns, max_chars=max_chars)
            for item in value
        ]
    return value


async def _collect_mute_rows(cursor: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    while True:
        row = await cursor.fetchone()
        if row is None:
            break
        rows.append(_format_mute_row(row))
    return rows


def _format_mute_row(row: Any) -> dict[str, Any]:
    return {
        "id": int(row[0]),
        "host": str(row[1]),
        "container_id": str(row[2]),
        "category": str(row[3]),
        "reason": str(row[4] or ""),
        "created_at": str(row[5] or ""),
        "expires_at": _normalize_timestamp_text(row[6]),
        "payload": _decode_json_payload(row[7]) if row[7] is not None else None,
    }


def _sqlite_normalized_timestamp_expr(column_name: str) -> str:
    return f"COALESCE(datetime({column_name}), {column_name})"


async def _ensure_alerts_columns(conn: Any) -> None:
    cursor = await conn.execute("PRAGMA table_info(alerts);")
    existing: set[str] = set()
    while True:
        row = await cursor.fetchone()
        if row is None:
            break
        existing.add(str(row[1]))

    required = {
        "host": "TEXT NOT NULL DEFAULT ''",
        "container_id": "TEXT NOT NULL DEFAULT ''",
        "container_name": "TEXT NOT NULL DEFAULT ''",
        "category": "TEXT NOT NULL DEFAULT ''",
        "pushed": "INTEGER NOT NULL DEFAULT 0",
    }
    for column, ddl in required.items():
        if column in existing:
            continue
        await conn.execute(f"ALTER TABLE alerts ADD COLUMN {column} {ddl};")


def _to_metric_insert_row(sample: dict[str, Any]) -> tuple[Any, ...]:
    required_fields = (
        "host_name",
        "container_id",
        "container_name",
        "timestamp",
    )
    missing = [key for key in required_fields if key not in sample]
    if missing:
        raise ValueError(f"metric sample missing keys: {missing}")

    return (
        str(sample["host_name"]),
        str(sample["container_id"]),
        str(sample["container_name"]),
        _normalize_sqlite_utc_timestamp(sample["timestamp"]),
        float(sample.get("cpu", 0.0)),
        int(sample.get("mem_used", 0)),
        int(sample.get("mem_limit", 0)),
        int(sample.get("net_rx", 0)),
        int(sample.get("net_tx", 0)),
        int(sample.get("disk_read", 0)),
        int(sample.get("disk_write", 0)),
        str(sample.get("status", "unknown")),
        int(sample.get("restart_count", 0)),
    )


def _normalize_sqlite_enum(
    value: Any, *, field_name: str, allowed_values: set[str]
) -> str:
    text = str(value or "").strip().lower()
    if text == "":
        raise ValueError(f"{field_name} must not be empty")
    if text not in allowed_values:
        raise ValueError(f"{field_name} must be one of {sorted(allowed_values)}")
    return text


def _normalize_sqlite_utc_timestamp(value: Any) -> str:
    if isinstance(value, bool):
        raise ValueError(f"invalid timestamp: {value}")
    if isinstance(value, (int, float)):
        return _normalize_unix_timestamp(float(value))

    text = str(value).strip()
    if not text:
        raise ValueError("timestamp must not be empty")

    numeric = _try_parse_numeric_timestamp(text)
    if numeric is not None:
        return _normalize_unix_timestamp(numeric)

    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"invalid timestamp: {value}") from exc

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)
    return parsed.strftime("%Y-%m-%d %H:%M:%S")


def _try_parse_numeric_timestamp(value: str) -> float | None:
    text = str(value).strip()
    if text == "":
        return None
    if _NUMERIC_TIMESTAMP_RE.fullmatch(text) is None:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _normalize_unix_timestamp(value: float) -> str:
    epoch_seconds = float(value)
    abs_value = abs(epoch_seconds)
    if abs_value >= 1e17:
        epoch_seconds /= 1_000_000_000.0
    elif abs_value >= 1e14:
        epoch_seconds /= 1_000_000.0
    elif abs_value >= 1e11:
        epoch_seconds /= 1_000.0
    try:
        parsed = datetime.fromtimestamp(epoch_seconds, tz=timezone.utc)
    except (OverflowError, OSError, ValueError) as exc:
        raise ValueError(f"invalid timestamp: {value}") from exc
    return parsed.strftime("%Y-%m-%d %H:%M:%S")


def _utc_now_sqlite_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _normalize_timestamp_text(value: Any) -> str:
    text = str(value or "")
    if not text:
        return ""
    try:
        return _normalize_sqlite_utc_timestamp(text)
    except ValueError:
        return text
