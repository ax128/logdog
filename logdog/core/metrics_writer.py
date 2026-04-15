from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable, Callable, Iterable
from typing import Any

from logdog.core.db import (
    apply_sqlite_pragmas,
    delete_mute,
    find_active_mute,
    init_db,
    insert_alert,
    insert_host_metric_sample,
    insert_metric_samples,
    insert_audit,
    insert_metric_sample,
    insert_mute,
    insert_send_failed,
    insert_storm_event,
    query_alerts,
    query_audit,
    query_host_metrics,
    query_host_container_latest_metrics,
    query_metrics,
    query_mutes,
    query_send_failed,
    query_storm_event_stats,
    query_storm_events,
    run_retention_cleanup,
)


ConnectFn = Callable[[str], Awaitable[Any] | Any]
InitDbFn = Callable[[Any], Awaitable[None] | None]
InsertMetricFn = Callable[[Any, dict[str, Any]], Awaitable[None] | None]
InsertMetricSamplesFn = Callable[[Any, list[dict[str, Any]]], Awaitable[None] | None]
InsertSendFailedFn = Callable[[Any, dict[str, Any]], Awaitable[None] | None]
QuerySendFailedFn = Callable[
    ..., Awaitable[list[dict[str, Any]]] | list[dict[str, Any]]
]
InsertStormEventFn = Callable[[Any, dict[str, Any]], Awaitable[None] | None]
QueryStormEventsFn = Callable[
    ..., Awaitable[list[dict[str, Any]]] | list[dict[str, Any]]
]
QueryStormEventStatsFn = Callable[..., Awaitable[dict[str, Any]] | dict[str, Any]]
InsertAlertFn = Callable[[Any, dict[str, Any]], Awaitable[None] | None]
QueryAlertsFn = Callable[..., Awaitable[list[dict[str, Any]]] | list[dict[str, Any]]]
InsertAuditFn = Callable[..., Awaitable[None] | None]
QueryAuditFn = Callable[..., Awaitable[list[dict[str, Any]]] | list[dict[str, Any]]]
InsertMuteFn = Callable[[Any, dict[str, Any]], Awaitable[None] | None]
DeleteMuteFn = Callable[..., Awaitable[int] | int]
QueryMutesFn = Callable[..., Awaitable[list[dict[str, Any]]] | list[dict[str, Any]]]
QueryMetricsFn = Callable[..., Awaitable[list[dict[str, Any]]] | list[dict[str, Any]]]
QueryHostContainerLatestMetricsFn = Callable[
    ..., Awaitable[list[dict[str, Any]]] | list[dict[str, Any]]
]
InsertHostMetricFn = Callable[[Any, dict[str, Any]], Awaitable[None] | None]
QueryHostMetricsFn = Callable[
    ..., Awaitable[list[dict[str, Any]]] | list[dict[str, Any]]
]
FindActiveMuteFn = Callable[
    ..., Awaitable[dict[str, Any] | None] | dict[str, Any] | None
]
ApplySqlitePragmasFn = Callable[..., Awaitable[None] | None]


class MetricsSqliteWriter:
    def __init__(
        self,
        *,
        db_path: str,
        db_connect: ConnectFn | None = None,
        init_db_fn: InitDbFn = init_db,
        insert_metric_fn: InsertMetricFn = insert_metric_sample,
        insert_metric_samples_fn: InsertMetricSamplesFn = insert_metric_samples,
        insert_send_failed_fn: InsertSendFailedFn = insert_send_failed,
        query_send_failed_fn: QuerySendFailedFn = query_send_failed,
        insert_storm_event_fn: InsertStormEventFn = insert_storm_event,
        query_storm_events_fn: QueryStormEventsFn = query_storm_events,
        query_storm_event_stats_fn: QueryStormEventStatsFn = query_storm_event_stats,
        insert_alert_fn: InsertAlertFn = insert_alert,
        query_alerts_fn: QueryAlertsFn = query_alerts,
        insert_audit_fn: InsertAuditFn = insert_audit,
        query_audit_fn: QueryAuditFn = query_audit,
        insert_mute_fn: InsertMuteFn = insert_mute,
        delete_mute_fn: DeleteMuteFn = delete_mute,
        query_mutes_fn: QueryMutesFn = query_mutes,
        query_metrics_fn: QueryMetricsFn = query_metrics,
        query_host_container_latest_metrics_fn: QueryHostContainerLatestMetricsFn = query_host_container_latest_metrics,
        insert_host_metric_fn: InsertHostMetricFn = insert_host_metric_sample,
        query_host_metrics_fn: QueryHostMetricsFn = query_host_metrics,
        find_active_mute_fn: FindActiveMuteFn = find_active_mute,
        apply_sqlite_pragmas_fn: ApplySqlitePragmasFn = apply_sqlite_pragmas,
        sqlite_journal_mode: str = "wal",
        sqlite_synchronous: str = "normal",
        sqlite_busy_timeout_ms: int = 5000,
    ) -> None:
        self._db_path = db_path
        self._db_connect = db_connect
        self._init_db_fn = init_db_fn
        self._insert_metric_fn = insert_metric_fn
        self._insert_metric_samples_fn = insert_metric_samples_fn
        self._insert_send_failed_fn = insert_send_failed_fn
        self._query_send_failed_fn = query_send_failed_fn
        self._insert_storm_event_fn = insert_storm_event_fn
        self._query_storm_events_fn = query_storm_events_fn
        self._query_storm_event_stats_fn = query_storm_event_stats_fn
        self._insert_alert_fn = insert_alert_fn
        self._query_alerts_fn = query_alerts_fn
        self._insert_audit_fn = insert_audit_fn
        self._query_audit_fn = query_audit_fn
        self._insert_mute_fn = insert_mute_fn
        self._delete_mute_fn = delete_mute_fn
        self._query_mutes_fn = query_mutes_fn
        self._query_metrics_fn = query_metrics_fn
        self._query_host_container_latest_metrics_fn = (
            query_host_container_latest_metrics_fn
        )
        self._insert_host_metric_fn = insert_host_metric_fn
        self._query_host_metrics_fn = query_host_metrics_fn
        self._find_active_mute_fn = find_active_mute_fn
        self._apply_sqlite_pragmas_fn = apply_sqlite_pragmas_fn
        self._sqlite_journal_mode = str(sqlite_journal_mode)
        self._sqlite_synchronous = str(sqlite_synchronous)
        self._sqlite_busy_timeout_ms = int(sqlite_busy_timeout_ms)
        self._conn: Any | None = None
        self._conn_lock = asyncio.Lock()
        self._write_lock = asyncio.Lock()
        self._read_lock = asyncio.Lock()
        self._closed = False

    async def write(self, sample: dict[str, Any]) -> None:
        if self._closed:
            raise RuntimeError("metrics writer is closed")

        async with self._write_lock:
            conn = await self._ensure_conn()
            await _maybe_await(self._insert_metric_fn(conn, sample))

    async def write_many(self, samples: list[dict[str, Any]]) -> None:
        if self._closed:
            raise RuntimeError("metrics writer is closed")
        if not samples:
            return

        async with self._write_lock:
            conn = await self._ensure_conn()
            await _maybe_await(self._insert_metric_samples_fn(conn, list(samples)))

    async def write_host_metric(self, sample: dict[str, Any]) -> None:
        if self._closed:
            raise RuntimeError("metrics writer is closed")

        async with self._write_lock:
            conn = await self._ensure_conn()
            await _maybe_await(self._insert_host_metric_fn(conn, sample))

    async def write_send_failed(self, payload: dict[str, Any]) -> None:
        if self._closed:
            raise RuntimeError("metrics writer is closed")

        async with self._write_lock:
            conn = await self._ensure_conn()
            await _maybe_await(self._insert_send_failed_fn(conn, payload))

    async def write_alert(self, payload: dict[str, Any]) -> None:
        if self._closed:
            raise RuntimeError("metrics writer is closed")

        async with self._write_lock:
            conn = await self._ensure_conn()
            await _maybe_await(self._insert_alert_fn(conn, payload))

    async def write_storm_event(self, payload: dict[str, Any]) -> None:
        if self._closed:
            raise RuntimeError("metrics writer is closed")

        async with self._write_lock:
            conn = await self._ensure_conn()
            await _maybe_await(self._insert_storm_event_fn(conn, payload))

    async def list_alerts(
        self,
        limit: int = 100,
        host: str | None = None,
        container: str | None = None,
        category: str | None = None,
    ) -> list[dict[str, Any]]:
        if self._closed:
            raise RuntimeError("metrics writer is closed")

        async with self._read_lock:
            conn = await self._ensure_conn()
            try:
                rows = await _maybe_await(
                    self._query_alerts_fn(
                        conn,
                        limit=int(limit),
                        host=host,
                        container=container,
                        category=category,
                    )
                )
            except TypeError:
                try:
                    rows = await _maybe_await(
                        self._query_alerts_fn(
                            conn,
                            limit=int(limit),
                            host=host,
                            container=container,
                        )
                    )
                except TypeError:
                    rows = await _maybe_await(
                        self._query_alerts_fn(conn, limit=int(limit))
                    )
            return list(rows)

    async def list_storm_events(
        self,
        *,
        limit: int = 100,
        phase: str | None = None,
        category: str | None = None,
    ) -> list[dict[str, Any]]:
        if self._closed:
            raise RuntimeError("metrics writer is closed")

        async with self._read_lock:
            conn = await self._ensure_conn()
            return list(
                await _maybe_await(
                    self._query_storm_events_fn(
                        conn,
                        limit=int(limit),
                        phase=phase,
                        category=category,
                    )
                )
            )

    async def storm_event_stats(
        self,
        *,
        start_time: str | None = None,
        end_time: str | None = None,
    ) -> dict[str, Any]:
        if self._closed:
            raise RuntimeError("metrics writer is closed")

        async with self._read_lock:
            conn = await self._ensure_conn()
            return dict(
                await _maybe_await(
                    self._query_storm_event_stats_fn(
                        conn,
                        start_time=start_time,
                        end_time=end_time,
                    )
                )
            )

    async def write_audit(
        self,
        payload: dict[str, Any],
        *,
        redact_patterns: Iterable[str] = (),
        max_chars: int = 10_000,
    ) -> None:
        if self._closed:
            raise RuntimeError("metrics writer is closed")

        async with self._write_lock:
            conn = await self._ensure_conn()
            await _maybe_await(
                self._insert_audit_fn(
                    conn,
                    payload,
                    redact_patterns=tuple(redact_patterns),
                    max_chars=int(max_chars),
                )
            )

    async def list_audit(self, limit: int = 100) -> list[dict[str, Any]]:
        if self._closed:
            raise RuntimeError("metrics writer is closed")

        async with self._read_lock:
            conn = await self._ensure_conn()
            return list(
                await _maybe_await(self._query_audit_fn(conn, limit=int(limit)))
            )

    async def write_mute(self, payload: dict[str, Any]) -> None:
        if self._closed:
            raise RuntimeError("metrics writer is closed")

        async with self._write_lock:
            conn = await self._ensure_conn()
            await _maybe_await(self._insert_mute_fn(conn, payload))

    async def delete_mute(self, *, host: str, container_id: str, category: str) -> int:
        if self._closed:
            raise RuntimeError("metrics writer is closed")

        async with self._write_lock:
            conn = await self._ensure_conn()
            return int(
                await _maybe_await(
                    self._delete_mute_fn(
                        conn,
                        host=str(host),
                        container_id=str(container_id),
                        category=str(category),
                    )
                )
            )

    async def list_mutes(self, limit: int = 100) -> list[dict[str, Any]]:
        if self._closed:
            raise RuntimeError("metrics writer is closed")

        async with self._read_lock:
            conn = await self._ensure_conn()
            return list(
                await _maybe_await(self._query_mutes_fn(conn, limit=int(limit)))
            )

    async def find_active_mute(
        self,
        *,
        host: str,
        container_id: str,
        category: str,
        at_time: str | None = None,
    ) -> dict[str, Any] | None:
        if self._closed:
            raise RuntimeError("metrics writer is closed")

        async with self._read_lock:
            conn = await self._ensure_conn()
            return await _maybe_await(
                self._find_active_mute_fn(
                    conn,
                    host=str(host),
                    container_id=str(container_id),
                    category=str(category),
                    at_time=at_time,
                )
            )

    async def query_metrics(
        self,
        *,
        host_name: str,
        container_id: str,
        start_time: str | None = None,
        end_time: str | None = None,
        limit: int = 5000,
    ) -> list[dict[str, Any]]:
        if self._closed:
            raise RuntimeError("metrics writer is closed")

        async with self._read_lock:
            conn = await self._ensure_conn()
            return list(
                await _maybe_await(
                    self._query_metrics_fn(
                        conn,
                        host_name=str(host_name),
                        container_id=str(container_id),
                        start_time=start_time,
                        end_time=end_time,
                        limit=int(limit),
                    )
                )
            )

    async def query_host_metrics(
        self,
        *,
        host_name: str,
        start_time: str | None = None,
        end_time: str | None = None,
        limit: int = 5000,
    ) -> list[dict[str, Any]]:
        if self._closed:
            raise RuntimeError("metrics writer is closed")

        async with self._read_lock:
            conn = await self._ensure_conn()
            return list(
                await _maybe_await(
                    self._query_host_metrics_fn(
                        conn,
                        host_name=str(host_name),
                        start_time=start_time,
                        end_time=end_time,
                        limit=int(limit),
                    )
                )
            )

    async def query_host_container_latest_metrics(
        self,
        *,
        host_name: str,
        start_time: str,
        end_time: str,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        if self._closed:
            raise RuntimeError("metrics writer is closed")

        async with self._read_lock:
            conn = await self._ensure_conn()
            return list(
                await _maybe_await(
                    self._query_host_container_latest_metrics_fn(
                        conn,
                        host_name=str(host_name),
                        start_time=str(start_time),
                        end_time=str(end_time),
                        limit=int(limit),
                    )
                )
            )

    async def list_send_failed(self, limit: int = 100) -> list[dict[str, Any]]:
        if self._closed:
            raise RuntimeError("metrics writer is closed")

        async with self._read_lock:
            conn = await self._ensure_conn()
            return list(
                await _maybe_await(self._query_send_failed_fn(conn, limit=int(limit)))
            )

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True

        conn = self._conn
        if conn is None:
            return
        self._conn = None
        await _close_connection(conn)

    async def cleanup(self, retention: dict[str, int]) -> None:
        if self._closed:
            raise RuntimeError("metrics writer is closed")

        async with self._write_lock:
            conn = await self._ensure_conn()
            await _maybe_await(run_retention_cleanup(conn, retention))

    async def _ensure_conn(self) -> Any:
        if self._conn is not None:
            return self._conn
        async with self._conn_lock:
            if self._conn is not None:
                return self._conn

            conn = await self._open_connection()
            await _maybe_await(self._init_db_fn(conn))
            if hasattr(conn, "execute"):
                await _maybe_await(
                    self._apply_sqlite_pragmas_fn(
                        conn,
                        journal_mode=self._sqlite_journal_mode,
                        synchronous=self._sqlite_synchronous,
                        busy_timeout_ms=self._sqlite_busy_timeout_ms,
                    )
                )
            self._conn = conn
            return conn

    async def _open_connection(self) -> Any:
        if self._db_connect is not None:
            return await _maybe_await(self._db_connect(self._db_path))

        import aiosqlite

        db_dir = os.path.dirname(self._db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        return await aiosqlite.connect(self._db_path)


async def _maybe_await(value: Any) -> Any:
    if asyncio.isfuture(value) or hasattr(value, "__await__"):
        return await value
    return value


async def _close_connection(conn: Any) -> None:
    close_fn = getattr(conn, "close", None)
    if close_fn is None:
        return
    await _maybe_await(close_fn())
