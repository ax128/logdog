from __future__ import annotations

import asyncio
from typing import Any

import pytest

from logdog.core.metrics_writer import MetricsSqliteWriter


class _ConnStub:
    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_metrics_writer_serializes_concurrent_writes_with_single_connect() -> (
    None
):
    conn = _ConnStub()
    connect_calls: list[str] = []
    init_calls: list[Any] = []
    insert_calls: list[dict[str, Any]] = []
    inflight = 0
    max_inflight = 0

    async def connect_db(db_path: str) -> _ConnStub:
        connect_calls.append(db_path)
        await asyncio.sleep(0)
        return conn

    async def init_db_fn(db_conn: Any) -> None:
        init_calls.append(db_conn)
        await asyncio.sleep(0)

    async def insert_metric_fn(_db_conn: Any, sample: dict[str, Any]) -> None:
        nonlocal inflight, max_inflight
        inflight += 1
        if inflight > max_inflight:
            max_inflight = inflight
        await asyncio.sleep(0.01)
        insert_calls.append(sample)
        inflight -= 1

    writer = MetricsSqliteWriter(
        db_path=":memory:",
        db_connect=connect_db,
        init_db_fn=init_db_fn,
        insert_metric_fn=insert_metric_fn,
    )

    async def write_one(i: int) -> None:
        await writer.write(
            {
                "host_name": "h1",
                "container_id": "c1",
                "container_name": "svc",
                "timestamp": f"2026-04-11T00:00:{i:02d}+00:00",
            }
        )

    await asyncio.gather(*[write_one(i) for i in range(8)])
    await writer.close()

    assert connect_calls == [":memory:"]
    assert init_calls == [conn]
    assert len(insert_calls) == 8
    assert max_inflight == 1
    assert conn.closed is True


@pytest.mark.asyncio
async def test_metrics_writer_write_many_metrics_calls_batch_insert_once() -> None:
    calls: list[list[dict[str, Any]]] = []

    async def insert_metric_samples_fn(
        _conn: Any, samples: list[dict[str, Any]]
    ) -> None:
        calls.append(list(samples))

    writer = MetricsSqliteWriter(
        db_path=":memory:",
        db_connect=lambda _p: _ConnStub(),
        init_db_fn=lambda _c: None,
        insert_metric_samples_fn=insert_metric_samples_fn,
    )
    await writer.write_many(
        [
            {
                "host_name": "h1",
                "container_id": "c1",
                "container_name": "api",
                "timestamp": "2026-04-12T00:00:01+00:00",
            },
            {
                "host_name": "h1",
                "container_id": "c2",
                "container_name": "worker",
                "timestamp": "2026-04-12T00:00:02+00:00",
            },
        ]
    )
    await writer.close()

    assert len(calls) == 1
    assert len(calls[0]) == 2


@pytest.mark.asyncio
async def test_metrics_writer_exposes_alert_audit_and_mute_helpers() -> None:
    conn = _ConnStub()
    insert_alert_calls: list[dict[str, Any]] = []
    insert_storm_event_calls: list[dict[str, Any]] = []
    insert_audit_calls: list[tuple[dict[str, Any], tuple[str, ...], int]] = []
    insert_mute_calls: list[dict[str, Any]] = []
    delete_mute_calls: list[tuple[str, str, str]] = []
    insert_host_metric_calls: list[dict[str, Any]] = []

    async def connect_db(db_path: str) -> _ConnStub:
        assert db_path == ":memory:"
        return conn

    async def init_db_fn(_db_conn: Any) -> None:
        return None

    async def insert_alert_fn(_db_conn: Any, payload: dict[str, Any]) -> None:
        insert_alert_calls.append(payload)

    async def query_alerts_fn(_db_conn: Any, *, limit: int) -> list[dict[str, Any]]:
        return [
            {
                "id": 1,
                "created_at": "2026-04-11 00:00:00",
                "payload": {"line": f"limit={limit}"},
            }
        ]

    async def insert_storm_event_fn(_db_conn: Any, payload: dict[str, Any]) -> None:
        insert_storm_event_calls.append(payload)

    async def query_storm_events_fn(
        _db_conn: Any,
        *,
        limit: int,
        phase: str | None,
        category: str | None,
    ) -> list[dict[str, Any]]:
        return [
            {
                "id": 9,
                "created_at": "2026-04-11 00:00:01",
                "phase": phase,
                "category": category,
                "payload": {"limit": limit},
            }
        ]

    async def query_storm_event_stats_fn(
        _db_conn: Any,
        *,
        start_time: str | None,
        end_time: str | None,
    ) -> dict[str, Any]:
        return {
            "total_events": 2,
            "host_count": 1,
            "by_category": [{"category": "ERROR", "count": 2}],
            "by_phase": [{"phase": "start", "count": 1}, {"phase": "end", "count": 1}],
            "start_time": start_time,
            "end_time": end_time,
        }

    async def insert_audit_fn(
        _db_conn: Any,
        payload: dict[str, Any],
        *,
        redact_patterns: tuple[str, ...],
        max_chars: int,
    ) -> None:
        insert_audit_calls.append((payload, redact_patterns, max_chars))

    async def query_audit_fn(_db_conn: Any, *, limit: int) -> list[dict[str, Any]]:
        return [
            {
                "id": 2,
                "created_at": "2026-04-11 00:00:01",
                "payload": {"event": f"limit={limit}"},
            }
        ]

    async def insert_mute_fn(_db_conn: Any, payload: dict[str, Any]) -> None:
        insert_mute_calls.append(payload)

    async def delete_mute_fn(
        _db_conn: Any,
        *,
        host: str,
        container_id: str,
        category: str,
    ) -> int:
        delete_mute_calls.append((host, container_id, category))
        return 1

    async def query_mutes_fn(_db_conn: Any, *, limit: int) -> list[dict[str, Any]]:
        return [
            {
                "id": 3,
                "host": "host-a",
                "container_id": "c1",
                "category": f"limit={limit}",
                "reason": "maintenance",
                "created_at": "2026-04-11 00:00:02",
                "expires_at": "2999-01-01 00:00:00",
            }
        ]

    async def find_active_mute_fn(
        _db_conn: Any,
        *,
        host: str,
        container_id: str,
        category: str,
        at_time: str | None = None,
    ) -> dict[str, Any] | None:
        return {
            "id": 4,
            "host": host,
            "container_id": container_id,
            "category": category,
            "reason": at_time or "active",
            "created_at": "2026-04-11 00:00:03",
            "expires_at": "2999-01-01 00:00:00",
        }

    async def insert_host_metric_fn(_db_conn: Any, payload: dict[str, Any]) -> None:
        insert_host_metric_calls.append(payload)

    async def query_host_metrics_fn(
        _db_conn: Any,
        *,
        host_name: str,
        start_time: str | None,
        end_time: str | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        return [
            {
                "host_name": host_name,
                "timestamp": start_time or "2026-04-11 00:00:00",
                "cpu_percent": 90.0,
                "mem_used": 900,
                "mem_total": 1000,
                "disk_root_used": 800,
                "disk_root_total": 1000,
                "source": "ssh",
                "query_limit": limit,
                "end_time": end_time,
            }
        ]

    writer = MetricsSqliteWriter(
        db_path=":memory:",
        db_connect=connect_db,
        init_db_fn=init_db_fn,
        insert_alert_fn=insert_alert_fn,
        query_alerts_fn=query_alerts_fn,
        insert_storm_event_fn=insert_storm_event_fn,
        query_storm_events_fn=query_storm_events_fn,
        query_storm_event_stats_fn=query_storm_event_stats_fn,
        insert_audit_fn=insert_audit_fn,
        query_audit_fn=query_audit_fn,
        insert_mute_fn=insert_mute_fn,
        delete_mute_fn=delete_mute_fn,
        query_mutes_fn=query_mutes_fn,
        find_active_mute_fn=find_active_mute_fn,
        insert_host_metric_fn=insert_host_metric_fn,
        query_host_metrics_fn=query_host_metrics_fn,
    )

    await writer.write_alert({"line": "boom"})
    alerts = await writer.list_alerts(limit=7)
    await writer.write_storm_event({"category": "ERROR", "storm_phase": "start"})
    storm_events = await writer.list_storm_events(
        limit=4,
        phase="start",
        category="ERROR",
    )
    storm_stats = await writer.storm_event_stats(
        start_time="2026-04-11 00:00:00",
        end_time="2026-04-11 23:59:59",
    )
    await writer.write_audit(
        {"event": "notify"},
        redact_patterns=[r"secret"],
        max_chars=42,
    )
    audit_rows = await writer.list_audit(limit=6)
    await writer.write_mute(
        {
            "host": "host-a",
            "container_id": "c1",
            "category": "error",
            "reason": "maintenance",
            "expires_at": "2999-01-01 00:00:00",
        }
    )
    deleted = await writer.delete_mute(
        host="host-a", container_id="c1", category="error"
    )
    mutes = await writer.list_mutes(limit=5)
    active = await writer.find_active_mute(
        host="host-a",
        container_id="c1",
        category="error",
        at_time="2026-04-11 00:00:00",
    )
    await writer.write_host_metric(
        {
            "host_name": "host-a",
            "timestamp": "2026-04-11 00:00:10",
            "cpu_percent": 70.0,
            "mem_total": 1000,
            "mem_used": 600,
            "mem_available": 400,
            "disk_root_total": 2000,
            "disk_root_used": 1400,
            "disk_root_free": 600,
            "net_rx": 1,
            "net_tx": 2,
            "source": "local",
        }
    )
    host_metrics = await writer.query_host_metrics(
        host_name="host-a",
        start_time="2026-04-11 00:00:00",
        end_time="2026-04-11 00:10:00",
        limit=3,
    )
    await writer.close()

    assert insert_alert_calls == [{"line": "boom"}]
    assert alerts[0]["payload"]["line"] == "limit=7"
    assert insert_storm_event_calls == [{"category": "ERROR", "storm_phase": "start"}]
    assert storm_events[0]["phase"] == "start"
    assert storm_events[0]["category"] == "ERROR"
    assert storm_stats["total_events"] == 2
    assert storm_stats["host_count"] == 1
    assert insert_audit_calls == [({"event": "notify"}, (r"secret",), 42)]
    assert audit_rows[0]["payload"]["event"] == "limit=6"
    assert insert_mute_calls[0]["category"] == "error"
    assert delete_mute_calls == [("host-a", "c1", "error")]
    assert deleted == 1
    assert mutes[0]["category"] == "limit=5"
    assert active is not None
    assert active["reason"] == "2026-04-11 00:00:00"
    assert insert_host_metric_calls[0]["host_name"] == "host-a"
    assert host_metrics[0]["host_name"] == "host-a"
    assert host_metrics[0]["query_limit"] == 3
    assert host_metrics[0]["source"] == "ssh"
