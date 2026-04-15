from __future__ import annotations

import json
from copy import deepcopy
from typing import Any

import pytest

from logdog.llm.tool_types import ToolResult
from logdog.llm.tools import create_tool_registry


class _HostManagerStub:
    def __init__(self) -> None:
        self._hosts = {
            "prod-a": {"name": "prod-a", "url": "unix:///var/run/docker.sock"}
        }

    def list_host_statuses(self) -> list[dict[str, Any]]:
        return [
            {
                "name": "prod-a",
                "url": "unix:///var/run/docker.sock",
                "status": "connected",
                "last_connected_at": "2026-04-11T00:00:00+00:00",
                "last_error": None,
                "failure_count": 0,
            }
        ]

    def get_host_config(self, name: str) -> dict[str, Any] | None:
        host = self._hosts.get(name)
        if host is None:
            return None
        return deepcopy(host)


class _MetricsWriterStub:
    def __init__(self) -> None:
        self.audit_calls: list[tuple[dict[str, Any], tuple[str, ...], int]] = []
        self.mute_calls: list[dict[str, Any]] = []
        self.unmute_calls: list[tuple[str, str, str]] = []

    async def write_audit(
        self,
        payload: dict[str, Any],
        *,
        redact_patterns: tuple[str, ...] = (),
        max_chars: int = 10_000,
    ) -> None:
        self.audit_calls.append((payload, redact_patterns, max_chars))

    async def list_alerts(self, limit: int = 100) -> list[dict[str, Any]]:
        return [{"id": 1, "payload": {"category": "error", "limit": limit}}]

    async def write_mute(self, payload: dict[str, Any]) -> None:
        self.mute_calls.append(payload)

    async def delete_mute(self, *, host: str, container_id: str, category: str) -> int:
        self.unmute_calls.append((host, container_id, category))
        return 1


@pytest.mark.asyncio
async def test_tool_registry_invokes_all_task6_tools_and_audits_calls() -> None:
    writer = _MetricsWriterStub()
    host_manager = _HostManagerStub()
    list_container_calls: list[str] = []
    query_log_calls: list[dict[str, Any]] = []
    metric_calls: list[dict[str, Any]] = []
    restart_calls: list[dict[str, Any]] = []

    async def list_containers_fn(host: dict[str, Any]) -> list[dict[str, Any]]:
        list_container_calls.append(host["name"])
        return [{"id": "c1", "name": "api", "status": "running", "restart_count": 2}]

    async def query_logs_fn(
        host: dict[str, Any],
        container: dict[str, Any],
        *,
        since: str,
        until: str | None,
        max_lines: int,
    ) -> list[dict[str, Any]]:
        query_log_calls.append(
            {
                "host": host["name"],
                "container_id": container["id"],
                "since": since,
                "until": until,
                "max_lines": max_lines,
            }
        )
        return [{"timestamp": "2026-04-11 00:00:00", "line": "boom"}]

    async def query_metrics_fn(
        *,
        writer: Any,
        host_name: str,
        container_id: str,
        start_time: str,
        end_time: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        metric_calls.append(
            {
                "writer": writer,
                "host_name": host_name,
                "container_id": container_id,
                "start_time": start_time,
                "end_time": end_time,
                "limit": limit,
            }
        )
        return [{"timestamp": "2026-04-11 00:00:00", "cpu": 12.5}]

    async def restart_container_fn(
        host: dict[str, Any],
        container: dict[str, Any],
        *,
        timeout: int,
    ) -> dict[str, Any]:
        restart_calls.append(
            {"host": host["name"], "container_id": container["id"], "timeout": timeout}
        )
        return {"ok": True, "container_id": container["id"]}

    registry = create_tool_registry(
        host_manager=host_manager,
        metrics_writer_factory=lambda: writer,
        app_config={
            "llm": {
                "permissions": {"dangerous_host_allowlist": ["prod-a"]},
                "tools": {
                    "query_logs": {"max_hours": 6, "max_lines": 50},
                    "rate_limit": {"limit": 20, "window_seconds": 60},
                },
            }
        },
        list_containers_fn=list_containers_fn,
        query_logs_fn=query_logs_fn,
        query_metrics_fn=query_metrics_fn,
        restart_container_fn=restart_container_fn,
    )

    assert set(registry) == {
        "list_hosts",
        "list_containers",
        "query_logs",
        "get_metrics",
        "get_alerts",
        "mute_alert",
        "unmute_alert",
        "restart_container",
        "get_system_metrics",
        "list_alert_mutes",
        "get_storm_events",
        "exec_container",
    }
    assert registry["query_logs"].read_only is True
    assert registry["restart_container"].read_only is False

    hosts = await registry["list_hosts"].invoke(user_id="alice", arguments={})
    containers = await registry["list_containers"].invoke(
        user_id="alice", arguments={"host": "prod-a"}
    )
    logs = await registry["query_logs"].invoke(
        user_id="alice",
        arguments={"host": "prod-a", "container_id": "c1", "hours": 2},
    )
    metrics = await registry["get_metrics"].invoke(
        user_id="alice",
        arguments={"host": "prod-a", "container_id": "c1", "hours": 2, "limit": 25},
    )
    alerts = await registry["get_alerts"].invoke(
        user_id="alice", arguments={"limit": 5}
    )
    muted = await registry["mute_alert"].invoke(
        user_id="alice",
        arguments={
            "host": "prod-a",
            "container_id": "c1",
            "category": "error",
            "hours": 4,
            "reason": "maintenance",
        },
    )
    unmuted = await registry["unmute_alert"].invoke(
        user_id="alice",
        arguments={"host": "prod-a", "container_id": "c1", "category": "error"},
    )
    restarted = await registry["restart_container"].invoke(
        user_id="alice",
        arguments={
            "host": "prod-a",
            "container_id": "c1",
            "timeout": 15,
            "confirmed": True,
        },
    )

    assert isinstance(hosts, ToolResult)
    assert not hosts.is_error
    hosts_data = json.loads(hosts.content)
    assert hosts_data["hosts"][0]["name"] == "prod-a"

    assert isinstance(containers, ToolResult)
    containers_data = json.loads(containers.content)
    assert containers_data["containers"][0]["id"] == "c1"
    assert list_container_calls == ["prod-a", "prod-a", "prod-a"]

    assert isinstance(logs, ToolResult)
    logs_data = json.loads(logs.content)
    assert logs_data["lines"][0]["line"] == "boom"
    assert query_log_calls[0]["max_lines"] == 50

    assert isinstance(metrics, ToolResult)
    metrics_data = json.loads(metrics.content)
    assert metrics_data["points"][0]["cpu"] == 12.5
    assert metric_calls[0]["writer"] is writer

    assert isinstance(alerts, ToolResult)
    alerts_data = json.loads(alerts.content)
    assert alerts_data["alerts"][0]["payload"]["limit"] == 5

    assert isinstance(muted, ToolResult)
    muted_data = json.loads(muted.content)
    assert muted_data["ok"] is True
    assert writer.mute_calls[0]["category"] == "error"

    assert isinstance(unmuted, ToolResult)
    unmuted_data = json.loads(unmuted.content)
    assert unmuted_data == {"ok": True, "deleted": 1}

    assert isinstance(restarted, ToolResult)
    restarted_data = json.loads(restarted.content)
    assert restarted_data["result"]["container_id"] == "c1"
    assert restart_calls == [{"host": "prod-a", "container_id": "c1", "timeout": 15}]
    # 8 original tools were invoked in this test (4 new tools are tested separately)
    assert len(writer.audit_calls) == 8
    invoked_tools = {payload["tool"] for payload, _, _ in writer.audit_calls}
    assert invoked_tools == {
        "list_hosts",
        "list_containers",
        "query_logs",
        "get_metrics",
        "get_alerts",
        "mute_alert",
        "unmute_alert",
        "restart_container",
    }
    assert all(payload["status"] == "ok" for payload, _, _ in writer.audit_calls)
    assert any("bearer" in pattern.lower() for pattern in writer.audit_calls[0][1])


@pytest.mark.asyncio
async def test_tool_registry_rate_limits_per_user_and_audits_rejection() -> None:
    now = [0.0]
    writer = _MetricsWriterStub()

    def now_fn() -> float:
        return now[0]

    registry = create_tool_registry(
        host_manager=_HostManagerStub(),
        metrics_writer_factory=lambda: writer,
        app_config={
            "llm": {"tools": {"rate_limit": {"limit": 1, "window_seconds": 60}}}
        },
        time_fn=now_fn,
    )

    await registry["list_hosts"].invoke(user_id="alice", arguments={})

    with pytest.raises(RuntimeError, match="rate limit"):
        await registry["list_hosts"].invoke(user_id="alice", arguments={})

    assert len(writer.audit_calls) == 2
    assert writer.audit_calls[0][0]["status"] == "ok"
    assert writer.audit_calls[1][0]["status"] == "rate_limited"


@pytest.mark.asyncio
async def test_tool_registry_returns_success_when_audit_write_fails_after_handler() -> None:
    class _AuditFailWriter(_MetricsWriterStub):
        async def write_audit(
            self,
            payload: dict[str, Any],
            *,
            redact_patterns: tuple[str, ...] = (),
            max_chars: int = 10_000,
        ) -> None:
            raise RuntimeError("audit storage unavailable")

    restart_calls: list[dict[str, Any]] = []

    async def restart_container_fn(
        host: dict[str, Any],
        container: dict[str, Any],
        *,
        timeout: int,
    ) -> dict[str, Any]:
        restart_calls.append(
            {"host": host["name"], "container_id": container["id"], "timeout": timeout}
        )
        return {"ok": True, "container_id": container["id"]}

    registry = create_tool_registry(
        host_manager=_HostManagerStub(),
        metrics_writer_factory=lambda: _AuditFailWriter(),
        app_config={
            "llm": {
                "permissions": {"dangerous_host_allowlist": ["prod-a"]},
            }
        },
        restart_container_fn=restart_container_fn,
        list_containers_fn=lambda _host: [
            {"id": "c1", "name": "api", "status": "running", "restart_count": 0}
        ],
    )

    result = await registry["restart_container"].invoke(
        user_id="alice",
        arguments={
            "host": "prod-a",
            "container_id": "c1",
            "timeout": 5,
            "confirmed": True,
        },
    )

    assert isinstance(result, ToolResult)
    result_data = json.loads(result.content)
    assert result_data["ok"] is True
    assert restart_calls == [{"host": "prod-a", "container_id": "c1", "timeout": 5}]
