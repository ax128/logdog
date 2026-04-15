"""Tests for the 4 new LLM agent tools: get_system_metrics, list_alert_mutes,
get_storm_events, and exec_container."""
from __future__ import annotations

import json
from copy import deepcopy
from typing import Any

import pytest

from logdog.llm.tool_types import ToolResult
from logdog.llm.tools import create_tool_registry


# ---------------------------------------------------------------------------
# Shared stubs
# ---------------------------------------------------------------------------


class _HostManagerStub:
    def __init__(self) -> None:
        self._hosts = {
            "prod-a": {"name": "prod-a", "url": "unix:///var/run/docker.sock"}
        }

    def list_host_statuses(self) -> list[dict[str, Any]]:
        return [{"name": "prod-a", "url": "unix:///var/run/docker.sock"}]

    def get_host_config(self, name: str) -> dict[str, Any] | None:
        host = self._hosts.get(name)
        return deepcopy(host) if host is not None else None


class _MetricsWriterStub:
    def __init__(self) -> None:
        self.audit_calls: list[dict[str, Any]] = []
        # Data stubs
        self._host_metrics: list[dict[str, Any]] = [
            {"ts": "2026-04-15 00:00:00", "cpu": 42.0, "mem_used": 1024}
        ]
        self._mutes: list[dict[str, Any]] = [
            {
                "host": "prod-a",
                "container_id": "c1",
                "category": "cpu_high",
                "expires_at": "2026-04-16 00:00:00",
            }
        ]
        self._storm_events: list[dict[str, Any]] = [
            {
                "id": 99,
                "category": "cpu_high",
                "phase": "start",
                "ts": "2026-04-15 01:00:00",
            }
        ]

    async def write_audit(
        self,
        payload: dict[str, Any],
        *,
        redact_patterns: tuple[str, ...] = (),
        max_chars: int = 10_000,
    ) -> None:
        self.audit_calls.append(payload)

    async def query_host_metrics(
        self,
        *,
        host_name: str,
        limit: int = 20,
        start_time: str | None = None,
        end_time: str | None = None,
    ) -> list[dict[str, Any]]:
        return list(self._host_metrics[:limit])

    async def list_mutes(self, limit: int = 100) -> list[dict[str, Any]]:
        return list(self._mutes[:limit])

    async def list_storm_events(
        self,
        *,
        limit: int = 100,
        phase: str | None = None,
        category: str | None = None,
    ) -> list[dict[str, Any]]:
        rows = self._storm_events
        if category is not None:
            rows = [r for r in rows if r.get("category") == category]
        return list(rows[:limit])


def _make_registry(
    *,
    writer: _MetricsWriterStub | None = None,
    exec_fn: Any | None = None,
    allow_exec_host: bool = True,
) -> dict[str, Any]:
    w = writer or _MetricsWriterStub()
    containers = [{"id": "c1", "name": "api", "status": "running", "restart_count": 0}]
    cfg: dict[str, Any] = {"llm": {}}
    if allow_exec_host:
        cfg["llm"]["permissions"] = {"dangerous_host_allowlist": ["prod-a"]}

    return create_tool_registry(
        host_manager=_HostManagerStub(),
        metrics_writer_factory=lambda: w,
        app_config=cfg,
        list_containers_fn=lambda _host: containers,
        exec_container_fn=exec_fn,
    )


# ---------------------------------------------------------------------------
# get_system_metrics
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_system_metrics_returns_host_metrics() -> None:
    writer = _MetricsWriterStub()
    registry = _make_registry(writer=writer)

    result = await registry["get_system_metrics"].invoke(
        user_id="alice", arguments={"host_name": "prod-a", "limit": 10}
    )

    assert isinstance(result, ToolResult)
    assert not result.is_error
    data = json.loads(result.content)
    assert data["host_name"] == "prod-a"
    assert data["points"][0]["cpu"] == 42.0


@pytest.mark.asyncio
async def test_get_system_metrics_via_injected_fn() -> None:
    calls: list[dict[str, Any]] = []

    async def query_fn(writer: Any, *, host_name: str, limit: int) -> list[dict[str, Any]]:
        calls.append({"host_name": host_name, "limit": limit})
        return [{"cpu": 99.0}]

    registry = create_tool_registry(
        host_manager=_HostManagerStub(),
        metrics_writer_factory=lambda: _MetricsWriterStub(),
        query_host_metrics_fn=query_fn,
    )

    result = await registry["get_system_metrics"].invoke(
        user_id="alice", arguments={"host_name": "prod-a", "limit": 5}
    )

    assert not result.is_error
    data = json.loads(result.content)
    assert data["points"] == [{"cpu": 99.0}]
    assert calls == [{"host_name": "prod-a", "limit": 5}]


@pytest.mark.asyncio
async def test_get_system_metrics_requires_host_name() -> None:
    registry = _make_registry()
    with pytest.raises(ValueError, match="host_name"):
        await registry["get_system_metrics"].invoke(
            user_id="alice", arguments={"limit": 5}
        )


@pytest.mark.asyncio
async def test_get_system_metrics_is_read_only() -> None:
    registry = _make_registry()
    assert registry["get_system_metrics"].read_only is True


# ---------------------------------------------------------------------------
# list_alert_mutes
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_alert_mutes_returns_mutes() -> None:
    writer = _MetricsWriterStub()
    registry = _make_registry(writer=writer)

    result = await registry["list_alert_mutes"].invoke(
        user_id="alice", arguments={"limit": 50}
    )

    assert isinstance(result, ToolResult)
    assert not result.is_error
    data = json.loads(result.content)
    assert data["mutes"][0]["category"] == "cpu_high"


@pytest.mark.asyncio
async def test_list_alert_mutes_via_injected_fn() -> None:
    calls: list[int] = []

    async def mutes_fn(writer: Any, *, limit: int) -> list[dict[str, Any]]:
        calls.append(limit)
        return [{"id": 1}]

    registry = create_tool_registry(
        host_manager=_HostManagerStub(),
        metrics_writer_factory=lambda: _MetricsWriterStub(),
        list_mutes_fn=mutes_fn,
    )

    result = await registry["list_alert_mutes"].invoke(
        user_id="alice", arguments={"limit": 30}
    )

    assert not result.is_error
    data = json.loads(result.content)
    assert data["mutes"] == [{"id": 1}]
    assert calls == [30]


@pytest.mark.asyncio
async def test_list_alert_mutes_is_read_only() -> None:
    registry = _make_registry()
    assert registry["list_alert_mutes"].read_only is True


# ---------------------------------------------------------------------------
# get_storm_events
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_storm_events_returns_events() -> None:
    writer = _MetricsWriterStub()
    registry = _make_registry(writer=writer)

    result = await registry["get_storm_events"].invoke(
        user_id="alice", arguments={"limit": 10}
    )

    assert isinstance(result, ToolResult)
    assert not result.is_error
    data = json.loads(result.content)
    assert data["events"][0]["id"] == 99


@pytest.mark.asyncio
async def test_get_storm_events_filters_by_category() -> None:
    writer = _MetricsWriterStub()
    # Add an event with a different category
    writer._storm_events = [
        {"id": 1, "category": "cpu_high", "phase": "start"},
        {"id": 2, "category": "mem_high", "phase": "start"},
    ]
    registry = _make_registry(writer=writer)

    result = await registry["get_storm_events"].invoke(
        user_id="alice", arguments={"limit": 20, "category": "cpu_high"}
    )

    data = json.loads(result.content)
    assert len(data["events"]) == 1
    assert data["events"][0]["id"] == 1


@pytest.mark.asyncio
async def test_get_storm_events_via_injected_fn() -> None:
    calls: list[dict[str, Any]] = []

    async def storm_fn(
        writer: Any, *, limit: int, category: str | None
    ) -> list[dict[str, Any]]:
        calls.append({"limit": limit, "category": category})
        return [{"id": 42}]

    registry = create_tool_registry(
        host_manager=_HostManagerStub(),
        metrics_writer_factory=lambda: _MetricsWriterStub(),
        list_storm_events_fn=storm_fn,
    )

    result = await registry["get_storm_events"].invoke(
        user_id="alice", arguments={"limit": 7, "category": "disk_full"}
    )

    data = json.loads(result.content)
    assert data["events"] == [{"id": 42}]
    assert calls == [{"limit": 7, "category": "disk_full"}]


@pytest.mark.asyncio
async def test_get_storm_events_is_read_only() -> None:
    registry = _make_registry()
    assert registry["get_storm_events"].read_only is True


# ---------------------------------------------------------------------------
# exec_container
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_exec_container_requires_confirmation() -> None:
    """exec_container is a dangerous tool; invoking without confirmed=True raises PermissionError."""
    registry = _make_registry()

    with pytest.raises(PermissionError, match="confirmation"):
        await registry["exec_container"].invoke(
            user_id="alice",
            arguments={
                "host": "prod-a",
                "container_id": "c1",
                "command": "echo hello",
                "confirmed": False,
            },
        )


@pytest.mark.asyncio
async def test_exec_container_runs_command_when_confirmed() -> None:
    exec_calls: list[dict[str, Any]] = []

    async def exec_fn(
        host: dict[str, Any], container: dict[str, Any], *, command: str
    ) -> dict[str, Any]:
        exec_calls.append({"host": host["name"], "container_id": container["id"], "command": command})
        return {
            "container_id": container["id"],
            "container_name": "api",
            "command": command,
            "exit_code": 0,
            "output": "hello",
        }

    registry = _make_registry(exec_fn=exec_fn)

    result = await registry["exec_container"].invoke(
        user_id="alice",
        arguments={
            "host": "prod-a",
            "container_id": "c1",
            "command": "echo hello",
            "confirmed": True,
        },
    )

    assert isinstance(result, ToolResult)
    assert not result.is_error
    data = json.loads(result.content)
    assert data["ok"] is True
    assert data["result"]["exit_code"] == 0
    assert data["result"]["output"] == "hello"
    assert exec_calls == [
        {"host": "prod-a", "container_id": "c1", "command": "echo hello"}
    ]


@pytest.mark.asyncio
async def test_exec_container_denied_when_host_not_allowlisted() -> None:
    registry = _make_registry(allow_exec_host=False)

    with pytest.raises(PermissionError, match="allowlist"):
        await registry["exec_container"].invoke(
            user_id="alice",
            arguments={
                "host": "prod-a",
                "container_id": "c1",
                "command": "ls",
                "confirmed": True,
            },
        )


@pytest.mark.asyncio
async def test_exec_container_requires_command() -> None:
    registry = _make_registry()

    with pytest.raises(ValueError, match="command"):
        await registry["exec_container"].invoke(
            user_id="alice",
            arguments={
                "host": "prod-a",
                "container_id": "c1",
                "command": "",
                "confirmed": True,
            },
        )


@pytest.mark.asyncio
async def test_exec_container_is_not_read_only() -> None:
    registry = _make_registry()
    assert registry["exec_container"].read_only is False
