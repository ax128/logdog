from __future__ import annotations

import importlib
from typing import Any

import pytest


class _HostManagerStub:
    def __init__(
        self, *, config: dict, state: dict, statuses: list[dict] | None = None
    ) -> None:
        self._config = config
        self._state = state
        self._statuses = statuses or [state]

    def get_host_config(self, name: str) -> dict | None:
        if name != self._config.get("name"):
            return None
        return dict(self._config)

    def get_host_state(self, name: str) -> dict | None:
        if name != self._state.get("name"):
            return None
        return dict(self._state)

    def list_host_statuses(self) -> list[dict]:
        return [dict(item) for item in self._statuses]


def _schedule_report_runner_cls():
    module = importlib.import_module("logwatch.collector.reports")
    return module.ScheduleReportRunner


@pytest.mark.asyncio
async def test_report_runner_pushes_normal_summary_when_enabled() -> None:
    notifications: list[tuple[str, str, str]] = []
    ScheduleReportRunner = _schedule_report_runner_cls()
    runner = ScheduleReportRunner(
        host_manager=_HostManagerStub(
            config={
                "name": "host-a",
                "output_template": "standard",
                "notify": {"push_on_normal": True, "heartbeat_interval_seconds": 3600},
            },
            state={
                "name": "host-a",
                "status": "connected",
                "last_connected_at": "2026-04-11T00:00:00Z",
            },
        ),
        list_containers=lambda _host: [
            {"id": "c1", "name": "api", "status": "running"}
        ],
        list_alerts=lambda **_kwargs: [],
        query_metrics=lambda *_args, **_kwargs: [
            {
                "cpu": 12.5,
                "mem_used": 128,
                "mem_limit": 1024,
                "timestamp": "2026-04-11 00:00:01",
            }
        ],
        send_host_notification=lambda host, message, category: (
            notifications.append((host, message, category)) or True
        ),
        analyze=lambda scene, context, template: (
            f"{scene}|{template}|{context['host_name']}"
        ),
        time_fn=lambda: 1000.0,
    )

    result = await runner.run_host_schedule(
        "host-a",
        {"name": "fast", "interval_seconds": 300, "template": "interval"},
    )

    assert result["sent"] is True
    assert notifications == [("host-a", "interval|interval|host-a", "INTERVAL")]


@pytest.mark.asyncio
async def test_report_runner_legacy_mode_disables_agent_invoke() -> None:
    analyze_kwargs: list[dict[str, object]] = []
    notifications: list[tuple[str, str, str]] = []
    ScheduleReportRunner = _schedule_report_runner_cls()
    runner = ScheduleReportRunner(
        host_manager=_HostManagerStub(
            config={
                "name": "host-a",
                "notify": {"push_on_normal": True},
            },
            state={
                "name": "host-a",
                "status": "connected",
                "last_connected_at": "2026-04-11T00:00:00Z",
            },
        ),
        list_containers=lambda _host: [{"id": "c1", "name": "api", "status": "running"}],
        list_alerts=lambda **_kwargs: [],
        query_metrics=lambda *_args, **_kwargs: [],
        send_host_notification=lambda host, message, category: (
            notifications.append((host, message, category)) or True
        ),
        analyze=lambda scene, context, template, **kwargs: (
            analyze_kwargs.append(dict(kwargs)) or "legacy-report"
        ),
        time_fn=lambda: 1000.0,
    )

    result = await runner.run_host_schedule(
        "host-a",
        {"name": "fast", "interval_seconds": 300, "template": "interval"},
    )

    assert result["sent"] is True
    assert notifications == [("host-a", "legacy-report", "INTERVAL")]
    assert analyze_kwargs
    assert analyze_kwargs[0].get("enable_agent") is False


@pytest.mark.asyncio
async def test_report_runner_includes_host_system_metrics_summary_and_warnings() -> (
    None
):
    notifications: list[tuple[str, str, str]] = []
    captured_context: dict[str, Any] = {}
    ScheduleReportRunner = _schedule_report_runner_cls()
    runner = ScheduleReportRunner(
        host_manager=_HostManagerStub(
            config={
                "name": "host-a",
                "output_template": "standard",
                "notify": {"push_on_normal": True},
            },
            state={
                "name": "host-a",
                "status": "connected",
                "last_connected_at": "2026-04-11T00:00:00Z",
            },
        ),
        list_containers=lambda _host: [
            {"id": "c1", "name": "api", "status": "running"}
        ],
        list_alerts=lambda **_kwargs: [],
        query_metrics=lambda *_args, **_kwargs: [
            {
                "cpu": 12.5,
                "mem_used": 128,
                "mem_limit": 1024,
                "timestamp": "2026-04-11 00:00:01",
            }
        ],
        query_host_metrics=lambda *_args, **_kwargs: [
            {
                "host_name": "host-a",
                "timestamp": "2026-04-11 00:00:01",
                "cpu_percent": 95.0,
                "mem_total": 1000,
                "mem_used": 910,
                "disk_root_total": 2000,
                "disk_root_used": 1900,
                "source": "local",
            },
            {
                "host_name": "host-a",
                "timestamp": "2026-04-11 00:05:01",
                "cpu_percent": 85.0,
                "mem_total": 1000,
                "mem_used": 920,
                "disk_root_total": 2000,
                "disk_root_used": 1910,
                "source": "local",
            },
        ],
        host_system_settings_getter=lambda: {
            "report": {
                "include_in_schedule": True,
                "warn_thresholds": {
                    "cpu_percent": 80,
                    "mem_used_percent": 90,
                    "disk_used_percent": 90,
                },
            }
        },
        send_host_notification=lambda host, message, category: (
            notifications.append((host, message, category)) or True
        ),
        analyze=lambda scene, context, template: (
            captured_context.update(context)
            or f"{scene}|{template}|{context['host_name']}"
        ),
        time_fn=lambda: 1000.0,
    )

    result = await runner.run_host_schedule(
        "host-a",
        {"name": "fast", "interval_seconds": 300, "template": "interval"},
    )

    assert result["sent"] is True
    assert notifications == [("host-a", "interval|interval|host-a", "INTERVAL")]
    assert "host_metrics" in captured_context
    assert "host_resource_warnings" in captured_context
    assert "cpu_avg" in str(captured_context["host_metrics"])
    warnings = captured_context["host_resource_warnings"]
    assert isinstance(warnings, list)
    assert any("cpu" in str(item).lower() for item in warnings)
    assert "Host system:" in str(captured_context["metrics"])


@pytest.mark.asyncio
async def test_report_runner_sends_heartbeat_when_normal_push_is_disabled() -> None:
    notifications: list[tuple[str, str, str]] = []
    ScheduleReportRunner = _schedule_report_runner_cls()
    runner = ScheduleReportRunner(
        host_manager=_HostManagerStub(
            config={
                "name": "host-a",
                "notify": {"push_on_normal": False, "heartbeat_interval_seconds": 60},
            },
            state={
                "name": "host-a",
                "status": "connected",
                "last_connected_at": "2026-04-11T00:00:00Z",
            },
        ),
        list_containers=lambda _host: [
            {"id": "c1", "name": "api", "status": "running"}
        ],
        list_alerts=lambda **_kwargs: [],
        query_metrics=lambda *_args, **_kwargs: [],
        send_host_notification=lambda host, message, category: (
            notifications.append((host, message, category)) or True
        ),
        analyze=lambda scene, context, template: (
            f"{scene}|{template}|{context['host_name']}"
        ),
        time_fn=lambda: 1000.0,
    )

    first = await runner.run_host_schedule(
        "host-a",
        {"name": "fast", "interval_seconds": 30, "template": "interval"},
    )
    second = await runner.run_host_schedule(
        "host-a",
        {"name": "fast", "interval_seconds": 30, "template": "interval"},
    )

    assert first["scene"] == "heartbeat"
    assert first["sent"] is True
    assert second["sent"] is False
    assert notifications[0][0] == "host-a"
    assert notifications[0][2] == "HEARTBEAT"
    assert "系统心跳" in notifications[0][1]


@pytest.mark.asyncio
async def test_report_runner_sends_global_daily_report_through_global_callback() -> (
    None
):
    sent: list[tuple[str, str]] = []
    ScheduleReportRunner = _schedule_report_runner_cls()
    runner = ScheduleReportRunner(
        host_manager=_HostManagerStub(
            config={"name": "host-a"},
            state={
                "name": "host-a",
                "status": "connected",
                "last_connected_at": "2026-04-11T00:00:00Z",
            },
            statuses=[
                {
                    "name": "host-a",
                    "status": "connected",
                    "last_connected_at": "2026-04-11T00:00:00Z",
                },
                {
                    "name": "host-b",
                    "status": "disconnected",
                    "last_connected_at": "2026-04-10T23:00:00Z",
                },
            ],
        ),
        list_containers=lambda host: (
            [{"id": f"{host}-c1", "name": f"{host}-api", "status": "running"}]
            if host == "host-a"
            else []
        ),
        list_alerts=lambda **kwargs: (
            [
                {
                    "payload": {"host": kwargs.get("host"), "line": "boom"},
                    "created_at": "2026-04-11 00:00:01",
                }
            ]
            if kwargs.get("host") == "host-a"
            else []
        ),
        query_metrics=lambda *_args, **_kwargs: [],
        send_host_notification=None,
        send_global_notification=lambda schedule, message, category: (
            sent.append((schedule["name"], f"{category}:{message}")) or True
        ),
        analyze=lambda scene, context, template: (
            f"{scene}|{template}|{len(context['alert_history'])}"
        ),
        time_fn=lambda: 1000.0,
    )

    result = await runner.run_global_schedule(
        {"name": "global_daily", "template": "daily", "scope": "all_hosts"}
    )

    assert result["sent"] is True
    assert sent == [("global_daily", "DAILY:daily|daily|2")]


@pytest.mark.asyncio
async def test_report_runner_compresses_global_summary_when_over_budget() -> None:
    captured_context: dict[str, object] = {}
    sent: list[tuple[str, str]] = []
    ScheduleReportRunner = _schedule_report_runner_cls()
    runner = ScheduleReportRunner(
        host_manager=_HostManagerStub(
            config={"name": "host-a"},
            state={
                "name": "host-a",
                "status": "connected",
                "last_connected_at": "2026-04-11T00:00:00Z",
            },
            statuses=[
                {
                    "name": "host-a",
                    "status": "connected",
                    "last_connected_at": "2026-04-11T00:00:00Z",
                },
                {
                    "name": "host-b",
                    "status": "connected",
                    "last_connected_at": "2026-04-11T00:00:00Z",
                },
                {
                    "name": "host-c",
                    "status": "connected",
                    "last_connected_at": "2026-04-11T00:00:00Z",
                },
            ],
        ),
        list_containers=lambda host: [
            {"id": f"{host}-c1", "name": f"{host}-api", "status": "running"}
        ],
        list_alerts=lambda **kwargs: [
            {
                "payload": {
                    "host": kwargs.get("host"),
                    "line": f"{kwargs.get('host')}-alert-{idx}",
                },
                "created_at": "2026-04-11 00:00:01",
            }
            for idx in range(4 if kwargs.get("host") == "host-a" else 1)
        ],
        query_metrics=lambda *_args, **_kwargs: [],
        send_host_notification=None,
        send_global_notification=lambda schedule, message, category: (
            sent.append((schedule["name"], f"{category}:{message}")) or True
        ),
        analyze=lambda scene, context, template: (
            captured_context.update({"scene": scene, "context": context}) or "global-ok"
        ),
        time_fn=lambda: 1000.0,
    )

    result = await runner.run_global_schedule(
        {
            "name": "global_daily",
            "template": "daily",
            "scope": "all_hosts",
            "max_global_summary_chars": 120,
            "top_hosts_on_overflow": 1,
        }
    )

    assert result["sent"] is True
    assert sent == [("global_daily", "DAILY:global-ok")]
    context = captured_context["context"]
    assert isinstance(context, dict)
    history = context["alert_history"]
    assert isinstance(history, list)
    assert any(str(line).startswith("[compact]") for line in history)
    assert any("top_alerts=" in str(line) for line in history)


@pytest.mark.asyncio
async def test_report_runner_prefers_bulk_metrics_query_over_per_container_queries() -> (
    None
):
    bulk_calls: list[tuple[str, str, str, int]] = []
    single_calls: list[int] = []

    ScheduleReportRunner = _schedule_report_runner_cls()
    runner = ScheduleReportRunner(
        host_manager=_HostManagerStub(
            config={"name": "host-a", "notify": {"push_on_normal": True}},
            state={"name": "host-a", "status": "connected"},
        ),
        list_containers=lambda _h: [
            {"id": "c1", "name": "api"},
            {"id": "c2", "name": "worker"},
        ],
        list_alerts=lambda **_kw: [],
        query_metrics=lambda *_a, **_kw: single_calls.append(1) or [],
        query_host_metrics=lambda *_a, **_kw: [],
        query_host_container_metrics=lambda host, start, end, limit: (
            bulk_calls.append((host, start, end, limit))
            or [
                {
                    "container_id": "c1",
                    "container_name": "api",
                    "cpu": 11,
                    "mem_used": 100,
                },
                {
                    "container_id": "c2",
                    "container_name": "worker",
                    "cpu": 22,
                    "mem_used": 200,
                },
            ]
        ),
        send_host_notification=lambda *_a, **_kw: True,
        analyze=lambda s, c, t: f"{s}|{t}|{c['host_name']}",
        time_fn=lambda: 1000.0,
    )

    await runner.run_host_schedule(
        "host-a",
        {"name": "fast", "interval_seconds": 300, "template": "interval"},
    )

    assert len(bulk_calls) == 1
    assert len(single_calls) == 0
