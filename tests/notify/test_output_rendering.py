from __future__ import annotations

import importlib
import importlib.util

import pytest


def _import_render_module():
    spec = importlib.util.find_spec("logdog.notify.render")
    assert spec is not None
    return importlib.import_module("logdog.notify.render")


def test_render_output_renders_alert_context_from_standard_template() -> None:
    render_module = _import_render_module()

    rendered = render_module.render_output(
        "standard",
        {
            "severity": "ERROR",
            "host_name": "prod-a",
            "container_name": "api",
            "llm_summary": "Disk usage is critically high",
            "llm_causes": "Runaway temp files",
            "llm_actions": "Clean temp files and expand disk",
            "cpu": 92,
            "mem_used": "512MiB",
            "mem_limit": "1GiB",
            "restart_count": 3,
            "timestamp": "2026-04-11T10:00:00Z",
        },
    )

    assert "ERROR" in rendered
    assert "prod-a" in rendered
    assert "api" in rendered
    assert "Disk usage is critically high" in rendered
    assert "Runaway temp files" in rendered
    assert "Clean temp files and expand disk" in rendered
    assert "512MiB" in rendered


def test_render_output_renders_report_context_from_detailed_template() -> None:
    render_module = _import_render_module()

    rendered = render_module.render_output(
        "detailed",
        {
            "severity": "WARN",
            "container_name": "api",
            "llm_summary": "2 alerts across 1 host",
            "llm_timeline": "10:00 cpu spike; 10:05 errors",
            "llm_causes": "traffic burst",
            "llm_impact": "slow responses",
            "llm_actions": "scale up workers",
            "log_snippet": "ERROR timeout",
            "metrics_detail": "cpu=95% mem=80%",
            "timestamp": "2026-04-11T10:10:00Z",
            "category": "RESOURCE",
        },
    )

    assert "WARN" in rendered
    assert "api" in rendered
    assert "2 alerts across 1 host" in rendered
    assert "10:00 cpu spike; 10:05 errors" in rendered
    assert "slow responses" in rendered
    assert "ERROR timeout" in rendered
    assert "RESOURCE" in rendered


def test_render_output_renders_business_context_from_business_template() -> None:
    render_module = _import_render_module()

    rendered = render_module.render_output(
        "business",
        {
            "container_name": "api",
            "llm_impact": "Checkout latency elevated",
            "llm_business_metrics": "conversion -12%",
            "llm_recovery_time": "15 minutes",
            "llm_summary": "Database saturation causing queueing",
        },
    )

    assert "api" in rendered
    assert "Checkout latency elevated" in rendered
    assert "conversion -12%" in rendered
    assert "15 minutes" in rendered
    assert "Database saturation causing queueing" in rendered


def test_render_output_renders_heartbeat_template() -> None:
    render_module = _import_render_module()

    rendered = render_module.render_output(
        "heartbeat",
        {
            "host_name": "prod-a",
            "timestamp": "2026-04-11T10:00:00Z",
            "total_containers": 8,
            "container_status_table": "api ok\nworker ok",
            "last_alert_time": "2026-04-11T09:30:00Z",
            "last_alert_summary": "disk usage recovered",
        },
    )

    assert "prod-a" in rendered
    assert "2026-04-11T10:00:00Z" in rendered
    assert "8" in rendered
    assert "api ok\nworker ok" in rendered
    assert "disk usage recovered" in rendered


def test_render_output_rejects_unknown_template_name() -> None:
    render_module = _import_render_module()

    with pytest.raises(ValueError, match="Unknown output template"):
        render_module.render_output("not_real", {"service": "svc-a"})
