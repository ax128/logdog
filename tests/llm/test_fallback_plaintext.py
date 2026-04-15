from __future__ import annotations

from typing import Any

import pytest

from logwatch.llm.analyzer import analyze_with_template
from logwatch.llm.prompts.base import BasePromptTemplate


def test_fallback_on_missing_required_vars():
    out = analyze_with_template("alert", {"host_name": "svc-a"}, "default_alert")
    assert out.startswith("[FALLBACK]")
    assert "reason=missing_required" in out
    assert "container_name" in out
    assert "logs" in out
    assert "timestamp" in out


def test_fallback_on_template_load_error():
    out = analyze_with_template(
        "alert",
        {
            "host_name": "prod-a",
            "container_name": "api",
            "timestamp": "2026-04-11T10:00:00Z",
            "logs": ["line1"],
        },
        "template_does_not_exist",
    )
    assert out.startswith("[FALLBACK]")
    assert "reason=template_error" in out


class _WhitespaceTemplate(BasePromptTemplate):
    def render(self, context: dict[str, Any]) -> str:
        return "   \n"


def test_fallback_on_blank_render():
    out = analyze_with_template(
        "heartbeat",
        {
            "host_name": "prod-a",
            "timestamp": "2026-04-11T10:00:00Z",
            "container_status": "api=ok",
            "total_containers": 2,
        },
        _WhitespaceTemplate(),
    )
    assert out.startswith("[FALLBACK]")
    assert "reason=empty_render" in out


class _CrashTemplate(BasePromptTemplate):
    def render(self, context: dict[str, Any]) -> str:
        raise RuntimeError("boom")


def test_fallback_on_render_exception():
    out = analyze_with_template(
        "heartbeat",
        {
            "host_name": "prod-a",
            "timestamp": "2026-04-11T10:00:00Z",
            "container_status": "api=ok",
            "total_containers": 2,
        },
        _CrashTemplate(),
    )
    assert out.startswith("[FALLBACK]")
    assert "reason=template_error" in out


def test_non_fallback_on_valid_input():
    out = analyze_with_template(
        "alert",
        {
            "host_name": "prod-a",
            "container_name": "api",
            "timestamp": "2026-04-11T10:00:00Z",
            "logs": ["line1", "line2"],
        },
        "default_alert",
    )
    assert not out.startswith("[FALLBACK]")
    assert "prod-a" in out
    assert "api" in out
    assert "2026-04-11T10:00:00Z" in out


def test_non_fallback_on_empty_interval_window():
    out = analyze_with_template(
        "interval",
        {
            "host_name": "prod-a",
            "container_name": "api",
            "timestamp": "2026-04-11T10:00:00Z",
            "logs": [],
            "metrics": {},
        },
        "default_interval",
    )
    assert not out.startswith("[FALLBACK]")


def test_fallback_writes_warning_log(caplog: pytest.LogCaptureFixture):
    with caplog.at_level("WARNING"):
        out = analyze_with_template(
            "alert",
            {
                "host_name": "prod-a",
                "container_name": "api",
                "timestamp": "2026-04-11T10:00:00Z",
                "logs": ["line1"],
            },
            "template_does_not_exist",
        )

    assert out.startswith("[FALLBACK]")
    assert any(
        "reason=template_error" in rec.message and "scene=alert" in rec.message
        for rec in caplog.records
    )
