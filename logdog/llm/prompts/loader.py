from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from logdog.llm.prompts.base import (
    BasePromptTemplate,
    coerce_logs_text,
    coerce_template_value,
)


PROMPTS_DIR = Path(__file__).resolve().parents[3] / "templates" / "prompts"
_VALID_TEMPLATE_NAME = re.compile(r"^[a-z0-9_]+$")
_PROMPT_ALIASES = {
    # alert scene
    "default": "default_alert",
    "alert": "default_alert",
    "default_alert": "default_alert",
    "alert_brief": "alert_brief",
    "alert_detailed": "alert_detailed",
    "alert_security": "alert_security",
    # interval scene
    "interval": "default_interval",
    "default_interval": "default_interval",
    # hourly scene
    "hourly": "default_hourly",
    "default_hourly": "default_hourly",
    # daily scene
    "daily": "default_daily",
    "default_daily": "default_daily",
    "daily_executive": "daily_executive",
    "daily_ops": "daily_ops",
    # heartbeat scene
    "heartbeat": "default_heartbeat",
    "default_heartbeat": "default_heartbeat",
    # crash scene (new)
    "crash": "crash_analysis",
    "crash_analysis": "crash_analysis",
    # security scene (new)
    "security": "alert_security",
    # deployment scene (new)
    "deployment": "deployment_check",
    "deployment_check": "deployment_check",
    # advanced alert analysis scenes
    "incident": "incident_commander",
    "incident_commander": "incident_commander",
    "perf": "perf_regression",
    "performance": "perf_regression",
    "perf_regression": "perf_regression",
    "dependency": "dependency_outage",
    "dependency_outage": "dependency_outage",
}
_BUILTIN_TEMPLATE_TEXTS = {
    # Minimal one-liner fallbacks used when the .md file is missing.
    "default_alert": "主机: {host_name} | 容器: {container_name} | 时间: {timestamp}\n{logs}",
    "alert_brief": "主机: {host_name} | 容器: {container_name} | 时间: {timestamp}\n{logs}\n请用3行给出诊断：🚨现象 / 📍原因 / 🔧建议",
    "alert_detailed": "主机: {host_name} | 容器: {container_name} | 时间: {timestamp}\n{logs}\n请给出详细根因分析和处置方案。",
    "alert_security": "主机: {host_name} | 容器: {container_name} | 时间: {timestamp}\n{logs}\n请分析安全威胁并给出处置建议。",
    "default_interval": "周期检查 {host_name}/{container_name} 时间: {timestamp}\n日志:\n{logs}\n指标:\n{metrics}",
    "default_hourly": "小时摘要 {host_name}/{container_name} 时间: {timestamp}\n日志:\n{logs}\n指标:\n{metrics}",
    "default_daily": "每日摘要 {host_name} 日期: {timestamp}\n告警历史:\n{alert_history}\n性能指标:\n{metrics}",
    "daily_executive": "主机: {host_name} 日期: {timestamp}\n告警: {alert_history}\n指标: {metrics}\n请生成管理层日报摘要（150字以内）。",
    "daily_ops": "主机: {host_name} 日期: {timestamp}\n告警: {alert_history}\n指标: {metrics}\n请生成运维日报，按容器分析。",
    "default_heartbeat": "心跳检查 {host_name} 时间: {timestamp} 容器数: {total_containers}\n{container_status}",
    "crash_analysis": "主机: {host_name} | 容器: {container_name} | 时间: {timestamp}\n退出码: {exit_code} | 重启次数: {restart_count}\n{logs}\n请分析崩溃原因并给出修复建议。",
    "deployment_check": "主机: {host_name} | 容器: {container_name} | 时间: {timestamp}\n镜像: {image_tag}\n{logs}\n请评估部署结果是否成功。",
    "incident_commander": "主机: {host_name} | 容器: {container_name} | 时间: {timestamp}\n日志:\n{logs}\n请给出分级、证据链、根因假设、分阶段处置与验证闭环。",
    "perf_regression": "主机: {host_name} | 容器: {container_name} | 时间: {timestamp}\n日志:\n{logs}\n请分析性能劣化证据、瓶颈位置、优化建议与回归验证指标。",
    "dependency_outage": "主机: {host_name} | 容器: {container_name} | 时间: {timestamp}\n日志:\n{logs}\n请判断依赖故障类型并给出隔离、降级、恢复方案。",
}


class DefaultAlertTemplate(BasePromptTemplate):
    """
    Minimal built-in template used by tests and as a safe default.
    """

    def render(self, context: dict[str, Any]) -> str:
        host_name = (
            _coerce_text(context.get("host_name"), default="logdog") or "logdog"
        )
        container_name = (
            _coerce_text(context.get("container_name"), default="container")
            or "container"
        )
        timestamp = _coerce_text(context.get("timestamp"), default="")
        logs_text = coerce_logs_text(context.get("logs"))

        heading = f"[{host_name}] {container_name}"
        if timestamp:
            heading = f"{heading} @ {timestamp}"

        parts: list[str] = [heading]
        if logs_text:
            parts.append(logs_text)
        return "\n".join(parts).strip()


class FilesystemPromptTemplate(BasePromptTemplate):
    def __init__(self, template_text: str) -> None:
        self._template_text = str(template_text)

    def render(self, context: dict[str, Any]) -> str:
        render_context = _stringify_context(context)
        return self._template_text.format_map(render_context).strip()


def load_template(name: str) -> BasePromptTemplate:
    """
    Load a prompt template by name.

    Requirements:
    - minimal viable loader
    - at least supports `default_alert`
    - failure raises a clear exception
    """

    n = _normalize_template_name(name)
    template_path = PROMPTS_DIR / f"{n}.md"
    if template_path.is_file():
        return FilesystemPromptTemplate(template_path.read_text(encoding="utf-8"))
    if n == "default_alert":
        return DefaultAlertTemplate()
    builtin_template_text = _BUILTIN_TEMPLATE_TEXTS.get(n)
    if builtin_template_text is not None:
        return FilesystemPromptTemplate(builtin_template_text)
    raise FileNotFoundError(f"Prompt template file not found: {template_path}")


def _coerce_text(v: Any, *, default: str) -> str:
    if v is None:
        return default
    return str(v).strip()


def _normalize_template_name(name: str) -> str:
    normalized = (name or "").strip().lower().replace("-", "_")
    if not normalized or not _VALID_TEMPLATE_NAME.fullmatch(normalized):
        raise ValueError(f"Invalid prompt template name: {name!r}")
    return _PROMPT_ALIASES.get(normalized, normalized)


def _stringify_context(context: dict[str, Any]) -> dict[str, str]:
    return {key: coerce_template_value(value) for key, value in context.items()}
