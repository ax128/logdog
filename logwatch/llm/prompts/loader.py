from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from logwatch.llm.prompts.base import (
    BasePromptTemplate,
    coerce_logs_text,
    coerce_template_value,
)


PROMPTS_DIR = Path(__file__).resolve().parents[3] / "templates" / "prompts"
_VALID_TEMPLATE_NAME = re.compile(r"^[a-z0-9_]+$")
_PROMPT_ALIASES = {
    "default": "default_alert",
    "alert": "default_alert",
    "default_alert": "default_alert",
    "interval": "default_interval",
    "default_interval": "default_interval",
    "hourly": "default_hourly",
    "default_hourly": "default_hourly",
    "daily": "default_daily",
    "default_daily": "default_daily",
    "heartbeat": "default_heartbeat",
    "default_heartbeat": "default_heartbeat",
}
_BUILTIN_TEMPLATE_TEXTS = {
    "default_alert": "Host: {host_name}\nContainer: {container_name}\nTimestamp: {timestamp}\n{logs}",
    "default_interval": "Interval summary for {host_name}/{container_name}\nTimestamp: {timestamp}\nLogs:\n{logs}\nMetrics:\n{metrics}",
    "default_hourly": "Hourly summary for {host_name}/{container_name}\nTimestamp: {timestamp}\nLogs:\n{logs}\nMetrics:\n{metrics}",
    "default_daily": "Daily summary for {host_name}\nTimestamp: {timestamp}\nAlert history:\n{alert_history}\nMetrics:\n{metrics}",
    "default_heartbeat": "Heartbeat check for {host_name}\nTimestamp: {timestamp}\nContainers: {total_containers}\nStatus:\n{container_status}",
}


class DefaultAlertTemplate(BasePromptTemplate):
    """
    Minimal built-in template used by tests and as a safe default.
    """

    def render(self, context: dict[str, Any]) -> str:
        host_name = (
            _coerce_text(context.get("host_name"), default="logwatch") or "logwatch"
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
