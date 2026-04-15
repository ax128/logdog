from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Iterable


REQUIRED_VARS_BY_SCENE: dict[str, set[str]] = {
    "alert": {"host_name", "container_name", "timestamp", "logs"},
    "interval": {"host_name", "container_name", "timestamp", "logs", "metrics"},
    "hourly": {"host_name", "container_name", "timestamp", "logs", "metrics"},
    "daily": {"host_name", "timestamp", "alert_history", "metrics"},
    "heartbeat": {"host_name", "timestamp", "container_status", "total_containers"},
    # --- new scenes ---
    # Container crash / OOM / non-zero exit
    "crash": {"host_name", "container_name", "timestamp", "logs", "exit_code", "restart_count"},
    # Security-related log events (auth failures, privilege escalation, etc.)
    "security": {"host_name", "container_name", "timestamp", "logs"},
    # Post-deployment health validation
    "deployment": {"host_name", "container_name", "timestamp", "logs", "image_tag"},
}
ALLOW_EMPTY_COLLECTIONS_BY_SCENE: dict[str, set[str]] = {
    "interval": {"logs", "metrics"},
    "hourly": {"logs", "metrics"},
    "daily": {"alert_history", "metrics"},
    "heartbeat": {"container_status"},
    "crash": {"logs"},
    "security": {"logs"},
    "deployment": {"logs"},
}


class BasePromptTemplate(ABC):
    """
    Base prompt template contract.

    The project intentionally keeps templates as simple, pure renderers:
    - `validate()` checks required fields for a given scene.
    - `render()` builds the final prompt string.
    """

    # Variables injected by the system (not required from user-provided context).
    BUILTIN_VARS: set[str] = {"scene"}

    @abstractmethod
    def render(self, context: dict[str, Any]) -> str:
        raise NotImplementedError

    @classmethod
    def required_vars(cls, scene: str) -> set[str]:
        """
        Return required variables for a given scene.

        Must at least cover:
        - alert
        - interval
        - hourly
        - daily
        - heartbeat
        """

        s = (scene or "").strip().lower()
        required = REQUIRED_VARS_BY_SCENE.get(s)
        if required is None:
            raise ValueError(f"Unknown scene: {scene!r}")
        return set(required)

    def validate(self, scene: str, context: dict[str, Any]) -> list[str]:
        required = set(self.required_vars(scene)) - set(self.BUILTIN_VARS)
        missing: list[str] = []
        for k in sorted(required):
            if _is_missing_value(scene, k, context.get(k)):
                missing.append(k)
        return missing


def _is_missing_value(scene: str, key: str, v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, str):
        return v.strip() == ""
    if isinstance(v, (dict, list, tuple, set, frozenset)):
        allowed_empty = ALLOW_EMPTY_COLLECTIONS_BY_SCENE.get(scene, set())
        if key in allowed_empty:
            return False
        return len(v) == 0
    return False


def coerce_logs_text(logs: Any, *, max_lines: int = 50) -> str:
    """
    Best-effort conversion for logs into a readable plaintext block.
    """

    if logs is None:
        return ""
    if isinstance(logs, str):
        return logs.strip()
    if isinstance(logs, Iterable):
        lines: list[str] = []
        for i, item in enumerate(logs):
            if i >= max_lines:
                break
            if item is None:
                continue
            s = str(item).rstrip("\n")
            if s.strip() == "":
                continue
            lines.append(s)
        return "\n".join(lines).strip()
    return str(logs).strip()


def coerce_template_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        if not value:
            return ""
        return "\n".join(
            f"{key}: {value[key]}" for key in sorted(value, key=lambda item: str(item))
        ).strip()
    if isinstance(value, (list, tuple, set, frozenset)):
        return coerce_logs_text(value, max_lines=500)
    return str(value).strip()
