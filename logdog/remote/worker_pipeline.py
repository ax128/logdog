from __future__ import annotations

import logging
import re
from collections import deque
from dataclasses import dataclass, replace
from typing import Any

from logdog.pipeline.filter import RuleResult, apply_rules
from logdog.pipeline.preprocessor.base import LogLine


logger = logging.getLogger(__name__)

_REMOTE_ALLOWED_CONFIG_KEYS = frozenset(
    {
        "alert_keywords",
        "custom_alerts",
        "dedup_window",
        "exclude",
        "head",
        "include",
        "min_level",
        "redact",
        "tail",
    }
)
_REMOTE_FORBIDDEN_CONFIG_KEYS = frozenset(
    {
        "module",
        "path",
        "preprocessor",
        "preprocessors",
    }
)

_LEVEL_ORDER: dict[str, int] = {
    "debug": 0,
    "info": 1,
    "warn": 2,
    "warning": 2,
    "error": 3,
    "fatal": 4,
    "panic": 4,
    "critical": 4,
}
_UNKNOWN_LEVEL = -1
_CONTENT_LEVEL_RE = re.compile(
    r"\b(debug|info|warn(?:ing)?|error|fatal|panic|critical)\b",
    re.IGNORECASE,
)
_MAX_PATTERN_LENGTH = 1024


def _validate_pattern(pattern: str, *, label: str) -> str:
    if len(pattern) > _MAX_PATTERN_LENGTH:
        raise ValueError(
            f"{label} pattern too long: {len(pattern)} > {_MAX_PATTERN_LENGTH}"
        )
    return pattern


def _copy_line(line: LogLine, *, content: str | None = None) -> LogLine:
    metadata = dict(line.metadata or {})
    return replace(
        line,
        content=line.content if content is None else content,
        metadata=metadata,
    )


def _coerce_level(raw: str | None) -> int:
    if raw is None:
        return _UNKNOWN_LEVEL
    return _LEVEL_ORDER.get(raw.lower(), _UNKNOWN_LEVEL)


def _detect_level(line: LogLine) -> tuple[int, str | None]:
    if line.level is not None:
        level_text = str(line.level).strip().lower()
        return _coerce_level(level_text), level_text

    match = _CONTENT_LEVEL_RE.search(line.content)
    if match is None:
        return _UNKNOWN_LEVEL, None
    level_text = match.group(1).lower()
    return _coerce_level(level_text), level_text


def _compile_patterns(patterns: list[Any], *, label: str) -> list[re.Pattern[str]]:
    compiled: list[re.Pattern[str]] = []
    for pattern in patterns:
        if not isinstance(pattern, str):
            raise TypeError(f"{label} patterns must be strings")
        compiled.append(re.compile(_validate_pattern(pattern, label=label)))
    return compiled


def _match_any(patterns: list[re.Pattern[str]], text: str) -> bool:
    return any(pattern.search(text) is not None for pattern in patterns)


def _normalize_redact_rules(raw: Any) -> list[Any]:
    if raw is None:
        return []
    if isinstance(raw, tuple):
        raw = list(raw)
    if not isinstance(raw, list):
        raise TypeError("redact must be a list of rules")
    normalized: list[Any] = []
    for item in raw:
        if isinstance(item, dict):
            pattern = item.get("pattern")
            if not isinstance(pattern, str):
                raise TypeError("redact rule dict must contain string pattern")
            replace_text = item.get("replace", "")
            if not isinstance(replace_text, str):
                raise TypeError("redact rule replace must be a string")
            normalized.append(
                {
                    "pattern": _validate_pattern(pattern, label="redact"),
                    "replace": replace_text,
                }
            )
            continue
        if isinstance(item, (list, tuple)) and len(item) == 2:
            pattern, replace_text = item
            if not isinstance(pattern, str) or not isinstance(replace_text, str):
                raise TypeError("redact tuple rules must be (str, str)")
            normalized.append(
                (
                    _validate_pattern(pattern, label="redact"),
                    replace_text,
                )
            )
            continue
        raise TypeError("redact rules must be dicts or (pattern, replace) tuples")
    return normalized


def _clone_with_metadata(
    line: LogLine,
    *,
    content: str | None = None,
    metadata_updates: dict[str, Any] | None = None,
) -> LogLine:
    metadata = dict(line.metadata or {})
    if metadata_updates:
        metadata.update(metadata_updates)
    return replace(
        line,
        content=line.content if content is None else content,
        metadata=metadata,
    )


def _scan_for_forbidden_keys(value: Any, *, path: str = "pipeline") -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            key_text = str(key)
            key_lower = key_text.lower()
            if key_lower in _REMOTE_FORBIDDEN_CONFIG_KEYS:
                raise ValueError(f"remote pipeline forbids key: {path}.{key_text}")
            _scan_for_forbidden_keys(item, path=f"{path}.{key_text}")
        return
    if isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _scan_for_forbidden_keys(item, path=f"{path}[{index}]")


def _validate_string_list(value: Any, *, label: str) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        raise TypeError(f"{label} must be a list of strings, not str")
    if not isinstance(value, (list, tuple)):
        raise TypeError(f"{label} must be a list of strings")
    items: list[str] = []
    for item in value:
        if not isinstance(item, str):
            raise TypeError(f"{label} patterns must be strings")
        items.append(_validate_pattern(item, label=label))
    return items


def _coerce_optional_int(value: Any, *, label: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise TypeError(f"{label} must be an integer")
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if value.is_integer():
            return int(value)
        raise TypeError(f"{label} must be an integer")
    if isinstance(value, str):
        stripped = value.strip()
        if stripped == "":
            return None
        try:
            return int(stripped)
        except ValueError as exc:
            raise TypeError(f"{label} must be an integer") from exc
    raise TypeError(f"{label} must be an integer")


def _validate_custom_alerts(value: Any) -> list[dict[str, Any]]:
    if value is None:
        return []
    if not isinstance(value, (list, tuple)):
        raise TypeError("custom_alerts must be a list of rules")
    out: list[dict[str, Any]] = []
    for item in value:
        if not isinstance(item, dict):
            raise TypeError("custom_alerts rules must be dicts")
        pattern = item.get("pattern")
        if not isinstance(pattern, str):
            raise TypeError("custom_alerts rule dict must contain string pattern")
        category = item.get("category")
        if category is not None and not isinstance(category, str):
            raise TypeError("custom_alerts rule category must be str or null")
        normalized_item = dict(item)
        normalized_item["pattern"] = _validate_pattern(pattern, label="custom_alerts")
        out.append(normalized_item)
    return out


def _validate_alert_keywords(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        raise TypeError("alert_keywords must be a list of strings, not str")
    if not isinstance(value, (list, tuple)):
        raise TypeError("alert_keywords must be a list of strings")
    out: list[str] = []
    for item in value:
        if not isinstance(item, str):
            raise TypeError("alert_keywords items must be str")
        out.append(item)
    return out


def validate_remote_pipeline_config(config: dict[str, Any] | None) -> dict[str, Any]:
    if config is None:
        return {}
    if not isinstance(config, dict):
        raise TypeError("remote pipeline config must be an object")

    _scan_for_forbidden_keys(config)
    unknown_keys = [
        str(key)
        for key in config
        if str(key) not in _REMOTE_ALLOWED_CONFIG_KEYS
    ]
    if unknown_keys:
        unknown_keys.sort()
        raise ValueError(
            "remote pipeline contains unsupported keys: " + ", ".join(unknown_keys)
        )

    validated: dict[str, Any] = {}
    for raw_key, raw_value in config.items():
        key = str(raw_key)
        if key in {"include", "exclude"}:
            validated[key] = _validate_string_list(raw_value, label=key)
            continue
        if key == "min_level":
            if raw_value is not None and not isinstance(raw_value, str):
                raise TypeError("min_level must be a string")
            validated[key] = raw_value
            continue
        if key == "redact":
            validated[key] = _normalize_redact_rules(raw_value)
            continue
        if key == "custom_alerts":
            validated[key] = _validate_custom_alerts(raw_value)
            continue
        if key == "alert_keywords":
            validated[key] = _validate_alert_keywords(raw_value)
            continue
        if key in {"head", "tail", "dedup_window"}:
            coerced = _coerce_optional_int(raw_value, label=key)
            if coerced is not None:
                validated[key] = coerced
            continue
        validated[key] = raw_value

    return validated


def _apply_head_tail(lines: list[LogLine], head: int, tail: int) -> tuple[list[LogLine], int]:
    threshold = head + tail
    if len(lines) <= threshold:
        return list(lines), 0
    head_lines = lines[:head]
    tail_lines = lines[-tail:]
    dropped = len(lines) - threshold
    boundary = lines[head]
    marker = _clone_with_metadata(
        boundary,
        content=f"[... 中间省略 {dropped} 条日志 ...]",
        metadata_updates={
            "head_tail_marker": True,
            "dropped": dropped,
        },
    )
    return head_lines + [marker] + tail_lines, dropped


@dataclass(slots=True)
class RemoteWorkerPipelineStats:
    received: int = 0
    included: int = 0
    excluded: int = 0
    level_dropped: int = 0
    deduped: int = 0
    head_tail_dropped: int = 0
    emitted: int = 0
    buffered_dropped: int = 0


class ContainerRingBuffer:
    def __init__(self, capacity: int) -> None:
        normalized_capacity = int(capacity)
        if normalized_capacity <= 0:
            raise ValueError("capacity must be > 0")
        self._capacity = normalized_capacity
        self._buffers: dict[str, deque[LogLine]] = {}
        self._dropped: dict[str, int] = {}

    @property
    def capacity(self) -> int:
        return self._capacity

    def append(self, line: LogLine) -> None:
        buffer = self._buffers.setdefault(line.container_id, deque())
        if len(buffer) >= self._capacity:
            buffer.popleft()
            self._dropped[line.container_id] = self._dropped.get(line.container_id, 0) + 1
        buffer.append(_copy_line(line))

    def extend(self, lines: list[LogLine]) -> None:
        for line in lines:
            self.append(line)

    def snapshot(self, container_id: str) -> list[LogLine]:
        return [_copy_line(line) for line in self._buffers.get(container_id, ())]

    def dropped(self, container_id: str) -> int:
        return self._dropped.get(container_id, 0)

    def total_dropped(self) -> int:
        return sum(self._dropped.values())


class RemoteWorkerPipeline:
    def __init__(
        self,
        config: dict[str, Any] | None = None,
        *,
        buffer_capacity: int = 128,
        dedup_window: int = 32,
    ) -> None:
        cfg = dict(config or {})
        dedup_window_cfg = cfg.pop("dedup_window", dedup_window)
        rules = cfg.get("rules")
        rules_cfg = rules if isinstance(rules, dict) else {}

        def _cfg(name: str, default: Any = None) -> Any:
            if name in cfg:
                return cfg[name]
            if name in rules_cfg:
                return rules_cfg[name]
            return default

        self._include_patterns = _compile_patterns(
            list(_cfg("include") or []), label="include"
        )
        self._exclude_patterns = _compile_patterns(
            list(_cfg("exclude") or []), label="exclude"
        )
        self._min_level = self._resolve_min_level(_cfg("min_level", "warn"))
        self._redact_rules = _normalize_redact_rules(_cfg("redact"))
        self._custom_alerts = _cfg("custom_alerts")
        self._alert_keywords = _cfg("alert_keywords")
        self._head = max(1, int(_cfg("head", 20)))
        self._tail = max(1, int(_cfg("tail", 20)))
        self._dedup_window = max(1, int(dedup_window_cfg))
        self.buffer = ContainerRingBuffer(buffer_capacity)
        self.last_stats = RemoteWorkerPipelineStats()
        self._dedup_history: dict[str, deque[tuple[str, str, str]]] = {}

    def classify(self, line: LogLine) -> RuleResult:
        return apply_rules(
            line.content,
            {
                "rules": {
                    "redact": self._redact_rules,
                    "custom_alerts": self._custom_alerts,
                    "alert_keywords": self._alert_keywords,
                }
            },
        )

    def process(self, lines: list[LogLine]) -> list[LogLine]:
        stats = RemoteWorkerPipelineStats()
        selected: list[LogLine] = []

        for line in lines:
            stats.received += 1
            if self._include_patterns and not _match_any(self._include_patterns, line.content):
                stats.excluded += 1
                continue
            if self._exclude_patterns and _match_any(self._exclude_patterns, line.content):
                stats.excluded += 1
                continue
            level_order, _ = _detect_level(line)
            if level_order != _UNKNOWN_LEVEL and level_order < self._min_level:
                stats.level_dropped += 1
                continue

            stats.included += 1
            processed = self._apply_redaction_and_classify(line)
            if self._is_duplicate(processed):
                stats.deduped += 1
                continue
            self._remember(processed)
            selected.append(processed)

        selected, dropped = _apply_head_tail(selected, self._head, self._tail)
        stats.head_tail_dropped = dropped
        before = self.buffer.total_dropped()
        self.buffer.extend(selected)
        stats.buffered_dropped = self.buffer.total_dropped() - before
        stats.emitted = len(selected)
        self.last_stats = stats
        return selected

    def _resolve_min_level(self, raw: Any) -> int:
        level_text = str(raw).strip().lower()
        if level_text not in _LEVEL_ORDER:
            logger.warning(
                "remote_worker_pipeline: unrecognized min_level %r, falling back to 'warn'",
                level_text,
            )
        return _LEVEL_ORDER.get(level_text, _LEVEL_ORDER["warn"])

    def _apply_redaction_and_classify(self, line: LogLine) -> LogLine:
        result = self.classify(line)
        level_order, level_text = _detect_level(line)
        metadata = {
            "worker_redacted": result.redacted_line,
            "triggered": result.triggered,
            "matched_category": result.matched_category,
            "detected_level": level_text,
            "detected_level_order": level_order,
        }
        return _clone_with_metadata(line, content=result.redacted_line, metadata_updates=metadata)

    def _is_duplicate(self, line: LogLine) -> bool:
        fingerprint = self._dedup_fingerprint(line)
        history = self._dedup_history.setdefault(
            line.container_id,
            deque(maxlen=self._dedup_window),
        )
        return fingerprint in history

    def _remember(self, line: LogLine) -> None:
        fingerprint = self._dedup_fingerprint(line)
        history = self._dedup_history.setdefault(
            line.container_id,
            deque(maxlen=self._dedup_window),
        )
        history.append(fingerprint)

    def _dedup_fingerprint(self, line: LogLine) -> tuple[str, str, str]:
        metadata = line.metadata or {}
        level_text = str(metadata.get("detected_level") or "").strip().lower()
        category = str(metadata.get("matched_category") or "").strip().upper()
        return (level_text, line.content, category)


__all__ = [
    "ContainerRingBuffer",
    "RemoteWorkerPipeline",
    "RemoteWorkerPipelineStats",
    "validate_remote_pipeline_config",
]
