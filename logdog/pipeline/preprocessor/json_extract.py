from __future__ import annotations

import json

from logdog.pipeline.preprocessor.base import BasePreprocessor, LogLine


_DEFAULT_FIELDS = ["level", "message", "msg", "error", "trace_id"]


def _try_parse_json(text: str) -> dict | None:
    stripped = text.strip()
    if not stripped.startswith("{"):
        return None
    try:
        parsed = json.loads(stripped)
        return parsed if isinstance(parsed, dict) else None
    except (json.JSONDecodeError, ValueError):
        return None


class JsonExtractPreprocessor(BasePreprocessor):
    """Parse JSON log lines and reformat as readable 'field=value' text.

    Config keys:
        fields (list[str], default ["level","message","msg","error","trace_id"]):
            JSON keys to extract, in order. Missing keys silently skipped.

    Non-JSON lines pass through unchanged.
    LogLine.level is backfilled from JSON only when not already set.
    """

    name = "json_extract"

    def __init__(self, config: dict | None = None) -> None:
        cfg = config or {}
        raw = cfg.get("fields", _DEFAULT_FIELDS)
        self._fields: list[str] = list(raw) if isinstance(raw, (list, tuple)) else list(_DEFAULT_FIELDS)

    def process(self, lines: list[LogLine]) -> list[LogLine]:
        result = []
        for line in lines:
            parsed = _try_parse_json(line.content)
            if parsed is None:
                result.append(line)
                continue
            parts = [
                f"{f}={parsed[f]}"
                for f in self._fields
                if f in parsed and str(parsed[f]).strip()
            ]
            json_level = str(parsed.get("level") or "").strip().lower() or None
            result.append(LogLine(
                host_name=line.host_name,
                container_id=line.container_id,
                container_name=line.container_name,
                timestamp=line.timestamp,
                content=" ".join(parts) if parts else line.content,
                level=line.level if line.level else json_level,
                metadata={**(line.metadata or {}), "json_parsed": True},
            ))
        return result
