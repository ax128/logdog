"""Regex Extract Preprocessor
=========================

Structured extraction for non-JSON logs using named capture groups.

Config example:

    preprocessors:
      - name: regex_extract
        patterns:
          - "level=(?P<level>\\w+)\\s+req=(?P<request_id>\\S+)\\s+msg=\"(?P<message>[^\"]+)\""
          - "\\[(?P<level>ERROR|WARN|INFO)\\]\\s+(?P<message>.*)"
        fields: [level, request_id, message]
        include_extra_fields: true

Behavior:
- First matching regex wins for each line.
- Uses named groups as extracted fields.
- If `level` group exists, backfills LogLine.level (when absent).
"""

from __future__ import annotations

import logging
import re

from logdog.pipeline.preprocessor.base import BasePreprocessor, LogLine


logger = logging.getLogger(__name__)

_KNOWN_LEVELS = frozenset(
    {
        "debug",
        "info",
        "warn",
        "warning",
        "error",
        "fatal",
        "panic",
        "critical",
    }
)


class RegexExtractPreprocessor(BasePreprocessor):
    name = "regex_extract"

    def __init__(self, config: dict | None = None) -> None:
        cfg = config or {}
        raw_patterns = cfg.get("patterns", [])
        if not isinstance(raw_patterns, (list, tuple)):
            raw_patterns = []
        self._compiled: list[re.Pattern[str]] = []
        for pattern in raw_patterns:
            text = str(pattern).strip()
            if text == "":
                continue
            try:
                self._compiled.append(re.compile(text))
            except re.error as exc:
                logger.warning("regex_extract: skip invalid pattern %r (%s)", text, exc)

        raw_fields = cfg.get("fields", ["level", "message", "error", "trace_id"])
        if isinstance(raw_fields, (list, tuple)):
            self._fields = [str(item).strip() for item in raw_fields if str(item).strip()]
        else:
            self._fields = ["level", "message", "error", "trace_id"]

        self._include_extra_fields = bool(cfg.get("include_extra_fields", False))

    def process(self, lines: list[LogLine]) -> list[LogLine]:
        if not self._compiled:
            return list(lines)

        out: list[LogLine] = []
        for line in lines:
            extracted = self._extract_one(line.content)
            if extracted is None:
                out.append(line)
                continue

            keys = [k for k in self._fields if k in extracted]
            if self._include_extra_fields:
                keys.extend([k for k in extracted if k not in set(keys)])
            rendered = " ".join(
                f"{k}={extracted[k]}"
                for k in keys
                if extracted[k] is not None and str(extracted[k]).strip() != ""
            ).strip()

            level = line.level
            if level is None:
                candidate = str(extracted.get("level") or "").strip().lower()
                if candidate in _KNOWN_LEVELS:
                    level = candidate

            out.append(
                LogLine(
                    host_name=line.host_name,
                    container_id=line.container_id,
                    container_name=line.container_name,
                    timestamp=line.timestamp,
                    content=rendered or line.content,
                    level=level,
                    metadata={**(line.metadata or {}), "regex_parsed": True},
                )
            )
        return out

    def _extract_one(self, content: str) -> dict[str, str] | None:
        for pattern in self._compiled:
            match = pattern.search(content)
            if not match:
                continue
            groups = {
                key: str(value)
                for key, value in match.groupdict().items()
                if key and value is not None
            }
            if groups:
                return groups
        return None


PREPROCESSOR = RegexExtractPreprocessor
