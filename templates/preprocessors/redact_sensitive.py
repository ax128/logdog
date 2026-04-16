"""Sensitive Data Redaction Preprocessor
=====================================

Mask common secrets in logs before alert analysis / notification.

Config example:

    preprocessors:
      - name: redact_sensitive
        mask: "***"
        redact_ipv4: true
        redact_email: true
        extra_patterns:
          - "(?i)session_id=[A-Za-z0-9_-]+"

This preprocessor is fail-open: if a regex is invalid, it is skipped.
"""

from __future__ import annotations

import re

from logdog.pipeline.preprocessor.base import BasePreprocessor, LogLine


_DEFAULT_PATTERNS = [
    r"(?i)(authorization\s*:\s*bearer\s+)[^\s]+",
    r"(?i)(api[_-]?key\s*[=:]\s*)[^\s,;]+",
    r"(?i)(token\s*[=:]\s*)[^\s,;]+",
    r"(?i)(password\s*[=:]\s*)[^\s,;]+",
]
_IPV4_PATTERN = r"\b(?:\d{1,3}\.){3}\d{1,3}\b"
_EMAIL_PATTERN = r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"


class RedactSensitivePreprocessor(BasePreprocessor):
    name = "redact_sensitive"

    def __init__(self, config: dict | None = None) -> None:
        cfg = config or {}
        self._mask = str(cfg.get("mask") or "***")

        patterns = list(_DEFAULT_PATTERNS)
        if bool(cfg.get("redact_ipv4", True)):
            patterns.append(_IPV4_PATTERN)
        if bool(cfg.get("redact_email", True)):
            patterns.append(_EMAIL_PATTERN)

        raw_extra = cfg.get("extra_patterns", [])
        if isinstance(raw_extra, (list, tuple)):
            for item in raw_extra:
                text = str(item).strip()
                if text:
                    patterns.append(text)

        self._compiled: list[re.Pattern[str]] = []
        for text in patterns:
            try:
                self._compiled.append(re.compile(text))
            except re.error:
                continue

    def process(self, lines: list[LogLine]) -> list[LogLine]:
        out: list[LogLine] = []
        for line in lines:
            masked = line.content
            for pattern in self._compiled:
                if pattern.groups >= 1:
                    masked = pattern.sub(r"\1" + self._mask, masked)
                else:
                    masked = pattern.sub(self._mask, masked)

            if masked == line.content:
                out.append(line)
                continue

            out.append(
                LogLine(
                    host_name=line.host_name,
                    container_id=line.container_id,
                    container_name=line.container_name,
                    timestamp=line.timestamp,
                    content=masked,
                    level=line.level,
                    metadata={**(line.metadata or {}), "redacted": True},
                )
            )
        return out


PREPROCESSOR = RedactSensitivePreprocessor
