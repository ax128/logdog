from __future__ import annotations

import logging
import re

from logdog.pipeline.preprocessor.base import BasePreprocessor, LogLine


logger = logging.getLogger(__name__)


_LEVEL_ORDER: dict[str, int] = {
    "debug": 0, "info": 1,
    "warn": 2, "warning": 2,
    "error": 3, "fatal": 4, "panic": 4, "critical": 4,
}
_UNKNOWN = -1   # fail-open sentinel
_CONTENT_LEVEL_RE = re.compile(
    r"\b(debug|info|warn(?:ing)?|error|fatal|panic|critical)\b", re.IGNORECASE
)


def _level_order(line: LogLine) -> int:
    if line.level:
        return _LEVEL_ORDER.get(str(line.level).lower(), _UNKNOWN)
    m = _CONTENT_LEVEL_RE.search(line.content)
    if m:
        return _LEVEL_ORDER.get(m.group(1).lower(), _UNKNOWN)
    return _UNKNOWN


class LevelFilterPreprocessor(BasePreprocessor):
    """Drop log lines below the configured minimum severity.

    Config keys:
        min_level (str, default "warn"): debug | info | warn | error | fatal

    Lines with undetectable severity are kept (fail-open).
    """

    name = "level_filter"

    def __init__(self, config: dict | None = None) -> None:
        cfg = config or {}
        raw = str(cfg.get("min_level", "warn")).lower()
        if raw not in _LEVEL_ORDER:
            logger.warning(
                "level_filter: unrecognized min_level %r, falling back to 'warn'", raw
            )
        self._min_order: int = _LEVEL_ORDER.get(raw, _LEVEL_ORDER["warn"])

    def process(self, lines: list[LogLine]) -> list[LogLine]:
        result = []
        for line in lines:
            order = _level_order(line)
            if order == _UNKNOWN or order >= self._min_order:
                result.append(line)
        return result
