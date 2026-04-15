from __future__ import annotations

from logdog.pipeline.preprocessor.base import BasePreprocessor, LogLine


class HeadTailPreprocessor(BasePreprocessor):
    """Keep first N and last N lines; insert a marker when lines are dropped.

    Config keys:
        head (int, default 20): lines to keep from the start.
        tail (int, default 20): lines to keep from the end.

    When len(lines) <= head + tail the batch is returned unchanged.
    """

    name = "head_tail"

    def __init__(self, config: dict | None = None) -> None:
        cfg = config or {}
        self._head = max(1, int(cfg.get("head", 20)))
        self._tail = max(1, int(cfg.get("tail", 20)))

    def process(self, lines: list[LogLine]) -> list[LogLine]:
        threshold = self._head + self._tail
        if len(lines) <= threshold:
            return lines
        head_lines = lines[: self._head]
        tail_lines = lines[-self._tail :]
        dropped = len(lines) - threshold
        boundary = lines[self._head]
        marker = LogLine(
            host_name=boundary.host_name,
            container_id=boundary.container_id,
            container_name=boundary.container_name,
            timestamp=boundary.timestamp,
            content=f"[... 中间省略 {dropped} 条日志 ...]",
            level=None,
            metadata={"head_tail_marker": True, "dropped": dropped},
        )
        return head_lines + [marker] + tail_lines
