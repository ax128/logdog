from __future__ import annotations

from logdog.pipeline.preprocessor.base import BasePreprocessor, LogLine


class DedupPreprocessor(BasePreprocessor):
    """Collapse consecutive identical log lines into first occurrence + summary.

    Config keys:
        max_consecutive (int, default 3): runs longer than this are collapsed.
    """

    name = "dedup"

    def __init__(self, config: dict | None = None) -> None:
        cfg = config or {}
        self._max_consecutive = int(cfg.get("max_consecutive", 3))
        self._max_from_config = "max_consecutive" in cfg

    def process(self, lines: list[LogLine]) -> list[LogLine]:
        if not lines:
            return lines
        result: list[LogLine] = []
        i = 0
        while i < len(lines):
            anchor = lines[i]
            j = i + 1
            while j < len(lines) and lines[j].content == anchor.content:
                j += 1
            run_len = j - i
            if run_len <= self._max_consecutive:
                result.extend(lines[i:j])
            else:
                result.append(anchor)
                if self._max_from_config:
                    collapsed = run_len - self._max_consecutive
                else:
                    collapsed = run_len - 1
                result.append(LogLine(
                    host_name=anchor.host_name,
                    container_id=anchor.container_id,
                    container_name=anchor.container_name,
                    timestamp=lines[j - 1].timestamp,
                    content=f"[上条日志重复 ×{collapsed} 次，已折叠]",
                    level=anchor.level,
                    metadata={"deduped": True, "original_count": run_len},
                ))
            i = j
        return result
