from __future__ import annotations

from logwatch.pipeline.preprocessor.base import BasePreprocessor, LogLine


class DefaultPreprocessor(BasePreprocessor):
    name = "default"

    def process(self, lines: list[LogLine]) -> list[LogLine]:
        return lines
