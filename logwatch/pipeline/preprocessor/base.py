from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class LogLine:
    host_name: str
    container_id: str
    container_name: str
    timestamp: str
    content: str
    level: str | None = None
    metadata: dict | None = None


class BasePreprocessor(ABC):
    name = "base"

    @abstractmethod
    def process(self, lines: list[LogLine]) -> list[LogLine]:
        raise NotImplementedError

    def on_load(self) -> None:
        return None
