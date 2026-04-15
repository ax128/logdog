from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ToolContext:
    """Tool execution context."""

    session_id: str = ""
    user_id: str = ""
    config: dict[str, Any] = field(default_factory=dict)


@dataclass
class ToolResult:
    """Tool execution result."""

    content: str = ""
    status: str = "ok"
    is_error: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def ok(cls, content: str = "", **kwargs: Any) -> ToolResult:
        return cls(content=content, is_error=False, metadata=kwargs)

    @classmethod
    def error(cls, content: str = "", **kwargs: Any) -> ToolResult:
        return cls(content=content, is_error=True, metadata=kwargs)
