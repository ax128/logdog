from __future__ import annotations

from logdog.llm.tool_types import ToolContext, ToolResult


def test_tool_result_ok_factory() -> None:
    result = ToolResult.ok("success", extra="value")
    assert result.content == "success"
    assert result.is_error is False
    assert result.status == "ok"
    assert result.metadata == {"extra": "value"}


def test_tool_result_error_factory() -> None:
    result = ToolResult.error("failed", code=42)
    assert result.content == "failed"
    assert result.is_error is True
    assert result.status == "ok"  # status stays "ok", is_error is the flag
    assert result.metadata == {"code": 42}


def test_tool_result_default_empty() -> None:
    result = ToolResult()
    assert result.content == ""
    assert result.is_error is False
    assert result.metadata == {}


def test_tool_context_defaults() -> None:
    ctx = ToolContext()
    assert ctx.user_id == ""
    assert ctx.session_id == ""
    assert ctx.config == {}


def test_tool_context_with_values() -> None:
    ctx = ToolContext(user_id="u1", session_id="s1", config={"key": "val"})
    assert ctx.user_id == "u1"
    assert ctx.session_id == "s1"
    assert ctx.config == {"key": "val"}
