from __future__ import annotations

from typing import Any

from logwatch.llm.agent_runtime import (
    DEFAULT_CHAT_FALLBACK_MESSAGE,
    build_chat_runtime,
    build_thread_id,
)


class _RecordingAgent:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def invoke(
        self, payload: dict[str, Any], config: dict[str, Any] | None = None
    ) -> dict[str, str]:
        self.calls.append({"payload": payload, "config": config})
        content = payload["messages"][0].content if "messages" in payload else payload["input"]
        return {"output": f"agent::{content}"}


def test_chat_runtime_builds_with_registry_tools_and_stable_thread_id() -> None:
    captured: dict[str, Any] = {}
    agent = _RecordingAgent()
    sentinel_checkpointer = object()
    tool_one = object()
    tool_two = object()

    def runtime_factory(**kwargs: Any) -> _RecordingAgent:
        captured.update(kwargs)
        return agent

    runtime = build_chat_runtime(
        tool_registry={"list_hosts": tool_one, "query_logs": tool_two},
        runtime_factory=runtime_factory,
        checkpointer_factory=lambda: sentinel_checkpointer,
    )

    first = runtime.invoke_text(
        "ping",
        user_id="ws-user",
        session_key="browser-tab-1",
        fallback=DEFAULT_CHAT_FALLBACK_MESSAGE,
    )
    second = runtime.invoke_text(
        "pong",
        user_id="ws-user",
        session_key="browser-tab-1",
        fallback=DEFAULT_CHAT_FALLBACK_MESSAGE,
    )

    assert first == "agent::ping"
    assert second == "agent::pong"
    assert captured["tools"] == [tool_one, tool_two]
    assert captured["checkpointer"] is sentinel_checkpointer
    assert agent.calls[0]["config"]["configurable"]["thread_id"] == build_thread_id(
        user_id="ws-user",
        session_key="browser-tab-1",
    )
    assert (
        agent.calls[0]["config"]["configurable"]["thread_id"]
        == agent.calls[1]["config"]["configurable"]["thread_id"]
    )


def test_chat_runtime_returns_bounded_fallback_when_creation_fails() -> None:
    def broken_runtime_factory(**_: Any) -> Any:
        raise RuntimeError("no provider")

    runtime = build_chat_runtime(
        tool_registry={},
        runtime_factory=broken_runtime_factory,
    )

    out = runtime.invoke_text(
        "ping",
        user_id="ws-user",
        session_key="browser-tab-1",
        fallback=DEFAULT_CHAT_FALLBACK_MESSAGE,
    )

    assert out == DEFAULT_CHAT_FALLBACK_MESSAGE
    assert len(out) <= 120


def test_chat_runtime_returns_bounded_fallback_when_agent_raises() -> None:
    class _RaisingAgent:
        def invoke(
            self, payload: dict[str, Any], config: dict[str, Any] | None = None
        ) -> dict[str, Any]:
            raise RuntimeError("boom")

    runtime = build_chat_runtime(
        tool_registry={},
        runtime_factory=lambda **_: _RaisingAgent(),
        checkpointer_factory=lambda: object(),
    )

    out = runtime.invoke_text(
        "ping",
        user_id="ws-user",
        session_key="browser-tab-1",
        fallback=DEFAULT_CHAT_FALLBACK_MESSAGE,
    )

    assert out == DEFAULT_CHAT_FALLBACK_MESSAGE
    assert len(out) <= 120


def test_wrap_registered_tool_uses_lc_tool_with_schema() -> None:
    """Verify wrapping produces a LangChain tool with args_schema from TOOL_META."""
    from logwatch.llm.agent_runtime import _wrap_registered_tool

    class _FakeTool:
        name = "list_containers"
        description = "List containers for a host."
        read_only = True

        async def invoke(self, *, user_id: str, arguments: dict) -> dict:
            return {"containers": []}

    wrapped = _wrap_registered_tool(_FakeTool())
    assert wrapped.name == "list_containers"
    assert wrapped.args_schema is not None
    assert "host" in wrapped.args_schema.model_fields
