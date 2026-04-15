from __future__ import annotations

import time
from typing import Any
from unittest.mock import MagicMock

from logdog.llm.agent_runtime import (
    DEFAULT_CHAT_FALLBACK_MESSAGE,
    _BoundedCheckpointer,
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
    # _build_checkpointer wraps the inner checkpointer in _BoundedCheckpointer
    assert isinstance(captured["checkpointer"], _BoundedCheckpointer)
    assert captured["checkpointer"]._inner is sentinel_checkpointer
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
    from logdog.llm.agent_runtime import _wrap_registered_tool

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


# ---------------------------------------------------------------------------
# _BoundedCheckpointer tests
# ---------------------------------------------------------------------------

def _make_config(thread_id: str) -> dict:
    return {"configurable": {"thread_id": thread_id}}


def _make_inner() -> MagicMock:
    """Return a mock that records delete_thread calls and supports put/put_writes."""
    inner = MagicMock()
    inner.put.side_effect = lambda config, *a, **kw: config
    inner.put_writes.side_effect = lambda config, *a, **kw: config
    inner.delete_thread.return_value = None
    return inner


def test_bounded_checkpointer_tracks_sessions_on_put() -> None:
    inner = _make_inner()
    bc = _BoundedCheckpointer(inner, max_sessions=10, ttl_seconds=3600.0)

    bc.put(_make_config("chat:aaa"))
    bc.put(_make_config("chat:bbb"))

    assert "chat:aaa" in bc._access_times
    assert "chat:bbb" in bc._access_times
    assert len(bc._access_times) == 2


def test_bounded_checkpointer_evicts_over_limit_sessions() -> None:
    inner = _make_inner()
    bc = _BoundedCheckpointer(inner, max_sessions=3, ttl_seconds=3600.0)

    for i in range(5):
        bc.put(_make_config(f"chat:{i:03d}"))

    # At most max_sessions live sessions should remain
    assert len(bc._access_times) <= 3
    # delete_thread should have been called for the excess sessions
    assert inner.delete_thread.call_count >= 2


def test_bounded_checkpointer_evicts_expired_sessions() -> None:
    inner = _make_inner()
    bc = _BoundedCheckpointer(inner, max_sessions=100, ttl_seconds=60.0)

    # Seed two sessions with an old timestamp
    old_time = time.monotonic() - 120.0  # 2 minutes ago → expired
    bc._access_times["chat:old1"] = old_time
    bc._access_times["chat:old2"] = old_time

    # Trigger a periodic eviction scan by writing enough to hit EVICT_EVERY_N
    for i in range(_BoundedCheckpointer._EVICT_EVERY_N):
        bc.put(_make_config(f"chat:new{i}"))

    assert "chat:old1" not in bc._access_times
    assert "chat:old2" not in bc._access_times
    assert inner.delete_thread.call_count >= 2


def test_bounded_checkpointer_delegates_unknown_attrs() -> None:
    inner = _make_inner()
    inner.some_attr = "hello"
    bc = _BoundedCheckpointer(inner, max_sessions=10, ttl_seconds=60.0)

    assert bc.some_attr == "hello"


def test_bounded_checkpointer_ignores_config_without_thread_id() -> None:
    inner = _make_inner()
    bc = _BoundedCheckpointer(inner, max_sessions=10, ttl_seconds=3600.0)

    bc.put({})  # no thread_id → should not crash, nothing tracked
    bc.put({"configurable": {}})

    assert len(bc._access_times) == 0
    inner.delete_thread.assert_not_called()


def test_bounded_checkpointer_put_writes_tracks_session() -> None:
    inner = _make_inner()
    bc = _BoundedCheckpointer(inner, max_sessions=10, ttl_seconds=3600.0)

    bc.put_writes(_make_config("chat:xyz"), [], "checkpoint-1")

    assert "chat:xyz" in bc._access_times
