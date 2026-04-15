from __future__ import annotations

from typing import Any

from logwatch.llm.analyzer import analyze_with_template
from logwatch.llm.prompts.base import BasePromptTemplate


class _StaticTemplate(BasePromptTemplate):
    def render(self, context: dict[str, Any]) -> str:
        return f"prompt::{context['host_name']}::{context['container_name']}"


class _RecordingRuntime:
    def __init__(
        self, response: str = "agent-answer", *, error: Exception | None = None
    ):
        self.response = response
        self.error = error
        self.calls: list[dict[str, str]] = []

    def invoke_text(
        self,
        prompt: str,
        *,
        user_id: str | None = None,
        session_key: str | None = None,
        fallback: str,
    ) -> str:
        self.calls.append(
            {
                "prompt": prompt,
                "user_id": str(user_id or ""),
                "session_key": str(session_key or ""),
                "fallback": fallback,
            }
        )
        if self.error is not None:
            raise self.error
        return self.response


def _alert_context() -> dict[str, Any]:
    return {
        "host_name": "prod-a",
        "container_name": "api",
        "timestamp": "2026-04-11T10:00:00Z",
        "logs": ["line1"],
    }


def test_analyzer_prefers_agent_runtime_response_when_available() -> None:
    runtime = _RecordingRuntime(response="agent-summary")

    out = analyze_with_template(
        "alert",
        _alert_context(),
        _StaticTemplate(),
        agent_runtime=runtime,
    )

    assert out == "agent-summary"
    assert runtime.calls == [
        {
            "prompt": "prompt::prod-a::api",
            "user_id": "",
            "session_key": "",
            "fallback": "prompt::prod-a::api",
        }
    ]


def test_analyzer_returns_rendered_prompt_when_runtime_invocation_raises() -> None:
    runtime = _RecordingRuntime(error=RuntimeError("boom"))

    out = analyze_with_template(
        "alert",
        _alert_context(),
        _StaticTemplate(),
        agent_runtime=runtime,
    )

    assert out == "prompt::prod-a::api"


def test_analyzer_returns_rendered_prompt_when_runtime_factory_fails() -> None:
    def broken_runtime_factory():
        raise RuntimeError("deepagents unavailable")

    out = analyze_with_template(
        "alert",
        _alert_context(),
        _StaticTemplate(),
        runtime_factory=broken_runtime_factory,
    )

    assert out == "prompt::prod-a::api"
