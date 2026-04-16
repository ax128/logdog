from __future__ import annotations

import logging
from typing import Any

from logdog.llm.agent_runtime import (
    AgentRuntime,
    DEFAULT_CHAT_FALLBACK_MESSAGE,
    build_analyzer_runtime,
)
from logdog.llm.prompts.base import BasePromptTemplate
from logdog.llm.prompts.loader import load_template


_REASON_MISSING_REQUIRED = "missing_required"
_REASON_EMPTY_RENDER = "empty_render"
_REASON_TEMPLATE_ERROR = "template_error"
_SENTINEL = object()
logger = logging.getLogger(__name__)


def analyze_with_template(
    scene: str,
    context: dict[str, Any] | None,
    template: str | BasePromptTemplate,
    *,
    agent_runtime: AgentRuntime | None = None,
    runtime_factory: Any | None = None,
    enable_agent: bool = True,
    model: str | None = None,
    api_base: str | None = None,
    api_key: str | None = None,
    provider_type: str | None = None,
    agent_fallback: Any = _SENTINEL,
) -> str | None:
    """
    Render prompt with template and downgrade to plaintext fallback on failures.

    Behavior:
    - Missing required vars -> returns fallback text starting with "[FALLBACK]"
    - Blank render -> fallback
    - Load/render exceptions -> fallback
    - enable_agent=True -> invoke LLM agent with rendered prompt
    - agent_fallback: controls what to return when LLM fails.
        - _SENTINEL (default): return plaintext (backward compat)
        - None: return None so callers can detect and handle failure
        - str: return that string
    """

    ctx = dict(context or {})
    ctx.setdefault("scene", scene)

    try:
        tmpl = (
            template
            if isinstance(template, BasePromptTemplate)
            else load_template(str(template))
        )
    except Exception:  # noqa: BLE001 - do not leak exception details to caller
        return _fallback(scene, reason=_REASON_TEMPLATE_ERROR)

    try:
        missing = tmpl.validate(scene, ctx)
    except Exception:  # noqa: BLE001 - do not leak exception details to caller
        return _fallback(scene, reason=_REASON_TEMPLATE_ERROR)

    if missing:
        return _fallback(scene, reason=_REASON_MISSING_REQUIRED, missing=missing)

    try:
        rendered = tmpl.render(ctx)
    except Exception:  # noqa: BLE001 - do not leak exception details to caller
        return _fallback(scene, reason=_REASON_TEMPLATE_ERROR)

    if rendered is None or str(rendered).strip() == "":
        return _fallback(scene, reason=_REASON_EMPTY_RENDER)

    plaintext = str(rendered)
    if not bool(enable_agent):
        return plaintext

    runtime = _resolve_agent_runtime(
        agent_runtime=agent_runtime,
        runtime_factory=runtime_factory,
        model=model,
        api_base=api_base,
        api_key=api_key,
        provider_type=provider_type,
    )
    if runtime is None:
        return _agent_failure_fallback(agent_fallback, plaintext)

    if agent_fallback is _SENTINEL:
        # Legacy behavior: pass plaintext as fallback to invoke_text
        try:
            return runtime.invoke_text(plaintext, fallback=plaintext)
        except Exception:  # noqa: BLE001 - preserve fallback path on runtime errors
            logger.warning(
                "analyzer deepagents invoke failed scene=%s", scene, exc_info=True
            )
            return plaintext

    # New behavior: detect LLM failure via sentinel fallback
    _llm_fail_sentinel = DEFAULT_CHAT_FALLBACK_MESSAGE
    try:
        result = runtime.invoke_text(plaintext, fallback=_llm_fail_sentinel)
    except Exception:  # noqa: BLE001 - preserve fallback path on runtime errors
        logger.warning(
            "analyzer deepagents invoke failed scene=%s", scene, exc_info=True
        )
        return agent_fallback

    if result == _llm_fail_sentinel:
        return agent_fallback
    return result


def _agent_failure_fallback(agent_fallback: Any, plaintext: str) -> str | None:
    """Return the appropriate value when the LLM agent fails."""
    if agent_fallback is _SENTINEL:
        return plaintext
    return agent_fallback


def _fallback(scene: str, *, reason: str, missing: list[str] | None = None) -> str:
    logger.warning(
        "prompt fallback reason=%s scene=%s missing=%s",
        reason,
        scene,
        ",".join(missing) if missing else "",
    )
    msg = f"[FALLBACK] reason={reason} scene={scene}"
    if missing:
        msg += f" missing={','.join(missing)}"
    return msg


def _resolve_agent_runtime(
    *,
    agent_runtime: AgentRuntime | None,
    runtime_factory: Any | None,
    model: str | None = None,
    api_base: str | None = None,
    api_key: str | None = None,
    provider_type: str | None = None,
) -> AgentRuntime | None:
    if agent_runtime is not None:
        return agent_runtime
    if runtime_factory is not None:
        try:
            runtime = runtime_factory()
        except Exception:  # noqa: BLE001 - analyzer must preserve plaintext fallback
            logger.warning("analyzer runtime factory failed", exc_info=True)
            return None
        return runtime
    return build_analyzer_runtime(model=model, api_base=api_base, api_key=api_key, provider_type=provider_type)
