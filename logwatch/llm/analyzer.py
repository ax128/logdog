from __future__ import annotations

import logging
from typing import Any

from logwatch.llm.agent_runtime import AgentRuntime, build_analyzer_runtime
from logwatch.llm.prompts.base import BasePromptTemplate
from logwatch.llm.prompts.loader import load_template


_REASON_MISSING_REQUIRED = "missing_required"
_REASON_EMPTY_RENDER = "empty_render"
_REASON_TEMPLATE_ERROR = "template_error"
logger = logging.getLogger(__name__)


def analyze_with_template(
    scene: str,
    context: dict[str, Any] | None,
    template: str | BasePromptTemplate,
    *,
    agent_runtime: AgentRuntime | None = None,
    runtime_factory: Any | None = None,
    enable_agent: bool = True,
) -> str:
    """
    Render prompt with template and downgrade to plaintext fallback on failures.

    Behavior:
    - Missing required vars -> returns fallback text starting with "[FALLBACK]"
    - Blank render -> fallback
    - Load/render exceptions -> fallback
    - Otherwise -> rendered text
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
    )
    if runtime is None:
        return plaintext

    try:
        return runtime.invoke_text(plaintext, fallback=plaintext)
    except Exception:  # noqa: BLE001 - preserve plaintext path on runtime errors
        logger.warning(
            "analyzer deepagents invoke failed scene=%s", scene, exc_info=True
        )
        return plaintext


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
    return build_analyzer_runtime()
