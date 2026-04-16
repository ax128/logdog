"""LLM provider resolution.

Resolves 'provider/model' references against llm.providers config.
Falls back gracefully when providers are not configured.
"""
from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)


class LLMParams:
    """Resolved LLM call parameters."""
    __slots__ = ("model", "api_base", "api_key", "provider_type")

    def __init__(
        self,
        model: str,
        api_base: str = "",
        api_key: str = "",
        provider_type: str = "",
    ) -> None:
        self.model = model
        self.api_base = api_base
        self.api_key = api_key
        self.provider_type = provider_type

    def to_factory_kwargs(self) -> dict[str, str]:
        """Return non-empty params suitable for passing to agent factory."""
        out: dict[str, str] = {}
        if self.model:
            out["model"] = self.model
        if self.api_base:
            out["api_base"] = self.api_base
        if self.api_key:
            out["api_key"] = self.api_key
        return out


def _resolve_provider_api_key(provider_cfg: dict[str, Any]) -> str:
    """Resolve API key from provider config.

    Checks explicit ``api_key`` first, then reads the environment variable
    named in ``api_key_env``.
    """
    explicit = str(provider_cfg.get("api_key") or "").strip()
    if explicit:
        return explicit
    env_var = str(provider_cfg.get("api_key_env") or "").strip()
    if env_var:
        return os.environ.get(env_var, "")
    return ""


def resolve_llm_params(
    model_ref: str | None,
    llm_config: dict[str, Any] | None = None,
) -> LLMParams:
    """Resolve a model reference to LLMParams.

    Formats:
      "provider/model"  -> lookup provider in llm_config["providers"]
      "provider:model"  -> legacy format, pass as-is (no provider lookup)
      "model"           -> plain model name, pass as-is
      None / ""         -> use llm_config["default"] or ["default_model"] or ["model"]
    """
    cfg = dict(llm_config or {})
    providers = cfg.get("providers") or {}

    # Resolve default if no model specified
    ref = str(model_ref or "").strip()
    if not ref:
        ref = str(
            cfg.get("default") or cfg.get("default_model") or cfg.get("model") or ""
        ).strip()
    if not ref:
        return LLMParams("")

    # New format: "provider/model" -> lookup provider
    if "/" in ref:
        provider_name, model_name = ref.split("/", 1)
        provider_name = provider_name.strip()
        model_name = model_name.strip()
        provider_cfg = providers.get(provider_name)
        if isinstance(provider_cfg, dict):
            return LLMParams(
                model=model_name,
                api_base=str(provider_cfg.get("api_base") or "").strip(),
                api_key=_resolve_provider_api_key(provider_cfg),
                provider_type=str(provider_cfg.get("provider_type") or "").strip(),
            )
        # Provider not found, pass full ref as model name
        logger.debug("LLM provider %r not found in config, using as plain model", provider_name)
        return LLMParams(ref)

    # Legacy format "provider:model" or plain model name -> pass as-is
    return LLMParams(ref)


def resolve_for_role(
    role: str,
    llm_config: dict[str, Any] | None = None,
) -> LLMParams:
    """Resolve LLM params for a named role.

    Looks up ``roles.<role>`` in *llm_config*. If the role is not found,
    falls back to the default model resolution.
    """
    if llm_config is None:
        return LLMParams("")
    roles = llm_config.get("roles") or {}
    role_ref = roles.get(role)
    if role_ref:
        return resolve_llm_params(str(role_ref), llm_config)
    return resolve_llm_params(None, llm_config)
