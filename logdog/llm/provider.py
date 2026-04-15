"""LLM provider resolution.

Resolves 'provider/model' references against llm.providers config.
Falls back gracefully when providers are not configured.
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


class LLMParams:
    """Resolved LLM call parameters."""
    __slots__ = ("model", "api_base", "api_key")

    def __init__(self, model: str, api_base: str = "", api_key: str = "") -> None:
        self.model = model
        self.api_base = api_base
        self.api_key = api_key

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


def resolve_llm_params(
    model_ref: str | None,
    llm_config: dict[str, Any] | None = None,
) -> LLMParams:
    """Resolve a model reference to LLMParams.

    Formats:
      "provider/model"  -> lookup provider in llm_config["providers"]
      "provider:model"  -> legacy format, pass as-is (no provider lookup)
      "model"           -> plain model name, pass as-is
      None / ""         -> use llm_config["default_model"] or llm_config["model"]
    """
    cfg = dict(llm_config or {})
    providers = cfg.get("providers") or {}

    # Resolve default if no model specified
    ref = str(model_ref or "").strip()
    if not ref:
        ref = str(cfg.get("default_model") or cfg.get("model") or "").strip()
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
                api_key=str(provider_cfg.get("api_key") or "").strip(),
            )
        # Provider not found, pass full ref as model name
        logger.debug("LLM provider %r not found in config, using as plain model", provider_name)
        return LLMParams(ref)

    # Legacy format "provider:model" or plain model name -> pass as-is
    return LLMParams(ref)
