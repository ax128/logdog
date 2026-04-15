from __future__ import annotations

from logdog.llm.provider import LLMParams, resolve_llm_params


class TestResolveLlmParams:
    def test_provider_slash_model_resolves(self) -> None:
        cfg = {
            "providers": {
                "agentop": {
                    "api_base": "https://api.agentop.dev/v1",
                    "api_key": "key-123",
                    "models": ["gpt-5.4"],
                }
            }
        }
        p = resolve_llm_params("agentop/gpt-5.4", cfg)
        assert p.model == "gpt-5.4"
        assert p.api_base == "https://api.agentop.dev/v1"
        assert p.api_key == "key-123"

    def test_provider_not_found_passes_full_ref(self) -> None:
        p = resolve_llm_params("unknown/model-x", {})
        assert p.model == "unknown/model-x"
        assert p.api_base == ""
        assert p.api_key == ""

    def test_legacy_colon_format_passes_as_is(self) -> None:
        p = resolve_llm_params("openai:gpt-4o", {})
        assert p.model == "openai:gpt-4o"
        assert p.api_base == ""

    def test_plain_model_name(self) -> None:
        p = resolve_llm_params("gpt-4o", {})
        assert p.model == "gpt-4o"

    def test_empty_ref_uses_default_model(self) -> None:
        cfg = {"default_model": "agentop/gpt-5.4", "providers": {
            "agentop": {"api_base": "https://example.com", "api_key": "k"}
        }}
        p = resolve_llm_params("", cfg)
        assert p.model == "gpt-5.4"
        assert p.api_base == "https://example.com"

    def test_empty_ref_falls_back_to_model_field(self) -> None:
        cfg = {"model": "openai:gpt-4o"}
        p = resolve_llm_params(None, cfg)
        assert p.model == "openai:gpt-4o"

    def test_none_config_returns_empty(self) -> None:
        p = resolve_llm_params(None, None)
        assert p.model == ""

    def test_to_factory_kwargs_omits_empty(self) -> None:
        p = LLMParams("gpt-4o", api_base="", api_key="key")
        kw = p.to_factory_kwargs()
        assert kw == {"model": "gpt-4o", "api_key": "key"}
        assert "api_base" not in kw

    def test_multiple_providers(self) -> None:
        cfg = {
            "providers": {
                "openai": {"api_base": "https://api.openai.com/v1", "api_key": "sk-1"},
                "xai": {"api_base": "https://api.x.ai/v1", "api_key": "xk-2"},
            }
        }
        p1 = resolve_llm_params("openai/gpt-4o", cfg)
        p2 = resolve_llm_params("xai/grok-4", cfg)
        assert p1.api_base == "https://api.openai.com/v1"
        assert p1.api_key == "sk-1"
        assert p2.api_base == "https://api.x.ai/v1"
        assert p2.api_key == "xk-2"
