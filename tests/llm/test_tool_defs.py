from __future__ import annotations

from logdog.llm.tool_defs import TOOL_METAS, build_args_schema


def test_all_eight_tools_defined() -> None:
    expected = {
        "list_hosts", "list_containers", "query_logs", "get_metrics",
        "get_alerts", "mute_alert", "unmute_alert", "restart_container",
        "get_system_metrics", "list_alert_mutes", "get_storm_events", "exec_container",
    }
    assert set(TOOL_METAS.keys()) == expected


def test_tool_meta_has_required_keys() -> None:
    for name, meta in TOOL_METAS.items():
        assert "name" in meta, f"{name} missing 'name'"
        assert "description" in meta, f"{name} missing 'description'"
        assert "parameters" in meta, f"{name} missing 'parameters'"
        assert meta["name"] == name


def test_required_params_are_marked() -> None:
    meta = TOOL_METAS["list_containers"]
    assert meta["parameters"]["host"]["required"] is True


def test_optional_params_have_defaults() -> None:
    meta = TOOL_METAS["query_logs"]
    assert meta["parameters"]["hours"]["required"] is False
    assert meta["parameters"]["hours"]["default"] == 1


def test_build_args_schema_creates_pydantic_model() -> None:
    meta = TOOL_METAS["list_containers"]
    schema = build_args_schema(meta["name"], meta["parameters"])
    assert schema is not None
    fields = schema.model_fields
    assert "host" in fields
    assert fields["host"].is_required()


def test_build_args_schema_optional_field_has_default() -> None:
    meta = TOOL_METAS["query_logs"]
    schema = build_args_schema(meta["name"], meta["parameters"])
    fields = schema.model_fields
    assert not fields["hours"].is_required()


def test_build_args_schema_empty_params_returns_none() -> None:
    assert build_args_schema("empty_tool", {}) is None


def test_build_args_schema_validates_required_field() -> None:
    meta = TOOL_METAS["list_containers"]
    schema = build_args_schema(meta["name"], meta["parameters"])
    import pytest
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        schema()  # missing required 'host'


def test_build_args_schema_accepts_valid_input() -> None:
    meta = TOOL_METAS["list_containers"]
    schema = build_args_schema(meta["name"], meta["parameters"])
    instance = schema(host="prod")
    assert instance.host == "prod"


def test_get_system_metrics_schema_requires_host_name() -> None:
    meta = TOOL_METAS["get_system_metrics"]
    schema = build_args_schema(meta["name"], meta["parameters"])
    import pytest
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        schema()
    instance = schema(host_name="prod-a")
    assert instance.host_name == "prod-a"
