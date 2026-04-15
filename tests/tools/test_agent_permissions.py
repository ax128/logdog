from __future__ import annotations

import pytest

from logwatch.llm.permissions import ensure_tool_allowed, load_permission_policy


def test_restart_container_requires_allowlisted_host() -> None:
    policy = load_permission_policy(
        {"llm": {"permissions": {"dangerous_host_allowlist": ["prod-a"]}}}
    )

    with pytest.raises(PermissionError, match="allowlist"):
        ensure_tool_allowed(
            "restart_container",
            {"host": "prod-b", "confirmed": True},
            policy=policy,
        )


def test_restart_container_requires_explicit_confirmation() -> None:
    policy = load_permission_policy(
        {"llm": {"permissions": {"dangerous_host_allowlist": ["prod-a"]}}}
    )

    with pytest.raises(PermissionError, match="confirmation"):
        ensure_tool_allowed(
            "restart_container",
            {"host": "prod-a"},
            policy=policy,
        )


def test_read_only_tool_is_allowed_without_confirmation() -> None:
    policy = load_permission_policy(
        {"llm": {"permissions": {"dangerous_host_allowlist": []}}}
    )

    ensure_tool_allowed(
        "query_logs",
        {"host": "prod-b", "container_id": "c1", "hours": 1},
        policy=policy,
    )
