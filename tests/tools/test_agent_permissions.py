from __future__ import annotations

import time

import pytest

from logdog.llm.permissions import (
    ensure_tool_allowed,
    issue_approval_token,
    issue_approval_token_for_policy,
    load_permission_policy,
)

_CURRENT_TS = int(time.time())


def test_restart_container_requires_allowlisted_host() -> None:
    policy = load_permission_policy(
        {
            "llm": {
                "permissions": {
                    "dangerous_host_allowlist": ["prod-a"],
                    "approval_secret": "test-secret",
                }
            }
        }
    )

    with pytest.raises(PermissionError, match="allowlist"):
        ensure_tool_allowed(
            "restart_container",
            {"host": "prod-b"},
            policy=policy,
        )


def test_restart_container_requires_approval_token() -> None:
    policy = load_permission_policy(
        {
            "llm": {
                "permissions": {
                    "dangerous_host_allowlist": ["prod-a"],
                    "approval_secret": "test-secret",
                }
            }
        }
    )

    with pytest.raises(PermissionError, match="approval token"):
        ensure_tool_allowed(
            "restart_container",
            {"host": "prod-a"},
            policy=policy,
        )


def test_restart_container_accepts_valid_approval_token() -> None:
    policy = load_permission_policy(
        {
            "llm": {
                "permissions": {
                    "dangerous_host_allowlist": ["prod-a"],
                    "approval_secret": "test-secret",
                }
            }
        }
    )
    arguments = {"host": "prod-a", "container_id": "c1", "timeout": 10}
    arguments["approval_token"] = issue_approval_token(
        "restart_container",
        arguments,
        secret="test-secret",
        issued_at=_CURRENT_TS,
        ttl_seconds=300,
    )

    ensure_tool_allowed(
        "restart_container",
        arguments,
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


def test_restart_container_rejects_expired_approval_token() -> None:
    policy = load_permission_policy(
        {
            "llm": {
                "permissions": {
                    "dangerous_host_allowlist": ["prod-a"],
                    "approval_secret": "test-secret",
                }
            }
        }
    )
    arguments = {"host": "prod-a", "container_id": "c1", "timeout": 10}
    arguments["approval_token"] = issue_approval_token(
        "restart_container",
        arguments,
        secret="test-secret",
        issued_at=_CURRENT_TS - 3_600,
        ttl_seconds=1,
    )

    with pytest.raises(PermissionError, match="approval token"):
        ensure_tool_allowed(
            "restart_container",
            arguments,
            policy=policy,
        )


def test_restart_container_rejects_tampered_approval_token() -> None:
    policy = load_permission_policy(
        {
            "llm": {
                "permissions": {
                    "dangerous_host_allowlist": ["prod-a"],
                    "approval_secret": "test-secret",
                }
            }
        }
    )
    arguments = {"host": "prod-a", "container_id": "c1", "timeout": 10}
    arguments["approval_token"] = issue_approval_token(
        "restart_container",
        arguments,
        secret="test-secret",
        issued_at=_CURRENT_TS,
        ttl_seconds=300,
    )
    arguments["timeout"] = 15

    with pytest.raises(PermissionError, match="approval token"):
        ensure_tool_allowed(
            "restart_container",
            arguments,
            policy=policy,
        )


def test_restart_container_rejects_model_confirmation_without_token() -> None:
    policy = load_permission_policy(
        {
            "llm": {
                "permissions": {
                    "dangerous_host_allowlist": ["prod-a"],
                    "approval_secret": "test-secret",
                }
            }
        }
    )

    with pytest.raises(PermissionError, match="approval token"):
        ensure_tool_allowed(
            "restart_container",
            {"host": "prod-a", "container_id": "c1", "confirmed": True},
            policy=policy,
        )


def test_issue_approval_token_for_policy_uses_configured_ttl() -> None:
    policy = load_permission_policy(
        {
            "llm": {
                "permissions": {
                    "dangerous_host_allowlist": ["prod-a"],
                    "approval_secret": "test-secret",
                    "approval_token_ttl_seconds": 90,
                }
            }
        }
    )

    token = issue_approval_token_for_policy(
        "restart_container",
        {"host": "prod-a", "container_id": "c1"},
        policy=policy,
        issued_at=1_000,
    )

    assert token.startswith("v1:1090:")


def test_approval_token_cannot_be_reused_across_tools() -> None:
    policy = load_permission_policy(
        {
            "llm": {
                "permissions": {
                    "dangerous_host_allowlist": ["prod-a"],
                    "approval_secret": "test-secret",
                }
            }
        }
    )
    arguments = {"host": "prod-a", "container_id": "c1", "command": "echo hello"}
    arguments["approval_token"] = issue_approval_token(
        "exec_container",
        arguments,
        secret="test-secret",
        issued_at=_CURRENT_TS,
        ttl_seconds=300,
    )

    with pytest.raises(PermissionError, match="approval token"):
        ensure_tool_allowed(
            "restart_container",
            arguments,
            policy=policy,
        )
