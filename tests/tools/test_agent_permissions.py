from __future__ import annotations

import time

import pytest

from logdog.llm.permissions import (
    ensure_tool_allowed,
    has_valid_approval_token,
    issue_approval_token,
    issue_approval_token_for_policy,
    load_permission_policy,
    ToolPermissionPolicy,
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


def test_approval_token_rejected_on_replay() -> None:
    """Same token used twice must be rejected the second time."""
    policy = ToolPermissionPolicy(
        dangerous_tools=frozenset({"restart_container"}),
        dangerous_host_allowlist=frozenset({"prod-a"}),
        approval_secret="test-secret",
        approval_token_ttl_seconds=300,
    )
    args = {"host": "prod-a", "container_id": "abc123"}
    token = issue_approval_token_for_policy(
        "restart_container", args, policy=policy
    )
    args_with_token = {**args, "approval_token": token}

    assert has_valid_approval_token(
        "restart_container", args_with_token, policy=policy
    ) is True

    assert has_valid_approval_token(
        "restart_container", args_with_token, policy=policy
    ) is False


def test_approval_token_replay_rejection_scoped_to_policy_instance() -> None:
    """Different policy instances have independent consumed-token stores."""
    secret = "shared-secret"
    base_kwargs = dict(
        dangerous_tools=frozenset({"restart_container"}),
        dangerous_host_allowlist=frozenset({"prod-a"}),
        approval_secret=secret,
        approval_token_ttl_seconds=300,
    )
    policy_a = ToolPermissionPolicy(**base_kwargs)
    policy_b = ToolPermissionPolicy(**base_kwargs)
    args = {"host": "prod-a", "container_id": "abc123"}
    token = issue_approval_token("restart_container", args, secret=secret)
    args_with_token = {**args, "approval_token": token}

    assert has_valid_approval_token(
        "restart_container", args_with_token, policy=policy_a
    ) is True
    assert has_valid_approval_token(
        "restart_container", args_with_token, policy=policy_b
    ) is True


def test_consumed_token_store_evicts_expired_entries() -> None:
    """Consumed tokens are cleaned up after they would have expired."""
    policy = ToolPermissionPolicy(
        dangerous_tools=frozenset({"restart_container"}),
        dangerous_host_allowlist=frozenset({"prod-a"}),
        approval_secret="test-secret",
        approval_token_ttl_seconds=2,
    )
    args = {"host": "prod-a", "container_id": "abc123"}
    issued_at = int(time.time())
    token = issue_approval_token_for_policy(
        "restart_container", args, policy=policy, issued_at=issued_at,
    )
    args_with_token = {**args, "approval_token": token}

    assert has_valid_approval_token(
        "restart_container", args_with_token, policy=policy, now=issued_at,
    ) is True
    assert has_valid_approval_token(
        "restart_container", args_with_token, policy=policy, now=issued_at,
    ) is False
    assert has_valid_approval_token(
        "restart_container", args_with_token, policy=policy, now=issued_at + 3,
    ) is False
    assert len(policy._consumed_tokens._consumed) == 0
