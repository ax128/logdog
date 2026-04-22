from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest

from logdog.remote.ssh_lifecycle import SSHSessionLifecycle, SSHSessionStatus


def _utc(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


@pytest.mark.asyncio
async def test_request_connect_is_idempotent_under_concurrency() -> None:
    lifecycle = SSHSessionLifecycle(
        now_fn=lambda: _utc(datetime(2026, 4, 20, 12, 0, 0))
    )

    first, second = await asyncio.gather(
        lifecycle.request_connect("prod"),
        lifecycle.request_connect("prod"),
    )

    assert first.created_new is True
    assert second.created_new is False
    assert first.state["status"] == SSHSessionStatus.CONNECTING.value
    assert second.state["status"] == SSHSessionStatus.CONNECTING.value
    assert first.state["session_token"] == second.state["session_token"]
    assert lifecycle.can_retry("prod") is False


@pytest.mark.asyncio
async def test_shutdown_requires_closed_tombstone_before_retry() -> None:
    lifecycle = SSHSessionLifecycle(
        now_fn=lambda: _utc(datetime(2026, 4, 20, 12, 0, 0))
    )

    started = await lifecycle.request_connect("prod")
    token = started.state["session_token"]

    await lifecycle.begin_handshake("prod", token)
    await lifecycle.mark_running("prod", token)

    draining = await lifecycle.begin_shutdown("prod", reason="operator requested")
    assert draining["status"] == SSHSessionStatus.DRAINING.value
    assert draining["disconnect_reason"] == "operator requested"
    assert lifecycle.can_retry("prod") is False

    blocked_retry = await lifecycle.request_connect("prod")
    assert blocked_retry.created_new is False
    assert blocked_retry.state["status"] == SSHSessionStatus.DRAINING.value

    closed = await lifecycle.mark_closed("prod", reason="operator requested")
    assert closed["status"] == SSHSessionStatus.CLOSED.value
    assert closed["disconnect_reason"] == "operator requested"
    assert closed["tombstone_at"] is not None
    assert lifecycle.can_retry("prod") is True

    restarted = await lifecycle.request_connect("prod")
    assert restarted.created_new is True
    assert restarted.state["status"] == SSHSessionStatus.CONNECTING.value
    assert restarted.state["session_token"] != token


@pytest.mark.asyncio
async def test_heartbeat_updates_last_heartbeat_and_rejects_wrong_token() -> None:
    heartbeat_at = _utc(datetime(2026, 4, 20, 12, 30, 0))
    lifecycle = SSHSessionLifecycle(now_fn=lambda: heartbeat_at)

    started = await lifecycle.request_connect("prod")
    token = started.state["session_token"]
    await lifecycle.begin_handshake("prod", token)
    await lifecycle.mark_running("prod", token)

    snapshot = await lifecycle.record_heartbeat("prod", token, at=heartbeat_at)
    assert snapshot["status"] == SSHSessionStatus.RUNNING.value
    assert snapshot["last_heartbeat_at"] == heartbeat_at.isoformat()

    with pytest.raises(ValueError, match="session token"):
        await lifecycle.record_heartbeat(
            "prod",
            "wrong-token",
            at=heartbeat_at,
        )
