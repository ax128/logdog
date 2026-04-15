from __future__ import annotations

from pathlib import Path

import pytest

from logwatch.core.host_manager import HostManager


@pytest.mark.asyncio
async def test_startup_connects_local_host() -> None:
    calls: list[str] = []

    async def connector(host: dict) -> dict:
        calls.append(host["name"])
        return {"server_version": "24.0.7"}

    manager = HostManager(
        hosts=[{"name": "local", "url": "unix:///var/run/docker.sock"}],
        connector=connector,
    )
    await manager.startup_check()
    statuses = manager.list_host_statuses()

    assert calls == ["local"]
    assert len(statuses) == 1
    assert statuses[0]["name"] == "local"
    assert statuses[0]["status"] == "connected"
    assert statuses[0]["last_connected_at"] is not None


@pytest.mark.asyncio
async def test_startup_marks_missing_ssh_key_disconnected() -> None:
    calls: list[str] = []

    async def connector(host: dict) -> dict:
        calls.append(host["name"])
        return {"server_version": "24.0.7"}

    manager = HostManager(
        hosts=[
            {
                "name": "prod",
                "url": "ssh://deploy@10.0.1.10",
                "ssh_key": "/tmp/non-existent-logwatch-key.pem",
            }
        ],
        connector=connector,
    )
    await manager.startup_check()
    statuses = manager.list_host_statuses()

    assert calls == []
    assert statuses[0]["status"] == "disconnected"
    assert "SSH key not found" in (statuses[0]["last_error"] or "")


@pytest.mark.asyncio
async def test_precheck_rejects_when_ssh_key_permission_too_open(
    tmp_path: Path,
) -> None:
    ssh_key = tmp_path / "id_rsa"
    ssh_key.write_text("dummy", encoding="utf-8")
    ssh_key.chmod(0o644)

    async def connector(host: dict) -> dict:
        return {"server_version": "24.0.7"}

    manager = HostManager(
        hosts=[
            {
                "name": "prod",
                "url": "ssh://deploy@10.0.1.10",
                "ssh_key": str(ssh_key),
            }
        ],
        connector=connector,
    )
    await manager.startup_check()

    statuses = manager.list_host_statuses()
    assert statuses[0]["status"] == "disconnected"
    assert "permissions too open" in (statuses[0]["last_error"] or "")


@pytest.mark.asyncio
async def test_connect_retry_uses_backoff_and_marks_disconnected_after_failures(
    tmp_path: Path,
) -> (
    None
):
    sleeps: list[float] = []
    ssh_key = tmp_path / "id_rsa"
    ssh_key.write_text("dummy", encoding="utf-8")
    ssh_key.chmod(0o600)

    async def connector(host: dict) -> dict:
        raise RuntimeError("docker unavailable")

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    manager = HostManager(
        hosts=[
            {
                "name": "prod",
                "url": "ssh://deploy@10.0.1.10",
                "ssh_key": str(ssh_key),
            }
        ],
        connector=connector,
        sleep_fn=fake_sleep,
        max_retries=3,
    )
    await manager.startup_check()
    statuses = manager.list_host_statuses()

    assert sleeps == [1.0, 2.0]
    assert statuses[0]["status"] == "disconnected"
    assert statuses[0]["failure_count"] == 3
    assert "docker unavailable" in (statuses[0]["last_error"] or "")


@pytest.mark.asyncio
async def test_reload_hosts_adds_new_host_and_keeps_removed_with_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    calls: list[str] = []

    async def connector(host: dict) -> dict:
        calls.append(host["name"])
        return {"server_version": "24.0.7"}

    manager = HostManager(
        hosts=[
            {"name": "local", "url": "unix:///var/run/docker.sock"},
            {"name": "legacy", "url": "unix:///var/run/docker.sock"},
        ],
        connector=connector,
    )
    await manager.startup_check()

    with caplog.at_level("WARNING"):
        summary = await manager.reload_hosts(
            [
                {"name": "local", "url": "unix:///var/run/docker.sock"},
                {"name": "new", "url": "unix:///var/run/docker.sock"},
            ]
        )

    assert summary["added"] == ["new"]
    assert summary["updated"] == ["local"]
    assert summary["removed_requires_restart"] == ["legacy"]
    assert any("cannot be hot-unloaded" in rec.message for rec in caplog.records)
    assert {x["name"] for x in manager.list_host_statuses()} == {
        "local",
        "legacy",
        "new",
    }
    assert "new" in calls


@pytest.mark.asyncio
async def test_reload_hosts_reconnects_updated_host() -> None:
    calls: list[str] = []

    async def connector(host: dict) -> dict:
        calls.append(str(host["url"]))
        return {"server_version": "24.0.7"}

    manager = HostManager(
        hosts=[{"name": "local", "url": "unix:///var/run/docker-old.sock"}],
        connector=connector,
        max_retries=1,
    )
    await manager.startup_check()

    summary = await manager.reload_hosts(
        [{"name": "local", "url": "unix:///var/run/docker-new.sock"}]
    )

    assert summary["updated"] == ["local"]
    assert calls == [
        "unix:///var/run/docker-old.sock",
        "unix:///var/run/docker-new.sock",
    ]


def test_host_manager_rejects_duplicate_host_names_on_init() -> None:
    with pytest.raises(ValueError, match="duplicate host name"):
        HostManager(
            hosts=[
                {"name": "dup", "url": "unix:///var/run/docker.sock"},
                {"name": "dup", "url": "ssh://ops@example-host"},
            ]
        )


@pytest.mark.asyncio
async def test_host_manager_reload_rejects_duplicate_host_names() -> None:
    manager = HostManager(hosts=[{"name": "local", "url": "unix:///var/run/docker.sock"}])
    with pytest.raises(ValueError, match="duplicate host name"):
        await manager.reload_hosts(
            [
                {"name": "local", "url": "unix:///var/run/docker.sock"},
                {"name": "local", "url": "ssh://ops@example-host"},
            ]
        )


@pytest.mark.asyncio
async def test_status_change_callback_emits_connected_and_disconnected() -> None:
    events: list[dict] = []
    state = {"up": False}

    async def connector(_host: dict) -> dict:
        if not state["up"]:
            raise RuntimeError("docker unavailable")
        return {"server_version": "24.0.7"}

    async def on_status_change(payload: dict) -> None:
        events.append(payload)

    manager = HostManager(
        hosts=[{"name": "local", "url": "unix:///var/run/docker.sock"}],
        connector=connector,
        max_retries=1,
        on_status_change=on_status_change,
    )

    # 初始状态 disconnected，第一次失败不应产生状态变更事件。
    await manager.startup_check()
    assert events == []

    state["up"] = True
    await manager.startup_check()
    assert len(events) == 1
    assert events[0]["old_status"] == "disconnected"
    assert events[0]["new_status"] == "connected"

    state["up"] = False
    await manager.startup_check()
    assert len(events) == 2
    assert events[1]["old_status"] == "connected"
    assert events[1]["new_status"] == "disconnected"


@pytest.mark.asyncio
async def test_connect_error_is_classified_for_observability(tmp_path: Path) -> None:
    ssh_key = tmp_path / "id_rsa"
    ssh_key.write_text("dummy", encoding="utf-8")
    ssh_key.chmod(0o600)

    async def connector(_host: dict) -> dict:
        raise RuntimeError("Authentication failed for deploy@10.0.1.10")

    manager = HostManager(
        hosts=[
            {
                "name": "prod",
                "url": "ssh://deploy@10.0.1.10",
                "ssh_key": str(ssh_key),
            }
        ],
        connector=connector,
        max_retries=1,
    )
    await manager.startup_check()

    state = manager.get_host_state("prod")
    assert state is not None
    assert state["status"] == "disconnected"
    assert state["last_error_kind"] == "auth"


@pytest.mark.asyncio
async def test_circuit_breaker_skips_connect_until_window_expires(
    tmp_path: Path,
) -> None:
    connector_calls: list[int] = []
    now = {"ts": 100.0}
    ssh_key = tmp_path / "id_rsa"
    ssh_key.write_text("dummy", encoding="utf-8")
    ssh_key.chmod(0o600)

    async def connector(_host: dict) -> dict:
        connector_calls.append(1)
        raise RuntimeError("network timeout")

    manager = HostManager(
        hosts=[
            {
                "name": "prod",
                "url": "ssh://deploy@10.0.1.10",
                "ssh_key": str(ssh_key),
            }
        ],
        connector=connector,
        max_retries=1,
        circuit_break_threshold=1,
        circuit_break_seconds=30,
        time_fn=lambda: now["ts"],
    )

    await manager.startup_check()
    state_after_open = manager.get_host_state("prod")
    assert state_after_open is not None
    assert state_after_open["last_error_kind"] == "network"
    assert state_after_open["circuit_open_until"] is not None
    assert len(connector_calls) == 1

    now["ts"] = 110.0
    await manager.startup_check()
    state_during_open = manager.get_host_state("prod")
    assert state_during_open is not None
    assert state_during_open["last_error_kind"] == "circuit_open"
    assert len(connector_calls) == 1

    now["ts"] = 131.0
    await manager.startup_check()
    assert len(connector_calls) == 2
