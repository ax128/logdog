from __future__ import annotations

from typing import Any

import pytest

from logdog.collector.host_metrics_probe import (
    HOST_METRICS_REMOTE_COMMAND,
    HostMetricsProbeAuthError,
    HostMetricsProbeParseError,
    assess_host_security,
    collect_host_metrics_for_host,
    collect_local_host_metrics,
    collect_remote_host_metrics,
)


def test_remote_probe_command_imports_datetime_timezone() -> None:
    assert "from datetime import datetime, timezone" in HOST_METRICS_REMOTE_COMMAND


class _ChannelStub:
    def __init__(self, exit_status: int) -> None:
        self._exit_status = int(exit_status)

    def recv_exit_status(self) -> int:
        return self._exit_status


class _StreamStub:
    def __init__(self, data: str, *, exit_status: int) -> None:
        self._data = data.encode("utf-8")
        self.channel = _ChannelStub(exit_status=exit_status)

    def read(self) -> bytes:
        return self._data


class _SshClientStub:
    def __init__(
        self,
        *,
        stdout: str = "{}",
        stderr: str = "",
        exit_status: int = 0,
        connect_error: Exception | None = None,
    ) -> None:
        self._stdout = stdout
        self._stderr = stderr
        self._exit_status = exit_status
        self._connect_error = connect_error
        self.commands: list[str] = []
        self.closed = False
        self.loaded_host_keys = False
        self.host_key_policy_name: str | None = None

    def load_system_host_keys(self) -> None:
        self.loaded_host_keys = True

    def set_missing_host_key_policy(self, policy) -> None:
        self.host_key_policy_name = type(policy).__name__

    def connect(self, **_kwargs) -> None:
        if self._connect_error is not None:
            raise self._connect_error

    def exec_command(self, command: str, timeout: int | float | None = None):
        _ = timeout
        self.commands.append(command)
        return (
            None,
            _StreamStub(self._stdout, exit_status=self._exit_status),
            _StreamStub(self._stderr, exit_status=self._exit_status),
        )

    def close(self) -> None:
        self.closed = True


def test_collect_local_host_metrics_supports_feature_flags(monkeypatch) -> None:
    monkeypatch.setattr(
        "logdog.collector.host_metrics_probe._utc_now_iso",
        lambda: "2026-04-12T10:30:00+00:00",
    )
    monkeypatch.setattr(
        "logdog.collector.host_metrics_probe._read_cpu_percent",
        lambda: 12.5,
    )
    monkeypatch.setattr(
        "logdog.collector.host_metrics_probe._read_load_averages",
        lambda: (1.0, 2.0, 3.0),
    )
    monkeypatch.setattr(
        "logdog.collector.host_metrics_probe._read_memory_info",
        lambda: {"mem_total": 1000, "mem_used": 600, "mem_available": 400},
    )
    monkeypatch.setattr(
        "logdog.collector.host_metrics_probe._read_disk_usage",
        lambda: {
            "disk_root_total": 2000,
            "disk_root_used": 800,
            "disk_root_free": 1200,
        },
    )
    monkeypatch.setattr(
        "logdog.collector.host_metrics_probe._read_network_totals",
        lambda: {"net_rx": 111, "net_tx": 222},
    )

    sample = collect_local_host_metrics(collect_load=False, collect_network=False)

    assert sample["timestamp"] == "2026-04-12T10:30:00+00:00"
    assert sample["source"] == "local"
    assert sample["cpu_percent"] == pytest.approx(12.5)
    assert sample["load_1"] == pytest.approx(0.0)
    assert sample["load_5"] == pytest.approx(0.0)
    assert sample["load_15"] == pytest.approx(0.0)
    assert sample["net_rx"] == 0
    assert sample["net_tx"] == 0


@pytest.mark.asyncio
async def test_collect_host_metrics_for_host_routes_unix_url_to_local(
    monkeypatch,
) -> None:
    calls: list[tuple[bool, bool]] = []

    def fake_local(*, collect_load: bool, collect_network: bool) -> dict[str, object]:
        calls.append((collect_load, collect_network))
        return {
            "timestamp": "2026-04-12T10:30:00+00:00",
            "cpu_percent": 0.0,
            "load_1": 0.0,
            "load_5": 0.0,
            "load_15": 0.0,
            "mem_total": 0,
            "mem_used": 0,
            "mem_available": 0,
            "disk_root_total": 0,
            "disk_root_used": 0,
            "disk_root_free": 0,
            "net_rx": 0,
            "net_tx": 0,
            "source": "local",
        }

    monkeypatch.setattr(
        "logdog.collector.host_metrics_probe.collect_local_host_metrics",
        fake_local,
    )

    out = await collect_host_metrics_for_host(
        {"name": "local", "url": "unix:///var/run/docker.sock"},
        collect_load=False,
        collect_network=False,
    )

    assert out["source"] == "local"
    assert calls == [(False, False)]


@pytest.mark.asyncio
async def test_collect_remote_host_metrics_runs_sync_probe_in_worker_thread(
    monkeypatch,
) -> None:
    seen: dict[str, Any] = {}

    async def fake_to_thread(func, /, *args, **kwargs):
        seen["func"] = func
        seen["args"] = args
        seen["kwargs"] = kwargs
        return {
            "timestamp": "2026-04-12T10:30:00+00:00",
            "cpu_percent": 0.0,
            "load_1": 0.0,
            "load_5": 0.0,
            "load_15": 0.0,
            "mem_total": 0,
            "mem_used": 0,
            "mem_available": 0,
            "disk_root_total": 0,
            "disk_root_used": 0,
            "disk_root_free": 0,
            "net_rx": 0,
            "net_tx": 0,
            "source": "ssh",
        }

    monkeypatch.setattr(
        "logdog.collector.host_metrics_probe.asyncio.to_thread",
        fake_to_thread,
    )

    def ssh_client_factory() -> _SshClientStub:
        return _SshClientStub()

    out = await collect_remote_host_metrics(
        {"name": "prod", "url": "ssh://deploy@example-host"},
        collect_load=False,
        collect_network=False,
        timeout_seconds=3,
        ssh_client_factory=ssh_client_factory,
    )

    assert out["source"] == "ssh"
    assert seen["func"].__name__ == "_collect_remote_host_metrics_sync"
    assert seen["args"] == ({"name": "prod", "url": "ssh://deploy@example-host"},)
    kwargs = seen["kwargs"]
    assert kwargs["collect_load"] is False
    assert kwargs["collect_network"] is False
    assert kwargs["timeout_seconds"] == 3
    assert kwargs["ssh_client_factory"] is ssh_client_factory


@pytest.mark.asyncio
async def test_collect_remote_host_metrics_maps_auth_error_to_typed_exception() -> None:
    from paramiko import AuthenticationException

    client = _SshClientStub(connect_error=AuthenticationException("denied"))

    with pytest.raises(HostMetricsProbeAuthError):
        await collect_remote_host_metrics(
            {"name": "prod", "url": "ssh://deploy@example-host"},
            ssh_client_factory=lambda: client,
            timeout_seconds=1,
        )

    assert client.closed is True


@pytest.mark.asyncio
async def test_collect_remote_host_metrics_rejects_invalid_json() -> None:
    client = _SshClientStub(stdout="{bad-json", stderr="", exit_status=0)

    with pytest.raises(HostMetricsProbeParseError):
        await collect_remote_host_metrics(
            {"name": "prod", "url": "ssh://deploy@example-host"},
            ssh_client_factory=lambda: client,
        )

    assert len(client.commands) == 1


@pytest.mark.asyncio
async def test_collect_remote_host_metrics_requires_explicit_username() -> None:
    client = _SshClientStub(stdout='{"cpu_percent": 1}', stderr="", exit_status=0)

    with pytest.raises(HostMetricsProbeParseError, match="missing username"):
        await collect_remote_host_metrics(
            {"name": "prod", "url": "ssh://example-host"},
            ssh_client_factory=lambda: client,
        )


@pytest.mark.asyncio
async def test_collect_remote_host_metrics_defaults_missing_fields_to_zero() -> None:
    client = _SshClientStub(stdout='{"cpu_percent": 77.7}', stderr="", exit_status=0)

    out = await collect_remote_host_metrics(
        {"name": "prod", "url": "ssh://deploy@example-host"},
        ssh_client_factory=lambda: client,
    )

    assert out["source"] == "ssh"
    assert out["cpu_percent"] == pytest.approx(77.7)
    assert out["mem_total"] == 0
    assert out["disk_root_total"] == 0
    assert out["net_rx"] == 0
    assert out["net_tx"] == 0


@pytest.mark.asyncio
async def test_collect_remote_host_metrics_uses_strict_host_key_policy_by_default() -> (
    None
):
    client = _SshClientStub(stdout='{"cpu_percent": 1}', stderr="", exit_status=0)

    await collect_remote_host_metrics(
        {"name": "prod", "url": "ssh://deploy@example-host"},
        ssh_client_factory=lambda: client,
    )

    assert client.loaded_host_keys is True
    assert client.host_key_policy_name == "RejectPolicy"


@pytest.mark.asyncio
async def test_collect_remote_host_metrics_coerces_strict_host_key_from_string() -> (
    None
):
    client = _SshClientStub(stdout='{"cpu_percent": 1}', stderr="", exit_status=0)

    await collect_remote_host_metrics(
        {
            "name": "prod",
            "url": "ssh://deploy@example-host",
            "strict_host_key": "false",
        },
        ssh_client_factory=lambda: client,
    )

    assert client.loaded_host_keys is True
    assert client.host_key_policy_name == "AutoAddPolicy"


def test_assess_host_security_reports_ssh_and_system_issues() -> None:
    report = assess_host_security(
        {
            "name": "prod",
            "url": "ssh://root@example-host",
            "ssh_password": "secret",
        },
        metric_sample={
            "mem_total": 1000,
            "mem_used": 995,
            "disk_root_total": 2000,
            "disk_root_used": 1985,
        },
    )

    assert report["ok"] is False
    assert report["host"] == "prod"
    assert report["scheme"] == "ssh"
    assert len(report["issues"]) >= 3
    assert any("root" in item.lower() for item in report["issues"])
    assert any("password" in item.lower() for item in report["issues"])
    assert any("disk usage" in item.lower() for item in report["issues"])
    assert any("mem usage" in item.lower() for item in report["issues"])
    assert report["analysis"]


def test_assess_host_security_returns_ok_for_non_ssh_host_without_pressure() -> None:
    report = assess_host_security(
        {
            "name": "local",
            "url": "unix:///var/run/docker.sock",
        },
        metric_sample={
            "mem_total": 1000,
            "mem_used": 400,
            "disk_root_total": 1000,
            "disk_root_used": 500,
        },
    )

    assert report["ok"] is True
    assert report["issues"] == []
    assert report["analysis"]


def test_assess_host_security_can_require_ssh_key_via_strict_flag() -> None:
    report = assess_host_security(
        {
            "name": "prod",
            "url": "ssh://deploy@example-host",
            "strict_ssh_key": True,
        },
        metric_sample={
            "mem_total": 1000,
            "mem_used": 400,
            "disk_root_total": 1000,
            "disk_root_used": 500,
        },
    )

    assert report["ok"] is False
    assert any("ssh key is missing" in item.lower() for item in report["issues"])
