from __future__ import annotations

import pytest

from logdog.collector.host_metrics_sampler import HostMetricsSampler


class _HostManagerStub:
    def __init__(self, statuses: list[dict], configs: dict[str, dict]) -> None:
        self._statuses = statuses
        self._configs = configs

    def list_host_statuses(self) -> list[dict]:
        return list(self._statuses)

    def get_host_config(self, name: str) -> dict | None:
        cfg = self._configs.get(name)
        if cfg is None:
            return None
        return dict(cfg)


@pytest.mark.asyncio
async def test_host_metrics_sampler_collects_connected_hosts_and_skips_failures() -> (
    None
):
    host_manager = _HostManagerStub(
        statuses=[
            {"name": "h1", "status": "connected"},
            {"name": "h2", "status": "disconnected"},
            {"name": "h3", "status": "connected"},
        ],
        configs={
            "h1": {"name": "h1", "url": "unix:///var/run/docker.sock"},
            "h3": {"name": "h3", "url": "ssh://deploy@example-host"},
        },
    )
    probe_calls: list[str] = []
    written: list[dict] = []

    async def collect_host_metrics(
        host_cfg: dict,
        *,
        collect_load: bool,
        collect_network: bool,
        timeout_seconds: int,
    ) -> dict:
        probe_calls.append(str(host_cfg.get("name")))
        if host_cfg.get("name") == "h3":
            raise RuntimeError("ssh timeout")
        return {
            "timestamp": "2026-04-12T10:30:00+00:00",
            "cpu_percent": 15.0,
            "load_1": 0.5,
            "load_5": 0.4,
            "load_15": 0.3,
            "mem_total": 1000,
            "mem_used": 500,
            "mem_available": 500,
            "disk_root_total": 2000,
            "disk_root_used": 1000,
            "disk_root_free": 1000,
            "net_rx": 10,
            "net_tx": 20,
            "source": "local",
            "collect_load": collect_load,
            "collect_network": collect_network,
            "timeout_seconds": timeout_seconds,
        }

    async def save_metric(sample: dict) -> None:
        written.append(sample)

    sampler = HostMetricsSampler(
        host_manager=host_manager,
        collect_host_metrics=collect_host_metrics,
        save_metric=save_metric,
        collect_load=False,
        collect_network=False,
        timeout_seconds=5,
    )

    count = await sampler.sample_connected_hosts()

    assert count == 1
    assert probe_calls == ["h1", "h3"]
    assert len(written) == 1
    assert written[0]["host_name"] == "h1"
    assert written[0]["collect_load"] is False
    assert written[0]["collect_network"] is False
    assert written[0]["timeout_seconds"] == 5
