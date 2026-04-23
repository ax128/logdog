from __future__ import annotations

import pytest

from logdog.collector.metrics import build_metric_sample, summarize_docker_stats
from logdog.collector.sampler import MetricsSampler


def test_summarize_docker_stats_extracts_core_fields() -> None:
    previous = {
        "cpu_stats": {
            "cpu_usage": {"total_usage": 100},
            "system_cpu_usage": 1000,
            "online_cpus": 2,
        }
    }
    current = {
        "cpu_stats": {
            "cpu_usage": {"total_usage": 300},
            "system_cpu_usage": 1400,
            "online_cpus": 2,
        },
        "memory_stats": {"usage": 256, "limit": 1024},
        "networks": {
            "eth0": {"rx_bytes": 10, "tx_bytes": 20},
            "eth1": {"rx_bytes": 5, "tx_bytes": 7},
        },
        "blkio_stats": {
            "io_service_bytes_recursive": [
                {"op": "Read", "value": 1000},
                {"op": "Write", "value": 4000},
                {"op": "read", "value": 1},
            ]
        },
    }

    out = summarize_docker_stats(current, previous_stats=previous)

    assert out["cpu"] == pytest.approx(100.0)
    assert out["mem_used"] == 256
    assert out["mem_limit"] == 1024
    assert out["net_rx"] == 15
    assert out["net_tx"] == 27
    assert out["disk_read"] == 1001
    assert out["disk_write"] == 4000


def test_summarize_docker_stats_accepts_cli_summary_payload_without_previous() -> None:
    current = {
        "cpu_percent": 12.5,
        "memory_stats": {"usage": 256, "limit": 1024},
        "networks": {"total": {"rx_bytes": 10, "tx_bytes": 20}},
        "blkio_stats": {
            "io_service_bytes_recursive": [
                {"op": "Read", "value": 5},
                {"op": "Write", "value": 8},
            ]
        },
    }

    out = summarize_docker_stats(current, previous_stats=None)

    assert out["cpu"] == pytest.approx(12.5)
    assert out["mem_used"] == 256
    assert out["mem_limit"] == 1024
    assert out["net_rx"] == 10
    assert out["net_tx"] == 20
    assert out["disk_read"] == 5
    assert out["disk_write"] == 8


@pytest.mark.asyncio
async def test_metrics_sampler_collect_once_writes_samples_and_skips_failures() -> None:
    written: list[dict] = []

    async def fetch_stats(_host: str, container: dict) -> dict:
        if container["id"] == "c2":
            raise RuntimeError("stats timeout")
        return {
            "cpu_stats": {
                "cpu_usage": {"total_usage": 300},
                "system_cpu_usage": 1400,
                "online_cpus": 2,
            },
            "memory_stats": {"usage": 256, "limit": 1024},
            "networks": {"eth0": {"rx_bytes": 10, "tx_bytes": 20}},
            "blkio_stats": {"io_service_bytes_recursive": []},
        }

    async def save_metric(sample: dict) -> None:
        written.append(sample)

    sampler = MetricsSampler(fetch_stats=fetch_stats, save_metric=save_metric)
    count = await sampler.sample_host(
        host_name="prod-quant",
        containers=[
            {"id": "c1", "name": "trading-engine", "status": "running", "restart_count": 2},
            {"id": "c2", "name": "risk-manager", "status": "running", "restart_count": 0},
        ],
    )

    assert count == 1
    assert len(written) == 1
    assert written[0]["host_name"] == "prod-quant"
    assert written[0]["container_id"] == "c1"
    assert written[0]["container_name"] == "trading-engine"
    assert written[0]["status"] == "running"
    assert written[0]["restart_count"] == 2


@pytest.mark.asyncio
async def test_metrics_sampler_logs_stats_fetch_duration(caplog) -> None:
    written: list[dict] = []

    async def fetch_stats(_host: str, _container: dict) -> dict:
        return {
            "cpu_stats": {
                "cpu_usage": {"total_usage": 300},
                "system_cpu_usage": 1400,
                "online_cpus": 2,
            },
            "memory_stats": {"usage": 256, "limit": 1024},
            "networks": {"eth0": {"rx_bytes": 10, "tx_bytes": 20}},
            "blkio_stats": {"io_service_bytes_recursive": []},
        }

    async def save_metric(sample: dict) -> None:
        written.append(sample)

    sampler = MetricsSampler(fetch_stats=fetch_stats, save_metric=save_metric)

    with caplog.at_level("INFO", logger="logdog.collector.sampler"):
        count = await sampler.sample_host(
            host_name="h1",
            containers=[{"id": "c1", "name": "api", "status": "running"}],
        )

    assert count == 1
    assert len(written) == 1
    assert "metrics container stats fetch completed host=h1 container=c1" in caplog.text
    assert "duration_ms=" in caplog.text


def test_build_metric_sample_defaults_status_and_restart_count() -> None:
    sample = build_metric_sample(
        host_name="h1",
        container_id="c1",
        container_name="svc",
        stats={"cpu_stats": {}, "memory_stats": {}, "blkio_stats": {}},
        previous_stats=None,
        timestamp="2026-04-11T00:00:00+00:00",
    )

    assert sample["status"] == "unknown"
    assert sample["restart_count"] == 0
    assert sample["timestamp"] == "2026-04-11T00:00:00+00:00"
