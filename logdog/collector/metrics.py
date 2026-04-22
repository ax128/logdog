from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def summarize_docker_stats(
    stats: dict[str, Any],
    *,
    previous_stats: dict[str, Any] | None = None,
) -> dict[str, float | int]:
    cpu = _compute_cpu_percent(stats, previous_stats)
    memory = stats.get("memory_stats") or {}

    return {
        "cpu": cpu,
        "mem_used": _to_int(memory.get("usage")),
        "mem_limit": _to_int(memory.get("limit")),
        "net_rx": _sum_network(stats, "rx_bytes"),
        "net_tx": _sum_network(stats, "tx_bytes"),
        "disk_read": _sum_blkio(stats, "read"),
        "disk_write": _sum_blkio(stats, "write"),
    }


def build_metric_sample(
    *,
    host_name: str,
    container_id: str,
    container_name: str,
    stats: dict[str, Any],
    previous_stats: dict[str, Any] | None,
    timestamp: str | None = None,
    status: str = "unknown",
    restart_count: int = 0,
) -> dict[str, Any]:
    summary = summarize_docker_stats(stats, previous_stats=previous_stats)
    return {
        "host_name": host_name,
        "container_id": container_id,
        "container_name": container_name,
        "timestamp": timestamp or datetime.now(timezone.utc).isoformat(),
        "cpu": float(summary["cpu"]),
        "mem_used": int(summary["mem_used"]),
        "mem_limit": int(summary["mem_limit"]),
        "net_rx": int(summary["net_rx"]),
        "net_tx": int(summary["net_tx"]),
        "disk_read": int(summary["disk_read"]),
        "disk_write": int(summary["disk_write"]),
        "status": str(status or "unknown"),
        "restart_count": _to_int(restart_count),
    }


def _compute_cpu_percent(
    stats: dict[str, Any],
    previous_stats: dict[str, Any] | None,
) -> float:
    fallback_cpu_percent = _to_float(stats.get("cpu_percent"))
    if not previous_stats:
        return fallback_cpu_percent

    cpu_stats = stats.get("cpu_stats") or {}
    prev_cpu_stats = previous_stats.get("cpu_stats") or {}
    curr_total = _to_float((cpu_stats.get("cpu_usage") or {}).get("total_usage"))
    prev_total = _to_float((prev_cpu_stats.get("cpu_usage") or {}).get("total_usage"))
    curr_system = _to_float(cpu_stats.get("system_cpu_usage"))
    prev_system = _to_float(prev_cpu_stats.get("system_cpu_usage"))

    cpu_delta = curr_total - prev_total
    system_delta = curr_system - prev_system
    if cpu_delta <= 0 or system_delta <= 0:
        return fallback_cpu_percent

    online_cpus = _to_int(cpu_stats.get("online_cpus"))
    if online_cpus <= 0:
        percpu = (cpu_stats.get("cpu_usage") or {}).get("percpu_usage") or []
        online_cpus = max(len(percpu), 1)

    percent = (cpu_delta / system_delta) * online_cpus * 100.0
    if percent < 0:
        return fallback_cpu_percent
    return float(percent)


def _sum_network(stats: dict[str, Any], field: str) -> int:
    total = 0
    networks = stats.get("networks") or {}
    if not isinstance(networks, dict):
        return 0

    for _, net in networks.items():
        if isinstance(net, dict):
            total += _to_int(net.get(field))
    return total


def _sum_blkio(stats: dict[str, Any], expected_op: str) -> int:
    entries = ((stats.get("blkio_stats") or {}).get("io_service_bytes_recursive")) or []
    if not isinstance(entries, list):
        return 0

    total = 0
    expected = expected_op.lower()
    for item in entries:
        if not isinstance(item, dict):
            continue
        op = str(item.get("op") or "").lower()
        if op == expected:
            total += _to_int(item.get("value"))
    return total


def _to_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _to_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
