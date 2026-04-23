from __future__ import annotations

import inspect
import logging
import time
from typing import Any

from logdog.collector.metrics import build_metric_sample


class MetricsSampler:
    def __init__(self, *, fetch_stats: Any, save_metric: Any) -> None:
        self._fetch_stats = fetch_stats
        self._save_metric = save_metric
        self._logger = logging.getLogger(__name__)
        self._previous_stats: dict[str, dict[str, Any]] = {}

    async def sample_host(
        self,
        *,
        host_name: str,
        containers: list[dict[str, Any]],
        timestamp: str | None = None,
    ) -> int:
        written = 0
        for container in containers:
            container_id = str(container.get("id") or container.get("container_id") or "").strip()
            if container_id == "":
                self._logger.warning("skip container without id on host=%s", host_name)
                continue
            container_name = str(container.get("name") or container.get("container_name") or container_id)
            status = str(container.get("status") or "unknown")
            restart_count = _to_int(container.get("restart_count"))

            container_started_at = time.perf_counter()
            try:
                fetch_started_at = time.perf_counter()
                raw_stats = await _maybe_await(self._fetch_stats(host_name, container))
                fetch_duration_ms = _elapsed_ms(fetch_started_at)
                self._logger.info(
                    "metrics container stats fetch completed host=%s container=%s "
                    "duration_ms=%.1f",
                    host_name,
                    container_id,
                    fetch_duration_ms,
                )
                sample = build_metric_sample(
                    host_name=host_name,
                    container_id=container_id,
                    container_name=container_name,
                    stats=raw_stats or {},
                    previous_stats=self._previous_stats.get(container_id),
                    timestamp=timestamp,
                    status=status,
                    restart_count=restart_count,
                )
                await _maybe_await(self._save_metric(sample))
                self._previous_stats[container_id] = raw_stats or {}
                written += 1
            except Exception:  # noqa: BLE001
                self._logger.exception(
                    "metrics sample failed host=%s container=%s duration_ms=%.1f",
                    host_name,
                    container_id,
                    _elapsed_ms(container_started_at),
                )

        return written


async def _maybe_await(value):
    if inspect.isawaitable(value):
        return await value
    return value


def _to_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _elapsed_ms(started_at: float) -> float:
    return (time.perf_counter() - started_at) * 1000.0
