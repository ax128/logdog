from __future__ import annotations

import inspect
import logging
import time
from typing import Any

from logdog.collector.host_metrics_probe import collect_host_metrics_for_host


class HostMetricsSampler:
    def __init__(
        self,
        *,
        host_manager: Any,
        collect_host_metrics: Any = collect_host_metrics_for_host,
        save_metric: Any,
        collect_load: bool = True,
        collect_network: bool = True,
        timeout_seconds: int = 8,
    ) -> None:
        self._host_manager = host_manager
        self._collect_host_metrics = collect_host_metrics
        self._save_metric = save_metric
        self._collect_load = bool(collect_load)
        self._collect_network = bool(collect_network)
        self._timeout_seconds = int(timeout_seconds)
        self._logger = logging.getLogger(__name__)

    async def sample_connected_hosts(self) -> int:
        written = 0
        statuses = self._host_manager.list_host_statuses()
        for status in statuses:
            if str(status.get("status") or "") != "connected":
                continue
            host_name = str(status.get("name") or "").strip()
            if host_name == "":
                continue
            host_cfg = self._host_manager.get_host_config(host_name)
            if not isinstance(host_cfg, dict):
                continue
            host_started_at = time.perf_counter()
            try:
                collect_started_at = time.perf_counter()
                sample = dict(
                    await _maybe_await(
                        self._collect_host_metrics(
                            host_cfg,
                            collect_load=self._collect_load,
                            collect_network=self._collect_network,
                            timeout_seconds=self._timeout_seconds,
                        )
                    )
                    or {}
                )
                collect_duration_ms = _elapsed_ms(collect_started_at)
                sample["host_name"] = host_name
                save_started_at = time.perf_counter()
                await _maybe_await(self._save_metric(sample))
                save_duration_ms = _elapsed_ms(save_started_at)
                written += 1
            except Exception:  # noqa: BLE001
                self._logger.exception(
                    "host metrics sample failed host=%s duration_ms=%.1f",
                    host_name,
                    _elapsed_ms(host_started_at),
                )
                continue
            self._logger.info(
                "host metrics sample completed host=%s collect_duration_ms=%.1f "
                "save_duration_ms=%.1f duration_ms=%.1f",
                host_name,
                collect_duration_ms,
                save_duration_ms,
                _elapsed_ms(host_started_at),
            )
        return written


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


def _elapsed_ms(started_at: float) -> float:
    return (time.perf_counter() - started_at) * 1000.0
