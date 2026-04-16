from __future__ import annotations

import asyncio
import inspect
import logging
from typing import Any

_CYCLE_TIMEOUT_SECONDS = 60


def _default_scheduler_factory():
    try:
        from apscheduler.schedulers.asyncio import AsyncIOScheduler
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("APScheduler is not installed") from exc
    return AsyncIOScheduler()


class MetricsSamplingScheduler:
    def __init__(
        self,
        *,
        host_manager: Any,
        sampler: Any,
        list_containers: Any,
        interval_seconds: int = 30,
        scheduler_factory: Any | None = None,
        max_instances: int = 1,
        coalesce: bool = True,
        misfire_grace_time: int = 30,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be > 0")
        if int(max_instances) <= 0:
            raise ValueError("max_instances must be > 0")
        if int(misfire_grace_time) <= 0:
            raise ValueError("misfire_grace_time must be > 0")

        self._host_manager = host_manager
        self._sampler = sampler
        self._list_containers = list_containers
        self._interval_seconds = int(interval_seconds)
        self._scheduler_factory = scheduler_factory or _default_scheduler_factory
        self._max_instances = int(max_instances)
        self._coalesce = bool(coalesce)
        self._misfire_grace_time = int(misfire_grace_time)
        self._scheduler = None
        self._logger = logging.getLogger(__name__)

    async def start(self) -> None:
        if self._scheduler is not None:
            return
        scheduler = self._scheduler_factory()
        scheduler.add_job(
            self._run_cycle_job,
            "interval",
            seconds=self._interval_seconds,
            id="metrics-sampling",
            replace_existing=True,
            max_instances=self._max_instances,
            coalesce=self._coalesce,
            misfire_grace_time=self._misfire_grace_time,
        )
        scheduler.start()
        self._scheduler = scheduler

    async def shutdown(self) -> None:
        scheduler = self._scheduler
        if scheduler is None:
            return
        shutdown_fn = getattr(scheduler, "shutdown", None)
        if callable(shutdown_fn):
            result = shutdown_fn(wait=False)
            if inspect.isawaitable(result):
                await result
        self._scheduler = None

    async def run_cycle_once(self) -> int:
        total = 0
        refresh_connectivity = getattr(self._host_manager, "startup_check", None)
        if callable(refresh_connectivity):
            try:
                await _maybe_await(refresh_connectivity())
            except Exception:  # noqa: BLE001
                self._logger.exception("host connectivity refresh failed")
        statuses = self._host_manager.list_host_statuses()
        for host in statuses:
            if host.get("status") != "connected":
                continue
            host_name = str(host.get("name") or "")
            if host_name == "":
                continue
            try:
                containers = await _maybe_await(self._list_containers(host_name))
                count = await self._sampler.sample_host(
                    host_name=host_name,
                    containers=list(containers or []),
                )
            except Exception:  # noqa: BLE001
                self._logger.exception(
                    "metrics sampling host cycle failed host=%s",
                    host_name,
                )
                continue
            total += int(count)
        return total

    async def _run_cycle_job(self) -> None:
        try:
            await asyncio.wait_for(
                self.run_cycle_once(), timeout=_CYCLE_TIMEOUT_SECONDS
            )
        except asyncio.TimeoutError:
            self._logger.warning(
                "metrics sampling cycle timed out after %ds",
                _CYCLE_TIMEOUT_SECONDS,
            )
        except Exception:  # noqa: BLE001
            self._logger.exception("metrics sampling cycle failed")


class HostMetricsSamplingScheduler:
    def __init__(
        self,
        *,
        sampler: Any,
        interval_seconds: int = 30,
        scheduler_factory: Any | None = None,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be > 0")

        self._sampler = sampler
        self._interval_seconds = int(interval_seconds)
        self._scheduler_factory = scheduler_factory or _default_scheduler_factory
        self._scheduler = None
        self._logger = logging.getLogger(__name__)

    async def start(self) -> None:
        if self._scheduler is not None:
            return
        scheduler = self._scheduler_factory()
        scheduler.add_job(
            self._run_cycle_job,
            "interval",
            seconds=self._interval_seconds,
            id="host-metrics-sampling",
            replace_existing=True,
        )
        scheduler.start()
        self._scheduler = scheduler

    async def shutdown(self) -> None:
        scheduler = self._scheduler
        if scheduler is None:
            return
        shutdown_fn = getattr(scheduler, "shutdown", None)
        if callable(shutdown_fn):
            result = shutdown_fn(wait=False)
            if inspect.isawaitable(result):
                await result
        self._scheduler = None

    async def run_cycle_once(self) -> int:
        return int(await _maybe_await(self._sampler.sample_connected_hosts()))

    async def _run_cycle_job(self) -> None:
        try:
            await asyncio.wait_for(
                self.run_cycle_once(), timeout=_CYCLE_TIMEOUT_SECONDS
            )
        except asyncio.TimeoutError:
            self._logger.warning(
                "host metrics sampling cycle timed out after %ds",
                _CYCLE_TIMEOUT_SECONDS,
            )
        except Exception:  # noqa: BLE001
            self._logger.exception("host metrics sampling cycle failed")


class ReportScheduler:
    def __init__(
        self,
        *,
        host_manager: Any,
        run_host_schedule: Any,
        run_global_schedule: Any,
        global_schedules: list[dict[str, Any]] | None = None,
        scheduler_factory: Any | None = None,
    ) -> None:
        self._host_manager = host_manager
        self._run_host_schedule = run_host_schedule
        self._run_global_schedule = run_global_schedule
        self._global_schedules = [dict(item) for item in list(global_schedules or [])]
        self._scheduler_factory = scheduler_factory or _default_scheduler_factory
        self._scheduler = None
        self._logger = logging.getLogger(__name__)

    async def start(self) -> None:
        if self._scheduler is not None:
            return
        scheduler = self._scheduler_factory()
        for host_name, schedule in self._iter_host_schedules():
            scheduler.add_job(
                self._build_host_job(host_name, schedule),
                _schedule_trigger(schedule),
                id=f"host-report:{host_name}:{schedule['name']}",
                replace_existing=True,
                **_schedule_trigger_kwargs(schedule),
            )
        for schedule in self._global_schedules:
            if str(schedule.get("scope") or "") != "all_hosts":
                continue
            scheduler.add_job(
                self._build_global_job(schedule),
                _schedule_trigger(schedule),
                id=f"global-report:{schedule['name']}",
                replace_existing=True,
                **_schedule_trigger_kwargs(schedule),
            )
        scheduler.start()
        self._scheduler = scheduler

    async def shutdown(self) -> None:
        scheduler = self._scheduler
        if scheduler is None:
            return
        shutdown_fn = getattr(scheduler, "shutdown", None)
        if callable(shutdown_fn):
            result = shutdown_fn(wait=False)
            if inspect.isawaitable(result):
                await result
        self._scheduler = None

    def _iter_host_schedules(self) -> list[tuple[str, dict[str, Any]]]:
        out: list[tuple[str, dict[str, Any]]] = []
        for status in self._host_manager.list_host_statuses():
            host_name = str(status.get("name") or "").strip()
            if host_name == "":
                continue
            host_config = self._host_manager.get_host_config(host_name)
            if not isinstance(host_config, dict):
                continue
            raw_schedules = host_config.get("schedules")
            if not isinstance(raw_schedules, list):
                continue
            for schedule in raw_schedules:
                if not isinstance(schedule, dict):
                    continue
                if str(schedule.get("name") or "").strip() == "":
                    continue
                out.append((host_name, dict(schedule)))
        return out

    def _build_host_job(self, host_name: str, schedule: dict[str, Any]):
        async def job() -> None:
            try:
                await _maybe_await(self._run_host_schedule(host_name, dict(schedule)))
            except Exception:  # noqa: BLE001
                self._logger.exception(
                    "host report schedule failed host=%s schedule=%s",
                    host_name,
                    schedule.get("name"),
                )

        return job

    def _build_global_job(self, schedule: dict[str, Any]):
        async def job() -> None:
            try:
                await _maybe_await(self._run_global_schedule(dict(schedule)))
            except Exception:  # noqa: BLE001
                self._logger.exception(
                    "global report schedule failed schedule=%s",
                    schedule.get("name"),
                )

        return job


def _schedule_trigger(schedule: dict[str, Any]) -> str:
    if "interval_seconds" in schedule:
        return "interval"
    if "cron" in schedule:
        return "cron"
    raise ValueError(f"schedule must define interval_seconds or cron: {schedule!r}")


def _schedule_trigger_kwargs(schedule: dict[str, Any]) -> dict[str, Any]:
    if "interval_seconds" in schedule:
        seconds = int(schedule["interval_seconds"])
        if seconds <= 0:
            raise ValueError("interval_seconds must be > 0")
        return {"seconds": seconds}

    cron_expr = str(schedule.get("cron") or "").strip()
    parts = cron_expr.split()
    if len(parts) != 5:
        raise ValueError(f"cron expression must have 5 fields: {cron_expr!r}")
    minute, hour, day, month, day_of_week = parts
    return {
        "minute": minute,
        "hour": hour,
        "day": day,
        "month": month,
        "day_of_week": day_of_week,
    }


async def _maybe_await(value):
    if inspect.isawaitable(value):
        return await value
    return value
