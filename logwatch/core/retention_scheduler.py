from __future__ import annotations

import inspect
import logging
from typing import Any


def _default_scheduler_factory():
    try:
        from apscheduler.schedulers.asyncio import AsyncIOScheduler
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("APScheduler is not installed") from exc
    return AsyncIOScheduler()


class RetentionCleanupScheduler:
    def __init__(
        self,
        *,
        run_cleanup: Any,
        interval_seconds: int = 86400,
        scheduler_factory: Any | None = None,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be > 0")
        self._run_cleanup = run_cleanup
        self._interval_seconds = int(interval_seconds)
        self._scheduler_factory = scheduler_factory or _default_scheduler_factory
        self._scheduler = None
        self._logger = logging.getLogger(__name__)

    async def start(self) -> None:
        if self._scheduler is not None:
            return
        scheduler = self._scheduler_factory()
        scheduler.add_job(
            self._run_cleanup_job,
            "interval",
            seconds=self._interval_seconds,
            id="retention-cleanup",
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

    async def run_cleanup_once(self) -> None:
        await _maybe_await(self._run_cleanup())

    async def _run_cleanup_job(self) -> None:
        try:
            await self.run_cleanup_once()
        except Exception:  # noqa: BLE001
            self._logger.exception("retention cleanup cycle failed")


async def _maybe_await(value):
    if inspect.isawaitable(value):
        return await value
    return value
