from __future__ import annotations

import pytest

from logdog.core.retention_scheduler import RetentionCleanupScheduler


class _FakeScheduler:
    def __init__(self) -> None:
        self.jobs: list[dict] = []
        self.started = False
        self.shutdown_called = False
        self.shutdown_wait = None

    def add_job(self, func, trigger: str, **kwargs) -> None:
        self.jobs.append({"func": func, "trigger": trigger, **kwargs})

    def start(self) -> None:
        self.started = True

    def shutdown(self, *, wait: bool = False) -> None:
        self.shutdown_called = True
        self.shutdown_wait = wait


@pytest.mark.asyncio
async def test_retention_scheduler_start_registers_interval_job_and_shutdown() -> None:
    fake_scheduler = _FakeScheduler()
    calls: list[str] = []

    async def run_cleanup() -> None:
        calls.append("cleanup")

    scheduler = RetentionCleanupScheduler(
        run_cleanup=run_cleanup,
        interval_seconds=60,
        scheduler_factory=lambda: fake_scheduler,
    )

    await scheduler.start()
    assert fake_scheduler.started is True
    assert len(fake_scheduler.jobs) == 1
    assert fake_scheduler.jobs[0]["trigger"] == "interval"
    assert fake_scheduler.jobs[0]["seconds"] == 60
    assert fake_scheduler.jobs[0]["id"] == "retention-cleanup"

    await scheduler.run_cleanup_once()
    assert calls == ["cleanup"]

    await scheduler.shutdown()
    assert fake_scheduler.shutdown_called is True
    assert fake_scheduler.shutdown_wait is False
