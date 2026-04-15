from __future__ import annotations

import pytest

from logdog.collector.scheduler import (
    HostMetricsSamplingScheduler,
    MetricsSamplingScheduler,
    ReportScheduler,
)


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


class _HostManagerStub:
    def __init__(self, statuses: list[dict]) -> None:
        self._statuses = statuses

    def list_host_statuses(self) -> list[dict]:
        return list(self._statuses)


class _SamplerStub:
    def __init__(self) -> None:
        self.calls: list[tuple[str, list[dict]]] = []

    async def sample_host(self, *, host_name: str, containers: list[dict]) -> int:
        self.calls.append((host_name, list(containers)))
        return len(containers)


class _ConfigHostManagerStub(_HostManagerStub):
    def __init__(self, statuses: list[dict], configs: dict[str, dict]) -> None:
        super().__init__(statuses)
        self._configs = configs

    def get_host_config(self, name: str) -> dict | None:
        cfg = self._configs.get(name)
        if cfg is None:
            return None
        return dict(cfg)


@pytest.mark.asyncio
async def test_scheduler_start_registers_interval_job_and_shutdown() -> None:
    fake_scheduler = _FakeScheduler()
    scheduler = MetricsSamplingScheduler(
        host_manager=_HostManagerStub([]),
        sampler=_SamplerStub(),
        list_containers=lambda _host: [],
        interval_seconds=15,
        scheduler_factory=lambda: fake_scheduler,
    )

    await scheduler.start()
    assert fake_scheduler.started is True
    assert len(fake_scheduler.jobs) == 1
    assert fake_scheduler.jobs[0]["trigger"] == "interval"
    assert fake_scheduler.jobs[0]["seconds"] == 15

    await scheduler.shutdown()
    assert fake_scheduler.shutdown_called is True
    assert fake_scheduler.shutdown_wait is False


@pytest.mark.asyncio
async def test_metrics_scheduler_applies_job_guardrails() -> None:
    fake_scheduler = _FakeScheduler()
    scheduler = MetricsSamplingScheduler(
        host_manager=_HostManagerStub([]),
        sampler=_SamplerStub(),
        list_containers=lambda _h: [],
        interval_seconds=30,
        scheduler_factory=lambda: fake_scheduler,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=30,
    )

    await scheduler.start()

    job = fake_scheduler.jobs[0]
    assert job["max_instances"] == 1
    assert job["coalesce"] is True
    assert job["misfire_grace_time"] == 30


@pytest.mark.asyncio
async def test_scheduler_run_cycle_only_connected_hosts() -> None:
    host_manager = _HostManagerStub(
        [
            {"name": "h1", "status": "connected"},
            {"name": "h2", "status": "disconnected"},
            {"name": "h3", "status": "connected"},
        ]
    )
    sampler = _SamplerStub()
    list_calls: list[str] = []

    async def list_containers(host_name: str) -> list[dict]:
        list_calls.append(host_name)
        if host_name == "h1":
            return [{"id": "c1"}]
        return [{"id": "c2"}, {"id": "c3"}]

    scheduler = MetricsSamplingScheduler(
        host_manager=host_manager,
        sampler=sampler,
        list_containers=list_containers,
        interval_seconds=30,
        scheduler_factory=_FakeScheduler,
    )

    count = await scheduler.run_cycle_once()

    assert list_calls == ["h1", "h3"]
    assert count == 3
    assert [x[0] for x in sampler.calls] == ["h1", "h3"]


@pytest.mark.asyncio
async def test_scheduler_run_cycle_isolates_host_failures() -> None:
    host_manager = _HostManagerStub(
        [
            {"name": "h1", "status": "connected"},
            {"name": "h2", "status": "connected"},
        ]
    )
    sampler = _SamplerStub()

    async def list_containers(host_name: str) -> list[dict]:
        if host_name == "h1":
            raise RuntimeError("docker unavailable")
        return [{"id": "c2"}]

    scheduler = MetricsSamplingScheduler(
        host_manager=host_manager,
        sampler=sampler,
        list_containers=list_containers,
        interval_seconds=30,
        scheduler_factory=_FakeScheduler,
    )

    count = await scheduler.run_cycle_once()

    assert count == 1
    assert [x[0] for x in sampler.calls] == ["h2"]


@pytest.mark.asyncio
async def test_scheduler_run_cycle_attempts_reconnect_before_sampling() -> None:
    class _ReconnectHostManagerStub:
        def __init__(self) -> None:
            self.check_calls = 0
            self._statuses = [{"name": "h1", "status": "disconnected"}]

        async def startup_check(self) -> list[dict]:
            self.check_calls += 1
            self._statuses = [{"name": "h1", "status": "connected"}]
            return self.list_host_statuses()

        def list_host_statuses(self) -> list[dict]:
            return list(self._statuses)

    host_manager = _ReconnectHostManagerStub()
    sampler = _SamplerStub()
    list_calls: list[str] = []

    async def list_containers(host_name: str) -> list[dict]:
        list_calls.append(host_name)
        return [{"id": "c1"}]

    scheduler = MetricsSamplingScheduler(
        host_manager=host_manager,
        sampler=sampler,
        list_containers=list_containers,
        interval_seconds=30,
        scheduler_factory=_FakeScheduler,
    )

    count = await scheduler.run_cycle_once()

    assert host_manager.check_calls == 1
    assert list_calls == ["h1"]
    assert [x[0] for x in sampler.calls] == ["h1"]
    assert count == 1


@pytest.mark.asyncio
async def test_report_scheduler_registers_host_and_global_jobs() -> None:
    fake_scheduler = _FakeScheduler()
    host_manager = _ConfigHostManagerStub(
        statuses=[
            {"name": "h1", "status": "connected"},
            {"name": "h2", "status": "connected"},
        ],
        configs={
            "h1": {
                "name": "h1",
                "schedules": [
                    {"name": "fast", "interval_seconds": 60, "template": "interval"},
                    {"name": "daily", "cron": "0 9 * * *", "template": "daily"},
                ],
            },
            "h2": {
                "name": "h2",
                "schedules": [
                    {"name": "hourly", "interval_seconds": 3600, "template": "hourly"}
                ],
            },
        },
    )
    calls: list[tuple[str, str]] = []

    async def run_host_schedule(host_name: str, schedule: dict) -> None:
        calls.append((host_name, str(schedule["name"])))

    async def run_global_schedule(schedule: dict) -> None:
        calls.append(("__global__", str(schedule["name"])))

    scheduler = ReportScheduler(
        host_manager=host_manager,
        run_host_schedule=run_host_schedule,
        run_global_schedule=run_global_schedule,
        global_schedules=[
            {
                "name": "global_daily",
                "cron": "0 8 * * *",
                "template": "daily",
                "scope": "all_hosts",
            }
        ],
        scheduler_factory=lambda: fake_scheduler,
    )

    await scheduler.start()

    assert fake_scheduler.started is True
    assert [job["id"] for job in fake_scheduler.jobs] == [
        "host-report:h1:fast",
        "host-report:h1:daily",
        "host-report:h2:hourly",
        "global-report:global_daily",
    ]
    assert fake_scheduler.jobs[0]["trigger"] == "interval"
    assert fake_scheduler.jobs[0]["seconds"] == 60
    assert fake_scheduler.jobs[1]["trigger"] == "cron"
    assert fake_scheduler.jobs[1]["minute"] == "0"
    assert fake_scheduler.jobs[1]["hour"] == "9"

    await fake_scheduler.jobs[0]["func"]()
    await fake_scheduler.jobs[3]["func"]()

    assert calls == [("h1", "fast"), ("__global__", "global_daily")]

    await scheduler.shutdown()
    assert fake_scheduler.shutdown_called is True


@pytest.mark.asyncio
async def test_report_scheduler_run_cycle_skips_hosts_without_schedule_config() -> None:
    calls: list[tuple[str, str]] = []
    host_manager = _ConfigHostManagerStub(
        statuses=[{"name": "h1", "status": "connected"}],
        configs={"h1": {"name": "h1"}},
    )
    scheduler = ReportScheduler(
        host_manager=host_manager,
        run_host_schedule=lambda *_args, **_kwargs: None,
        run_global_schedule=lambda *_args, **_kwargs: None,
        scheduler_factory=_FakeScheduler,
    )

    fake_scheduler = _FakeScheduler()
    scheduler = ReportScheduler(
        host_manager=host_manager,
        run_host_schedule=lambda host_name, schedule: calls.append(
            (host_name, schedule["name"])
        ),
        run_global_schedule=lambda schedule: calls.append(
            ("__global__", schedule["name"])
        ),
        scheduler_factory=lambda: fake_scheduler,
    )
    await scheduler.start()

    assert fake_scheduler.jobs == []
    assert calls == []


class _HostMetricsSamplerStub:
    def __init__(self, *, count: int = 2) -> None:
        self.calls = 0
        self.count = count

    async def sample_connected_hosts(self) -> int:
        self.calls += 1
        return self.count


@pytest.mark.asyncio
async def test_host_metrics_scheduler_start_registers_interval_job_and_shutdown() -> (
    None
):
    fake_scheduler = _FakeScheduler()
    sampler = _HostMetricsSamplerStub(count=1)
    scheduler = HostMetricsSamplingScheduler(
        sampler=sampler,
        interval_seconds=25,
        scheduler_factory=lambda: fake_scheduler,
    )

    await scheduler.start()
    assert fake_scheduler.started is True
    assert len(fake_scheduler.jobs) == 1
    assert fake_scheduler.jobs[0]["trigger"] == "interval"
    assert fake_scheduler.jobs[0]["seconds"] == 25

    count = await scheduler.run_cycle_once()
    assert count == 1
    assert sampler.calls == 1

    await scheduler.shutdown()
    assert fake_scheduler.shutdown_called is True
