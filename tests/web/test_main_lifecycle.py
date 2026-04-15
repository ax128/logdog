import asyncio
from typing import Any, cast

import logdog.main as main_module
from logdog.main import create_app


def test_main_app_uses_lifespan_not_on_event_hooks() -> None:
    app = create_app(enable_metrics_scheduler=False)
    assert app.router.on_startup == []
    assert app.router.on_shutdown == []


def test_host_status_change_triggers_status_notify_sender() -> None:
    state = {"up": False}
    sent: list[tuple[str, str, str]] = []

    async def connector(_host: dict) -> dict:
        if not state["up"]:
            raise RuntimeError("docker unavailable")
        return {"server_version": "24.0.7"}

    async def notify_sender(host: str, message: str, category: str) -> bool:
        sent.append((host, message, category))
        return True

    async def run_case() -> None:
        app = create_app(
            hosts=[{"name": "local", "url": "unix:///var/run/docker.sock"}],
            host_connector=connector,
            status_notify_sender=notify_sender,
            enable_metrics_scheduler=False,
        )
        await app.state.host_manager.startup_check()
        assert sent == []

        state["up"] = True
        await app.state.host_manager.startup_check()
        assert len(sent) == 1
        assert sent[0][0] == "local"
        assert sent[0][2] == "HOST_STATUS"
        assert "disconnected -> connected" in sent[0][1]

        state["up"] = False
        await app.state.host_manager.startup_check()
        assert len(sent) == 2
        assert "connected -> disconnected" in sent[1][1]

    asyncio.run(run_case())


def test_host_status_change_sender_failure_does_not_block_extra_handlers() -> None:
    extra_calls: list[str] = []

    async def failing_notify_sender(_host: str, _message: str, _category: str) -> bool:
        raise RuntimeError("notify sender unavailable")

    async def extra_handler(payload: dict[str, Any]) -> None:
        extra_calls.append(str(payload.get("name") or ""))

    handler = main_module._build_host_status_change_handler(
        failing_notify_sender,
        extra_handlers=(extra_handler,),
    )

    async def run_case() -> None:
        await handler(
            {
                "name": "local",
                "old_status": "disconnected",
                "new_status": "connected",
                "last_error": None,
            }
        )

    asyncio.run(run_case())
    assert extra_calls == ["local"]


def test_main_app_lifespan_starts_and_stops_retention_scheduler() -> None:
    class _RetentionSchedulerStub:
        def __init__(self) -> None:
            self.started = 0
            self.stopped = 0

        async def start(self) -> None:
            self.started += 1

        async def shutdown(self) -> None:
            self.stopped += 1

    scheduler = _RetentionSchedulerStub()
    app = create_app(
        enable_metrics_scheduler=False,
        enable_retention_cleanup=True,
        retention_scheduler=cast(Any, scheduler),
    )

    async def run_case() -> None:
        async with app.router.lifespan_context(app):
            assert scheduler.started == 1
            assert scheduler.stopped == 0
        assert scheduler.stopped == 1

    asyncio.run(run_case())


def test_main_app_lifespan_closes_docker_client_pool() -> None:
    class _DockerPoolStub:
        def __init__(self) -> None:
            self.connect_calls = 0
            self.closed = 0

        async def connect_host(self, _host: dict) -> dict:
            self.connect_calls += 1
            return {"server_version": "24.0.7"}

        async def close_all(self) -> None:
            self.closed += 1

    pool = _DockerPoolStub()
    app = create_app(
        hosts=[{"name": "local", "url": "unix:///var/run/docker.sock"}],
        docker_client_pool=cast(Any, pool),
        enable_metrics_scheduler=False,
        enable_retention_cleanup=False,
    )

    async def run_case() -> None:
        async with app.router.lifespan_context(app):
            assert pool.connect_calls == 1
            assert pool.closed == 0
        assert pool.closed == 1

    asyncio.run(run_case())


def test_startup_check_does_not_start_watchers_before_lifespan() -> None:
    async def connector(_host: dict) -> dict:
        return {"server_version": "24.0.7"}

    async def run_case() -> None:
        app = create_app(
            hosts=[{"name": "local", "url": "unix:///var/run/docker.sock"}],
            host_connector=connector,
            enable_metrics_scheduler=False,
            enable_retention_cleanup=False,
        )

        await app.state.host_manager.startup_check()

        assert app.state.watch_manager is not None
        assert app.state.watch_manager._log_watchers == {}
        assert app.state.watch_manager._event_watchers == {}

    asyncio.run(run_case())


def test_main_app_lifespan_starts_and_stops_report_scheduler() -> None:
    class _ReportSchedulerStub:
        def __init__(self) -> None:
            self.started = 0
            self.stopped = 0

        async def start(self) -> None:
            self.started += 1

        async def shutdown(self) -> None:
            self.stopped += 1

    scheduler = _ReportSchedulerStub()
    app = create_app(
        enable_metrics_scheduler=False,
        enable_retention_cleanup=False,
        enable_watch_manager=False,
        report_scheduler=cast(Any, scheduler),
    )

    async def run_case() -> None:
        async with app.router.lifespan_context(app):
            assert scheduler.started == 1
            assert scheduler.stopped == 0
        assert scheduler.stopped == 1

    asyncio.run(run_case())
