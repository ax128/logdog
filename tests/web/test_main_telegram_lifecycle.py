from __future__ import annotations

import asyncio

from logwatch.main import create_app


class _FakeUpdater:
    def __init__(self, events: list[str]) -> None:
        self._events = events

    async def start_polling(self, **_kw) -> None:
        self._events.append("polling:start")

    async def stop(self) -> None:
        self._events.append("polling:stop")


class _FakeApplication:
    def __init__(self, events: list[str]) -> None:
        self._events = events
        self.handlers: list[object] = []
        self.updater = _FakeUpdater(events)

    def add_handler(self, handler: object) -> None:
        self.handlers.append(handler)

    async def initialize(self) -> None:
        self._events.append("app:initialize")

    async def start(self) -> None:
        self._events.append("app:start")

    async def stop(self) -> None:
        self._events.append("app:stop")

    async def shutdown(self) -> None:
        self._events.append("app:shutdown")


def test_main_app_lifespan_starts_and_stops_telegram_runtime(monkeypatch) -> None:
    events: list[str] = []
    application = _FakeApplication(events)
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "telegram-token")

    app = create_app(
        app_config={"agent": {"authorized_users": {"telegram": [42]}}},
        enable_metrics_scheduler=False,
        enable_retention_cleanup=False,
        enable_watch_manager=False,
        telegram_application_factory=lambda _token: application,
        telegram_handler_binder=lambda app, handler: app.add_handler(handler),
    )

    async def run_case() -> None:
        async with app.router.lifespan_context(app):
            assert app.state.telegram_runtime is not None
            assert len(application.handlers) == 1
            assert events == ["app:initialize", "app:start", "polling:start"]

        assert events == [
            "app:initialize",
            "app:start",
            "polling:start",
            "polling:stop",
            "app:stop",
            "app:shutdown",
        ]

    asyncio.run(run_case())
