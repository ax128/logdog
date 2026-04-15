from __future__ import annotations

import asyncio

import logwatch.main as main_module
from logwatch.main import create_app


def test_main_app_uses_app_config_metrics_interval_when_not_explicit() -> None:
    app = create_app(
        app_config={"metrics": {"sample_interval_seconds": 77}},
    )
    assert app.state.metrics_scheduler._interval_seconds == 77


def test_main_app_explicit_metrics_interval_overrides_app_config() -> None:
    app = create_app(
        metrics_interval_seconds=21,
        app_config={"metrics": {"sample_interval_seconds": 77}},
    )
    assert app.state.metrics_scheduler._interval_seconds == 21


def test_main_app_uses_app_config_retention_interval_when_not_explicit() -> None:
    app = create_app(
        enable_metrics_scheduler=False,
        app_config={"storage": {"retention": {"cleanup_interval_seconds": 123}}},
    )
    assert app.state.retention_scheduler._interval_seconds == 123


def test_main_app_builds_report_scheduler_from_host_and_global_schedules() -> None:
    app = create_app(
        enable_metrics_scheduler=False,
        enable_retention_cleanup=False,
        enable_watch_manager=False,
        app_config={
            "defaults": {
                "schedules": [
                    {"name": "fast", "interval_seconds": 60, "template": "interval"}
                ]
            },
            "hosts": [
                {"name": "host-a", "url": "unix:///var/run/docker.sock"},
            ],
            "global_schedules": [
                {
                    "name": "global-daily",
                    "cron": "0 8 * * *",
                    "template": "daily",
                    "scope": "all_hosts",
                }
            ],
        },
    )
    assert app.state.report_scheduler is not None


def test_main_app_builds_storm_controller_from_app_config() -> None:
    app = create_app(
        enable_metrics_scheduler=False,
        enable_retention_cleanup=False,
        enable_watch_manager=False,
        app_config={
            "alert_storm": {
                "enabled": True,
                "window_seconds": 120,
                "threshold": 5,
                "suppress_minutes": 10,
            }
        },
    )
    assert app.state.storm_controller is not None


def test_main_app_explicit_retention_interval_overrides_app_config() -> None:
    app = create_app(
        enable_metrics_scheduler=False,
        retention_interval_seconds=11,
        app_config={"storage": {"retention": {"cleanup_interval_seconds": 123}}},
    )
    assert app.state.retention_scheduler._interval_seconds == 11


def test_main_app_passes_docker_pool_limits_to_pool_ctor(monkeypatch) -> None:
    captured: list[dict] = []

    class _PoolStub:
        async def connect_host(self, _host: dict) -> dict:
            return {"server_version": "24.0.7"}

        async def close_all(self) -> None:
            return None

    def fake_pool_ctor(*, max_clients=None, max_idle_seconds=None):
        captured.append(
            {
                "max_clients": max_clients,
                "max_idle_seconds": max_idle_seconds,
            }
        )
        return _PoolStub()

    monkeypatch.setattr(main_module, "DockerClientPool", fake_pool_ctor)

    app = create_app(
        hosts=[{"name": "local", "url": "unix:///var/run/docker.sock"}],
        enable_metrics_scheduler=False,
        enable_retention_cleanup=False,
        docker_pool_max_clients=3,
        docker_pool_max_idle_seconds=60.0,
    )
    assert app.state.docker_client_pool is not None
    assert captured == [{"max_clients": 3, "max_idle_seconds": 60.0}]


def test_main_app_uses_app_config_docker_pool_limits_when_not_explicit(
    monkeypatch,
) -> None:
    captured: list[dict] = []

    class _PoolStub:
        async def connect_host(self, _host: dict) -> dict:
            return {"server_version": "24.0.7"}

        async def close_all(self) -> None:
            return None

    def fake_pool_ctor(*, max_clients=None, max_idle_seconds=None):
        captured.append(
            {
                "max_clients": max_clients,
                "max_idle_seconds": max_idle_seconds,
            }
        )
        return _PoolStub()

    monkeypatch.setattr(main_module, "DockerClientPool", fake_pool_ctor)

    app = create_app(
        hosts=[{"name": "local", "url": "unix:///var/run/docker.sock"}],
        enable_metrics_scheduler=False,
        enable_retention_cleanup=False,
        app_config={"docker": {"pool": {"max_clients": 7, "max_idle_seconds": 42}}},
    )
    assert app.state.docker_client_pool is not None
    assert captured == [{"max_clients": 7, "max_idle_seconds": 42.0}]


def test_global_schedule_notify_target_overrides_global_notify_targets() -> None:
    sent_targets: list[str] = []

    async def telegram_send(target: str, _message: str, parse_mode: str = "") -> None:
        sent_targets.append(target)

    notifiers = main_module._build_global_schedule_notifiers(
        schedule={
            "notify_target": {
                "telegram": {
                    "enabled": True,
                    "chat_ids": ["tg-schedule"],
                }
            }
        },
        app_config={
            "notify": {
                "telegram": {
                    "enabled": True,
                    "chat_ids": ["tg-global"],
                }
            }
        },
        telegram_send_func=telegram_send,
        wechat_send_func=None,
    )

    assert len(notifiers) == 1

    asyncio.run(notifiers[0].send("__global__", "report", "REPORT"))

    assert sent_targets == ["tg-schedule"]


def test_global_schedule_notify_target_supports_weixin_and_wecom() -> None:
    weixin_targets: list[str] = []
    wecom_targets: list[str] = []

    async def weixin_send(target: str, _message: str, parse_mode: str = "") -> None:
        weixin_targets.append(target)

    async def wecom_send(target: str, _message: str, parse_mode: str = "") -> None:
        wecom_targets.append(target)

    notifiers = main_module._build_global_schedule_notifiers(
        schedule={
            "notify_target": {
                "weixin": {
                    "enabled": True,
                    "targets": ["wx-user-1"],
                },
                "wecom": {
                    "enabled": True,
                    "targets": ["wecom-room-1"],
                },
            }
        },
        app_config={"notify": {}},
        telegram_send_func=None,
        wechat_send_func=None,
        weixin_send_func=weixin_send,
        wecom_send_func=wecom_send,
    )

    assert len(notifiers) == 2
    for notifier in notifiers:
        asyncio.run(notifier.send("__global__", "report", "REPORT"))

    assert weixin_targets == ["wx-user-1"]
    assert wecom_targets == ["wecom-room-1"]


def test_main_app_notify_router_supports_global_reload_notifications() -> None:
    sent_targets: list[str] = []

    async def telegram_send(target: str, _message: str, parse_mode: str = "") -> None:
        sent_targets.append(target)

    app = create_app(
        web_auth_token="web-token",
        web_admin_token="admin-token",
        hosts=[{"name": "local", "url": "unix:///var/run/docker.sock"}],
        enable_metrics_scheduler=False,
        enable_retention_cleanup=False,
        enable_watch_manager=False,
        telegram_send_func=telegram_send,
        app_config={
            "notify": {
                "telegram": {
                    "enabled": True,
                    "chat_ids": ["tg-global"],
                }
            }
        },
    )
    assert app.state.notify_router is not None

    pushed = asyncio.run(
        app.state.notify_router.send(
            "__global__",
            "config reload failed",
            "CONFIG_RELOAD",
        )
    )

    assert pushed is True
    assert sent_targets == ["tg-global"]
