import asyncio
import json
import sqlite3

import pytest

import logwatch.main as main_module
from logwatch.main import create_app


class _SyncCursor:
    def __init__(self, cur: sqlite3.Cursor):
        self._cur = cur

    async def fetchone(self):
        return self._cur.fetchone()


class _SyncAioSqliteConn:
    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    async def execute(self, sql: str, params=()):
        return _SyncCursor(self._conn.execute(sql, params))

    async def commit(self) -> None:
        self._conn.commit()

    async def close(self) -> None:
        self._conn.close()


class _NotifyRouterStub:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str]] = []

    async def send(self, host: str, message: str, category: str) -> bool:
        self.calls.append((host, message, category))
        return True


def test_main_app_binds_status_notify_sender_from_notify_router() -> None:
    state = {"up": False}
    router = _NotifyRouterStub()

    async def connector(_host: dict) -> dict:
        if not state["up"]:
            raise RuntimeError("docker unavailable")
        return {"server_version": "24.0.7"}

    async def run_case() -> None:
        app = create_app(
            hosts=[{"name": "local", "url": "unix:///var/run/docker.sock"}],
            host_connector=connector,
            notify_router=router,
            enable_metrics_scheduler=False,
        )

        await app.state.host_manager.startup_check()
        assert router.calls == []

        state["up"] = True
        await app.state.host_manager.startup_check()
        assert len(router.calls) == 1
        assert router.calls[0][0] == "local"
        assert router.calls[0][2] == "HOST_STATUS"

    asyncio.run(run_case())


def test_main_app_prefers_explicit_status_sender_over_notify_router() -> None:
    state = {"up": False}
    router = _NotifyRouterStub()
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
            notify_router=router,
            enable_metrics_scheduler=False,
        )

        await app.state.host_manager.startup_check()
        assert sent == []
        assert router.calls == []

        state["up"] = True
        await app.state.host_manager.startup_check()
        assert len(sent) == 1
        assert len(router.calls) == 0
        assert sent[0][0] == "local"
        assert sent[0][2] == "HOST_STATUS"

    asyncio.run(run_case())


def test_main_app_builds_notify_router_from_app_config_targets() -> None:
    state = {"up": False}
    telegram_calls: list[tuple[str, str]] = []
    wechat_calls: list[tuple[str, str]] = []

    async def connector(_host: dict) -> dict:
        if not state["up"]:
            raise RuntimeError("docker unavailable")
        return {"server_version": "24.0.7"}

    async def telegram_send(target: str, message: str, parse_mode: str = "") -> None:
        telegram_calls.append((target, message))

    async def wechat_send(target: str, message: str, parse_mode: str = "") -> None:
        wechat_calls.append((target, message))

    async def run_case() -> None:
        app = create_app(
            hosts=[{"name": "local", "url": "unix:///var/run/docker.sock"}],
            host_connector=connector,
            enable_metrics_scheduler=False,
            app_config={
                "notify": {
                    "telegram": {
                        "enabled": True,
                        "chat_ids": ["tg-1", "tg-2"],
                    },
                    "wechat": {
                        "enabled": True,
                        "webhook_urls": ["wx-1"],
                    },
                }
            },
            telegram_send_func=telegram_send,
            wechat_send_func=wechat_send,
        )

        await app.state.host_manager.startup_check()
        assert telegram_calls == []
        assert wechat_calls == []

        state["up"] = True
        await app.state.host_manager.startup_check()

        assert [x[0] for x in telegram_calls] == ["tg-1", "tg-2"]
        assert [x[0] for x in wechat_calls] == ["wx-1"]
        assert "disconnected -> connected" in telegram_calls[0][1]

    asyncio.run(run_case())


def test_main_app_notify_router_uses_env_targets_when_config_empty(monkeypatch) -> None:
    state = {"up": False}
    telegram_calls: list[tuple[str, str]] = []
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "tg-env-1, tg-env-2")

    async def connector(_host: dict) -> dict:
        if not state["up"]:
            raise RuntimeError("docker unavailable")
        return {"server_version": "24.0.7"}

    async def telegram_send(target: str, message: str, parse_mode: str = "") -> None:
        telegram_calls.append((target, message))

    async def run_case() -> None:
        app = create_app(
            hosts=[{"name": "local", "url": "unix:///var/run/docker.sock"}],
            host_connector=connector,
            enable_metrics_scheduler=False,
            app_config={
                "notify": {
                    "telegram": {
                        "enabled": True,
                        "chat_ids": [],
                    }
                }
            },
            telegram_send_func=telegram_send,
        )

        await app.state.host_manager.startup_check()
        state["up"] = True
        await app.state.host_manager.startup_check()

        assert [x[0] for x in telegram_calls] == ["tg-env-1", "tg-env-2"]
        assert "disconnected -> connected" in telegram_calls[0][1]

    asyncio.run(run_case())


def test_main_app_notify_router_persists_send_failures_by_default() -> None:
    state = {"up": False}
    sync_conn = sqlite3.connect(":memory:")
    wrapped_conn = _SyncAioSqliteConn(sync_conn)
    connect_calls: list[str] = []

    async def connector(_host: dict) -> dict:
        if not state["up"]:
            raise RuntimeError("docker unavailable")
        return {"server_version": "24.0.7"}

    async def telegram_send(_target: str, _message: str, parse_mode: str = "") -> None:
        raise RuntimeError("telegram down")

    def connect_db(db_path: str) -> _SyncAioSqliteConn:
        connect_calls.append(db_path)
        return wrapped_conn

    async def run_case() -> None:
        app = create_app(
            hosts=[{"name": "local", "url": "unix:///var/run/docker.sock"}],
            host_connector=connector,
            enable_metrics_scheduler=False,
            enable_retention_cleanup=False,
            metrics_db_path=":memory:",
            metrics_db_connect=connect_db,
            app_config={
                "notify": {
                    "telegram": {
                        "enabled": True,
                        "chat_ids": ["tg-1"],
                    }
                }
            },
            telegram_send_func=telegram_send,
        )

        await app.state.host_manager.startup_check()
        state["up"] = True
        await app.state.host_manager.startup_check()

    asyncio.run(run_case())

    try:
        (count,) = sync_conn.execute("SELECT COUNT(*) FROM send_failed;").fetchone()
        assert count == 1

        (raw_payload,) = sync_conn.execute(
            "SELECT payload FROM send_failed ORDER BY id DESC LIMIT 1;"
        ).fetchone()
    finally:
        sync_conn.close()

    payload = json.loads(raw_payload)
    assert payload["host"] == "local"
    assert payload["channel"] == "telegram"
    assert payload["category"] == "HOST_STATUS"
    assert "telegram down" in payload["error"]
    assert connect_calls == [":memory:"]


def test_main_app_notify_router_applies_telegram_message_mode_from_config() -> None:
    telegram_calls: list[tuple[str, str]] = []

    async def telegram_send(target: str, message: str, parse_mode: str = "") -> None:
        telegram_calls.append((target, message))

    async def run_case() -> None:
        app = create_app(
            hosts=[{"name": "local", "url": "unix:///var/run/docker.sock"}],
            enable_metrics_scheduler=False,
            enable_retention_cleanup=False,
            enable_watch_manager=False,
            app_config={
                "notify": {
                    "telegram": {
                        "enabled": True,
                        "chat_ids": ["tg-1"],
                        "message_mode": "doc",
                    }
                }
            },
            telegram_send_func=telegram_send,
        )
        assert app.state.notify_router is not None
        ok = await app.state.notify_router.send("local", "service down", "ERROR")
        assert ok is True

    asyncio.run(run_case())

    assert len(telegram_calls) == 1
    assert telegram_calls[0][0] == "tg-1"
    assert "LogWatch Notification" in telegram_calls[0][1]
    assert "service down" in telegram_calls[0][1]


def test_main_app_notify_router_routes_specific_container_to_specific_channel() -> None:
    telegram_calls: list[tuple[str, str]] = []
    wechat_calls: list[tuple[str, str]] = []

    async def telegram_send(target: str, message: str, parse_mode: str = "") -> None:
        telegram_calls.append((target, message))

    async def wechat_send(target: str, message: str, parse_mode: str = "") -> None:
        wechat_calls.append((target, message))

    async def run_case() -> None:
        app = create_app(
            hosts=[{"name": "prod-a", "url": "unix:///var/run/docker.sock"}],
            enable_metrics_scheduler=False,
            enable_watch_manager=False,
            app_config={
                "notify": {
                    "telegram": {"enabled": True, "chat_ids": ["tg-default"]},
                    "wechat": {"enabled": True, "webhook_urls": ["wx-api"]},
                    "routing": {
                        "default_channels": ["telegram"],
                        "rules": [
                            {
                                "name": "api-route",
                                "match": {"hosts": ["prod-a"], "containers": ["api"]},
                                "deliver": {
                                    "channels": ["wechat"],
                                    "message_mode": "md",
                                },
                            }
                        ],
                    },
                }
            },
            telegram_send_func=telegram_send,
            wechat_send_func=wechat_send,
        )

        await app.state.notify_router.send(
            "prod-a", "api down", "ERROR", context={"container_name": "api"}
        )
        await app.state.notify_router.send(
            "prod-a", "worker down", "ERROR", context={"container_name": "worker"}
        )

    asyncio.run(run_case())

    assert [x[0] for x in wechat_calls] == ["wx-api"]
    assert [x[0] for x in telegram_calls] == ["tg-default"]


def test_main_app_notify_router_supports_named_multi_channels() -> None:
    telegram_calls: list[tuple[str, str]] = []
    wechat_calls: list[tuple[str, str]] = []

    async def telegram_send(target: str, message: str, parse_mode: str = "") -> None:
        telegram_calls.append((target, message))

    async def wechat_send(target: str, message: str, parse_mode: str = "") -> None:
        wechat_calls.append((target, message))

    async def run_case() -> None:
        app = create_app(
            hosts=[{"name": "prod-a", "url": "unix:///var/run/docker.sock"}],
            enable_metrics_scheduler=False,
            enable_watch_manager=False,
            app_config={
                "notify": {
                    "channels": {
                        "tel1": {
                            "type": "telegram",
                            "enabled": True,
                            "chat_ids": ["tg-1"],
                        },
                        "tel2": {
                            "type": "telegram",
                            "enabled": True,
                            "chat_ids": ["tg-2"],
                        },
                        "wx1": {
                            "type": "wechat",
                            "enabled": True,
                            "webhook_urls": ["wx-1"],
                        },
                    },
                    "routing": {
                        "default_channels": ["tel1"],
                        "rules": [
                            {
                                "name": "api-route",
                                "match": {
                                    "hosts": ["prod-a"],
                                    "containers": ["api"],
                                },
                                "deliver": {
                                    "channels": ["tel2", "wx1"],
                                },
                            }
                        ],
                    },
                }
            },
            telegram_send_func=telegram_send,
            wechat_send_func=wechat_send,
        )

        await app.state.notify_router.send(
            "prod-a", "api down", "ERROR", context={"container_name": "api"}
        )
        await app.state.notify_router.send(
            "prod-a", "worker down", "ERROR", context={"container_name": "worker"}
        )

    asyncio.run(run_case())

    assert [x[0] for x in telegram_calls] == ["tg-2", "tg-1"]
    assert [x[0] for x in wechat_calls] == ["wx-1"]


def test_main_app_host_notify_channels_filters_named_channels() -> None:
    telegram_calls: list[tuple[str, str]] = []

    async def telegram_send(target: str, message: str, parse_mode: str = "") -> None:
        telegram_calls.append((target, message))

    async def run_case() -> None:
        app = create_app(
            hosts=[
                {
                    "name": "prod-a",
                    "url": "unix:///var/run/docker.sock",
                    "notify": {"channels": ["tel2"]},
                }
            ],
            enable_metrics_scheduler=False,
            enable_watch_manager=False,
            app_config={
                "notify": {
                    "channels": {
                        "tel1": {
                            "type": "telegram",
                            "enabled": True,
                            "chat_ids": ["tg-1"],
                        },
                        "tel2": {
                            "type": "telegram",
                            "enabled": True,
                            "chat_ids": ["tg-2"],
                        },
                    },
                    "routing": {
                        "default_channels": ["tel1"],
                    },
                }
            },
            telegram_send_func=telegram_send,
        )

        await app.state.notify_router.send(
            "prod-a", "worker down", "ERROR", context={"container_name": "worker"}
        )

    asyncio.run(run_case())

    assert [x[0] for x in telegram_calls] == ["tg-2"]


def test_main_app_rejects_named_channel_without_type() -> None:
    with pytest.raises(ValueError, match="type is required"):
        create_app(
            hosts=[{"name": "prod-a", "url": "unix:///var/run/docker.sock"}],
            enable_metrics_scheduler=False,
            enable_watch_manager=False,
            app_config={
                "notify": {
                    "channels": {
                        "tel1": {
                            "enabled": True,
                            "chat_ids": ["tg-1"],
                        }
                    }
                }
            },
        )


def test_main_app_notify_router_supports_weixin_and_wecom_named_channels() -> None:
    weixin_calls: list[tuple[str, str]] = []
    wecom_calls: list[tuple[str, str]] = []

    async def weixin_send(target: str, message: str, parse_mode: str = "") -> None:
        weixin_calls.append((target, message))

    async def wecom_send(target: str, message: str, parse_mode: str = "") -> None:
        wecom_calls.append((target, message))

    async def run_case() -> None:
        app = create_app(
            hosts=[{"name": "prod-a", "url": "unix:///var/run/docker.sock"}],
            enable_metrics_scheduler=False,
            enable_watch_manager=False,
            app_config={
                "notify": {
                    "channels": {
                        "wx-user": {
                            "type": "weixin",
                            "enabled": True,
                            "targets": ["wx-user-1"],
                        },
                        "wecom-room": {
                            "type": "wecom",
                            "enabled": True,
                            "targets": ["wecom-room-1"],
                        },
                    },
                    "routing": {
                        "default_channels": ["wx-user"],
                        "rules": [
                            {
                                "name": "critical-to-wecom",
                                "match": {
                                    "hosts": ["prod-a"],
                                    "categories": ["ERROR"],
                                },
                                "deliver": {"channels": ["wecom-room"]},
                            }
                        ],
                    },
                }
            },
            weixin_send_func=weixin_send,
            wecom_send_func=wecom_send,
        )

        await app.state.notify_router.send("prod-a", "error msg", "ERROR")
        await app.state.notify_router.send("prod-a", "info msg", "INFO")

    asyncio.run(run_case())

    assert [x[0] for x in wecom_calls] == ["wecom-room-1"]
    assert [x[0] for x in weixin_calls] == ["wx-user-1"]


def test_main_app_notify_router_supports_legacy_wechat_alias_for_wecom() -> None:
    wecom_calls: list[tuple[str, str]] = []

    async def wecom_send(target: str, message: str, parse_mode: str = "") -> None:
        wecom_calls.append((target, message))

    async def run_case() -> None:
        app = create_app(
            hosts=[{"name": "prod-a", "url": "unix:///var/run/docker.sock"}],
            enable_metrics_scheduler=False,
            enable_watch_manager=False,
            app_config={
                "notify": {
                    "wechat": {
                        "enabled": True,
                        "webhook_urls": ["legacy-wechat-target"],
                    },
                    "routing": {
                        "default_channels": ["wechat"],
                    },
                }
            },
            wecom_send_func=wecom_send,
        )

        await app.state.notify_router.send("prod-a", "legacy alias", "INFO")

    asyncio.run(run_case())

    assert [x[0] for x in wecom_calls] == ["legacy-wechat-target"]


def test_main_app_notify_router_drops_message_when_rule_selects_unknown_channel() -> (
    None
):
    telegram_calls: list[tuple[str, str]] = []

    async def telegram_send(target: str, message: str, parse_mode: str = "") -> None:
        telegram_calls.append((target, message))

    async def run_case() -> bool:
        app = create_app(
            hosts=[{"name": "prod-a", "url": "unix:///var/run/docker.sock"}],
            enable_metrics_scheduler=False,
            enable_watch_manager=False,
            app_config={
                "notify": {
                    "channels": {
                        "tel1": {
                            "type": "telegram",
                            "enabled": True,
                            "chat_ids": ["tg-1"],
                        }
                    },
                    "routing": {
                        "default_channels": ["tel1"],
                        "rules": [
                            {
                                "name": "bad-channel",
                                "match": {"hosts": ["prod-a"], "categories": ["ERROR"]},
                                "deliver": {"channels": ["missing-channel"]},
                            }
                        ],
                    },
                }
            },
            telegram_send_func=telegram_send,
        )

        return await app.state.notify_router.send("prod-a", "error msg", "ERROR")

    ok = asyncio.run(run_case())

    assert ok is False
    assert telegram_calls == []


def test_main_app_notify_router_wechat_route_does_not_duplicate_when_wecom_also_configured() -> (
    None
):
    wechat_calls: list[tuple[str, str]] = []
    wecom_calls: list[tuple[str, str]] = []

    async def wechat_send(target: str, message: str, parse_mode: str = "") -> None:
        wechat_calls.append((target, message))

    async def wecom_send(target: str, message: str, parse_mode: str = "") -> None:
        wecom_calls.append((target, message))

    async def run_case() -> bool:
        app = create_app(
            hosts=[{"name": "prod-a", "url": "unix:///var/run/docker.sock"}],
            enable_metrics_scheduler=False,
            enable_watch_manager=False,
            app_config={
                "notify": {
                    "wechat": {
                        "enabled": True,
                        "webhook_urls": ["wechat-legacy"],
                    },
                    "wecom": {
                        "enabled": True,
                        "targets": ["wecom-room"],
                    },
                    "routing": {
                        "default_channels": ["wechat"],
                    },
                }
            },
            wechat_send_func=wechat_send,
            wecom_send_func=wecom_send,
        )

        return await app.state.notify_router.send("prod-a", "route wechat", "INFO")

    ok = asyncio.run(run_case())

    assert ok is True
    assert [x[0] for x in wechat_calls] == ["wechat-legacy"]
    assert wecom_calls == []


def test_build_global_schedule_notifiers_supports_home_channels_and_channel_ids_aliases() -> (
    None
):
    weixin_calls: list[tuple[str, str]] = []
    wecom_calls: list[tuple[str, str]] = []

    async def weixin_send(target: str, message: str, parse_mode: str = "") -> None:
        weixin_calls.append((target, message))

    async def wecom_send(target: str, message: str, parse_mode: str = "") -> None:
        wecom_calls.append((target, message))

    notifiers = main_module._build_global_schedule_notifiers(
        schedule={
            "notify_target": {
                "weixin": {
                    "enabled": True,
                    "home_channels": ["wx-alias-room"],
                },
                "wecom": {
                    "enabled": True,
                    "channel_ids": ["wecom-alias-room"],
                },
            }
        },
        app_config={"notify": {}},
        telegram_send_func=None,
        wechat_send_func=None,
        weixin_send_func=weixin_send,
        wecom_send_func=wecom_send,
    )

    async def run_case() -> None:
        for notifier in notifiers:
            await notifier.send("__global__", "daily summary", "DAILY")

    asyncio.run(run_case())

    assert [x[0] for x in weixin_calls] == ["wx-alias-room"]
    assert [x[0] for x in wecom_calls] == ["wecom-alias-room"]


def test_main_app_notify_router_uses_builtin_telegram_sender_with_auto_target(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = {"up": False}
    telegram_calls: list[tuple[str, str]] = []
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "telegram-bot-token")

    def fake_builder(_token: str):
        def send(target: str, message: str, parse_mode: str = "") -> None:
            telegram_calls.append((target, message))

        setattr(send, "_logwatch_supports_auto_target", True)
        return send

    async def connector(_host: dict) -> dict:
        if not state["up"]:
            raise RuntimeError("docker unavailable")
        return {"server_version": "24.0.7"}

    async def run_case() -> None:
        monkeypatch.setattr(main_module, "build_telegram_bot_token_sender", fake_builder)
        app = create_app(
            hosts=[{"name": "local", "url": "unix:///var/run/docker.sock"}],
            host_connector=connector,
            enable_metrics_scheduler=False,
            app_config={
                "notify": {
                    "telegram": {
                        "enabled": True,
                        "chat_ids": [],
                        "auto_chat_id": True,
                    }
                }
            },
        )

        await app.state.host_manager.startup_check()
        state["up"] = True
        await app.state.host_manager.startup_check()

    asyncio.run(run_case())

    assert len(telegram_calls) == 1
    assert telegram_calls[0][0] == "__auto__"
    assert "disconnected -> connected" in telegram_calls[0][1]


def test_main_app_notify_router_supports_multiple_telegram_bot_tokens_from_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = {"up": False}
    telegram_calls: list[tuple[str, str, str]] = []
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)

    def fake_builder(token: str):
        normalized = str(token or "").strip()
        if normalized == "":
            return None

        def send(target: str, message: str, parse_mode: str = "") -> None:
            telegram_calls.append((normalized, target, message))

        setattr(send, "_logwatch_supports_auto_target", True)
        return send

    async def connector(_host: dict) -> dict:
        if not state["up"]:
            raise RuntimeError("docker unavailable")
        return {"server_version": "24.0.7"}

    async def run_case() -> None:
        monkeypatch.setattr(main_module, "build_telegram_bot_token_sender", fake_builder)
        app = create_app(
            hosts=[{"name": "local", "url": "unix:///var/run/docker.sock"}],
            host_connector=connector,
            enable_metrics_scheduler=False,
            app_config={
                "notify": {
                    "telegram": {
                        "enabled": True,
                        "bot_tokens": ["bot-token-1", "bot-token-2"],
                        "chat_ids": [],
                        "auto_chat_id": True,
                    }
                }
            },
        )

        await app.state.host_manager.startup_check()
        state["up"] = True
        await app.state.host_manager.startup_check()

    asyncio.run(run_case())

    assert [x[0] for x in telegram_calls] == ["bot-token-1", "bot-token-2"]
    assert [x[1] for x in telegram_calls] == ["__auto__", "__auto__"]
    assert "disconnected -> connected" in telegram_calls[0][2]


def test_main_app_notify_router_uses_builtin_wecom_sender_from_webhook_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = {"up": False}
    wecom_calls: list[tuple[str, str]] = []
    monkeypatch.setenv(
        "WECOM_WEBHOOK_URL",
        "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=test",
    )

    def fake_builder(*, timeout_seconds: float = 10.0, max_retries: int = 3):
        def send(target: str, message: str, parse_mode: str = "") -> None:
            wecom_calls.append((target, message))

        return send

    async def connector(_host: dict) -> dict:
        if not state["up"]:
            raise RuntimeError("docker unavailable")
        return {"server_version": "24.0.7"}

    async def run_case() -> None:
        monkeypatch.setattr(main_module, "build_wecom_webhook_sender", fake_builder)
        app = create_app(
            hosts=[{"name": "local", "url": "unix:///var/run/docker.sock"}],
            host_connector=connector,
            enable_metrics_scheduler=False,
            app_config={
                "notify": {
                    "wecom": {
                        "enabled": True,
                        "targets": [],
                    }
                }
            },
        )

        await app.state.host_manager.startup_check()
        state["up"] = True
        await app.state.host_manager.startup_check()

    asyncio.run(run_case())

    assert len(wecom_calls) == 1
    assert wecom_calls[0][0].startswith(
        "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key="
    )
    assert "disconnected -> connected" in wecom_calls[0][1]
