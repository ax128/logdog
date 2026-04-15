from __future__ import annotations

import inspect
from pathlib import Path
from textwrap import dedent

import pytest
from fastapi.testclient import TestClient

import logwatch.main as main_module


def _write_yaml(path: Path, content: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(dedent(content).strip() + "\n", encoding="utf-8")
    return path


def test_reload_error_message_sanitizes_token_like_fields() -> None:
    exc = RuntimeError("token=abc api_key=xyz password=ppp")
    message = main_module._sanitize_reload_error_message(exc)

    assert "token=abc" not in message
    assert "api_key=xyz" not in message
    assert "password=ppp" not in message
    assert "***REDACTED***" in message


def test_reload_error_message_sanitizes_json_token_like_fields() -> None:
    exc = RuntimeError('{"token":"abc","api_key":"xyz","password":"ppp"}')
    message = main_module._sanitize_reload_error_message(exc)

    assert '"token":"abc"' not in message
    assert '"api_key":"xyz"' not in message
    assert '"password":"ppp"' not in message
    assert "***REDACTED***" in message


def test_create_app_requires_tokens_when_not_explicit_or_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("WEB_AUTH_TOKEN", raising=False)
    monkeypatch.delenv("WEB_ADMIN_TOKEN", raising=False)

    with pytest.raises(ValueError, match="WEB_AUTH_TOKEN"):
        main_module.create_app(
            hosts=[{"name": "local", "url": "unix:///var/run/docker.sock"}],
            enable_metrics_scheduler=False,
            enable_retention_cleanup=False,
        )


def test_create_app_reads_web_tokens_from_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("WEB_AUTH_TOKEN", "env-web-token")
    monkeypatch.setenv("WEB_ADMIN_TOKEN", "env-admin-token")

    app = main_module.create_app(
        hosts=[{"name": "local", "url": "unix:///var/run/docker.sock"}],
        enable_metrics_scheduler=False,
        enable_retention_cleanup=False,
    )

    with TestClient(app) as client:
        ok = client.get(
            "/api/hosts",
            headers={"Authorization": "Bearer env-web-token"},
        )
        forbidden = client.get(
            "/api/hosts",
            headers={"Authorization": "Bearer web-token"},
        )

    assert ok.status_code == 200
    assert forbidden.status_code == 403


def test_create_app_prefers_explicit_app_config_over_config_path_and_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert "config_path" in inspect.signature(main_module.create_app).parameters

    arg_path = _write_yaml(
        tmp_path / "from-arg.yaml",
        """
        hosts:
          - name: from-arg
            url: unix:///from-arg.sock
        """,
    )
    env_path = _write_yaml(
        tmp_path / "from-env.yaml",
        """
        hosts:
          - name: from-env
            url: unix:///from-env.sock
        """,
    )
    monkeypatch.setenv("LOGWATCH_CONFIG", str(env_path))

    app = main_module.create_app(
        app_config={"hosts": [{"name": "from-app", "url": "unix:///from-app.sock"}]},
        config_path=str(arg_path),
        enable_metrics_scheduler=False,
        enable_retention_cleanup=False,
    )

    loaded_host = app.state.host_manager.get_host_config("from-app")
    assert loaded_host is not None
    assert loaded_host["name"] == "from-app"
    assert loaded_host["url"] == "unix:///from-app.sock"
    assert app.state.host_manager.get_host_config("from-arg") is None
    assert app.state.host_manager.get_host_config("from-env") is None


def test_create_app_loads_hosts_from_config_path_and_expands_defaults(
    tmp_path: Path,
) -> None:
    assert "config_path" in inspect.signature(main_module.create_app).parameters

    config_path = _write_yaml(
        tmp_path / "logwatch.yaml",
        """
        defaults:
          ssh_key: /keys/default.pem
        hosts:
          - name: prod
            url: ssh://deploy@10.0.1.10
        """,
    )

    app = main_module.create_app(
        config_path=str(config_path),
        enable_metrics_scheduler=False,
        enable_retention_cleanup=False,
    )

    host = app.state.host_manager.get_host_config("prod")
    assert host is not None
    assert host["name"] == "prod"
    assert host["url"] == "ssh://deploy@10.0.1.10"
    assert host["ssh_key"] == "/keys/default.pem"


def test_create_app_uses_default_config_path_when_present(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert "config_path" in inspect.signature(main_module.create_app).parameters

    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("LOGWATCH_CONFIG", raising=False)
    _write_yaml(
        tmp_path / "config" / "logwatch.yaml",
        """
        hosts:
          - name: default-file
            url: unix:///default.sock
        """,
    )

    app = main_module.create_app(
        enable_metrics_scheduler=False,
        enable_retention_cleanup=False,
    )

    host = app.state.host_manager.get_host_config("default-file")
    assert host is not None
    assert host["name"] == "default-file"
    assert host["url"] == "unix:///default.sock"


def test_create_app_preserves_explicit_hosts_seam_over_loaded_config(
    tmp_path: Path,
) -> None:
    assert "config_path" in inspect.signature(main_module.create_app).parameters

    config_path = _write_yaml(
        tmp_path / "logwatch.yaml",
        """
        hosts:
          - name: from-file
            url: unix:///from-file.sock
        """,
    )

    app = main_module.create_app(
        config_path=str(config_path),
        hosts=[{"name": "explicit", "url": "unix:///explicit.sock"}],
        enable_metrics_scheduler=False,
        enable_retention_cleanup=False,
    )

    assert app.state.host_manager.get_host_config("explicit") == {
        "name": "explicit",
        "url": "unix:///explicit.sock",
    }
    assert app.state.host_manager.get_host_config("from-file") is None


def test_create_app_explicit_hosts_seam_ignores_invalid_implicit_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("LOGWATCH_CONFIG", raising=False)
    _write_yaml(
        tmp_path / "config" / "logwatch.yaml",
        """
        - name: broken
          url: unix:///broken.sock
        """,
    )

    app = main_module.create_app(
        hosts=[{"name": "explicit", "url": "unix:///explicit.sock"}],
        enable_metrics_scheduler=False,
        enable_retention_cleanup=False,
    )

    assert app.state.host_manager.get_host_config("explicit") == {
        "name": "explicit",
        "url": "unix:///explicit.sock",
    }


@pytest.mark.asyncio
async def test_main_app_default_reload_action_reloads_hosts_from_config_file(
    tmp_path: Path,
) -> None:
    config_path = _write_yaml(
        tmp_path / "logwatch.yaml",
        """
        hosts:
          - name: alpha
            url: unix:///alpha.sock
        """,
    )

    app = main_module.create_app(
        config_path=str(config_path),
        enable_metrics_scheduler=False,
        enable_retention_cleanup=False,
        enable_watch_manager=False,
    )

    _write_yaml(
        config_path,
        """
        hosts:
          - name: beta
            url: unix:///beta.sock
        """,
    )

    summary = await app.state.reload_action()

    assert summary["ok"] is True
    assert summary["added"] == ["beta"]
    assert summary["removed_requires_restart"] == ["alpha"]
    assert app.state.host_manager.get_host_config("beta") is not None
    assert app.state.host_manager.get_host_config("beta")["url"] == "unix:///beta.sock"


@pytest.mark.asyncio
async def test_main_app_default_reload_action_preserves_old_state_on_invalid_config(
    tmp_path: Path,
) -> None:
    config_path = _write_yaml(
        tmp_path / "logwatch.yaml",
        """
        hosts:
          - name: alpha
            url: unix:///alpha.sock
        """,
    )

    app = main_module.create_app(
        config_path=str(config_path),
        enable_metrics_scheduler=False,
        enable_retention_cleanup=False,
        enable_watch_manager=False,
    )

    _write_yaml(
        config_path,
        """
        - name: broken
          url: unix:///broken.sock
        """,
    )

    with pytest.raises(TypeError):
        await app.state.reload_action()

    preserved = app.state.host_manager.get_host_config("alpha")
    assert preserved is not None
    assert preserved["name"] == "alpha"
    assert preserved["url"] == "unix:///alpha.sock"


@pytest.mark.asyncio
async def test_main_app_default_reload_action_rebuilds_notify_router_targets(
    tmp_path: Path,
) -> None:
    config_path = _write_yaml(
        tmp_path / "logwatch.yaml",
        """
        hosts:
          - name: local
            url: unix:///var/run/docker.sock
            notify:
              telegram:
                enabled: true
                chat_ids: ["tg-old"]
        """,
    )

    sent_targets: list[tuple[str, str]] = []

    async def telegram_send(target: str, message: str) -> None:
        sent_targets.append((target, message))

    app = main_module.create_app(
        config_path=str(config_path),
        enable_metrics_scheduler=False,
        enable_retention_cleanup=False,
        enable_watch_manager=False,
        telegram_send_func=telegram_send,
    )

    assert app.state.notify_router is not None
    await app.state.notify_router.send("local", "before", "TEST")

    _write_yaml(
        config_path,
        """
        hosts:
          - name: local
            url: unix:///var/run/docker.sock
            notify:
              telegram:
                enabled: true
                chat_ids: ["tg-new"]
        """,
    )

    summary = await app.state.reload_action()
    assert summary["ok"] is True

    assert app.state.notify_router is not None
    await app.state.notify_router.send("local", "after", "TEST")

    assert sent_targets[0][0] == "tg-old"
    assert sent_targets[1][0] == "tg-new"


@pytest.mark.asyncio
async def test_main_app_reload_rebuilds_telegram_runtime_when_lifespan_running(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeUpdater:
        def __init__(self, events: list[str], name: str) -> None:
            self._events = events
            self._name = name

        async def start_polling(self) -> None:
            self._events.append(f"{self._name}:polling:start")

        async def stop(self) -> None:
            self._events.append(f"{self._name}:polling:stop")

    class _FakeApplication:
        def __init__(self, events: list[str], name: str) -> None:
            self._events = events
            self._name = name
            self.handlers: list[object] = []
            self.updater = _FakeUpdater(events, name)

        def add_handler(self, handler: object) -> None:
            self.handlers.append(handler)

        async def initialize(self) -> None:
            self._events.append(f"{self._name}:app:initialize")

        async def start(self) -> None:
            self._events.append(f"{self._name}:app:start")

        async def stop(self) -> None:
            self._events.append(f"{self._name}:app:stop")

        async def shutdown(self) -> None:
            self._events.append(f"{self._name}:app:shutdown")

    config_path = _write_yaml(
        tmp_path / "logwatch.yaml",
        """
        hosts:
          - name: local
            url: unix:///var/run/docker.sock
        agent:
          authorized_users:
            telegram: ["1"]
        """,
    )
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "telegram-token")

    events: list[str] = []
    apps: list[_FakeApplication] = []

    def application_factory(_token: str) -> _FakeApplication:
        app_name = f"app{len(apps) + 1}"
        app = _FakeApplication(events, app_name)
        apps.append(app)
        return app

    app = main_module.create_app(
        config_path=str(config_path),
        enable_metrics_scheduler=False,
        enable_retention_cleanup=False,
        enable_watch_manager=False,
        telegram_application_factory=application_factory,
        telegram_handler_binder=lambda app_obj, handler: app_obj.add_handler(handler),
    )

    async with app.router.lifespan_context(app):
        assert len(apps) == 1
        assert len(apps[0].handlers) == 1

        _write_yaml(
            config_path,
            """
            hosts:
              - name: local
                url: unix:///var/run/docker.sock
            agent:
              authorized_users:
                telegram: ["2"]
            """,
        )

        summary = await app.state.reload_action()
        assert summary["ok"] is True
        assert len(apps) == 2

        old_shutdown_index = events.index("app1:app:shutdown")
        new_start_index = events.index("app2:app:start")
        assert new_start_index < old_shutdown_index

        runtime = app.state.telegram_runtime
        assert runtime is not None
        assert runtime._authorized_user_ids == {"2"}

    assert "app2:app:shutdown" in events


@pytest.mark.asyncio
async def test_main_app_reload_keeps_old_telegram_runtime_when_candidate_start_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeUpdater:
        def __init__(self, events: list[str], name: str) -> None:
            self._events = events
            self._name = name

        async def start_polling(self) -> None:
            self._events.append(f"{self._name}:polling:start")

        async def stop(self) -> None:
            self._events.append(f"{self._name}:polling:stop")

    class _FakeApplication:
        def __init__(
            self,
            events: list[str],
            name: str,
            *,
            fail_on_start: bool,
        ) -> None:
            self._events = events
            self._name = name
            self._fail_on_start = fail_on_start
            self.handlers: list[object] = []
            self.updater = _FakeUpdater(events, name)

        def add_handler(self, handler: object) -> None:
            self.handlers.append(handler)

        async def initialize(self) -> None:
            self._events.append(f"{self._name}:app:initialize")

        async def start(self) -> None:
            self._events.append(f"{self._name}:app:start")
            if self._fail_on_start:
                raise RuntimeError("candidate start failed")

        async def stop(self) -> None:
            self._events.append(f"{self._name}:app:stop")

        async def shutdown(self) -> None:
            self._events.append(f"{self._name}:app:shutdown")

    config_path = _write_yaml(
        tmp_path / "logwatch.yaml",
        """
        hosts:
          - name: local
            url: unix:///var/run/docker.sock
        agent:
          authorized_users:
            telegram: ["1"]
        """,
    )
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "telegram-token")

    events: list[str] = []
    apps: list[_FakeApplication] = []

    def application_factory(_token: str) -> _FakeApplication:
        app_name = f"app{len(apps) + 1}"
        app = _FakeApplication(
            events,
            app_name,
            fail_on_start=len(apps) == 1,
        )
        apps.append(app)
        return app

    app = main_module.create_app(
        config_path=str(config_path),
        enable_metrics_scheduler=False,
        enable_retention_cleanup=False,
        enable_watch_manager=False,
        telegram_application_factory=application_factory,
        telegram_handler_binder=lambda app_obj, handler: app_obj.add_handler(handler),
    )

    async with app.router.lifespan_context(app):
        assert len(apps) == 1
        runtime = app.state.telegram_runtime
        assert runtime is not None
        assert runtime._authorized_user_ids == {"1"}

        _write_yaml(
            config_path,
            """
            hosts:
              - name: local
                url: unix:///var/run/docker.sock
            agent:
              authorized_users:
                telegram: ["2"]
            """,
        )

        with pytest.raises(RuntimeError, match="candidate start failed"):
            await app.state.reload_action()

        assert len(apps) == 2
        runtime = app.state.telegram_runtime
        assert runtime is not None
        assert runtime._authorized_user_ids == {"1"}
        assert "app1:app:shutdown" not in events
        assert "app2:app:start" in events
        assert "app2:app:shutdown" in events

    assert "app1:app:shutdown" in events


@pytest.mark.asyncio
async def test_main_app_reload_restores_host_manager_state_when_candidate_start_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeUpdater:
        async def start_polling(self) -> None:
            return None

        async def stop(self) -> None:
            return None

    class _FakeApplication:
        def __init__(self, *, fail_on_start: bool) -> None:
            self._fail_on_start = fail_on_start
            self.updater = _FakeUpdater()

        def add_handler(self, _handler: object) -> None:
            return None

        async def initialize(self) -> None:
            return None

        async def start(self) -> None:
            if self._fail_on_start:
                raise RuntimeError("candidate start failed")

        async def stop(self) -> None:
            return None

        async def shutdown(self) -> None:
            return None

    config_path = _write_yaml(
        tmp_path / "logwatch.yaml",
        """
        hosts:
          - name: alpha
            url: unix:///alpha.sock
        agent:
          authorized_users:
            telegram: ["1"]
        """,
    )
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "telegram-token")

    build_count = {"n": 0}

    def application_factory(_token: str) -> _FakeApplication:
        build_count["n"] += 1
        return _FakeApplication(fail_on_start=build_count["n"] >= 2)

    app = main_module.create_app(
        config_path=str(config_path),
        enable_metrics_scheduler=False,
        enable_retention_cleanup=False,
        enable_watch_manager=False,
        telegram_application_factory=application_factory,
        telegram_handler_binder=lambda app_obj, handler: app_obj.add_handler(handler),
    )

    async with app.router.lifespan_context(app):
        _write_yaml(
            config_path,
            """
            hosts:
              - name: beta
                url: unix:///beta.sock
            agent:
              authorized_users:
                telegram: ["2"]
            """,
        )

        with pytest.raises(RuntimeError, match="candidate start failed"):
            await app.state.reload_action()

        assert app.state.host_manager.get_host_config("alpha") is not None
        assert app.state.host_manager.get_host_config("beta") is None
        assert app.state.reload_degraded is None


@pytest.mark.asyncio
async def test_main_app_reload_starts_report_scheduler_when_added_during_running_lifespan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _ReportSchedulerSpy:
        instances: list["_ReportSchedulerSpy"] = []

        def __init__(self, **_kwargs: object) -> None:
            self.start_calls = 0
            self.shutdown_calls = 0
            self._scheduler: object | None = None
            type(self).instances.append(self)

        async def start(self) -> None:
            self.start_calls += 1
            self._scheduler = object()

        async def shutdown(self) -> None:
            self.shutdown_calls += 1
            self._scheduler = None

    monkeypatch.setattr(main_module, "ReportScheduler", _ReportSchedulerSpy)

    config_path = _write_yaml(
        tmp_path / "logwatch.yaml",
        """
        hosts:
          - name: local
            url: unix:///var/run/docker.sock
        """,
    )

    app = main_module.create_app(
        config_path=str(config_path),
        enable_metrics_scheduler=False,
        enable_retention_cleanup=False,
        enable_watch_manager=False,
    )

    async with app.router.lifespan_context(app):
        assert app.state.report_scheduler is None

        _write_yaml(
            config_path,
            """
            hosts:
              - name: local
                url: unix:///var/run/docker.sock
                schedules:
                  - name: every-minute
                    interval_seconds: 60
                    template: interval
            """,
        )

        summary = await app.state.reload_action()
        assert summary["ok"] is True

        scheduler = app.state.report_scheduler
        assert isinstance(scheduler, _ReportSchedulerSpy)
        assert scheduler.start_calls == 1
        assert scheduler._scheduler is not None
