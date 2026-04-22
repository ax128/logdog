from __future__ import annotations

import asyncio
import inspect
import json
from pathlib import Path
from textwrap import dedent

import pytest

import logdog.main as main_module


def _write_yaml(path: Path, content: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(dedent(content).strip() + "\n", encoding="utf-8")
    return path


class _HybridAdapterStub:
    instances: list["_HybridAdapterStub"] = []

    def __init__(self, *, direct_backend, worker_backend) -> None:
        self.direct_backend = direct_backend
        self.worker_backend = worker_backend
        self.connect_calls: list[dict[str, object]] = []
        self.collect_calls: list[tuple[dict[str, object], dict[str, object]]] = []
        self.close_host_calls: list[object] = []
        self.close_all_calls = 0
        _HybridAdapterStub.instances.append(self)

    async def connect_host(self, host: dict[str, object]) -> dict[str, object]:
        self.connect_calls.append(dict(host))
        return {"server_version": "24.0.7"}

    async def list_containers_for_host(self, host: dict[str, object]) -> list[dict]:
        _ = host
        return [{"id": "c1", "name": "svc"}]

    async def fetch_container_stats(
        self, host: dict[str, object], container: dict[str, object]
    ) -> dict[str, object]:
        _ = host, container
        return {"cpu_stats": {"cpu_usage": {"total_usage": 1}, "system_cpu_usage": 1}}

    async def query_container_logs(
        self, host: dict[str, object], container: dict[str, object], **kwargs
    ) -> list[dict[str, object]]:
        _ = host, container, kwargs
        return []

    async def stream_container_logs(
        self, host: dict[str, object], container: dict[str, object], **kwargs
    ):
        _ = host, container, kwargs
        if False:
            yield {}

    async def stream_docker_events(self, host: dict[str, object], **kwargs):
        _ = host, kwargs
        if False:
            yield {}

    async def restart_container_for_host(
        self, host: dict[str, object], container: dict[str, object], **kwargs
    ) -> dict[str, object]:
        _ = host, container, kwargs
        return {}

    async def exec_container_for_host(
        self, host: dict[str, object], container: dict[str, object], **kwargs
    ) -> dict[str, object]:
        _ = host, container, kwargs
        return {}

    async def collect_host_metrics_for_host(
        self, host: dict[str, object], **kwargs
    ) -> dict[str, object]:
        self.collect_calls.append((dict(host), dict(kwargs)))
        return {"source": "remote-worker", "cpu_percent": 12.5}

    async def close_host(self, host_name_or_host) -> None:
        self.close_host_calls.append(host_name_or_host)

    async def close_all(self) -> None:
        self.close_all_calls += 1


class _StickyHybridAdapterStub(_HybridAdapterStub):
    def __init__(self, *, direct_backend, worker_backend) -> None:
        super().__init__(direct_backend=direct_backend, worker_backend=worker_backend)
        self.active_sessions: dict[str, dict[str, object]] = {}

    async def connect_host(self, host: dict[str, object]) -> dict[str, object]:
        self.connect_calls.append(dict(host))
        host_name = str(host.get("name") or "")
        session = self.active_sessions.get(host_name)
        if session is None:
            session = dict(host)
            self.active_sessions[host_name] = session
        return {
            "server_version": "24.0.7",
            "session_host": dict(session),
        }

    async def close_host(self, host_name_or_host) -> None:
        self.close_host_calls.append(host_name_or_host)
        if isinstance(host_name_or_host, dict):
            host_name = str(host_name_or_host.get("name") or "")
        else:
            host_name = str(host_name_or_host or "")
        self.active_sessions.pop(host_name, None)


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

    assert (
        main_module._resolve_web_auth_token(
            explicit_token=None,
            allow_insecure_default_tokens=False,
        )
        == "env-web-token"
    )
    assert (
        main_module._resolve_web_admin_token(
            explicit_token=None,
            allow_insecure_default_tokens=False,
        )
        == "env-admin-token"
    )

    # Create the app to ensure create_app accepts env-provided tokens without explicit args.
    main_module.create_app(
        hosts=[{"name": "local", "url": "unix:///var/run/docker.sock"}],
        host_connector=lambda _host: {"server_version": "24.0.7"},
        enable_metrics_scheduler=False,
        enable_retention_cleanup=False,
        enable_watch_manager=False,
    )


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
    monkeypatch.setenv("LOGDOG_CONFIG", str(env_path))

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


def test_create_app_uses_hybrid_host_adapter_for_remote_worker_hosts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _HybridAdapterStub.instances.clear()
    monkeypatch.setattr(main_module, "HybridHostAdapter", _HybridAdapterStub)

    app = main_module.create_app(
        app_config={
            "hosts": [
                {
                    "name": "prod",
                    "url": "ssh://deploy@10.0.1.10:22",
                    "remote_worker": {"enabled": True},
                }
            ]
        },
        host_connector=lambda _host: {"server_version": "24.0.7"},
        enable_metrics_scheduler=False,
        enable_retention_cleanup=False,
        enable_watch_manager=False,
    )

    assert len(_HybridAdapterStub.instances) == 1
    adapter = _HybridAdapterStub.instances[0]
    assert app.state.docker_client_pool is adapter
    assert adapter.direct_backend is not None
    assert adapter.worker_backend is not None


def test_create_app_prefers_adapter_host_metrics_collection_for_remote_worker_hosts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _HybridAdapterStub.instances.clear()
    monkeypatch.setattr(main_module, "HybridHostAdapter", _HybridAdapterStub)

    ssh_key_path = tmp_path / "id_ed25519"
    ssh_key_path.write_text("key", encoding="utf-8")
    ssh_key_path.chmod(0o600)

    saved_samples: list[dict[str, object]] = []

    async def save_host_metric(sample: dict[str, object]) -> None:
        saved_samples.append(dict(sample))

    app = main_module.create_app(
        app_config={
            "hosts": [
                {
                    "name": "prod",
                    "url": "ssh://deploy@10.0.1.10:22",
                    "ssh_key": str(ssh_key_path),
                    "remote_worker": {"enabled": True},
                }
            ],
            "metrics": {
                "host_system": {
                    "enabled": True,
                    "sample_interval_seconds": 30,
                    "collect_load": True,
                    "collect_network": True,
                }
            },
        },
        host_connector=lambda _host: {"server_version": "24.0.7"},
        save_host_metric_fn=save_host_metric,
        enable_metrics_scheduler=False,
        enable_retention_cleanup=False,
        enable_watch_manager=False,
    )

    async def run_case() -> None:
        await app.state.host_manager.startup_check()
        written = await app.state.host_metrics_scheduler.run_cycle_once()
        assert written == 1

    asyncio.run(run_case())

    adapter = _HybridAdapterStub.instances[0]
    assert adapter.connect_calls[0]["name"] == "prod"
    assert adapter.connect_calls[0]["url"] == "ssh://deploy@10.0.1.10:22"
    assert adapter.connect_calls[0]["ssh_key"] == str(ssh_key_path)
    assert adapter.connect_calls[0]["remote_worker"] == {"enabled": True}
    assert adapter.collect_calls[0][0]["name"] == "prod"
    assert adapter.collect_calls[0][0]["url"] == "ssh://deploy@10.0.1.10:22"
    assert adapter.collect_calls[0][0]["ssh_key"] == str(ssh_key_path)
    assert adapter.collect_calls[0][0]["remote_worker"] == {"enabled": True}
    assert adapter.collect_calls[0][1] == {
        "collect_load": True,
        "collect_network": True,
        "timeout_seconds": 8,
    }
    assert saved_samples == [
        {
            "source": "remote-worker",
            "cpu_percent": 12.5,
            "host_name": "prod",
        }
    ]


def test_create_app_loads_hosts_from_config_path_and_expands_defaults(
    tmp_path: Path,
) -> None:
    assert "config_path" in inspect.signature(main_module.create_app).parameters

    config_path = _write_yaml(
        tmp_path / "logdog.yaml",
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
    monkeypatch.delenv("LOGDOG_CONFIG", raising=False)
    _write_yaml(
        tmp_path / "config" / "logdog.yaml",
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
        tmp_path / "logdog.yaml",
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
    monkeypatch.delenv("LOGDOG_CONFIG", raising=False)
    _write_yaml(
        tmp_path / "config" / "logdog.yaml",
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


def test_build_telegram_runtime_prefers_explicit_authorized_users_over_persisted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeRuntime:
        def __init__(
            self,
            authorized_user_ids: set[str],
            pairing_code: str | None,
        ) -> None:
            self._authorized_user_ids = set(authorized_user_ids)
            self._pairing_code = pairing_code

        def set_authorized_user_persist_callback(self, _callback) -> None:
            return None

        def set_sender(self, _sender) -> None:
            return None

    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "telegram-token")
    monkeypatch.setattr(
        main_module,
        "_load_persisted_authorized_users",
        lambda: {"persisted-user"},
    )

    def build_runtime(**kwargs):
        return _FakeRuntime(
            set(kwargs["authorized_user_ids"]),
            kwargs.get("pairing_code"),
        )

    monkeypatch.setattr(main_module, "build_telegram_bot_runtime", build_runtime)

    runtime = main_module._build_telegram_runtime_from_config(
        app_config={"agent": {"authorized_users": {"telegram": ["cfg-user"]}}},
        chat_runtime=None,
    )

    assert runtime is not None
    assert runtime._authorized_user_ids == {"cfg-user"}


def test_build_telegram_runtime_clears_persisted_users_when_config_list_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeRuntime:
        def __init__(
            self,
            authorized_user_ids: set[str],
            pairing_code: str | None,
        ) -> None:
            self._authorized_user_ids = set(authorized_user_ids)
            self._pairing_code = pairing_code

        def set_authorized_user_persist_callback(self, _callback) -> None:
            return None

        def set_sender(self, _sender) -> None:
            return None

    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "telegram-token")
    monkeypatch.setattr(
        main_module,
        "_load_persisted_authorized_users",
        lambda: {"persisted-user"},
    )

    def build_runtime(**kwargs):
        return _FakeRuntime(
            set(kwargs["authorized_user_ids"]),
            kwargs.get("pairing_code"),
        )

    monkeypatch.setattr(main_module, "build_telegram_bot_runtime", build_runtime)

    runtime = main_module._build_telegram_runtime_from_config(
        app_config={
            "agent": {
                "authorized_users": {
                    "telegram": [],
                    "telegram_pairing_code": "123123",
                }
            }
        },
        chat_runtime=None,
    )

    assert runtime is not None
    assert runtime._authorized_user_ids == set()
    assert runtime._pairing_code == "123123"


def test_build_telegram_runtime_rejects_null_telegram_authorized_users() -> None:
    with pytest.raises(TypeError, match="must be a list"):
        main_module._build_telegram_runtime_from_config(
            app_config={
                "agent": {
                    "authorized_users": {
                        "telegram": None,
                        "telegram_pairing_code": "123123",
                    }
                }
            },
            chat_runtime=None,
        )


def test_build_telegram_runtime_clears_persisted_users_file_when_config_list_empty(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeRuntime:
        def __init__(
            self,
            authorized_user_ids: set[str],
            pairing_code: str | None,
        ) -> None:
            self._authorized_user_ids = set(authorized_user_ids)
            self._pairing_code = pairing_code

        def set_authorized_user_persist_callback(self, _callback) -> None:
            return None

        def set_sender(self, _sender) -> None:
            return None

    state_path = tmp_path / "logdog_authorized.json"
    state_path.write_text('{"telegram": ["persisted-user"]}', encoding="utf-8")
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "telegram-token")
    monkeypatch.setattr(main_module, "_AUTHORIZED_USERS_STATE_FILE", str(state_path))

    def build_runtime(**kwargs):
        return _FakeRuntime(
            set(kwargs["authorized_user_ids"]),
            kwargs.get("pairing_code"),
        )

    monkeypatch.setattr(main_module, "build_telegram_bot_runtime", build_runtime)

    runtime = main_module._build_telegram_runtime_from_config(
        app_config={
            "agent": {
                "authorized_users": {
                    "telegram": [],
                    "telegram_pairing_code": "123123",
                }
            }
        },
        chat_runtime=None,
    )

    assert runtime is not None
    assert runtime._authorized_user_ids == set()
    assert main_module._load_persisted_authorized_users() == set()
    assert json.loads(state_path.read_text(encoding="utf-8")) == {"telegram": []}


def test_chat_id_cache_is_scoped_by_authorized_telegram_users(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache_path = tmp_path / "logdog_chat_id.json"
    monkeypatch.setattr(main_module, "_CHAT_ID_CACHE_FILE", str(cache_path))

    main_module._persist_chat_id(
        "telegram-token",
        "chat-old",
        authorized_user_ids={"user-old"},
    )

    assert (
        main_module._load_cached_chat_id(
            "telegram-token",
            authorized_user_ids={"user-old"},
        )
        == "chat-old"
    )
    assert (
        main_module._load_cached_chat_id(
            "telegram-token",
            authorized_user_ids={"user-new"},
        )
        is None
    )


@pytest.mark.asyncio
async def test_main_app_default_reload_action_reloads_hosts_from_config_file(
    tmp_path: Path,
) -> None:
    config_path = _write_yaml(
        tmp_path / "logdog.yaml",
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
    assert summary["removed"] == ["alpha"]
    assert summary["removed_requires_restart"] == []
    assert app.state.host_manager.get_host_config("alpha") is None
    assert app.state.host_manager.get_host_config("beta") is not None
    assert app.state.host_manager.get_host_config("beta")["url"] == "unix:///beta.sock"


@pytest.mark.asyncio
async def test_main_app_reload_action_closes_removed_host_resources(
    tmp_path: Path,
) -> None:
    config_path = _write_yaml(
        tmp_path / "logdog.yaml",
        """
        hosts:
          - name: alpha
            url: unix:///alpha.sock
        """,
    )
    closed_hosts: list[str] = []
    reloaded_hosts: list[list[str]] = []

    class _DockerPoolStub:
        async def connect_host(self, _host: dict) -> dict:
            return {"server_version": "24.0.7"}

        async def close_all(self) -> None:
            return None

        async def close_host(self, host_name: str) -> None:
            closed_hosts.append(host_name)

    class _WatchManagerStub:
        async def reload_host_configs(self, host_names: list[str]) -> None:
            reloaded_hosts.append(list(host_names))

        def snapshot_state(self) -> dict[str, object]:
            return {}

    app = main_module.create_app(
        config_path=str(config_path),
        docker_client_pool=_DockerPoolStub(),
        watch_manager=_WatchManagerStub(),
        enable_metrics_scheduler=False,
        enable_retention_cleanup=False,
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

    assert summary["removed"] == ["alpha"]
    assert len(closed_hosts) == 1
    assert closed_hosts[0]["name"] == "alpha"
    assert closed_hosts[0]["url"] == "unix:///alpha.sock"
    assert reloaded_hosts == [["beta", "alpha"]]


@pytest.mark.asyncio
async def test_main_app_reload_and_shutdown_close_remote_worker_backend(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _HybridAdapterStub.instances.clear()
    monkeypatch.setattr(main_module, "HybridHostAdapter", _HybridAdapterStub)

    config_path = _write_yaml(
        tmp_path / "logdog.yaml",
        """
        hosts:
          - name: alpha
            url: ssh://deploy@10.0.1.10:22
            remote_worker:
              enabled: true
        """,
    )

    app = main_module.create_app(
        config_path=str(config_path),
        host_connector=lambda _host: {"server_version": "24.0.7"},
        enable_metrics_scheduler=False,
        enable_retention_cleanup=False,
        enable_watch_manager=False,
    )

    async with app.router.lifespan_context(app):
        _write_yaml(
            config_path,
            """
            hosts:
              - name: beta
                url: unix:///beta.sock
            """,
        )
        summary = await app.state.reload_action()

        adapter = _HybridAdapterStub.instances[0]
        assert summary["removed"] == ["alpha"]
        assert adapter.close_host_calls
        assert adapter.close_host_calls[0]["name"] == "alpha"
        assert adapter.close_host_calls[0]["url"] == "ssh://deploy@10.0.1.10:22"
        assert adapter.close_host_calls[0]["remote_worker"] == {"enabled": True}

    adapter = _HybridAdapterStub.instances[0]
    assert adapter.close_all_calls == 1


@pytest.mark.asyncio
async def test_main_app_reload_restarts_updated_remote_worker_session(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _HybridAdapterStub.instances.clear()
    monkeypatch.setattr(main_module, "HybridHostAdapter", _StickyHybridAdapterStub)
    ssh_key_path = tmp_path / "id_ed25519"
    ssh_key_path.write_text("key", encoding="utf-8")
    ssh_key_path.chmod(0o600)

    config_path = _write_yaml(
        tmp_path / "logdog.yaml",
        """
        hosts:
          - name: alpha
            url: ssh://deploy@10.0.1.10:22
            ssh_key: __SSH_KEY_PATH__
            remote_worker:
              enabled: true
              temp_root: /tmp/old
        """,
    ).with_name("logdog.yaml")
    config_path.write_text(
        config_path.read_text(encoding="utf-8").replace(
            "__SSH_KEY_PATH__", str(ssh_key_path)
        ),
        encoding="utf-8",
    )

    app = main_module.create_app(
        config_path=str(config_path),
        host_connector=lambda _host: {"server_version": "24.0.7"},
        enable_metrics_scheduler=False,
        enable_retention_cleanup=False,
        enable_watch_manager=False,
    )

    async with app.router.lifespan_context(app):
        await app.state.host_manager.startup_check()
        adapter = _StickyHybridAdapterStub.instances[0]
        assert adapter.active_sessions["alpha"]["remote_worker"] == {
            "enabled": True,
            "temp_root": "/tmp/old",
        }

        _write_yaml(
            config_path,
            """
            hosts:
              - name: alpha
                url: ssh://deploy@10.0.1.10:22
                ssh_key: __SSH_KEY_PATH__
                remote_worker:
                  enabled: true
                  temp_root: /tmp/new
            """,
        )
        config_path.write_text(
            config_path.read_text(encoding="utf-8").replace(
                "__SSH_KEY_PATH__", str(ssh_key_path)
            ),
            encoding="utf-8",
        )

        summary = await app.state.reload_action()

        assert summary["updated"] == ["alpha"]
        assert adapter.close_host_calls
        assert adapter.close_host_calls[0]["remote_worker"] == {
            "enabled": True,
            "temp_root": "/tmp/old",
        }
        assert adapter.active_sessions["alpha"]["remote_worker"] == {
            "enabled": True,
            "temp_root": "/tmp/new",
        }


@pytest.mark.asyncio
async def test_main_app_reload_failure_keeps_existing_remote_worker_session(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeUpdater:
        async def start_polling(self, **_kw) -> None:
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

    _HybridAdapterStub.instances.clear()
    monkeypatch.setattr(main_module, "HybridHostAdapter", _StickyHybridAdapterStub)
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "telegram-token")

    ssh_key_path = tmp_path / "id_ed25519"
    ssh_key_path.write_text("key", encoding="utf-8")
    ssh_key_path.chmod(0o600)

    config_path = _write_yaml(
        tmp_path / "logdog.yaml",
        """
        hosts:
          - name: alpha
            url: ssh://deploy@10.0.1.10:22
            ssh_key: __SSH_KEY_PATH__
            remote_worker:
              enabled: true
              temp_root: /tmp/old
        agent:
          authorized_users:
            telegram: ["1"]
        """,
    ).with_name("logdog.yaml")
    config_path.write_text(
        config_path.read_text(encoding="utf-8").replace(
            "__SSH_KEY_PATH__", str(ssh_key_path)
        ),
        encoding="utf-8",
    )

    build_count = {"n": 0}

    def application_factory(_token: str) -> _FakeApplication:
        build_count["n"] += 1
        return _FakeApplication(fail_on_start=build_count["n"] >= 2)

    app = main_module.create_app(
        config_path=str(config_path),
        host_connector=lambda _host: {"server_version": "24.0.7"},
        enable_metrics_scheduler=False,
        enable_retention_cleanup=False,
        enable_watch_manager=False,
        telegram_application_factory=application_factory,
        telegram_handler_binder=lambda app_obj, handler: app_obj.add_handler(handler),
    )

    async with app.router.lifespan_context(app):
        await app.state.host_manager.startup_check()
        adapter = _StickyHybridAdapterStub.instances[0]
        assert adapter.active_sessions["alpha"]["remote_worker"] == {
            "enabled": True,
            "temp_root": "/tmp/old",
        }

        _write_yaml(
            config_path,
            """
            hosts:
              - name: alpha
                url: ssh://deploy@10.0.1.10:22
                ssh_key: __SSH_KEY_PATH__
                remote_worker:
                  enabled: true
                  temp_root: /tmp/new
            agent:
              authorized_users:
                telegram: ["2"]
            """,
        )
        config_path.write_text(
            config_path.read_text(encoding="utf-8").replace(
                "__SSH_KEY_PATH__", str(ssh_key_path)
            ),
            encoding="utf-8",
        )

        with pytest.raises(RuntimeError, match="candidate start failed"):
            await app.state.reload_action()

        assert adapter.close_host_calls == []
        assert adapter.active_sessions["alpha"]["remote_worker"] == {
            "enabled": True,
            "temp_root": "/tmp/old",
        }


@pytest.mark.asyncio
async def test_main_app_reload_failure_closes_new_remote_worker_session(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeUpdater:
        async def start_polling(self, **_kw) -> None:
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

    _HybridAdapterStub.instances.clear()
    monkeypatch.setattr(main_module, "HybridHostAdapter", _StickyHybridAdapterStub)
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "telegram-token")

    ssh_key_path = tmp_path / "id_ed25519"
    ssh_key_path.write_text("key", encoding="utf-8")
    ssh_key_path.chmod(0o600)

    config_path = _write_yaml(
        tmp_path / "logdog.yaml",
        """
        hosts:
          - name: alpha
            url: ssh://deploy@10.0.1.10:22
            ssh_key: __SSH_KEY_PATH__
            remote_worker:
              enabled: true
              temp_root: /tmp/old
        agent:
          authorized_users:
            telegram: ["1"]
        """,
    ).with_name("logdog.yaml")
    config_path.write_text(
        config_path.read_text(encoding="utf-8").replace(
            "__SSH_KEY_PATH__", str(ssh_key_path)
        ),
        encoding="utf-8",
    )

    build_count = {"n": 0}

    def application_factory(_token: str) -> _FakeApplication:
        build_count["n"] += 1
        return _FakeApplication(fail_on_start=build_count["n"] >= 2)

    app = main_module.create_app(
        config_path=str(config_path),
        host_connector=lambda _host: {"server_version": "24.0.7"},
        enable_metrics_scheduler=False,
        enable_retention_cleanup=False,
        enable_watch_manager=False,
        telegram_application_factory=application_factory,
        telegram_handler_binder=lambda app_obj, handler: app_obj.add_handler(handler),
    )

    async with app.router.lifespan_context(app):
        await app.state.host_manager.startup_check()
        adapter = _StickyHybridAdapterStub.instances[0]
        assert adapter.active_sessions["alpha"]["remote_worker"] == {
            "enabled": True,
            "temp_root": "/tmp/old",
        }

        _write_yaml(
            config_path,
            """
            hosts:
              - name: alpha
                url: ssh://deploy@10.0.1.10:22
                ssh_key: __SSH_KEY_PATH__
                remote_worker:
                  enabled: true
                  temp_root: /tmp/old
              - name: beta
                url: ssh://deploy@10.0.1.11:22
                ssh_key: __SSH_KEY_PATH__
                remote_worker:
                  enabled: true
            agent:
              authorized_users:
                telegram: ["2"]
            """,
        )
        config_path.write_text(
            config_path.read_text(encoding="utf-8").replace(
                "__SSH_KEY_PATH__", str(ssh_key_path)
            ),
            encoding="utf-8",
        )

        with pytest.raises(RuntimeError, match="candidate start failed"):
            await app.state.reload_action()

        assert "alpha" in adapter.active_sessions
        assert "beta" not in adapter.active_sessions
        assert [str(item.get("name") or "") for item in adapter.close_host_calls] == ["beta"]


@pytest.mark.asyncio
async def test_main_app_reload_failure_closes_candidate_remote_session_on_local_to_remote_switch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeUpdater:
        async def start_polling(self, **_kw) -> None:
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

    class _DirectPoolStub:
        def __init__(self) -> None:
            self.sessions: set[str] = set()

        async def connect_host(self, host: dict) -> dict:
            host_name = str(host.get("name") or "")
            self.sessions.add(host_name)
            return {"server_version": "24.0.7"}

        async def close_host(self, host_name_or_host) -> None:
            if isinstance(host_name_or_host, dict):
                host_name = str(host_name_or_host.get("name") or "")
            else:
                host_name = str(host_name_or_host or "")
            self.sessions.discard(host_name)

        async def close_all(self) -> None:
            self.sessions.clear()

    class _RemoteWorkerBackendStub:
        instances: list["_RemoteWorkerBackendStub"] = []

        def __init__(self, **_kwargs: object) -> None:
            self.sessions: set[str] = set()
            self.connect_calls: list[str] = []
            self.close_calls: list[str] = []
            _RemoteWorkerBackendStub.instances.append(self)

        async def connect_host(self, host: dict) -> dict:
            host_name = str(host.get("name") or "")
            self.connect_calls.append(host_name)
            self.sessions.add(host_name)
            return {"server_version": "24.0.7"}

        async def close_host(self, host_name_or_host) -> None:
            if isinstance(host_name_or_host, dict):
                host_name = str(host_name_or_host.get("name") or "")
            else:
                host_name = str(host_name_or_host or "")
            self.close_calls.append(host_name)
            self.sessions.discard(host_name)

        async def close_all(self) -> None:
            self.sessions.clear()

    _RemoteWorkerBackendStub.instances.clear()
    monkeypatch.setattr(main_module, "RemoteWorkerBackend", _RemoteWorkerBackendStub)
    monkeypatch.setattr(main_module, "ParamikoRemoteWorkerLauncher", lambda: object())
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "telegram-token")

    ssh_key_path = tmp_path / "id_ed25519"
    ssh_key_path.write_text("key", encoding="utf-8")
    ssh_key_path.chmod(0o600)

    config_path = _write_yaml(
        tmp_path / "logdog.yaml",
        """
        hosts:
          - name: alpha
            url: ssh://deploy@10.0.1.10:22
            ssh_key: __SSH_KEY_PATH__
            remote_worker:
              enabled: true
          - name: beta
            url: ssh://deploy@10.0.1.11:22
            ssh_key: __SSH_KEY_PATH__
            remote_worker:
              enabled: false
        agent:
          authorized_users:
            telegram: ["1"]
        """,
    )
    config_path.write_text(
        config_path.read_text(encoding="utf-8").replace(
            "__SSH_KEY_PATH__", str(ssh_key_path)
        ),
        encoding="utf-8",
    )

    build_count = {"n": 0}

    def application_factory(_token: str) -> _FakeApplication:
        build_count["n"] += 1
        return _FakeApplication(fail_on_start=build_count["n"] >= 2)

    direct_pool = _DirectPoolStub()
    app = main_module.create_app(
        config_path=str(config_path),
        docker_client_pool=direct_pool,
        enable_metrics_scheduler=False,
        enable_retention_cleanup=False,
        enable_watch_manager=False,
        telegram_application_factory=application_factory,
        telegram_handler_binder=lambda app_obj, handler: app_obj.add_handler(handler),
    )

    async with app.router.lifespan_context(app):
        await app.state.host_manager.startup_check()
        remote_backend = _RemoteWorkerBackendStub.instances[0]
        assert remote_backend.sessions == {"alpha"}
        assert "beta" in direct_pool.sessions

        _write_yaml(
            config_path,
            """
            hosts:
              - name: alpha
                url: ssh://deploy@10.0.1.10:22
                ssh_key: __SSH_KEY_PATH__
                remote_worker:
                  enabled: true
              - name: beta
                url: ssh://deploy@10.0.1.11:22
                ssh_key: __SSH_KEY_PATH__
                remote_worker:
                  enabled: true
            agent:
              authorized_users:
                telegram: ["2"]
            """,
        )
        config_path.write_text(
            config_path.read_text(encoding="utf-8").replace(
                "__SSH_KEY_PATH__", str(ssh_key_path)
            ),
            encoding="utf-8",
        )

        with pytest.raises(RuntimeError, match="candidate start failed"):
            await app.state.reload_action()

        assert remote_backend.sessions == {"alpha"}
        assert remote_backend.close_calls == ["beta"]


@pytest.mark.asyncio
async def test_main_app_default_reload_action_preserves_old_state_on_invalid_config(
    tmp_path: Path,
) -> None:
    config_path = _write_yaml(
        tmp_path / "logdog.yaml",
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
        tmp_path / "logdog.yaml",
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

    async def telegram_send(target: str, message: str, parse_mode: str = "") -> None:
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

        async def start_polling(self, **_kw) -> None:
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
        tmp_path / "logdog.yaml",
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
    monkeypatch.setattr(main_module, "_load_persisted_authorized_users", lambda: set())

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
        # New runtime starts before old one is shut down: this preserves availability
        # (old keeps serving if new fails to start). drop_pending_updates=True on
        # start_polling prevents stale-message replay during the brief overlap.
        assert new_start_index < old_shutdown_index

        runtime = app.state.telegram_runtime
        assert runtime is not None
        assert runtime._authorized_user_ids == {"2"}

    assert "app2:app:shutdown" in events


@pytest.mark.asyncio
async def test_main_app_reload_rebinds_telegram_chat_id_sync_to_new_router(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeSendFunc:
        def __init__(self, label: str) -> None:
            self.label = label
            self.pin_calls: list[str] = []

        def __call__(self, _target: str, _message: str, parse_mode: str = "") -> None:
            _ = parse_mode

        def pin_chat_id(self, chat_id: str) -> None:
            self.pin_calls.append(chat_id)

    class _FakeNotifier:
        def __init__(self, send_func: _FakeSendFunc) -> None:
            self._send_func = send_func

    class _FakeRouter:
        def __init__(self, label: str) -> None:
            self.label = label
            self.send_func = _FakeSendFunc(label)
            self._notifiers = [_FakeNotifier(self.send_func)]
            self._host_notifiers: dict[str, list[_FakeNotifier]] = {}

        async def send(
            self,
            _host: str,
            _message: str,
            _category: str,
            context: dict | None = None,
        ) -> bool:
            _ = context
            return True

    class _FakeRuntime:
        def __init__(self, label: str) -> None:
            self.label = label
            self.callback = None

        def set_chat_id_seen_callback(self, callback) -> None:
            self.callback = callback

        async def start(self) -> None:
            return None

        async def shutdown(self) -> None:
            return None

    def _chat_id_from_config(app_config: dict | None) -> str:
        notify_cfg = (app_config or {}).get("notify") or {}
        telegram_cfg = notify_cfg.get("telegram") or {}
        chat_ids = telegram_cfg.get("chat_ids") or []
        return str(chat_ids[0]) if chat_ids else "missing"

    routers: list[_FakeRouter] = []

    def build_router(*, app_config=None, **_kwargs):
        router = _FakeRouter(_chat_id_from_config(app_config))
        routers.append(router)
        return router

    def build_runtime(*, app_config=None, **_kwargs):
        return _FakeRuntime(_chat_id_from_config(app_config))

    config_path = _write_yaml(
        tmp_path / "logdog.yaml",
        """
        hosts:
          - name: local
            url: unix:///var/run/docker.sock
        notify:
          telegram:
            enabled: true
            chat_ids: ["old-target"]
        agent:
          authorized_users:
            telegram: ["1"]
        """,
    )

    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "telegram-token")
    monkeypatch.setattr(main_module, "_load_persisted_authorized_users", lambda: set())
    monkeypatch.setattr(
        main_module,
        "_load_cached_chat_id",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        main_module,
        "_persist_chat_id",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(main_module, "_resolve_notify_router", build_router)
    monkeypatch.setattr(
        main_module,
        "_build_telegram_runtime_from_config",
        build_runtime,
    )
    monkeypatch.setattr(main_module, "build_chat_runtime", lambda **_kwargs: object())
    monkeypatch.setattr(main_module, "create_tool_registry", lambda **_kwargs: {})

    app = main_module.create_app(
        config_path=str(config_path),
        web_auth_token="web-token",
        web_admin_token="admin-token",
        enable_metrics_scheduler=False,
        enable_retention_cleanup=False,
        enable_watch_manager=False,
    )

    old_router = app.state.notify_router

    _write_yaml(
        config_path,
        """
        hosts:
          - name: local
            url: unix:///var/run/docker.sock
        notify:
          telegram:
            enabled: true
            chat_ids: ["new-target"]
        agent:
          authorized_users:
            telegram: ["1"]
        """,
    )

    summary = await app.state.reload_action()
    assert summary["ok"] is True

    new_router = app.state.notify_router
    runtime = app.state.telegram_runtime
    assert runtime is not None
    assert runtime.callback is not None

    runtime.callback("chat-123")

    assert old_router.send_func.pin_calls == []
    assert new_router.send_func.pin_calls == ["chat-123"]


@pytest.mark.asyncio
async def test_main_app_reload_keeps_old_telegram_runtime_when_candidate_start_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeUpdater:
        def __init__(self, events: list[str], name: str) -> None:
            self._events = events
            self._name = name

        async def start_polling(self, **_kw) -> None:
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
        tmp_path / "logdog.yaml",
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
    monkeypatch.setattr(main_module, "_load_persisted_authorized_users", lambda: set())

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
        async def start_polling(self, **_kw) -> None:
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
        tmp_path / "logdog.yaml",
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
        tmp_path / "logdog.yaml",
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
