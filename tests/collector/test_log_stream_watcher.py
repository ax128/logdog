from __future__ import annotations

import asyncio

import pytest

import logdog.collector.log_stream as log_stream_module
from logdog.collector.log_stream import LogStreamWatcher, run_alert_once, _parse_log_timestamp
from logdog.collector.storm import AlertStormController
from logdog.pipeline.cooldown import CooldownStore
from logdog.pipeline.preprocessor.base import BasePreprocessor, LogLine


class _SuffixPreprocessor(BasePreprocessor):
    name = "suffix"

    def process(self, lines: list[LogLine]) -> list[LogLine]:
        return [
            LogLine(
                host_name=line.host_name,
                container_id=line.container_id,
                container_name=line.container_name,
                timestamp=line.timestamp,
                content=f"{line.content} [preprocessed]",
                level=line.level,
                metadata=line.metadata,
            )
            for line in lines
        ]


class _MetricsWriterStub:
    def __init__(self) -> None:
        self.mute_queries: list[tuple[str, str, str, str | None]] = []
        self.alerts: list[dict] = []

    async def find_active_mute(
        self,
        *,
        host: str,
        container_id: str,
        category: str,
        at_time: str | None = None,
    ) -> dict | None:
        self.mute_queries.append((host, container_id, category, at_time))
        if category == "OOM":
            return {"id": 1, "reason": "maintenance"}
        return None

    async def write_alert(self, payload: dict) -> None:
        self.alerts.append(payload)


class _RecordingAlertRunner:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def __call__(self, line: str, **kwargs) -> None:
        self.calls.append({"line": line, **kwargs})


@pytest.mark.asyncio
async def test_log_stream_watcher_applies_preprocessors_and_skips_muted_alerts() -> (
    None
):
    notifications: list[tuple[str, str, str]] = []
    metrics_writer = _MetricsWriterStub()

    async def stream_logs(_host: dict, _container: dict, **_kwargs):
        yield {"timestamp": "2026-04-11T10:00:01Z", "line": "OOM muted event"}
        yield {"timestamp": "2026-04-11T10:00:02Z", "line": "ERROR live event"}

    async def notify(host: str, message: str, category: str) -> bool:
        notifications.append((host, message, category))
        return True

    watcher = LogStreamWatcher(
        host={"name": "host-a", "url": "unix:///var/run/docker.sock"},
        stream_logs=stream_logs,
        notifier_send=notify,
        metrics_writer=metrics_writer,
        preprocessors=[_SuffixPreprocessor()],
    )

    await watcher.watch_container({"id": "c1", "name": "svc-api"})

    assert metrics_writer.mute_queries == [
        ("host-a", "c1", "OOM", "2026-04-11T10:00:01Z"),
        ("host-a", "c1", "ERROR", "2026-04-11T10:00:02Z"),
    ]
    assert len(notifications) == 1
    assert notifications[0][0] == "host-a"
    assert notifications[0][2] == "ERROR"
    assert "host-a / svc-api" in notifications[0][1]
    assert "主机：host-a" in notifications[0][1]
    assert "容器：svc-api" in notifications[0][1]
    assert "时间：2026-04-11T10:00:02Z" in notifications[0][1]
    assert "ERROR live event [preprocessed]" in notifications[0][1]
    assert len(metrics_writer.alerts) == 1
    saved_alert = metrics_writer.alerts[0]
    assert saved_alert["host"] == "host-a"
    assert saved_alert["container_id"] == "c1"
    assert saved_alert["category"] == "ERROR"
    assert saved_alert["line"] == "ERROR live event [preprocessed]"
    assert saved_alert["pushed"] is True
    assert "主机：host-a" in saved_alert["analysis"]
    assert "容器：svc-api" in saved_alert["analysis"]
    assert "时间：2026-04-11T10:00:02Z" in saved_alert["analysis"]
    assert "ERROR live event [preprocessed]" in saved_alert["analysis"]


@pytest.mark.asyncio
async def test_log_stream_watcher_refresh_respects_container_include_exclude_and_runtime_config() -> (
    None
):
    alert_runner = _RecordingAlertRunner()

    async def list_containers(_host_name: str) -> list[dict]:
        return [
            {"id": "c-allow", "name": "api"},
            {"id": "c-skip", "name": "infra-sidecar"},
            {"id": "c-other", "name": "worker"},
        ]

    async def stream_logs(_host: dict, container: dict, **_kwargs):
        if container["name"] == "api":
            yield {"timestamp": "2026-04-11T10:00:02Z", "line": "CRIT token=abc"}
        else:
            yield {"timestamp": "2026-04-11T10:00:03Z", "line": "ERROR should not run"}

    watcher = LogStreamWatcher(
        host={
            "name": "host-a",
            "url": "unix:///var/run/docker.sock",
            "prompt_template": "default_alert",
            "rules": {
                "ignore": [],
                "redact": [{"pattern": "token=\\S+", "replace": "token=***"}],
                "custom_alerts": [{"pattern": "CRIT", "category": "FATAL"}],
            },
            "containers": {"include": ["api", "infra-sidecar"], "exclude": ["infra"]},
        },
        list_containers=list_containers,
        stream_logs=stream_logs,
        run_alert=alert_runner,
    )

    await watcher.refresh_containers()
    await asyncio.sleep(0)
    await watcher.shutdown()

    assert len(alert_runner.calls) == 1
    call = dict(alert_runner.calls[0])
    assert call["line"] == "CRIT token=abc"
    assert call["host"] == "host-a"
    assert call["container_id"] == "c-allow"
    assert call["container_name"] == "api"
    assert call["timestamp"] == "2026-04-11T10:00:02Z"
    assert isinstance(call["cooldown_store"], CooldownStore)
    assert call["notifier_send"] is None
    assert call["save_alert"] is None
    assert call["save_storm_event"] is None
    assert call["mute_checker"] is None
    assert call["prompt_template"] == "default_alert"
    assert call["output_template"] == "standard"
    assert call["config"] == {
        "name": "host-a",
        "url": "unix:///var/run/docker.sock",
        "prompt_template": "default_alert",
        "rules": {
            "ignore": [],
            "redact": [{"pattern": "token=\\S+", "replace": "token=***"}],
            "custom_alerts": [{"pattern": "CRIT", "category": "FATAL"}],
        },
        "containers": {
            "include": ["api", "infra-sidecar"],
            "exclude": ["infra"],
        },
    }


@pytest.mark.asyncio
async def test_log_stream_watcher_passes_remote_pipeline_to_remote_worker_stream() -> None:
    stream_kwargs_calls: list[dict[str, object]] = []
    alert_runner = _RecordingAlertRunner()

    async def stream_logs(_host: dict, _container: dict, **kwargs):
        stream_kwargs_calls.append(dict(kwargs))
        yield {"timestamp": "2026-04-11T10:00:02Z", "line": "ERROR token=abc"}

    watcher = LogStreamWatcher(
        host={
            "name": "host-a",
            "url": "ssh://deploy@example-host",
            "remote_worker": {"enabled": True},
            "rules": {
                "ignore": ["healthcheck ok", {"pattern": "GET /ping"}],
                "redact": [{"pattern": "token=\\S+", "replace": "token=***"}],
                "custom_alerts": [{"pattern": "ERROR", "category": "BUSINESS"}],
            },
            "alert_keywords": ["ERROR", "FATAL"],
            "preprocessors": [
                {"name": "json_extract"},
                {"name": "level_filter", "min_level": "error"},
                {"name": "dedup", "max_consecutive": 3},
            ],
        },
        stream_logs=stream_logs,
        run_alert=alert_runner,
    )

    await watcher.watch_container({"id": "c1", "name": "api"})

    assert len(stream_kwargs_calls) == 1
    assert stream_kwargs_calls[0]["pipeline"] == {
        "exclude": ["healthcheck ok", "GET /ping"],
        "redact": [{"pattern": "token=\\S+", "replace": "token=***"}],
        "custom_alerts": [{"pattern": "ERROR", "category": "BUSINESS"}],
        "alert_keywords": ["ERROR", "FATAL"],
        "min_level": "error",
        "dedup_window": 3,
    }
    assert alert_runner.calls[0]["line"] == "ERROR token=abc"


@pytest.mark.asyncio
async def test_log_stream_watcher_maps_only_safe_builtin_preprocessors_to_remote_pipeline() -> (
    None
):
    stream_kwargs_calls: list[dict[str, object]] = []
    alert_runner = _RecordingAlertRunner()

    async def stream_logs(_host: dict, _container: dict, **kwargs):
        stream_kwargs_calls.append(dict(kwargs))
        yield {"timestamp": "2026-04-11T10:00:02Z", "line": "ERROR token=abc"}

    watcher = LogStreamWatcher(
        host={
            "name": "host-a",
            "url": "ssh://deploy@example-host",
            "remote_worker": {"enabled": True},
            "rules": {
                "ignore": ["healthcheck ok", {"pattern": "GET /ping"}],
                "redact": [{"pattern": "token=\\S+", "replace": "token=***"}],
                "custom_alerts": [{"pattern": "ERROR", "category": "BUSINESS"}],
            },
            "alert_keywords": ["ERROR", "FATAL"],
            "preprocessors": [
                {"name": "json_extract", "field": "payload"},
                {"name": "level_filter", "min_level": "error"},
                {"name": "head_tail", "head": 4, "tail": 2},
                {"name": "dedup", "max_consecutive": 9},
                {"name": "kv_extract", "delimiter": "="},
                {"name": "custom_template", "pattern": "token"},
            ],
        },
        stream_logs=stream_logs,
        run_alert=alert_runner,
    )

    await watcher.watch_container({"id": "c1", "name": "api"})

    assert len(stream_kwargs_calls) == 1
    assert stream_kwargs_calls[0]["pipeline"] == {
        "exclude": ["healthcheck ok", "GET /ping"],
        "redact": [{"pattern": "token=\\S+", "replace": "token=***"}],
        "custom_alerts": [{"pattern": "ERROR", "category": "BUSINESS"}],
        "alert_keywords": ["ERROR", "FATAL"],
        "min_level": "error",
        "head": 4,
        "tail": 2,
        "dedup_window": 9,
    }
    assert alert_runner.calls[0]["line"] == "ERROR token=abc"


@pytest.mark.asyncio
async def test_log_stream_watcher_skips_remote_pipeline_for_non_remote_worker_host() -> None:
    stream_kwargs_calls: list[dict[str, object]] = []
    alert_runner = _RecordingAlertRunner()

    async def stream_logs(_host: dict, _container: dict, **kwargs):
        stream_kwargs_calls.append(dict(kwargs))
        yield {"timestamp": "2026-04-11T10:00:02Z", "line": "ERROR live event"}

    watcher = LogStreamWatcher(
        host={
            "name": "host-a",
            "url": "unix:///var/run/docker.sock",
            "remote_worker": {"enabled": True},
            "rules": {
                "redact": [{"pattern": "token=\\S+", "replace": "token=***"}],
            },
            "preprocessors": [{"name": "level_filter", "min_level": "error"}],
        },
        stream_logs=stream_logs,
        run_alert=alert_runner,
    )

    await watcher.watch_container({"id": "c1", "name": "api"})

    assert len(stream_kwargs_calls) == 1
    assert "pipeline" not in stream_kwargs_calls[0]


@pytest.mark.asyncio
async def test_log_stream_watcher_refresh_cancels_task_when_container_is_no_longer_active() -> (
    None
):
    listed_batches = [
        [{"id": "c1", "name": "api", "status": "running"}],
        [{"id": "c1", "name": "api", "status": "exited"}],
    ]
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def list_containers(_host_name: str) -> list[dict]:
        if listed_batches:
            return listed_batches.pop(0)
        return []

    async def stream_logs(_host: dict, _container: dict, **_kwargs):
        started.set()
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            cancelled.set()
            raise
        if False:
            yield {}

    watcher = LogStreamWatcher(
        host={"name": "host-a", "url": "unix:///var/run/docker.sock"},
        list_containers=list_containers,
        stream_logs=stream_logs,
        run_alert=lambda *_args, **_kwargs: None,
    )

    await watcher.refresh_containers()
    await asyncio.wait_for(started.wait(), timeout=1.0)

    assert "c1" in watcher._container_tasks

    await watcher.refresh_containers()
    await asyncio.wait_for(cancelled.wait(), timeout=1.0)

    assert "c1" not in watcher._container_tasks

    await watcher.shutdown()


@pytest.mark.asyncio
async def test_log_stream_watcher_limits_ssh_host_container_streams_by_default() -> (
    None
):
    async def list_containers(_host_name: str) -> list[dict]:
        return [
            {"id": f"c{i}", "name": f"svc-{i}", "status": "running"}
            for i in range(12)
        ]

    async def stream_logs(_host: dict, _container: dict, **_kwargs):
        await asyncio.sleep(3600)
        if False:
            yield {}

    watcher = LogStreamWatcher(
        host={
            "name": "host-a",
            "url": "ssh://deploy@example-host",
            "remote_worker": {"enabled": True},
        },
        list_containers=list_containers,
        stream_logs=stream_logs,
        run_alert=lambda *_args, **_kwargs: None,
    )

    await watcher.refresh_containers()

    assert len(watcher._container_tasks) == 8
    assert set(watcher._container_tasks.keys()) == {
        "c0",
        "c1",
        "c2",
        "c3",
        "c4",
        "c5",
        "c6",
        "c7",
    }

    await watcher.shutdown()


@pytest.mark.asyncio
async def test_log_stream_watcher_allows_disabling_container_streams_for_ssh_host() -> (
    None
):
    async def list_containers(_host_name: str) -> list[dict]:
        return [
            {"id": "c1", "name": "svc-1", "status": "running"},
            {"id": "c2", "name": "svc-2", "status": "running"},
        ]

    async def stream_logs(_host: dict, _container: dict, **_kwargs):
        await asyncio.sleep(3600)
        if False:
            yield {}

    watcher = LogStreamWatcher(
        host={
            "name": "host-a",
            "url": "ssh://deploy@example-host",
            "watch": {"max_containers": 0},
        },
        list_containers=list_containers,
        stream_logs=stream_logs,
        run_alert=lambda *_args, **_kwargs: None,
    )

    await watcher.refresh_containers()

    assert watcher._container_tasks == {}

    await watcher.shutdown()


@pytest.mark.asyncio
async def test_log_stream_watcher_default_cooldown_store_is_per_instance() -> None:
    alert_runner = _RecordingAlertRunner()

    async def stream_logs(_host: dict, _container: dict, **_kwargs):
        yield {"timestamp": "2026-04-11T10:00:02Z", "line": "ERROR first"}

    watcher_a = LogStreamWatcher(
        host={"name": "host-a", "url": "unix:///var/run/docker.sock"},
        stream_logs=stream_logs,
        run_alert=alert_runner,
    )
    watcher_b = LogStreamWatcher(
        host={"name": "host-b", "url": "unix:///var/run/docker.sock"},
        stream_logs=stream_logs,
        run_alert=alert_runner,
    )

    await watcher_a.watch_container({"id": "c1", "name": "svc-a"})
    await watcher_b.watch_container({"id": "c2", "name": "svc-b"})

    assert len(alert_runner.calls) == 2
    cooldown_a = alert_runner.calls[0]["cooldown_store"]
    cooldown_b = alert_runner.calls[1]["cooldown_store"]
    assert isinstance(cooldown_a, CooldownStore)
    assert isinstance(cooldown_b, CooldownStore)
    assert cooldown_a is not cooldown_b


@pytest.mark.asyncio
async def test_run_alert_once_uses_script_message_when_llm_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    analyze_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
    notifications: list[tuple[str, str, str]] = []
    saved_payloads: list[dict[str, object]] = []

    def forbidden_analyze(*args, **kwargs):
        analyze_calls.append((args, dict(kwargs)))
        return "should-not-be-used"

    async def notify(host: str, message: str, category: str) -> bool:
        notifications.append((host, message, category))
        return True

    async def save_alert(payload: dict[str, object]) -> bool:
        saved_payloads.append(dict(payload))
        return True

    monkeypatch.setattr(log_stream_module, "analyze_with_template", forbidden_analyze)

    result = await run_alert_once(
        "ERROR live event",
        host="host-a",
        container_id="c1",
        container_name="svc-api",
        timestamp="2026-04-11T10:00:02Z",
        notifier_send=notify,
        save_alert=save_alert,
        cooldown_store=CooldownStore(),
        config={"llm": {"enabled": False}},
    )

    assert result.triggered is True
    assert result.pushed is True
    assert analyze_calls == []
    assert result.analysis
    assert "host-a" in result.analysis
    assert "svc-api" in result.analysis
    assert "ERROR live event" in result.analysis
    assert notifications
    assert notifications[0][0] == "host-a"
    assert notifications[0][2] == "ERROR"
    assert "ERROR live event" in notifications[0][1]
    assert saved_payloads
    assert saved_payloads[0]["analysis"] == result.analysis


@pytest.mark.asyncio
async def test_run_alert_once_legacy_mode_disables_agent_invoke(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_kwargs: list[dict[str, object]] = []

    def fake_analyze(*_args, **kwargs):
        captured_kwargs.append(dict(kwargs))
        return "legacy-analysis"

    monkeypatch.setattr(log_stream_module, "analyze_with_template", fake_analyze)

    result = await run_alert_once(
        "ERROR legacy event",
        host="host-a",
        container_id="c1",
        container_name="svc-api",
        timestamp="2026-04-11T10:00:02Z",
        notifier_send=lambda *_a, **_kw: True,
        save_alert=lambda *_a, **_kw: True,
        cooldown_store=CooldownStore(),
        config={},
    )

    assert result.triggered is True
    assert result.analysis == "legacy-analysis"
    assert captured_kwargs
    assert captured_kwargs[0].get("enable_agent") is False


@pytest.mark.asyncio
async def test_log_stream_watcher_drops_events_when_queue_full() -> None:
    started = asyncio.Event()

    async def slow_alert(*_args, **_kwargs):
        started.set()
        await asyncio.sleep(0.05)

    async def stream_logs(_host, _container, **_kwargs):
        for i in range(20):
            yield {"timestamp": "2026-04-12T00:00:00Z", "line": f"ERROR {i}"}

    watcher = LogStreamWatcher(
        host={
            "name": "host-a",
            "url": "unix:///var/run/docker.sock",
            "watch": {"queue_maxsize": 2, "worker_count": 1, "drop_when_full": True},
        },
        stream_logs=stream_logs,
        run_alert=slow_alert,
    )

    await watcher.watch_container({"id": "c1", "name": "api"})
    await watcher.shutdown()

    assert watcher.dropped_events > 0


@pytest.mark.asyncio
async def test_run_alert_once_reports_unsaved_when_storm_start_save_fails() -> None:
    async def notify(_host: str, _message: str, _category: str) -> bool:
        return True

    async def save_alert(_payload: dict[str, object]) -> bool:
        raise RuntimeError("db unavailable")

    storm_controller = AlertStormController(
        enabled=True,
        window_seconds=60,
        threshold=1,
        suppress_minutes=1,
    )
    cooldown_store = CooldownStore()

    try:
        result = await run_alert_once(
            "ERROR storm save fail event",
            host="host-a",
            container_id="c1",
            container_name="svc-api",
            cooldown_store=cooldown_store,
            notifier_send=notify,
            save_alert=save_alert,
            storm_controller=storm_controller,
            config={"dedup": {"enabled": False}},
        )

        assert result.triggered is True
        assert result.saved is False
    finally:
        await log_stream_module.cancel_pending_dedup_tasks()


@pytest.mark.asyncio
async def test_run_alert_once_does_not_emit_dedup_summary_for_muted_events() -> None:
    notifications: list[tuple[str, str, str]] = []
    saved_payloads: list[dict[str, object]] = []

    async def notify(host: str, message: str, category: str) -> bool:
        notifications.append((host, category, message))
        return True

    async def save_alert(payload: dict[str, object]) -> bool:
        saved_payloads.append(dict(payload))
        return True

    async def always_muted(**_kwargs):
        return {"id": 1, "reason": "mute-hit"}

    cooldown_store = CooldownStore(default_minutes=0.001)
    first = await run_alert_once(
        "ERROR muted first",
        host="host-a",
        container_id="c1",
        container_name="svc-api",
        timestamp="2026-04-11T10:00:00Z",
        cooldown_store=cooldown_store,
        notifier_send=notify,
        save_alert=save_alert,
        mute_checker=always_muted,
        config={"llm": {"enabled": False}},
    )
    second = await run_alert_once(
        "ERROR muted second",
        host="host-a",
        container_id="c1",
        container_name="svc-api",
        timestamp="2026-04-11T10:00:01Z",
        cooldown_store=cooldown_store,
        notifier_send=notify,
        save_alert=save_alert,
        mute_checker=always_muted,
        config={"llm": {"enabled": False}},
    )
    await asyncio.sleep(0.08)
    await log_stream_module.cancel_pending_dedup_tasks()

    assert first.triggered is True
    assert second.triggered is True
    assert notifications == []
    assert saved_payloads == []


@pytest.mark.asyncio
async def test_log_stream_watcher_shutdown_cancels_pending_dedup_for_its_host() -> None:
    async def _notify(_host: str, _message: str, _category: str) -> bool:
        return True

    async def _save_alert(_payload: dict[str, object]) -> bool:
        return True

    first_store = CooldownStore(default_minutes=0.001)
    await run_alert_once(
        "ERROR first host-a",
        host="host-a",
        container_id="c1",
        container_name="svc-a",
        timestamp="2026-04-11T10:00:00Z",
        cooldown_store=first_store,
        notifier_send=_notify,
        save_alert=_save_alert,
        config={"llm": {"enabled": False}},
    )
    await run_alert_once(
        "ERROR second host-a",
        host="host-a",
        container_id="c1",
        container_name="svc-a",
        timestamp="2026-04-11T10:00:01Z",
        cooldown_store=first_store,
        notifier_send=_notify,
        save_alert=_save_alert,
        config={"llm": {"enabled": False}},
    )

    second_store = CooldownStore(default_minutes=0.001)
    await run_alert_once(
        "ERROR first host-b",
        host="host-b",
        container_id="c2",
        container_name="svc-b",
        timestamp="2026-04-11T10:00:00Z",
        cooldown_store=second_store,
        notifier_send=_notify,
        save_alert=_save_alert,
        config={"llm": {"enabled": False}},
    )
    await run_alert_once(
        "ERROR second host-b",
        host="host-b",
        container_id="c2",
        container_name="svc-b",
        timestamp="2026-04-11T10:00:01Z",
        cooldown_store=second_store,
        notifier_send=_notify,
        save_alert=_save_alert,
        config={"llm": {"enabled": False}},
    )

    watcher = LogStreamWatcher(
        host={"name": "host-a", "url": "unix:///var/run/docker.sock"},
        stream_logs=None,
    )

    assert any(key[0] == "host-a" for key in log_stream_module._PENDING_DEDUP_TASKS)
    assert any(key[0] == "host-b" for key in log_stream_module._PENDING_DEDUP_TASKS)

    try:
        await watcher.shutdown()
        assert not any(
            key[0] == "host-a" for key in log_stream_module._PENDING_DEDUP_TASKS
        )
        assert any(
            key[0] == "host-b" for key in log_stream_module._PENDING_DEDUP_TASKS
        )
    finally:
        await log_stream_module.cancel_pending_dedup_tasks()


@pytest.mark.asyncio
async def test_log_stream_watcher_shutdown_cancels_pending_storm_tasks_for_its_controller() -> None:
    async def _notify(_host: str, _message: str, _category: str) -> bool:
        return True

    async def _save_alert(_payload: dict[str, object]) -> bool:
        return True

    async def _save_storm_event(_payload: dict[str, object]) -> bool:
        return True

    storm_controller_a = AlertStormController(
        enabled=True,
        window_seconds=60,
        threshold=1,
        suppress_minutes=1,
    )
    storm_controller_b = AlertStormController(
        enabled=True,
        window_seconds=60,
        threshold=1,
        suppress_minutes=1,
    )
    cooldown_store = CooldownStore()

    await run_alert_once(
        "ERROR storm-a",
        host="host-a",
        container_id="c1",
        container_name="svc-a",
        timestamp="2026-04-11T10:00:00Z",
        cooldown_store=cooldown_store,
        notifier_send=_notify,
        save_alert=_save_alert,
        save_storm_event=_save_storm_event,
        storm_controller=storm_controller_a,
        config={"llm": {"enabled": False}},
    )
    await run_alert_once(
        "ERROR storm-b",
        host="host-b",
        container_id="c2",
        container_name="svc-b",
        timestamp="2026-04-11T10:00:00Z",
        cooldown_store=cooldown_store,
        notifier_send=_notify,
        save_alert=_save_alert,
        save_storm_event=_save_storm_event,
        storm_controller=storm_controller_b,
        config={"llm": {"enabled": False}},
    )

    watcher = LogStreamWatcher(
        host={"name": "host-a", "url": "unix:///var/run/docker.sock"},
        stream_logs=None,
        storm_controller=storm_controller_a,
    )

    key_a = (id(storm_controller_a), "ERROR")
    key_b = (id(storm_controller_b), "ERROR")
    assert key_a in log_stream_module._PENDING_STORM_END_TASKS
    assert key_b in log_stream_module._PENDING_STORM_END_TASKS

    try:
        await watcher.shutdown()
        assert key_a not in log_stream_module._PENDING_STORM_END_TASKS
        assert key_b in log_stream_module._PENDING_STORM_END_TASKS
    finally:
        await log_stream_module.cancel_pending_dedup_tasks()


@pytest.mark.asyncio
async def test_log_stream_watcher_shutdown_does_not_leave_new_dedup_tasks_from_queued_items() -> None:
    async def _notify(_host: str, _message: str, _category: str) -> bool:
        return True

    async def _save_alert(_payload: dict[str, object]) -> bool:
        return True

    cooldown_store = CooldownStore(default_minutes=0.001)
    watcher = LogStreamWatcher(
        host={"name": "host-a", "url": "unix:///var/run/docker.sock"},
        stream_logs=None,
        notifier_send=_notify,
        save_alert=_save_alert,
    )

    await watcher._ensure_workers()
    for idx in range(2):
        await watcher._enqueue_alert_item(
            log_stream_module._AlertItem(
                line=f"ERROR queued dedup {idx}",
                kwargs={
                    "host": "host-a",
                    "container_id": "c1",
                    "container_name": "svc-a",
                    "timestamp": f"2026-04-19T10:00:0{idx}Z",
                    "cooldown_store": cooldown_store,
                    "notifier_send": _notify,
                    "save_alert": _save_alert,
                    "save_storm_event": None,
                    "mute_checker": None,
                    "prompt_template": "default_alert",
                    "output_template": "standard",
                    "config": {"llm": {"enabled": False}},
                },
            )
        )

    try:
        await watcher.shutdown()
        assert not any(
            key[0] == "host-a" for key in log_stream_module._PENDING_DEDUP_TASKS
        )
    finally:
        await log_stream_module.cancel_pending_dedup_tasks()


@pytest.mark.asyncio
async def test_log_stream_watcher_shutdown_does_not_leave_new_storm_tasks_from_queued_items() -> None:
    async def _notify(_host: str, _message: str, _category: str) -> bool:
        return True

    async def _save_alert(_payload: dict[str, object]) -> bool:
        return True

    async def _save_storm_event(_payload: dict[str, object]) -> bool:
        return True

    storm_controller = AlertStormController(
        enabled=True,
        window_seconds=60,
        threshold=1,
        suppress_minutes=1,
    )
    watcher = LogStreamWatcher(
        host={"name": "host-a", "url": "unix:///var/run/docker.sock"},
        stream_logs=None,
        notifier_send=_notify,
        save_alert=_save_alert,
        storm_controller=storm_controller,
    )

    await watcher._ensure_workers()
    await watcher._enqueue_alert_item(
        log_stream_module._AlertItem(
            line="ERROR queued storm",
            kwargs={
                "host": "host-a",
                "container_id": "c1",
                "container_name": "svc-a",
                "timestamp": "2026-04-19T10:00:00Z",
                "cooldown_store": CooldownStore(),
                "notifier_send": _notify,
                "save_alert": _save_alert,
                "save_storm_event": _save_storm_event,
                "mute_checker": None,
                "prompt_template": "default_alert",
                "output_template": "standard",
                "config": {"llm": {"enabled": False}},
                "storm_controller": storm_controller,
            },
        )
    )

    try:
        await watcher.shutdown()
        assert not any(
            key[0] == id(storm_controller)
            for key in log_stream_module._PENDING_STORM_END_TASKS
        )
        assert storm_controller._active_by_category == {}
        assert storm_controller._recent_by_category == {}
    finally:
        await log_stream_module.cancel_pending_dedup_tasks()


@pytest.mark.asyncio
async def test_mark_dedup_task_done_ignores_stale_task_for_same_key() -> None:
    key = ("host-a", "c1", "ERROR")
    stale_task = asyncio.create_task(asyncio.sleep(0))
    replacement_task = asyncio.create_task(asyncio.sleep(3600))

    try:
        await stale_task
        with log_stream_module._DEDUP_LOCK:
            log_stream_module._PENDING_DEDUP_TASKS[key] = replacement_task

        log_stream_module._mark_dedup_task_done(key, stale_task)
        assert log_stream_module._PENDING_DEDUP_TASKS[key] is replacement_task

        log_stream_module._mark_dedup_task_done(key, replacement_task)
        assert key not in log_stream_module._PENDING_DEDUP_TASKS
    finally:
        replacement_task.cancel()
        await asyncio.gather(replacement_task, return_exceptions=True)
        await log_stream_module.cancel_pending_dedup_tasks()


@pytest.mark.asyncio
async def test_mark_storm_task_done_ignores_stale_task_for_same_key() -> None:
    key = (12345, "ERROR")
    stale_task = asyncio.create_task(asyncio.sleep(0))
    replacement_task = asyncio.create_task(asyncio.sleep(3600))

    try:
        await stale_task
        with log_stream_module._STORM_TASK_LOCK:
            log_stream_module._PENDING_STORM_END_TASKS[key] = replacement_task

        log_stream_module._mark_storm_task_done(key, stale_task)
        assert log_stream_module._PENDING_STORM_END_TASKS[key] is replacement_task

        log_stream_module._mark_storm_task_done(key, replacement_task)
        assert key not in log_stream_module._PENDING_STORM_END_TASKS
    finally:
        replacement_task.cancel()
        await asyncio.gather(replacement_task, return_exceptions=True)
        await log_stream_module.cancel_pending_dedup_tasks()


@pytest.mark.asyncio
async def test_log_stream_watcher_reconnects_after_stream_eof() -> None:
    attempts = {"n": 0}
    reconnected = asyncio.Event()

    async def list_containers(_host_name: str) -> list[dict]:
        return [{"id": "c1", "name": "svc-api"}]

    async def stream_logs(_host: dict, _container: dict, **_kwargs):
        attempts["n"] += 1
        if attempts["n"] == 1:
            yield {"timestamp": "2026-04-11T10:00:01Z", "line": "ERROR first"}
            return
        reconnected.set()
        await asyncio.sleep(3600)
        if False:
            yield {}

    watcher = LogStreamWatcher(
        host={
            "name": "host-a",
            "url": "unix:///var/run/docker.sock",
            "watch": {"reconnect_backoff_seconds": 0},
        },
        list_containers=list_containers,
        stream_logs=stream_logs,
        run_alert=lambda *_args, **_kwargs: None,
    )

    await watcher.start()
    try:
        await asyncio.wait_for(reconnected.wait(), timeout=1.0)
    finally:
        await watcher.shutdown()

    assert attempts["n"] >= 2


class TestParseLogTimestamp:
    def test_docker_timestamp_with_nanos(self) -> None:
        ts = _parse_log_timestamp("2024-01-15T10:30:00.123456789Z")
        assert ts is not None
        assert abs(ts - 1705314600.123456) < 1.0  # within 1 second

    def test_docker_timestamp_without_nanos(self) -> None:
        ts = _parse_log_timestamp("2024-01-15T10:30:00Z")
        assert ts is not None

    def test_empty_returns_none(self) -> None:
        assert _parse_log_timestamp("") is None
        assert _parse_log_timestamp(None) is None

    def test_invalid_returns_none(self) -> None:
        assert _parse_log_timestamp("not-a-timestamp") is None


class TestWatchLookbackConfig:
    def test_default_lookback_is_300(self) -> None:
        watcher = LogStreamWatcher(
            host={"name": "test"},
            stream_logs=None,
        )
        assert watcher._watch_lookback_seconds == 300

    def test_custom_lookback_from_config(self) -> None:
        watcher = LogStreamWatcher(
            host={"name": "test", "watch": {"lookback_seconds": 60}},
            stream_logs=None,
        )
        assert watcher._watch_lookback_seconds == 60

    def test_zero_lookback_means_all_history(self) -> None:
        watcher = LogStreamWatcher(
            host={"name": "test", "watch": {"lookback_seconds": 0}},
            stream_logs=None,
        )
        assert watcher._watch_lookback_seconds == 0


@pytest.mark.asyncio
async def test_reconnect_backoff_increases_on_repeated_crashes() -> None:
    """After repeated crashes, backoff delay should increase exponentially."""
    sleep_durations: list[float] = []
    stop_after = 4

    async def stream_logs(_host: dict, _container: dict, **_kwargs):
        raise ConnectionError("Too many open files")
        if False:
            yield {}

    async def fake_sleep(seconds: float) -> None:
        sleep_durations.append(seconds)
        if len(sleep_durations) >= stop_after:
            raise asyncio.CancelledError

    watcher = LogStreamWatcher(
        host={
            "name": "host-a",
            "url": "unix:///var/run/docker.sock",
            "watch": {"reconnect_backoff_seconds": 1.0},
        },
        list_containers=lambda _name: [{"id": "c1", "name": "svc"}],
        stream_logs=stream_logs,
        run_alert=lambda *_args, **_kwargs: None,
    )

    import unittest.mock
    _real_sleep = asyncio.sleep
    with (
        unittest.mock.patch("logdog.collector.log_stream.asyncio.sleep", side_effect=fake_sleep),
        unittest.mock.patch("logdog.collector.log_stream.random.random", return_value=0.5),
    ):
        await watcher.start()
        try:
            await _real_sleep(1.0)
        except asyncio.CancelledError:
            pass
        finally:
            await watcher.shutdown()

    # With random.random()=0.5 and base=1.0:
    # attempt 0 -> crash -> _backoff_attempt becomes 1: delay=1.0*2^1=2.0, jitter=2.0*0.3*0.5=0.30 -> 2.30
    # attempt 1 -> crash -> _backoff_attempt becomes 2: delay=1.0*2^2=4.0, jitter=4.0*0.3*0.5=0.60 -> 4.60
    # attempt 2 -> crash -> _backoff_attempt becomes 3: delay=1.0*2^3=8.0, jitter=8.0*0.3*0.5=1.20 -> 9.20
    # Each must be strictly larger than the previous
    assert len(sleep_durations) >= 3
    for i in range(1, len(sleep_durations)):
        assert sleep_durations[i] > sleep_durations[i - 1], (
            f"backoff did not increase: {sleep_durations}"
        )


@pytest.mark.asyncio
async def test_reconnect_backoff_resets_after_successful_stream() -> None:
    """After a successful stream (normal EOF), backoff should reset to base."""
    sleep_durations: list[float] = []
    attempt = {"n": 0}

    async def stream_logs(_host: dict, _container: dict, **_kwargs):
        attempt["n"] += 1
        if attempt["n"] == 1:
            raise ConnectionError("first crash")
        if attempt["n"] == 2:
            raise ConnectionError("second crash")
        if attempt["n"] == 3:
            yield {"timestamp": "2026-04-18T10:00:00Z", "line": "OK recovered"}
            return  # normal EOF
        if attempt["n"] == 4:
            raise ConnectionError("crash after recovery")
        raise asyncio.CancelledError
        if False:
            yield {}

    async def fake_sleep(seconds: float) -> None:
        sleep_durations.append(seconds)

    watcher = LogStreamWatcher(
        host={
            "name": "host-a",
            "url": "unix:///var/run/docker.sock",
            "watch": {"reconnect_backoff_seconds": 1.0},
        },
        list_containers=lambda _name: [{"id": "c1", "name": "svc"}],
        stream_logs=stream_logs,
        run_alert=lambda *_args, **_kwargs: None,
    )

    import unittest.mock
    _real_sleep = asyncio.sleep
    with (
        unittest.mock.patch("logdog.collector.log_stream.asyncio.sleep", side_effect=fake_sleep),
        unittest.mock.patch("logdog.collector.log_stream.random.random", return_value=0.0),
    ):
        await watcher.start()
        try:
            await asyncio.wait_for(_real_sleep(2.0), timeout=2.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass
        finally:
            await watcher.shutdown()

    # sleep_durations: crash(2.0), crash(4.0), eof(1.0), crash(2.0)
    # After successful stream (attempt 3), attempt resets.
    # So attempt 4's crash should get base-ish backoff again, not 8.0.
    assert len(sleep_durations) >= 4
    assert sleep_durations[2] < sleep_durations[1], (
        f"backoff should reset after EOF: {sleep_durations}"
    )


# ---------------------------------------------------------------------------
# _truncate_line tests
# ---------------------------------------------------------------------------


def test_truncate_line_short_line_unchanged() -> None:
    from logdog.collector.log_stream import _truncate_line, DEFAULT_MAX_LINE_CHARS

    short_line = "normal log line"
    result = _truncate_line(short_line, DEFAULT_MAX_LINE_CHARS)
    assert result == short_line


def test_truncate_line_oversized_line_truncated() -> None:
    from logdog.collector.log_stream import _truncate_line, DEFAULT_MAX_LINE_CHARS

    long_line = "X" * 20000
    result = _truncate_line(long_line, DEFAULT_MAX_LINE_CHARS)
    assert len(result) < len(long_line)
    assert result.startswith("X" * 100)
    assert "truncated" in result
    assert "3616" in result  # 20000 - 16384 = 3616


def test_truncate_line_exact_limit_unchanged() -> None:
    from logdog.collector.log_stream import _truncate_line

    line = "A" * 100
    result = _truncate_line(line, 100)
    assert result == line


def test_truncate_line_one_over_limit() -> None:
    from logdog.collector.log_stream import _truncate_line

    line = "A" * 101
    result = _truncate_line(line, 100)
    assert result.startswith("A" * 100)
    assert "truncated 1 chars" in result
