from __future__ import annotations

import asyncio

import pytest

import logwatch.collector.log_stream as log_stream_module
from logwatch.collector.log_stream import LogStreamWatcher, run_alert_once
from logwatch.collector.storm import AlertStormController
from logwatch.pipeline.cooldown import CooldownStore
from logwatch.pipeline.preprocessor.base import BasePreprocessor, LogLine


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
    assert "Host: host-a" in notifications[0][1]
    assert "Container: svc-api" in notifications[0][1]
    assert "Timestamp: 2026-04-11T10:00:02Z" in notifications[0][1]
    assert "ERROR live event [preprocessed]" in notifications[0][1]
    assert metrics_writer.alerts == [
        {
            "host": "host-a",
            "container_id": "c1",
            "category": "ERROR",
            "line": "ERROR live event [preprocessed]",
            "analysis": "Host: host-a\nContainer: svc-api\nTimestamp: 2026-04-11T10:00:02Z\nERROR live event [preprocessed]",
            "pushed": True,
        }
    ]


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

    result = await run_alert_once(
        "ERROR storm save fail event",
        host="host-a",
        container_id="c1",
        container_name="svc-api",
        notifier_send=notify,
        save_alert=save_alert,
        storm_controller=storm_controller,
        config={"dedup": {"enabled": False}},
    )

    assert result.triggered is True
    assert result.saved is False


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
