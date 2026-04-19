from __future__ import annotations

import pytest

import logdog.collector.log_stream as log_stream_module
from logdog.collector.log_stream import LogStreamWatcher, run_alert_once
from logdog.collector.storm import AlertStormController
from logdog.pipeline.cooldown import CooldownStore
from logdog.collector.watch_manager import WatchManager


class _HostManagerStub:
    def __init__(self) -> None:
        self._statuses = [
            {"name": "host-a", "status": "connected"},
            {"name": "host-b", "status": "disconnected"},
        ]
        self._configs = {
            "host-a": {"name": "host-a", "url": "unix:///var/run/docker.sock"},
            "host-b": {"name": "host-b", "url": "unix:///var/run/docker-b.sock"},
        }

    def list_host_statuses(self) -> list[dict]:
        return list(self._statuses)

    def get_host_config(self, name: str) -> dict | None:
        return self._configs.get(name)

    def set_status(self, name: str, status: str) -> None:
        for item in self._statuses:
            if item["name"] == name:
                item["status"] = status
                return


class _LogDogerStub:
    def __init__(self, host: dict) -> None:
        self.host = host
        self.started = 0
        self.refresh_calls = 0
        self.shutdown_calls = 0

    async def start(self) -> None:
        self.started += 1

    async def refresh_containers(self) -> None:
        self.refresh_calls += 1

    async def shutdown(self) -> None:
        self.shutdown_calls += 1


class _EventWatcherStub:
    def __init__(self, host: dict) -> None:
        self.host = host
        self.started = 0
        self.shutdown_calls = 0

    async def start(self) -> None:
        self.started += 1

    async def shutdown(self) -> None:
        self.shutdown_calls += 1


class _FailingEventWatcherStub(_EventWatcherStub):
    def __init__(self, host: dict, *, fail_on_start: bool) -> None:
        super().__init__(host)
        self._fail_on_start = fail_on_start

    async def start(self) -> None:
        if self._fail_on_start:
            raise RuntimeError("event watcher start failed")
        await super().start()


@pytest.mark.asyncio
async def test_watch_manager_starts_connected_hosts_and_refreshes_target_host() -> None:
    created_log_watchers: dict[str, _LogDogerStub] = {}
    created_event_watchers: dict[str, _EventWatcherStub] = {}

    def build_log_watcher(host: dict) -> _LogDogerStub:
        watcher = _LogDogerStub(host)
        created_log_watchers[host["name"]] = watcher
        return watcher

    def build_event_watcher(host: dict) -> _EventWatcherStub:
        watcher = _EventWatcherStub(host)
        created_event_watchers[host["name"]] = watcher
        return watcher

    manager = WatchManager(
        host_manager=_HostManagerStub(),
        log_watcher_factory=build_log_watcher,
        event_watcher_factory=build_event_watcher,
    )

    await manager.start()
    await manager.refresh_host("host-a")
    await manager.shutdown()

    assert set(created_log_watchers) == {"host-a"}
    assert set(created_event_watchers) == {"host-a"}
    assert created_log_watchers["host-a"].started == 1
    assert created_event_watchers["host-a"].started == 1
    assert created_log_watchers["host-a"].refresh_calls == 2
    assert created_log_watchers["host-a"].shutdown_calls == 1
    assert created_event_watchers["host-a"].shutdown_calls == 1


@pytest.mark.asyncio
async def test_watch_manager_reacts_to_host_status_transitions() -> None:
    host_manager = _HostManagerStub()
    log_watchers: list[_LogDogerStub] = []
    event_watchers: list[_EventWatcherStub] = []

    def build_log_watcher(host: dict) -> _LogDogerStub:
        watcher = _LogDogerStub(host)
        log_watchers.append(watcher)
        return watcher

    def build_event_watcher(host: dict) -> _EventWatcherStub:
        watcher = _EventWatcherStub(host)
        event_watchers.append(watcher)
        return watcher

    manager = WatchManager(
        host_manager=host_manager,
        log_watcher_factory=build_log_watcher,
        event_watcher_factory=build_event_watcher,
    )

    await manager.start()
    host_manager.set_status("host-a", "disconnected")
    await manager.handle_host_status_change(
        {"name": "host-a", "old_status": "connected", "new_status": "disconnected"}
    )
    host_manager.set_status("host-a", "connected")
    await manager.handle_host_status_change(
        {"name": "host-a", "old_status": "disconnected", "new_status": "connected"}
    )
    await manager.shutdown()

    assert len(log_watchers) == 2
    assert len(event_watchers) == 2
    assert log_watchers[0].shutdown_calls == 1
    assert event_watchers[0].shutdown_calls == 1
    assert log_watchers[1].started == 1
    assert event_watchers[1].started == 1
    assert log_watchers[1].refresh_calls == 1


@pytest.mark.asyncio
async def test_watch_manager_records_and_clears_degraded_hosts_on_recovery() -> None:
    host_manager = _HostManagerStub()
    event_build_count = {"n": 0}

    def build_log_watcher(host: dict) -> _LogDogerStub:
        return _LogDogerStub(host)

    def build_event_watcher(host: dict) -> _FailingEventWatcherStub:
        event_build_count["n"] += 1
        return _FailingEventWatcherStub(
            host,
            fail_on_start=event_build_count["n"] == 1,
        )

    manager = WatchManager(
        host_manager=host_manager,
        log_watcher_factory=build_log_watcher,
        event_watcher_factory=build_event_watcher,
    )

    await manager.start()

    assert manager.get_degraded_hosts() == {"host-a": "event watcher start failed"}
    assert manager._log_watchers == {}
    assert manager._event_watchers == {}

    await manager.refresh_host("host-a")

    assert manager.get_degraded_hosts() == {}
    assert "host-a" in manager._log_watchers
    assert "host-a" in manager._event_watchers

    await manager.shutdown()


@pytest.mark.asyncio
async def test_watch_manager_reload_keeps_old_watchers_when_candidate_start_fails() -> (
    None
):
    host_manager = _HostManagerStub()
    created_log_watchers: list[_LogDogerStub] = []
    created_event_watchers: list[_FailingEventWatcherStub] = []
    creation_round = {"n": 0}

    def build_log_watcher(host: dict) -> _LogDogerStub:
        watcher = _LogDogerStub(host)
        created_log_watchers.append(watcher)
        return watcher

    def build_event_watcher(host: dict) -> _FailingEventWatcherStub:
        creation_round["n"] += 1
        watcher = _FailingEventWatcherStub(
            host,
            fail_on_start=creation_round["n"] > 1,
        )
        created_event_watchers.append(watcher)
        return watcher

    manager = WatchManager(
        host_manager=host_manager,
        log_watcher_factory=build_log_watcher,
        event_watcher_factory=build_event_watcher,
    )

    await manager.start()
    original_log = manager._log_watchers["host-a"]
    original_event = manager._event_watchers["host-a"]

    with pytest.raises(RuntimeError, match="event watcher start failed"):
        await manager.reload_host_configs(["host-a"])

    assert manager._log_watchers["host-a"] is original_log
    assert manager._event_watchers["host-a"] is original_event

    await manager.shutdown()


@pytest.mark.asyncio
async def test_watch_manager_reload_host_configs_cleans_old_log_watcher_pending_tasks() -> (
    None
):
    host_manager = _HostManagerStub()
    build_round = {"n": 0}
    controllers: list[AlertStormController] = []

    async def _notify(_host: str, _message: str, _category: str) -> bool:
        return True

    async def _save_alert(_payload: dict[str, object]) -> bool:
        return True

    async def _save_storm_event(_payload: dict[str, object]) -> bool:
        return True

    def build_log_watcher(host: dict) -> LogStreamWatcher:
        build_round["n"] += 1
        controller = AlertStormController(
            enabled=True,
            window_seconds=60,
            threshold=1,
            suppress_minutes=1,
        )
        controllers.append(controller)
        return LogStreamWatcher(
            host=host,
            stream_logs=None,
            notifier_send=_notify,
            save_alert=_save_alert,
            storm_controller=controller,
        )

    def build_event_watcher(host: dict) -> _EventWatcherStub:
        return _EventWatcherStub(host)

    manager = WatchManager(
        host_manager=host_manager,
        log_watcher_factory=build_log_watcher,
        event_watcher_factory=build_event_watcher,
    )

    await manager.start()
    original_controller = controllers[0]
    dedup_store = CooldownStore(default_minutes=0.001)

    await run_alert_once(
        "ERROR reload dedup",
        host="host-a",
        container_id="c1",
        container_name="svc-a",
        timestamp="2026-04-19T10:00:00Z",
        cooldown_store=dedup_store,
        notifier_send=_notify,
        save_alert=_save_alert,
        config={"llm": {"enabled": False}},
    )
    await run_alert_once(
        "ERROR reload dedup",
        host="host-a",
        container_id="c1",
        container_name="svc-a",
        timestamp="2026-04-19T10:00:01Z",
        cooldown_store=dedup_store,
        notifier_send=_notify,
        save_alert=_save_alert,
        config={"llm": {"enabled": False}},
    )
    storm_store = CooldownStore()
    await run_alert_once(
        "ERROR reload storm",
        host="host-a",
        container_id="c1",
        container_name="svc-a",
        timestamp="2026-04-19T10:00:02Z",
        cooldown_store=storm_store,
        notifier_send=_notify,
        save_alert=_save_alert,
        save_storm_event=_save_storm_event,
        storm_controller=original_controller,
        config={"llm": {"enabled": False}},
    )

    assert any(key[0] == "host-a" for key in log_stream_module._PENDING_DEDUP_TASKS)
    assert any(
        key[0] == id(original_controller)
        for key in log_stream_module._PENDING_STORM_END_TASKS
    )

    try:
        await manager.reload_host_configs(["host-a"])
        assert len(controllers) == 2
        assert not any(
            key[0] == id(original_controller)
            for key in log_stream_module._PENDING_STORM_END_TASKS
        )
        assert not any(
            key[0] == "host-a" for key in log_stream_module._PENDING_DEDUP_TASKS
        )
        assert original_controller._active_by_category == {}
        assert original_controller._recent_by_category == {}
    finally:
        await manager.shutdown()
        await log_stream_module.cancel_pending_dedup_tasks()
