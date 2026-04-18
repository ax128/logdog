from __future__ import annotations

import asyncio

import pytest

from logdog.collector.event_stream import EventStreamWatcher


@pytest.mark.asyncio
async def test_event_stream_watcher_normalizes_restart_and_oom_into_alert_lines() -> (
    None
):
    alert_calls: list[dict] = []

    async def stream_events(_host: dict, **_kwargs):
        yield {
            "time": "2026-04-11T10:00:01Z",
            "action": "restart",
            "container_id": "c1",
            "container_name": "svc-api",
        }
        yield {
            "time": "2026-04-11T10:00:02Z",
            "action": "oom",
            "container_id": "c1",
            "container_name": "svc-api",
        }

    async def run_alert(line: str, **kwargs) -> None:
        alert_calls.append({"line": line, **kwargs})

    watcher = EventStreamWatcher(
        host={"name": "host-a", "url": "unix:///var/run/docker.sock"},
        stream_events=stream_events,
        run_alert=run_alert,
    )

    await watcher.watch_forever()

    assert alert_calls == [
        {
            "line": "ERROR docker event: container svc-api restarted",
            "host": "host-a",
            "container_id": "c1",
            "container_name": "svc-api",
            "timestamp": "2026-04-11T10:00:01Z",
            "notifier_send": None,
            "save_alert": None,
            "save_storm_event": None,
            "mute_checker": None,
            "prompt_template": "default_alert",
            "output_template": "standard",
            "config": {"name": "host-a", "url": "unix:///var/run/docker.sock"},
        },
        {
            "line": "OOM docker event: container svc-api exceeded memory limits",
            "host": "host-a",
            "container_id": "c1",
            "container_name": "svc-api",
            "timestamp": "2026-04-11T10:00:02Z",
            "notifier_send": None,
            "save_alert": None,
            "save_storm_event": None,
            "mute_checker": None,
            "prompt_template": "default_alert",
            "output_template": "standard",
            "config": {"name": "host-a", "url": "unix:///var/run/docker.sock"},
        },
    ]


@pytest.mark.asyncio
async def test_event_stream_watcher_requests_refresh_for_container_lifecycle_events() -> (
    None
):
    refresh_calls: list[str] = []

    async def stream_events(_host: dict, **_kwargs):
        yield {
            "time": "2026-04-11T10:00:01Z",
            "action": "start",
            "container_id": "c1",
            "container_name": "svc-api",
        }
        yield {
            "time": "2026-04-11T10:00:02Z",
            "action": "die",
            "container_id": "c1",
            "container_name": "svc-api",
        }

    async def refresh_host(host_name: str) -> None:
        refresh_calls.append(host_name)

    watcher = EventStreamWatcher(
        host={"name": "host-a", "url": "unix:///var/run/docker.sock"},
        stream_events=stream_events,
        run_alert=lambda *_args, **_kwargs: None,
        refresh_host=refresh_host,
    )

    await watcher.watch_forever()

    assert refresh_calls == ["host-a", "host-a"]


@pytest.mark.asyncio
async def test_event_stream_watcher_passes_runtime_dependencies_into_alert_pipeline() -> (
    None
):
    alert_calls: list[dict] = []

    async def stream_events(_host: dict, **_kwargs):
        yield {
            "time": "2026-04-11T10:00:02Z",
            "action": "oom",
            "container_id": "c1",
            "container_name": "svc-api",
        }

    async def notify(_host: str, _message: str, _category: str) -> bool:
        return True

    async def save_alert(_payload: dict) -> bool:
        return True

    async def mute_checker(**_kwargs) -> dict | None:
        return None

    async def run_alert(line: str, **kwargs) -> None:
        alert_calls.append({"line": line, **kwargs})

    watcher = EventStreamWatcher(
        host={
            "name": "host-a",
            "url": "unix:///var/run/docker.sock",
            "prompt_template": "default_alert",
            "rules": {"custom_alerts": [{"pattern": "OOM", "category": "OOM"}]},
        },
        stream_events=stream_events,
        run_alert=run_alert,
        notifier_send=notify,
        save_alert=save_alert,
        mute_checker=mute_checker,
        prompt_template="default_alert",
        config={"rules": {"custom_alerts": [{"pattern": "OOM", "category": "OOM"}]}},
    )

    await watcher.watch_forever()

    assert alert_calls == [
        {
            "line": "OOM docker event: container svc-api exceeded memory limits",
            "host": "host-a",
            "container_id": "c1",
            "container_name": "svc-api",
            "timestamp": "2026-04-11T10:00:02Z",
            "notifier_send": notify,
            "save_alert": save_alert,
            "save_storm_event": None,
            "mute_checker": mute_checker,
            "prompt_template": "default_alert",
            "output_template": "standard",
            "config": {
                "rules": {"custom_alerts": [{"pattern": "OOM", "category": "OOM"}]}
            },
        }
    ]


@pytest.mark.asyncio
async def test_event_stream_watcher_recovers_after_stream_exception() -> None:
    attempts = {"n": 0}
    alert_calls: list[dict] = []

    async def stream_events(_host: dict, **_kwargs):
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise RuntimeError("stream broken")
        yield {
            "time": "2026-04-11T10:00:02Z",
            "action": "oom",
            "container_id": "c1",
            "container_name": "svc-api",
        }

    async def run_alert(line: str, **kwargs) -> None:
        alert_calls.append({"line": line, **kwargs})

    watcher = EventStreamWatcher(
        host={"name": "host-a", "url": "unix:///var/run/docker.sock"},
        stream_events=stream_events,
        run_alert=run_alert,
        reconnect_backoff_seconds=0,
    )

    await watcher.watch_forever()

    assert attempts["n"] == 2
    assert len(alert_calls) == 1
    assert "exceeded memory limits" in alert_calls[0]["line"]


@pytest.mark.asyncio
async def test_event_stream_watcher_reconnects_after_stream_eof() -> None:
    attempts = {"n": 0}
    reconnected = asyncio.Event()

    async def stream_events(_host: dict, **_kwargs):
        attempts["n"] += 1
        if attempts["n"] == 1:
            yield {
                "time": "2026-04-11T10:00:01Z",
                "action": "oom",
                "container_id": "c1",
                "container_name": "svc-api",
            }
            return
        reconnected.set()
        await asyncio.sleep(3600)
        if False:
            yield {}

    watcher = EventStreamWatcher(
        host={"name": "host-a", "url": "unix:///var/run/docker.sock"},
        stream_events=stream_events,
        run_alert=lambda *_args, **_kwargs: None,
        reconnect_backoff_seconds=0,
    )

    await watcher.start()
    try:
        await asyncio.wait_for(reconnected.wait(), timeout=1.0)
    finally:
        await watcher.shutdown()

    assert attempts["n"] >= 2


@pytest.mark.asyncio
async def test_event_stream_backoff_increases_on_repeated_crashes() -> None:
    """After repeated crashes, backoff delay should increase exponentially."""
    sleep_durations: list[float] = []

    async def stream_events(_host: dict, **_kwargs):
        raise ConnectionError("Too many open files")
        if False:
            yield {}

    async def fake_sleep(seconds: float) -> None:
        sleep_durations.append(seconds)
        if len(sleep_durations) >= 3:
            raise asyncio.CancelledError

    watcher = EventStreamWatcher(
        host={"name": "host-a", "url": "unix:///var/run/docker.sock"},
        stream_events=stream_events,
        run_alert=lambda *_args, **_kwargs: None,
        reconnect_backoff_seconds=1.0,
    )

    import unittest.mock
    _real_sleep = asyncio.sleep
    with (
        unittest.mock.patch("logdog.collector.event_stream.asyncio.sleep", side_effect=fake_sleep),
        unittest.mock.patch("logdog.collector.event_stream.random.random", return_value=0.5),
    ):
        await watcher.start()
        try:
            await _real_sleep(1.0)
        except asyncio.CancelledError:
            pass
        finally:
            await watcher.shutdown()

    assert len(sleep_durations) >= 2
    for i in range(1, len(sleep_durations)):
        assert sleep_durations[i] > sleep_durations[i - 1], (
            f"backoff did not increase: {sleep_durations}"
        )


@pytest.mark.asyncio
async def test_event_stream_backoff_resets_after_successful_stream() -> None:
    """After a successful stream (normal EOF), backoff should reset."""
    sleep_durations: list[float] = []
    attempt = {"n": 0}

    async def stream_events(_host: dict, **_kwargs):
        attempt["n"] += 1
        if attempt["n"] == 1:
            raise ConnectionError("crash")
        if attempt["n"] == 2:
            raise ConnectionError("crash again")
        if attempt["n"] == 3:
            yield {"time": "2026-04-18T10:00:00Z", "action": "start",
                   "container_id": "c1", "container_name": "svc"}
            return  # normal EOF
        if attempt["n"] == 4:
            raise ConnectionError("crash after recovery")
        raise asyncio.CancelledError
        if False:
            yield {}

    async def fake_sleep(seconds: float) -> None:
        sleep_durations.append(seconds)

    watcher = EventStreamWatcher(
        host={"name": "host-a", "url": "unix:///var/run/docker.sock"},
        stream_events=stream_events,
        run_alert=lambda *_args, **_kwargs: None,
        reconnect_backoff_seconds=1.0,
    )

    import unittest.mock
    _real_sleep = asyncio.sleep
    with (
        unittest.mock.patch("logdog.collector.event_stream.asyncio.sleep", side_effect=fake_sleep),
        unittest.mock.patch("logdog.collector.event_stream.random.random", return_value=0.0),
    ):
        await watcher.start()
        try:
            await asyncio.wait_for(_real_sleep(2.0), timeout=2.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass
        finally:
            await watcher.shutdown()

    # After recovery (attempt 3, normal EOF), backoff resets
    assert len(sleep_durations) >= 4
    assert sleep_durations[2] < sleep_durations[1], (
        f"backoff should reset after EOF: {sleep_durations}"
    )
