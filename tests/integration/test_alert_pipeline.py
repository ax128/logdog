import asyncio
import importlib
from collections.abc import AsyncIterator
import pytest
import pytest_asyncio

from logdog.collector.log_stream import (
    run_alert_once,
    reset_alert_runtime_state_for_tests,
)
from logdog.pipeline.cooldown import CooldownStore


def _alert_storm_controller_cls():
    module = importlib.import_module("logdog.collector.storm")
    return module.AlertStormController


@pytest_asyncio.fixture(autouse=True)
async def _reset_alert_runtime_state() -> AsyncIterator[None]:
    await reset_alert_runtime_state_for_tests()
    yield
    await reset_alert_runtime_state_for_tests()


@pytest.mark.asyncio
async def test_alert_pipeline_end_to_end_trigger_push_and_save() -> None:
    pushed_records: list[tuple[str, str, str]] = []
    saved_records: list[dict] = []

    async def fake_notify(host: str, message: str, category: str) -> bool:
        pushed_records.append((host, message, category))
        return True

    async def fake_save(payload: dict) -> bool:
        saved_records.append(payload)
        return True

    result = await run_alert_once(
        "Bearer abc ERROR",
        host="h1",
        container_id="c1",
        now=0,
        notifier_send=fake_notify,
        save_alert=fake_save,
    )

    assert result.triggered is True
    assert result.pushed is True
    assert result.saved is True
    assert len(pushed_records) == 1
    assert len(saved_records) == 1
    assert saved_records[0]["host"] == "h1"
    assert saved_records[0]["container_id"] == "c1"
    assert saved_records[0]["category"] == "ERROR"


@pytest.mark.asyncio
async def test_alert_pipeline_non_triggered_line_skips_push_and_save() -> None:
    pushed_records: list[tuple[str, str, str]] = []
    saved_records: list[dict] = []

    async def fake_notify(host: str, message: str, category: str) -> bool:
        pushed_records.append((host, message, category))
        return True

    async def fake_save(payload: dict) -> bool:
        saved_records.append(payload)
        return True

    result = await run_alert_once(
        "service started normally",
        host="h1",
        container_id="c1",
        now=0,
        notifier_send=fake_notify,
        save_alert=fake_save,
    )

    assert result.triggered is False
    assert result.pushed is False
    assert result.saved is False
    assert pushed_records == []
    assert saved_records == []


@pytest.mark.asyncio
async def test_alert_pipeline_cooldown_suppresses_second_event() -> None:
    pushed_records: list[tuple[str, str, str]] = []
    saved_records: list[dict] = []
    cooldown = CooldownStore(default_minutes=1)

    async def fake_notify(host: str, message: str, category: str) -> bool:
        pushed_records.append((host, message, category))
        return True

    async def fake_save(payload: dict) -> bool:
        saved_records.append(payload)
        return True

    first = await run_alert_once(
        "ERROR first",
        host="h1",
        container_id="c1",
        now=0,
        cooldown_store=cooldown,
        notifier_send=fake_notify,
        save_alert=fake_save,
    )
    second = await run_alert_once(
        "ERROR second",
        host="h1",
        container_id="c1",
        now=10,
        cooldown_store=cooldown,
        notifier_send=fake_notify,
        save_alert=fake_save,
    )

    assert first.triggered is True
    assert first.pushed is True
    assert first.saved is True

    assert second.triggered is True
    assert second.pushed is False
    assert second.saved is False

    assert len(pushed_records) == 1
    assert len(saved_records) == 1


@pytest.mark.asyncio
async def test_alert_pipeline_default_cooldown_suppresses_second_event() -> None:
    pushed_records: list[tuple[str, str, str]] = []
    saved_records: list[dict] = []

    async def fake_notify(host: str, message: str, category: str) -> bool:
        pushed_records.append((host, message, category))
        return True

    async def fake_save(payload: dict) -> bool:
        saved_records.append(payload)
        return True

    first = await run_alert_once(
        "ERROR first",
        host="h1",
        container_id="c1",
        now=0,
        notifier_send=fake_notify,
        save_alert=fake_save,
    )
    second = await run_alert_once(
        "ERROR second",
        host="h1",
        container_id="c1",
        now=10,
        notifier_send=fake_notify,
        save_alert=fake_save,
    )

    assert first.triggered is True
    assert first.pushed is True
    assert first.saved is True

    assert second.triggered is True
    assert second.pushed is False
    assert second.saved is False

    assert len(pushed_records) == 1
    assert len(saved_records) == 1


@pytest.mark.asyncio
async def test_alert_pipeline_sends_dedup_summary_after_window(monkeypatch) -> None:
    pushed_records: list[tuple[str, str, str]] = []
    saved_records: list[dict] = []
    cooldown = CooldownStore(default_minutes=0.001)  # 0.06s
    clock = {"t": 1_000.0}

    def fake_time() -> float:
        current = clock["t"]
        clock["t"] += 0.01
        return current

    monkeypatch.setattr("logdog.collector.log_stream.time.time", fake_time)

    async def fake_notify(host: str, message: str, category: str) -> bool:
        pushed_records.append((host, message, category))
        return True

    async def fake_save(payload: dict) -> bool:
        saved_records.append(payload)
        return True

    first = await run_alert_once(
        "ERROR first",
        host="h1",
        container_id="c1",
        cooldown_store=cooldown,
        notifier_send=fake_notify,
        save_alert=fake_save,
    )
    second = await run_alert_once(
        "ERROR second",
        host="h1",
        container_id="c1",
        cooldown_store=cooldown,
        notifier_send=fake_notify,
        save_alert=fake_save,
    )
    third = await run_alert_once(
        "ERROR third",
        host="h1",
        container_id="c1",
        cooldown_store=cooldown,
        notifier_send=fake_notify,
        save_alert=fake_save,
    )

    assert first.pushed is True
    assert second.pushed is False
    assert third.pushed is False

    await asyncio.sleep(0.12)

    assert len(pushed_records) == 2
    assert len(saved_records) == 2
    summary_payload = saved_records[1]
    assert summary_payload["dedup_summary"] is True
    assert summary_payload["dedup_repeat_count"] == 2
    assert "repeat_count=2" in pushed_records[1][1]


@pytest.mark.asyncio
async def test_alert_pipeline_triggers_storm_alert_and_suppresses_followups() -> None:
    pushed_records: list[tuple[str, str, str]] = []
    saved_records: list[dict] = []
    AlertStormController = _alert_storm_controller_cls()
    storm = AlertStormController(
        enabled=True,
        window_seconds=120,
        threshold=2,
        suppress_minutes=10,
    )

    async def fake_notify(host: str, message: str, category: str) -> bool:
        pushed_records.append((host, message, category))
        return True

    async def fake_save(payload: dict) -> bool:
        saved_records.append(payload)
        return True

    first = await run_alert_once(
        "ERROR alpha",
        host="h1",
        container_id="c1",
        now=0,
        notifier_send=fake_notify,
        save_alert=fake_save,
        storm_controller=storm,
    )
    second = await run_alert_once(
        "ERROR beta",
        host="h2",
        container_id="c2",
        now=10,
        notifier_send=fake_notify,
        save_alert=fake_save,
        storm_controller=storm,
    )
    third = await run_alert_once(
        "ERROR gamma",
        host="h3",
        container_id="c3",
        now=20,
        notifier_send=fake_notify,
        save_alert=fake_save,
        storm_controller=storm,
    )

    assert first.pushed is True
    assert second.pushed is True
    assert third.pushed is False
    assert len(saved_records) == 3
    assert len(pushed_records) == 2
    assert pushed_records[1][2] == "STORM"
    assert "[STORM] ERROR" in pushed_records[1][1]


@pytest.mark.asyncio
async def test_alert_pipeline_sends_storm_end_summary_after_suppress_window() -> None:
    pushed_records: list[tuple[str, str, str]] = []
    AlertStormController = _alert_storm_controller_cls()
    storm = AlertStormController(
        enabled=True,
        window_seconds=120,
        threshold=2,
        suppress_minutes=1,
    )

    async def fake_notify(host: str, message: str, category: str) -> bool:
        pushed_records.append((host, message, category))
        return True

    await run_alert_once(
        "ERROR alpha",
        host="h1",
        container_id="c1",
        now=0,
        notifier_send=fake_notify,
        storm_controller=storm,
    )
    await run_alert_once(
        "ERROR beta",
        host="h2",
        container_id="c2",
        now=10,
        notifier_send=fake_notify,
        storm_controller=storm,
    )
    after = await run_alert_once(
        "ERROR delta",
        host="h4",
        container_id="c4",
        now=80,
        notifier_send=fake_notify,
        storm_controller=storm,
    )

    assert after.pushed is True
    assert any(category == "STORM_END" for _host, _message, category in pushed_records)
    assert any("风暴结束" in message for _host, message, _category in pushed_records)


@pytest.mark.asyncio
async def test_alert_pipeline_emits_storm_end_without_followup_alert() -> None:
    pushed_records: list[tuple[str, str, str]] = []
    AlertStormController = _alert_storm_controller_cls()
    storm = AlertStormController(
        enabled=True,
        window_seconds=120,
        threshold=1,
        suppress_minutes=0.001,
    )

    async def fake_notify(host: str, message: str, category: str) -> bool:
        pushed_records.append((host, message, category))
        return True

    await run_alert_once(
        "ERROR alpha",
        host="h1",
        container_id="c1",
        notifier_send=fake_notify,
        storm_controller=storm,
    )
    await asyncio.sleep(0.12)

    assert any(category == "STORM" for _host, _message, category in pushed_records)
    assert any(category == "STORM_END" for _host, _message, category in pushed_records)
