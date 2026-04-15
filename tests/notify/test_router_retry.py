import asyncio
import time

import pytest

from logdog.notify.base import BaseNotifier
from logdog.notify.base import split_message
from logdog.notify.router import NotifyRouter
from logdog.notify.telegram import TelegramNotifier
from logdog.notify.weixin import WeixinNotifier
from logdog.notify.wecom import WecomNotifier
from logdog.notify.wechat import WechatNotifier
from logdog.main import _build_multi_target_send_func


class _FailingNotifier(BaseNotifier):
    def __init__(self) -> None:
        super().__init__(name="failing")
        self.calls = 0

    async def send(self, host: str, message: str, category: str) -> None:
        self.calls += 1
        raise RuntimeError("send failed")


class _FlakyNotifier(BaseNotifier):
    def __init__(self) -> None:
        super().__init__(name="flaky")
        self.calls = 0

    async def send(self, host: str, message: str, category: str) -> None:
        self.calls += 1
        if self.calls == 1:
            raise RuntimeError("first fail")


class _SlowNotifier(BaseNotifier):
    def __init__(self, name: str, delay: float) -> None:
        super().__init__(name=name)
        self.delay = delay
        self.calls = 0

    async def send(self, host: str, message: str, category: str) -> None:
        await asyncio.sleep(self.delay)
        self.calls += 1


class _FailingNotifierWithSecret(BaseNotifier):
    def __init__(self) -> None:
        super().__init__(name="failing-secret")

    async def send(self, host: str, message: str, category: str) -> None:
        _ = (host, message, category)
        raise RuntimeError("Bearer TOP_SECRET password=abc")


@pytest.mark.asyncio
async def test_retry_once_and_record_failure() -> None:
    notifier = _FailingNotifier()
    failures: list[dict] = []

    async def recorder(payload: dict) -> None:
        failures.append(payload)

    router = NotifyRouter([notifier], max_retries=1, failure_recorder=recorder)
    ok = await router.send("h1", "msg", "ERROR")

    assert ok is False
    assert notifier.calls == 2
    assert len(failures) == 1
    assert failures[0]["host"] == "h1"
    assert failures[0]["channel"] == "failing"


@pytest.mark.asyncio
async def test_retry_once_then_success_no_failure_record() -> None:
    notifier = _FlakyNotifier()
    failures: list[dict] = []

    async def recorder(payload: dict) -> None:
        failures.append(payload)

    router = NotifyRouter([notifier], max_retries=1, failure_recorder=recorder)
    ok = await router.send("h1", "msg", "ERROR")

    assert ok is True
    assert notifier.calls == 2
    assert failures == []


@pytest.mark.asyncio
async def test_failure_recorder_error_does_not_raise_and_send_returns_false() -> None:
    notifier = _FailingNotifier()

    async def bad_recorder(payload: dict) -> None:
        raise RuntimeError("db down")

    router = NotifyRouter([notifier], max_retries=0, failure_recorder=bad_recorder)
    ok = await router.send("h1", "msg", "ERROR")

    assert ok is False
    assert notifier.calls == 1


@pytest.mark.asyncio
async def test_send_is_concurrent_across_notifiers() -> None:
    n1 = _SlowNotifier("n1", delay=0.08)
    n2 = _SlowNotifier("n2", delay=0.08)
    router = NotifyRouter([n1, n2], max_retries=0)

    start = time.perf_counter()
    ok = await router.send("h1", "msg", "ERROR")
    elapsed = time.perf_counter() - start

    assert ok is True
    assert n1.calls == 1
    assert n2.calls == 1
    assert elapsed < 0.14


@pytest.mark.asyncio
async def test_router_reports_success_when_any_channel_succeeds() -> None:
    ok_notifier = _SlowNotifier("ok", delay=0)
    fail_notifier = _FailingNotifier()
    router = NotifyRouter([ok_notifier, fail_notifier], max_retries=0)

    ok = await router.send("h1", "msg", "ERROR")

    assert ok is True
    assert ok_notifier.calls == 1
    assert fail_notifier.calls == 1


@pytest.mark.asyncio
async def test_telegram_chunks_large_message_to_4096() -> None:
    chunks: list[str] = []

    async def sender(target: str, message: str, parse_mode: str = "") -> None:
        assert target == "chat-id"
        chunks.append(message)

    notifier = TelegramNotifier(send_func=sender)
    await notifier.send("chat-id", "x" * 5000, "ERROR")

    assert len(chunks) == 2
    assert len(chunks[0]) == 4096
    assert len(chunks[1]) == 904


@pytest.mark.asyncio
async def test_wechat_chunks_large_message_to_4096() -> None:
    chunks: list[str] = []

    async def sender(target: str, message: str) -> None:
        assert target == "webhook-url"
        chunks.append(message)

    notifier = WechatNotifier(send_func=sender)
    await notifier.send("webhook-url", "x" * 5000, "ERROR")

    assert len(chunks) == 2
    assert len(chunks[0]) == 4096
    assert len(chunks[1]) == 904


@pytest.mark.asyncio
async def test_weixin_chunks_large_message_to_4096() -> None:
    chunks: list[str] = []

    async def sender(target: str, message: str) -> None:
        assert target == "wx-user"
        chunks.append(message)

    notifier = WeixinNotifier(send_func=sender)
    await notifier.send("wx-user", "x" * 5000, "ERROR")

    assert len(chunks) == 2
    assert len(chunks[0]) == 4096
    assert len(chunks[1]) == 904


@pytest.mark.asyncio
async def test_wecom_chunks_large_message_to_4096() -> None:
    chunks: list[str] = []

    async def sender(target: str, message: str) -> None:
        assert target == "wecom-room"
        chunks.append(message)

    notifier = WecomNotifier(send_func=sender)
    await notifier.send("wecom-room", "x" * 5000, "ERROR")

    assert len(chunks) == 2
    assert len(chunks[0]) == 4096
    assert len(chunks[1]) == 904


@pytest.mark.asyncio
async def test_router_records_sanitized_failure_message() -> None:
    notifier = _FailingNotifier()
    failures: list[dict] = []

    async def recorder(payload: dict) -> None:
        failures.append(payload)

    router = NotifyRouter([notifier], max_retries=0, failure_recorder=recorder)
    ok = await router.send("h1", "Bearer SECRET_TOKEN password=abc token=xyz", "ERROR")

    assert ok is False
    assert len(failures) == 1
    assert "SECRET_TOKEN" not in failures[0]["message"]
    assert "password=abc" not in failures[0]["message"]
    assert "token=xyz" not in failures[0]["message"]
    assert "***REDACTED***" in failures[0]["message"]


@pytest.mark.asyncio
async def test_router_records_sanitized_json_style_secrets() -> None:
    notifier = _FailingNotifier()
    failures: list[dict] = []

    async def recorder(payload: dict) -> None:
        failures.append(payload)

    router = NotifyRouter([notifier], max_retries=0, failure_recorder=recorder)
    ok = await router.send(
        "h1",
        '{"token":"xyz","api_key":"ak","password":"pw"}',
        "ERROR",
    )

    assert ok is False
    assert len(failures) == 1
    assert '"token":"xyz"' not in failures[0]["message"]
    assert '"api_key":"ak"' not in failures[0]["message"]
    assert '"password":"pw"' not in failures[0]["message"]
    assert "***REDACTED***" in failures[0]["message"]


@pytest.mark.asyncio
async def test_router_sanitizes_failure_error_field() -> None:
    notifier = _FailingNotifierWithSecret()
    failures: list[dict] = []

    async def recorder(payload: dict) -> None:
        failures.append(payload)

    router = NotifyRouter([notifier], max_retries=0, failure_recorder=recorder)
    ok = await router.send("h1", "msg", "ERROR")

    assert ok is False
    assert len(failures) == 1
    assert "TOP_SECRET" not in failures[0]["error"]
    assert "password=abc" not in failures[0]["error"]
    assert "***REDACTED***" in failures[0]["error"]


@pytest.mark.asyncio
async def test_retry_uses_backoff_between_attempts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    notifier = _FailingNotifier()
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr("logdog.notify.router.asyncio.sleep", fake_sleep)

    router = NotifyRouter([notifier], max_retries=2, retry_backoff_seconds=0.01)
    ok = await router.send("h1", "msg", "ERROR")

    assert ok is False
    assert notifier.calls == 3
    assert len(sleeps) == 2


def test_split_message_empty_returns_no_chunks() -> None:
    assert split_message("", 4096) == []


def test_split_message_prefers_newline_boundaries() -> None:
    message = "line-1\nline-2\nline-3"
    chunks = split_message(message, 10)

    assert chunks == ["line-1", "line-2", "line-3"]


@pytest.mark.asyncio
async def test_telegram_notifier_supports_doc_message_mode() -> None:
    chunks: list[str] = []

    async def sender(target: str, message: str, parse_mode: str = "") -> None:
        assert target == "chat-id"
        chunks.append(message)

    notifier = TelegramNotifier(
        send_func=sender,
        message_mode_getter=lambda: "doc",
    )
    await notifier.send("chat-id", "service down", "ERROR")

    assert len(chunks) == 1
    assert "LogDog Notification" in chunks[0]
    assert "service down" in chunks[0]


@pytest.mark.asyncio
async def test_router_prefers_route_selector_when_context_matches() -> None:
    chosen = _SlowNotifier("chosen", delay=0)
    default = _SlowNotifier("default", delay=0)

    def selector(host: str, category: str, context: dict | None):
        if (context or {}).get("container_name") == "api":
            return [chosen]
        return [default]

    router = NotifyRouter([default], route_selector=selector)
    ok = await router.send("prod-a", "msg", "ERROR", context={"container_name": "api"})

    assert ok is True
    assert chosen.calls == 1
    assert default.calls == 0


@pytest.mark.asyncio
async def test_router_route_selector_empty_result_blocks_fallback() -> None:
    default = _SlowNotifier("default", delay=0)

    def selector(_host: str, _category: str, _context: dict | None):
        return []

    router = NotifyRouter([default], route_selector=selector)
    ok = await router.send("prod-a", "msg", "ERROR", context={"container_name": "api"})

    assert ok is False
    assert default.calls == 0


@pytest.mark.asyncio
async def test_multi_target_send_func_retries_only_failed_targets() -> None:
    calls: list[str] = []
    failures = {"tg-2": 1}

    async def telegram_send(target: str, _message: str, parse_mode: str = "") -> None:
        calls.append(target)
        if target == "tg-2" and failures["tg-2"] > 0:
            failures["tg-2"] -= 1
            raise RuntimeError("fail once")

    notifier = TelegramNotifier(
        send_func=_build_multi_target_send_func(telegram_send, ["tg-1", "tg-2"])
    )
    router = NotifyRouter([notifier], max_retries=1, retry_backoff_seconds=0)
    ok = await router.send("host-a", "msg", "ERROR")

    assert ok is True
    assert calls == ["tg-1", "tg-2", "tg-2"]


def _extract_retry_state_from_wrapper(send_func):
    closure = getattr(send_func, "__closure__", None) or ()
    for cell in closure:
        value = getattr(cell, "cell_contents", None)
        if isinstance(value, dict):
            if not value:
                return value
            sample_value = next(iter(value.values()))
            if isinstance(sample_value, tuple) and len(sample_value) == 2:
                return value
    raise AssertionError("retry state map not found")


@pytest.mark.asyncio
async def test_multi_target_send_func_prunes_expired_retry_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = {"value": 0.0}
    monkeypatch.setattr("logdog.main.time.monotonic", lambda: now["value"])

    async def telegram_send(_target: str, _message: str, parse_mode: str = "") -> None:
        raise RuntimeError("always fail")

    send_func = _build_multi_target_send_func(telegram_send, ["tg-1"])
    retry_state = _extract_retry_state_from_wrapper(send_func)

    for idx in range(5):
        with pytest.raises(RuntimeError):
            await send_func("host-a", f"msg-{idx}")
    assert len(retry_state) == 5

    now["value"] = 5.0
    with pytest.raises(RuntimeError):
        await send_func("host-a", "msg-final")
    assert len(retry_state) == 1
