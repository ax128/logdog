from __future__ import annotations

from collections.abc import Awaitable, Callable

import pytest

from logwatch.notify.telegram import build_telegram_bot_runtime, TELEGRAM_BOT_COMMANDS


class _FakeUpdater:
    def __init__(self, events: list[str]) -> None:
        self._events = events

    async def start_polling(self) -> None:
        self._events.append("polling:start")

    async def stop(self) -> None:
        self._events.append("polling:stop")


class _FakeApplication:
    def __init__(self, events: list[str]) -> None:
        self._events = events
        self.handlers: list[Callable[[object, object], Awaitable[None]]] = []
        self.updater = _FakeUpdater(events)

    def add_handler(self, handler: Callable[[object, object], Awaitable[None]]) -> None:
        self.handlers.append(handler)

    async def initialize(self) -> None:
        self._events.append("app:initialize")

    async def start(self) -> None:
        self._events.append("app:start")

    async def stop(self) -> None:
        self._events.append("app:stop")

    async def shutdown(self) -> None:
        self._events.append("app:shutdown")


class _FailingStartApplication(_FakeApplication):
    async def start(self) -> None:
        self._events.append("app:start")
        raise RuntimeError("start failed")


class _RecordingChatRuntime:
    def __init__(self) -> None:
        self.calls: list[dict[str, str]] = []

    def invoke_text(
        self,
        prompt: str,
        *,
        user_id: str | None = None,
        session_key: str | None = None,
        fallback: str,
    ) -> str:
        self.calls.append(
            {
                "prompt": prompt,
                "user_id": str(user_id or ""),
                "session_key": str(session_key or ""),
                "fallback": fallback,
            }
        )
        return f"agent::{prompt}"


class _FakeMessage:
    def __init__(self, text: str) -> None:
        self.text = text
        self.replies: list[str] = []

    async def reply_text(self, text: str) -> None:
        self.replies.append(text)


class _FakeUpdate:
    def __init__(self, *, user_id: int, chat_id: int, text: str) -> None:
        self.effective_user = type("User", (), {"id": user_id})()
        self.effective_chat = type("Chat", (), {"id": chat_id})()
        self.effective_message = _FakeMessage(text)


@pytest.mark.asyncio
async def test_telegram_runtime_lifecycle_and_authorized_message_flow() -> None:
    events: list[str] = []
    application = _FakeApplication(events)
    chat_runtime = _RecordingChatRuntime()

    runtime = build_telegram_bot_runtime(
        bot_token="token",
        chat_runtime=chat_runtime,
        authorized_user_ids={"42"},
        application_factory=lambda _token: application,
        handler_binder=lambda app, handler: app.add_handler(handler),
    )

    assert runtime is not None
    assert len(application.handlers) == 1

    await runtime.start()

    handler = application.handlers[0]
    update = _FakeUpdate(user_id=42, chat_id=9001, text="ping")
    await handler(update, None)

    assert chat_runtime.calls == [
        {
            "prompt": "ping",
            "user_id": "42",
            "session_key": "telegram:9001",
            "fallback": "Agent unavailable right now. Please try again shortly.",
        }
    ]
    assert update.effective_message.replies == ["agent::ping"]

    unauthorized = _FakeUpdate(user_id=7, chat_id=9001, text="pong")
    await handler(unauthorized, None)

    assert len(chat_runtime.calls) == 1
    assert unauthorized.effective_message.replies == []

    await runtime.shutdown()

    assert events == [
        "app:initialize",
        "app:start",
        "polling:start",
        "polling:stop",
        "app:stop",
        "app:shutdown",
    ]


@pytest.mark.asyncio
async def test_telegram_runtime_rolls_back_when_start_fails() -> None:
    events: list[str] = []
    application = _FailingStartApplication(events)
    chat_runtime = _RecordingChatRuntime()

    runtime = build_telegram_bot_runtime(
        bot_token="token",
        chat_runtime=chat_runtime,
        authorized_user_ids={"42"},
        application_factory=lambda _token: application,
        handler_binder=lambda app, handler: app.add_handler(handler),
    )

    assert runtime is not None

    with pytest.raises(RuntimeError, match="start failed"):
        await runtime.start()

    assert events == [
        "app:initialize",
        "app:start",
        "app:shutdown",
    ]


@pytest.mark.asyncio
async def test_telegram_runtime_supports_msg_command_for_hot_mode_switch() -> None:
    events: list[str] = []
    application = _FakeApplication(events)
    chat_runtime = _RecordingChatRuntime()
    mode_state = {"value": "text"}

    def set_mode(raw: str) -> str:
        candidate = str(raw or "").strip().lower()
        if candidate not in {"txt", "md", "doc", "text"}:
            raise ValueError("unsupported message mode")
        mode_state["value"] = candidate
        return candidate

    runtime = build_telegram_bot_runtime(
        bot_token="token",
        chat_runtime=chat_runtime,
        authorized_user_ids={"42"},
        message_mode_setter=set_mode,
        message_mode_getter=lambda: mode_state["value"],
        application_factory=lambda _token: application,
        handler_binder=lambda app, handler: app.add_handler(handler),
    )

    assert runtime is not None

    await runtime.start()
    handler = application.handlers[0]

    update_switch = _FakeUpdate(user_id=42, chat_id=9001, text="/msg md")
    await handler(update_switch, None)

    assert mode_state["value"] == "md"
    assert chat_runtime.calls == []
    assert update_switch.effective_message.replies
    assert "md" in update_switch.effective_message.replies[0]

    update_help = _FakeUpdate(user_id=42, chat_id=9001, text="/msg")
    await handler(update_help, None)

    assert update_help.effective_message.replies
    assert "txt" in update_help.effective_message.replies[0]
    assert "doc" in update_help.effective_message.replies[0]

    await runtime.shutdown()


class _FakeSender:
    def __init__(self) -> None:
        self.chat_action_calls: list[str] = []
        self.set_commands_calls: list[list[dict[str, str]]] = []

    def send_chat_action(self, chat_id: str, action: str = "typing") -> None:
        self.chat_action_calls.append(chat_id)

    def set_my_commands(self, commands: list[dict[str, str]]) -> None:
        self.set_commands_calls.append(commands)


class _FakeMessageWithCaption:
    def __init__(self, *, text: str | None = None, caption: str | None = None, document: object | None = None) -> None:
        self.text = text
        self.caption = caption
        self.document = document
        self.replies: list[str] = []

    async def reply_text(self, text: str) -> None:
        self.replies.append(text)


class _FakeUpdateWithCaption:
    def __init__(self, *, user_id: int, chat_id: int, text: str | None = None, caption: str | None = None, document: object | None = None) -> None:
        self.effective_user = type("User", (), {"id": user_id})()
        self.effective_chat = type("Chat", (), {"id": chat_id})()
        self.effective_message = _FakeMessageWithCaption(text=text, caption=caption, document=document)


@pytest.mark.asyncio
async def test_typing_indicator_sent_before_reply() -> None:
    events: list[str] = []
    application = _FakeApplication(events)
    chat_runtime = _RecordingChatRuntime()
    sender = _FakeSender()

    runtime = build_telegram_bot_runtime(
        bot_token="token",
        chat_runtime=chat_runtime,
        authorized_user_ids={"42"},
        application_factory=lambda _token: application,
        handler_binder=lambda app, handler: app.add_handler(handler),
    )

    assert runtime is not None
    runtime.set_sender(sender)
    await runtime.start()

    handler = application.handlers[0]
    update = _FakeUpdate(user_id=42, chat_id=9001, text="ping")
    await handler(update, None)

    assert sender.chat_action_calls == ["9001"]
    assert update.effective_message.replies == ["agent::ping"]

    await runtime.shutdown()


@pytest.mark.asyncio
async def test_file_message_with_caption_is_handled() -> None:
    events: list[str] = []
    application = _FakeApplication(events)
    chat_runtime = _RecordingChatRuntime()

    runtime = build_telegram_bot_runtime(
        bot_token="token",
        chat_runtime=chat_runtime,
        authorized_user_ids={"42"},
        application_factory=lambda _token: application,
        handler_binder=lambda app, handler: app.add_handler(handler),
    )

    assert runtime is not None
    await runtime.start()

    handler = application.handlers[0]
    update = _FakeUpdateWithCaption(
        user_id=42,
        chat_id=9001,
        caption="describe this file",
        document=object(),
    )
    await handler(update, None)

    assert chat_runtime.calls == [
        {
            "prompt": "describe this file",
            "user_id": "42",
            "session_key": "telegram:9001",
            "fallback": "Agent unavailable right now. Please try again shortly.",
        }
    ]
    assert update.effective_message.replies == ["agent::describe this file"]

    await runtime.shutdown()


@pytest.mark.asyncio
async def test_bot_commands_registered_on_start_when_sender_set() -> None:
    events: list[str] = []
    application = _FakeApplication(events)
    chat_runtime = _RecordingChatRuntime()
    sender = _FakeSender()

    runtime = build_telegram_bot_runtime(
        bot_token="token",
        chat_runtime=chat_runtime,
        authorized_user_ids={"42"},
        application_factory=lambda _token: application,
        handler_binder=lambda app, handler: app.add_handler(handler),
    )

    assert runtime is not None
    runtime.set_sender(sender)
    await runtime.start()

    assert len(sender.set_commands_calls) == 1
    assert sender.set_commands_calls[0] == TELEGRAM_BOT_COMMANDS

    await runtime.shutdown()
