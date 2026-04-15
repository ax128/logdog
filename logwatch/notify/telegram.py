from __future__ import annotations

import json
import importlib
import inspect
import logging
import ssl
import time
from collections.abc import Awaitable, Callable
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from logwatch.llm.agent_runtime import DEFAULT_CHAT_FALLBACK_MESSAGE
from logwatch.notify.base import (
    BaseNotifier,
    format_message_for_mode,
    normalize_message_mode,
    split_message,
)


SendFunc = Callable[[str, str], Awaitable[None]]
logger = logging.getLogger(__name__)
TELEGRAM_AUTO_TARGET = "__auto__"
_AUTO_TARGET_ATTR = "_logwatch_supports_auto_target"


class TelegramBotTokenSender:
    """Minimal Telegram Bot API sender for notify path."""

    def __init__(
        self,
        bot_token: str,
        *,
        timeout_seconds: float = 10.0,
        auto_chat_cache_ttl_seconds: float = 300.0,
    ) -> None:
        normalized_token = str(bot_token or "").strip()
        if normalized_token == "":
            raise ValueError("bot_token must not be empty")
        if float(timeout_seconds) <= 0:
            raise ValueError("timeout_seconds must be > 0")
        if float(auto_chat_cache_ttl_seconds) <= 0:
            raise ValueError("auto_chat_cache_ttl_seconds must be > 0")
        self._bot_token = normalized_token
        self._timeout_seconds = float(timeout_seconds)
        self._auto_chat_cache_ttl_seconds = float(auto_chat_cache_ttl_seconds)
        self._cached_chat_id: str | None = None
        self._cached_at_ts = 0.0

    def send(self, target: str, message: str) -> None:
        chat_id = str(target or "").strip()
        if chat_id == "" or chat_id == TELEGRAM_AUTO_TARGET:
            chat_id = self._resolve_auto_chat_id()
        self._request(
            "sendMessage",
            {
                "chat_id": chat_id,
                "text": str(message),
            },
        )

    def _resolve_auto_chat_id(self) -> str:
        now = time.monotonic()
        if (
            self._cached_chat_id is not None
            and (now - self._cached_at_ts) <= self._auto_chat_cache_ttl_seconds
        ):
            return self._cached_chat_id

        updates = self._request("getUpdates", {"timeout": 0, "limit": 100})
        if not isinstance(updates, list):
            raise RuntimeError("telegram getUpdates returned invalid payload")
        chat_id = self._extract_latest_chat_id(updates)
        if chat_id is None:
            raise RuntimeError(
                "telegram chat_id not discovered; send a message to the bot first"
            )

        self._cached_chat_id = chat_id
        self._cached_at_ts = now
        return chat_id

    @staticmethod
    def _extract_latest_chat_id(updates: list[Any]) -> str | None:
        for item in reversed(updates):
            if not isinstance(item, dict):
                continue
            message = (
                item.get("message")
                or item.get("edited_message")
                or item.get("channel_post")
            )
            if not isinstance(message, dict):
                continue
            chat = message.get("chat")
            if not isinstance(chat, dict):
                continue
            chat_id = str(chat.get("id") or "").strip()
            if chat_id != "":
                return chat_id
        return None

    def _request(self, method: str, payload: dict[str, Any] | None = None) -> Any:
        base_url = f"https://api.telegram.org/bot{self._bot_token}/{method}"
        request_payload = payload if isinstance(payload, dict) else {}
        request_data = None
        headers = {}
        if request_payload:
            request_data = json.dumps(request_payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = Request(base_url, data=request_data, method="POST", headers=headers)

        try:
            with urlopen(
                req,
                timeout=self._timeout_seconds,
                context=ssl.create_default_context(),
            ) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
        except HTTPError as exc:
            detail = str(exc)
            try:
                detail_payload = json.loads(exc.read().decode("utf-8", errors="replace"))
                detail = str(detail_payload.get("description") or detail_payload)
            except Exception:  # noqa: BLE001
                pass
            raise RuntimeError(detail) from exc
        except URLError as exc:
            raise RuntimeError(str(getattr(exc, "reason", exc))) from exc

        try:
            decoded = json.loads(raw)
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError("telegram response is not valid json") from exc
        if not isinstance(decoded, dict):
            raise RuntimeError("telegram response is not an object")
        if not bool(decoded.get("ok")):
            raise RuntimeError(str(decoded.get("description") or decoded))
        return decoded.get("result")


def build_telegram_bot_token_sender(
    bot_token: str,
    *,
    timeout_seconds: float = 10.0,
    auto_chat_cache_ttl_seconds: float = 300.0,
) -> Callable[[str, str], None] | None:
    normalized = str(bot_token or "").strip()
    if normalized == "":
        return None
    sender = TelegramBotTokenSender(
        normalized,
        timeout_seconds=timeout_seconds,
        auto_chat_cache_ttl_seconds=auto_chat_cache_ttl_seconds,
    )

    def send_func(target: str, message: str) -> None:
        sender.send(target, message)

    setattr(send_func, _AUTO_TARGET_ATTR, True)
    return send_func


def supports_auto_telegram_target(send_func: Any) -> bool:
    return bool(getattr(send_func, _AUTO_TARGET_ATTR, False))


class TelegramNotifier(BaseNotifier):
    def __init__(
        self,
        send_func: SendFunc,
        *,
        max_message_chars: int = 4096,
        message_mode_getter: Callable[[], str] | None = None,
    ) -> None:
        super().__init__(name="telegram", max_message_chars=max_message_chars)
        self._send_func = send_func
        self._message_mode_getter = message_mode_getter or (
            lambda: normalize_message_mode(None)
        )

    async def send(self, host: str, message: str, category: str) -> None:
        mode = normalize_message_mode(self._message_mode_getter())
        formatted = format_message_for_mode(
            message,
            mode=mode,
            host=host,
            category=category,
        )
        for chunk in split_message(formatted, self.max_message_chars):
            await self._send_func(host, chunk)


class TelegramBotRuntime:
    def __init__(
        self,
        *,
        application: Any,
        chat_runtime: Any,
        authorized_user_ids: set[str],
        message_mode_setter: Callable[[str], str] | None = None,
        message_mode_getter: Callable[[], str] | None = None,
    ) -> None:
        self._application = application
        self._chat_runtime = chat_runtime
        self._authorized_user_ids = {str(item).strip() for item in authorized_user_ids}
        self._message_mode_setter = message_mode_setter
        self._message_mode_getter = message_mode_getter
        self._started = False

    async def start(self) -> None:
        if self._started:
            return
        updater = getattr(self._application, "updater", None)
        initialized = False
        app_started = False
        polling_started = False
        try:
            await self._application.initialize()
            initialized = True
            await self._application.start()
            app_started = True
            if updater is not None and hasattr(updater, "start_polling"):
                await updater.start_polling()
                polling_started = True
            self._started = True
        except Exception:
            if polling_started and updater is not None and hasattr(updater, "stop"):
                try:
                    await updater.stop()
                except Exception:  # noqa: BLE001
                    logger.warning(
                        "telegram updater stop failed during startup rollback",
                        exc_info=True,
                    )
            if app_started:
                try:
                    await self._application.stop()
                except Exception:  # noqa: BLE001
                    logger.warning(
                        "telegram application stop failed during startup rollback",
                        exc_info=True,
                    )
            if initialized:
                try:
                    await self._application.shutdown()
                except Exception:  # noqa: BLE001
                    logger.warning(
                        "telegram application shutdown failed during startup rollback",
                        exc_info=True,
                    )
            raise

    async def shutdown(self) -> None:
        if not self._started:
            return
        updater = getattr(self._application, "updater", None)
        if updater is not None and hasattr(updater, "stop"):
            await updater.stop()
        await self._application.stop()
        await self._application.shutdown()
        self._started = False

    async def handle_text_message(
        self,
        *,
        user_id: str,
        chat_id: str,
        text: str,
        reply_text: Callable[[str], Awaitable[None] | None],
    ) -> bool:
        normalized_user_id = str(user_id).strip()
        if normalized_user_id not in self._authorized_user_ids:
            return False

        if await self._handle_message_mode_command(text=text, reply_text=reply_text):
            return True

        try:
            reply = self._chat_runtime.invoke_text(
                text,
                user_id=normalized_user_id,
                session_key=f"telegram:{chat_id}",
                fallback=DEFAULT_CHAT_FALLBACK_MESSAGE,
            )
        except Exception:  # noqa: BLE001
            logger.warning("telegram chat runtime invoke failed", exc_info=True)
            reply = DEFAULT_CHAT_FALLBACK_MESSAGE

        result = reply_text(reply)
        if inspect.isawaitable(result):
            await result
        return True

    async def _handle_message_mode_command(
        self,
        *,
        text: str,
        reply_text: Callable[[str], Awaitable[None] | None],
    ) -> bool:
        normalized_text = str(text or "").strip()
        if not normalized_text.startswith("/msg"):
            return False

        parts = normalized_text.split(maxsplit=1)
        if len(parts) == 1:
            current_mode = self._resolve_current_message_mode()
            await _maybe_await_reply(
                reply_text,
                f"usage: /msg <text|txt|md|doc> (current: {current_mode})",
            )
            return True

        if self._message_mode_setter is None:
            await _maybe_await_reply(
                reply_text,
                "message mode switch is not enabled",
            )
            return True

        requested_mode = parts[1].strip()
        try:
            updated_mode = normalize_message_mode(self._message_mode_setter(requested_mode))
        except Exception:
            await _maybe_await_reply(
                reply_text,
                "unsupported mode, use one of: text|txt|md|doc",
            )
            return True

        await _maybe_await_reply(
            reply_text,
            f"message mode updated: {updated_mode}",
        )
        return True

    def _resolve_current_message_mode(self) -> str:
        if self._message_mode_getter is None:
            return normalize_message_mode(None)
        try:
            return normalize_message_mode(self._message_mode_getter())
        except Exception:
            return normalize_message_mode(None)


def build_telegram_bot_runtime(
    *,
    bot_token: str,
    chat_runtime: Any,
    authorized_user_ids: set[str],
    message_mode_setter: Callable[[str], str] | None = None,
    message_mode_getter: Callable[[], str] | None = None,
    application_factory: Callable[[str], Any] | None = None,
    handler_binder: Callable[[Any, Any], None] | None = None,
) -> TelegramBotRuntime | None:
    normalized_token = str(bot_token or "").strip()
    if normalized_token == "":
        return None

    build_application = application_factory or _load_application_factory()
    bind_handler = handler_binder or _load_handler_binder()
    if build_application is None or bind_handler is None:
        return None

    application = build_application(normalized_token)
    runtime = TelegramBotRuntime(
        application=application,
        chat_runtime=chat_runtime,
        authorized_user_ids=authorized_user_ids,
        message_mode_setter=message_mode_setter,
        message_mode_getter=message_mode_getter,
    )

    async def on_text(update: Any, _context: Any) -> None:
        message = getattr(update, "effective_message", None)
        user = getattr(update, "effective_user", None)
        chat = getattr(update, "effective_chat", None)
        text = getattr(message, "text", None)
        if message is None or user is None or chat is None or not isinstance(text, str):
            return
        await runtime.handle_text_message(
            user_id=str(getattr(user, "id", "")),
            chat_id=str(getattr(chat, "id", "")),
            text=text,
            reply_text=message.reply_text,
        )

    bind_handler(application, on_text)
    return runtime


def _load_application_factory() -> Callable[[str], Any] | None:
    try:
        ext = importlib.import_module("telegram.ext")
    except Exception:  # noqa: BLE001
        logger.warning("python-telegram-bot is not available", exc_info=True)
        return None

    builder_cls = getattr(ext, "ApplicationBuilder", None)
    if builder_cls is None:
        return None

    def factory(token: str) -> Any:
        return builder_cls().token(token).build()

    return factory


def _load_handler_binder() -> Callable[[Any, Any], None] | None:
    try:
        ext = importlib.import_module("telegram.ext")
    except Exception:  # noqa: BLE001
        logger.warning("python-telegram-bot is not available", exc_info=True)
        return None

    message_handler_cls = getattr(ext, "MessageHandler", None)
    filters_obj = getattr(ext, "filters", None)
    if message_handler_cls is None or filters_obj is None:
        return None

    def binder(application: Any, callback: Any) -> None:
        application.add_handler(message_handler_cls(filters_obj.TEXT, callback))

    return binder


async def _maybe_await_reply(
    reply_text: Callable[[str], Awaitable[None] | None],
    text: str,
) -> None:
    result = reply_text(text)
    if inspect.isawaitable(result):
        await result
