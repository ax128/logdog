from __future__ import annotations

import json
import importlib
import inspect
import logging
import mimetypes
import ssl
import time
import uuid
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from logdog.llm.agent_runtime import DEFAULT_CHAT_FALLBACK_MESSAGE
from logdog.notify.base import (
    BaseNotifier,
    format_message_for_mode,
    normalize_message_mode,
    split_message,
)


SendFunc = Callable[..., Awaitable[None] | None]
logger = logging.getLogger(__name__)
TELEGRAM_AUTO_TARGET = "__auto__"
TELEGRAM_BOT_COMMANDS = [
    {"command": "msg", "description": "Switch message mode (txt|md|doc)"},
    {"command": "help", "description": "Show available commands"},
    {"command": "status", "description": "Show agent status"},
]
_AUTO_TARGET_ATTR = "_logdog_supports_auto_target"
_STREAM_EDIT_MIN_INTERVAL_SEC = 0.35


def telegram_markdown_retryable(error: BaseException | None) -> bool:
    """Return True if the error is a Telegram markdown parse failure that can be retried as plain text."""
    text = str(error or "").lower()
    return "can't parse entities" in text or "can't find end of the entity" in text


def telegram_markdown_unsafe(text: str) -> bool:
    """Detect text that will likely fail Telegram legacy Markdown parsing.
    Rule: outside code spans, an unescaped '_' with alphanumeric neighbors on both sides is unsafe.
    """
    value = str(text or "")
    if "_" not in value:
        return False
    in_code = False
    for idx, ch in enumerate(value):
        prev = value[idx - 1] if idx > 0 else ""
        escaped = prev == "\\"
        if ch == "`" and not escaped:
            in_code = not in_code
            continue
        if in_code:
            continue
        if ch != "_" or escaped:
            continue
        left = value[idx - 1] if idx > 0 else ""
        right = value[idx + 1] if idx + 1 < len(value) else ""
        if left.isalnum() and right.isalnum():
            return True
    return False


class TelegramBotTokenSender:
    """Minimal Telegram Bot API sender for notify path."""

    _DEFAULT_MAX_DOWNLOAD_BYTES = 50 * 1024 * 1024
    _DOWNLOAD_CHUNK_BYTES = 64 * 1024

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
        self._pinned_chat_id: str | None = None  # set externally from bot runtime

    def pin_chat_id(self, chat_id: str) -> None:
        """Inject a known chat_id discovered by the bot runtime (avoids getUpdates conflict)."""
        if str(chat_id or "").strip():
            self._pinned_chat_id = str(chat_id).strip()

    def send(self, target: str, message: str, parse_mode: str = "") -> None:
        chat_id = str(target or "").strip()
        if chat_id == "" or chat_id == TELEGRAM_AUTO_TARGET:
            if self._pinned_chat_id:
                # Use chat_id pinned by bot runtime (avoids getUpdates/polling conflict).
                chat_id = self._pinned_chat_id
            else:
                # No pinned chat_id yet — skip rather than call getUpdates which
                # would conflict with start_polling on the same token.
                logger.debug(
                    "telegram send skipped: no chat_id pinned yet "
                    "(send a message to the bot to activate push notifications)"
                )
                return
        self.send_message(chat_id, str(message), parse_mode=parse_mode)

    def send_message(
        self, chat_id: str, text: str, parse_mode: str = ""
    ) -> Any:
        effective_parse_mode = str(parse_mode or "").strip()
        truncated = str(text)[:4096]
        if effective_parse_mode == "Markdown" and telegram_markdown_unsafe(truncated):
            effective_parse_mode = ""
        payload: dict[str, Any] = {"chat_id": chat_id, "text": truncated}
        if effective_parse_mode:
            payload["parse_mode"] = effective_parse_mode
        try:
            return self._request("sendMessage", payload)
        except Exception as exc:
            if effective_parse_mode == "Markdown" and telegram_markdown_retryable(exc):
                payload.pop("parse_mode", None)
                return self._request("sendMessage", payload)
            raise

    def edit_message_text(
        self, chat_id: str, message_id: int, text: str, parse_mode: str = ""
    ) -> Any:
        effective_parse_mode = str(parse_mode or "").strip()
        truncated = str(text)[:4096]
        if effective_parse_mode == "Markdown" and telegram_markdown_unsafe(truncated):
            effective_parse_mode = ""
        payload: dict[str, Any] = {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": truncated,
        }
        if effective_parse_mode:
            payload["parse_mode"] = effective_parse_mode
        try:
            return self._request("editMessageText", payload)
        except Exception as exc:
            if effective_parse_mode == "Markdown" and telegram_markdown_retryable(exc):
                payload.pop("parse_mode", None)
                return self._request("editMessageText", payload)
            raise

    def send_chat_action(self, chat_id: str, action: str = "typing") -> Any:
        return self._request("sendChatAction", {"chat_id": chat_id, "action": action})

    def set_my_commands(self, commands: list[dict[str, str]]) -> Any:
        return self._request("setMyCommands", {"commands": commands})

    def delete_webhook(self) -> Any:
        return self._request("deleteWebhook", {})

    def send_document(
        self, chat_id: str, file_path: str | Path, caption: str = ""
    ) -> Any:
        truncated_caption = str(caption or "")[:1024]
        payload: dict[str, str] = {"chat_id": str(chat_id)}
        if truncated_caption:
            payload["caption"] = truncated_caption
        return self._request_multipart(
            "sendDocument", payload, files={"document": str(file_path)}
        )

    def download_file(
        self,
        file_id: str,
        target_path: str | Path,
        *,
        max_bytes: int = 0,
    ) -> Path:
        effective_max = max_bytes if max_bytes > 0 else self._DEFAULT_MAX_DOWNLOAD_BYTES
        result = self._request("getFile", {"file_id": file_id})
        if not isinstance(result, dict) or "file_path" not in result:
            raise RuntimeError("telegram getFile did not return file_path")
        remote_path = result["file_path"]
        url = f"https://api.telegram.org/file/bot{self._bot_token}/{remote_path}"
        dest = Path(target_path)
        req = Request(url, method="GET")
        downloaded = 0
        try:
            with urlopen(
                req,
                timeout=self._timeout_seconds,
                context=ssl.create_default_context(),
            ) as resp:
                with dest.open("wb") as fh:
                    while True:
                        chunk = resp.read(self._DOWNLOAD_CHUNK_BYTES)
                        if not chunk:
                            break
                        downloaded += len(chunk)
                        if downloaded > effective_max:
                            raise RuntimeError(
                                f"download exceeds max size {effective_max} bytes"
                            )
                        fh.write(chunk)
        except Exception:
            if dest.exists():
                dest.unlink(missing_ok=True)
            raise
        return dest

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

    def _request_multipart(
        self,
        method: str,
        payload: dict[str, str],
        *,
        files: dict[str, str],
    ) -> Any:
        boundary = uuid.uuid4().hex
        body = b""
        for key, value in payload.items():
            body += f"--{boundary}\r\n".encode()
            body += f'Content-Disposition: form-data; name="{key}"\r\n\r\n'.encode()
            body += f"{value}\r\n".encode()
        for field_name, file_path_str in files.items():
            p = Path(file_path_str)
            mime_type = mimetypes.guess_type(p.name)[0] or "application/octet-stream"
            body += f"--{boundary}\r\n".encode()
            body += (
                f'Content-Disposition: form-data; name="{field_name}"; '
                f'filename="{p.name}"\r\n'
            ).encode()
            body += f"Content-Type: {mime_type}\r\n\r\n".encode()
            body += p.read_bytes()
            body += b"\r\n"
        body += f"--{boundary}--\r\n".encode()

        base_url = f"https://api.telegram.org/bot{self._bot_token}/{method}"
        headers = {"Content-Type": f"multipart/form-data; boundary={boundary}"}
        req = Request(base_url, data=body, method="POST", headers=headers)

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


class TelegramStreamSender:
    """Streams long-running text to a single Telegram message, editing in place."""

    def __init__(self, bot: TelegramBotTokenSender, chat_id: str) -> None:
        self._bot = bot
        self._chat_id = str(chat_id or "").strip()
        self._message_id: int | None = None
        self._last_edit = 0.0

    def update(self, full_text: str) -> None:
        if not self._chat_id:
            return
        truncated = str(full_text)[:4096]
        if self._message_id is None:
            result = self._bot.send_message(self._chat_id, truncated, parse_mode="")
            if isinstance(result, dict):
                self._message_id = result.get("message_id")
            self._last_edit = time.monotonic()
            return
        now = time.monotonic()
        if (now - self._last_edit) < _STREAM_EDIT_MIN_INTERVAL_SEC:
            return
        try:
            self._bot.edit_message_text(
                self._chat_id, self._message_id, truncated, parse_mode=""
            )
        except Exception:  # noqa: BLE001
            logger.warning("telegram stream edit failed", exc_info=True)
        self._last_edit = time.monotonic()

    def finalize(self, full_text: str, *, fmt: str = "plain") -> None:
        if not self._chat_id:
            return
        truncated = str(full_text)[:4096]
        parse_mode = "Markdown" if fmt == "markdown" else ""

        if self._message_id is None:
            try:
                self._bot.send_message(self._chat_id, truncated, parse_mode=parse_mode)
            except Exception as exc:
                if parse_mode == "Markdown" and telegram_markdown_retryable(exc):
                    try:
                        self._bot.send_message(self._chat_id, truncated, parse_mode="")
                    except Exception:  # noqa: BLE001
                        logger.warning("telegram stream finalize fallback failed", exc_info=True)
                else:
                    logger.warning("telegram stream finalize send failed", exc_info=True)
            return

        try:
            self._bot.edit_message_text(
                self._chat_id, self._message_id, truncated, parse_mode=parse_mode
            )
        except Exception as exc:
            if parse_mode == "Markdown" and telegram_markdown_retryable(exc):
                try:
                    self._bot.edit_message_text(
                        self._chat_id, self._message_id, truncated, parse_mode=""
                    )
                except Exception:  # noqa: BLE001
                    logger.warning("telegram stream finalize edit fallback failed", exc_info=True)
            else:
                logger.warning("telegram stream finalize edit failed", exc_info=True)


def build_telegram_bot_token_sender(
    bot_token: str,
    *,
    timeout_seconds: float = 10.0,
    auto_chat_cache_ttl_seconds: float = 300.0,
) -> Callable[..., None] | None:
    normalized = str(bot_token or "").strip()
    if normalized == "":
        return None
    sender = TelegramBotTokenSender(
        normalized,
        timeout_seconds=timeout_seconds,
        auto_chat_cache_ttl_seconds=auto_chat_cache_ttl_seconds,
    )

    def send_func(target: str, message: str, parse_mode: str = "") -> None:
        sender.send(target, message, parse_mode=parse_mode)

    setattr(send_func, _AUTO_TARGET_ATTR, True)
    setattr(send_func, "pin_chat_id", sender.pin_chat_id)
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
        parse_mode = "Markdown" if mode in ("md", "doc") else ""
        for chunk in split_message(formatted, self.max_message_chars):
            result = self._send_func(host, chunk, parse_mode)
            if inspect.isawaitable(result):
                await result


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
        self._sender: Any | None = None
        self._started = False
        self._auto_authorize = len(self._authorized_user_ids) == 0
        self._on_new_authorized_user: Callable[[str], None] | None = None
        self._last_chat_id: str | None = None
        self._on_chat_id_seen: Callable[[str], None] | None = None

    def set_chat_id_seen_callback(self, callback: Callable[[str], None]) -> None:
        """Register a callback invoked whenever a new chat_id is observed."""
        self._on_chat_id_seen = callback

    def set_authorized_user_persist_callback(self, callback: Callable[[str], None]) -> None:
        """Register a callback invoked when a new user is auto-authorized."""
        self._on_new_authorized_user = callback

    def set_sender(self, sender: Any) -> None:
        """Attach a TelegramBotTokenSender for typing indicators and file operations."""
        self._sender = sender

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
                await updater.start_polling(drop_pending_updates=True)
                polling_started = True
            self._started = True
            self._register_bot_commands()
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
            if self._auto_authorize and normalized_user_id:
                # python-telegram-bot dispatches updates serially on a single asyncio
                # event loop, so this check-then-set is safe in the standard deployment.
                # Multi-process deployments would need an external lock here.
                self._authorized_user_ids.add(normalized_user_id)
                self._auto_authorize = False
                logger.info("auto-authorized first Telegram user: %s", normalized_user_id)
                if self._on_new_authorized_user is not None:
                    try:
                        self._on_new_authorized_user(normalized_user_id)
                    except Exception:  # noqa: BLE001
                        logger.warning(
                            "failed to persist authorized user %s — user is authorized "
                            "this session but will need to message again after restart",
                            normalized_user_id,
                            exc_info=True,
                        )
            else:
                return False

        # Track the most recent chat_id so notify sender can use it without
        # calling getUpdates (which conflicts with start_polling).
        new_chat_id = str(chat_id)
        if new_chat_id != self._last_chat_id:
            self._last_chat_id = new_chat_id
            if self._on_chat_id_seen is not None:
                try:
                    self._on_chat_id_seen(new_chat_id)
                except Exception:  # noqa: BLE001
                    logger.warning("on_chat_id_seen callback failed", exc_info=True)

        normalized_text = str(text or "").strip()

        # Route slash commands before touching the agent.
        if normalized_text.startswith("/"):
            return await self._dispatch_command(
                text=normalized_text,
                user_id=normalized_user_id,
                chat_id=chat_id,
                reply_text=reply_text,
            )

        # Typing indicator while the agent is thinking.
        if self._sender is not None:
            try:
                self._sender.send_chat_action(chat_id)
            except Exception:  # noqa: BLE001
                logger.debug("send_chat_action failed", exc_info=True)

        await self._invoke_and_reply(
            text=normalized_text or text,
            user_id=normalized_user_id,
            chat_id=chat_id,
            reply_text=reply_text,
        )
        return True

    async def _dispatch_command(
        self,
        *,
        text: str,
        user_id: str,
        chat_id: str,
        reply_text: Callable[[str], Awaitable[None] | None],
    ) -> bool:
        """Route a slash command to the appropriate handler."""
        cmd_raw = text.split(maxsplit=1)[0].lower()
        # Strip optional @BotName suffix (e.g. /help@MyBot).
        cmd = cmd_raw.split("@", 1)[0]

        if cmd == "/msg":
            return await self._handle_message_mode_command(text=text, reply_text=reply_text)

        if cmd == "/help":
            lines = ["Available commands:"]
            for entry in TELEGRAM_BOT_COMMANDS:
                lines.append(f"  /{entry['command']} — {entry['description']}")
            lines.append("\nSend any message to chat with the agent.")
            await _maybe_await_reply(reply_text, "\n".join(lines))
            return True

        if cmd == "/status":
            available = bool(getattr(self._chat_runtime, "is_available", False))
            status = "✅ Agent ready" if available else "⚠️ Agent unavailable"
            await _maybe_await_reply(reply_text, status)
            return True

        # Unknown command — pass through to the agent as plain text.
        if self._sender is not None:
            try:
                self._sender.send_chat_action(chat_id)
            except Exception:  # noqa: BLE001
                logger.debug("send_chat_action failed", exc_info=True)

        await self._invoke_and_reply(
            text=text,
            user_id=user_id,
            chat_id=chat_id,
            reply_text=reply_text,
        )
        return True

    async def _invoke_and_reply(
        self,
        *,
        text: str,
        user_id: str,
        chat_id: str,
        reply_text: Callable[[str], Awaitable[None] | None],
    ) -> None:
        """Invoke the agent and deliver the response.

        Uses streaming + TelegramStreamSender when the sender supports it
        (i.e. has send_message), which gives the user incremental feedback as
        the model produces tokens.  Falls back to a single reply_text call when
        the sender is absent or does not have send_message (e.g. in tests).
        """
        use_streaming = (
            self._sender is not None
            and hasattr(self._sender, "send_message")
            and hasattr(self._chat_runtime, "ainvoke_text_streamed")
        )

        if use_streaming:
            mode = self._resolve_current_message_mode()
            fmt = "markdown" if mode in ("md", "doc") else "plain"
            stream_sender = TelegramStreamSender(self._sender, chat_id)

            try:
                result = await self._chat_runtime.ainvoke_text_streamed(
                    text,
                    user_id=user_id,
                    session_key=f"telegram:{chat_id}",
                    fallback=DEFAULT_CHAT_FALLBACK_MESSAGE,
                    on_chunk=stream_sender.update,
                )
            except Exception:  # noqa: BLE001
                logger.warning("telegram streaming invoke failed", exc_info=True)
                result = DEFAULT_CHAT_FALLBACK_MESSAGE

            # Finalize first 4096-char chunk via stream sender; send overflow as
            # separate messages so nothing is silently truncated.
            chunks = split_message(result, max_chars=4096)
            stream_sender.finalize(chunks[0], fmt=fmt)
            parse_mode = "Markdown" if fmt == "markdown" else ""
            for extra in chunks[1:]:
                try:
                    self._sender.send_message(chat_id, extra, parse_mode=parse_mode)
                except Exception:  # noqa: BLE001
                    logger.warning("telegram overflow chunk send failed", exc_info=True)
            return

        # Non-streaming path.
        try:
            reply = await self._chat_runtime.ainvoke_text(
                text,
                user_id=user_id,
                session_key=f"telegram:{chat_id}",
                fallback=DEFAULT_CHAT_FALLBACK_MESSAGE,
            )
        except Exception:  # noqa: BLE001
            logger.warning("telegram chat runtime invoke failed", exc_info=True)
            reply = DEFAULT_CHAT_FALLBACK_MESSAGE

        result_r = reply_text(reply)
        if inspect.isawaitable(result_r):
            await result_r

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
                f"usage: /msg <txt|md|doc> (current: {current_mode})",
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
                "unsupported mode, use one of: txt|md|doc",
            )
            return True

        await _maybe_await_reply(
            reply_text,
            f"message mode updated: {updated_mode}",
        )
        return True

    def _register_bot_commands(self) -> None:
        if self._sender is None:
            return
        try:
            self._sender.set_my_commands(TELEGRAM_BOT_COMMANDS)
        except Exception:
            logger.warning("telegram bot command registration failed", exc_info=True)

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
        if message is None or user is None or chat is None:
            return

        text = getattr(message, "text", None) or getattr(message, "caption", None) or ""
        if not isinstance(text, str):
            text = ""

        document = getattr(message, "document", None)
        photo = getattr(message, "photo", None)

        # When the user sends a file or photo without a caption, synthesise a
        # minimal context hint so the agent receives something meaningful.
        if not text:
            if document is not None:
                file_name = getattr(document, "file_name", None) or "file"
                text = f"[File: {file_name}]"
            elif photo is not None:
                text = "[Image]"
            else:
                return  # nothing to handle

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

    document_filter = getattr(getattr(filters_obj, "Document", None), "ALL", None)
    text_filter = filters_obj.TEXT
    combined_filter = (text_filter | document_filter) if document_filter is not None else text_filter

    def binder(application: Any, callback: Any) -> None:
        application.add_handler(message_handler_cls(combined_filter, callback))

    return binder


async def _maybe_await_reply(
    reply_text: Callable[[str], Awaitable[None] | None],
    text: str,
) -> None:
    result = reply_text(text)
    if inspect.isawaitable(result):
        await result
