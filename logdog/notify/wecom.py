from __future__ import annotations

import json
import time
from collections.abc import Awaitable, Callable
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from logdog.notify.base import (
    BaseNotifier,
    format_message_for_mode,
    normalize_message_mode,
    split_message,
)


SendFunc = Callable[[str, str], Awaitable[None]]
WECOM_MARKDOWN_MAX_BYTES = 4096


class WecomNotifier(BaseNotifier):
    def __init__(
        self,
        send_func: SendFunc,
        *,
        max_message_chars: int = 4096,
        message_mode_getter: Callable[[], str] | None = None,
    ) -> None:
        super().__init__(name="wecom", max_message_chars=max_message_chars)
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


class WecomWebhookSender:
    """Minimal enterprise wecom webhook sender."""

    def __init__(
        self,
        *,
        timeout_seconds: float = 10.0,
        max_retries: int = 3,
    ) -> None:
        if float(timeout_seconds) <= 0:
            raise ValueError("timeout_seconds must be > 0")
        if int(max_retries) <= 0:
            raise ValueError("max_retries must be > 0")
        self._timeout_seconds = float(timeout_seconds)
        self._max_retries = int(max_retries)

    def send(self, webhook_url: str, message: str) -> None:
        normalized_url = str(webhook_url or "").strip()
        if normalized_url == "":
            raise ValueError("wecom webhook url must not be empty")

        payload = {
            "msgtype": "markdown",
            "markdown": {
                "content": self._truncate_utf8(
                    str(message or "").strip() or "(empty)",
                    WECOM_MARKDOWN_MAX_BYTES,
                )
            },
        }

        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        last_error: Exception | None = None
        for attempt in range(self._max_retries):
            req = Request(
                normalized_url,
                data=body,
                method="POST",
                headers={"Content-Type": "application/json; charset=utf-8"},
            )
            try:
                with urlopen(req, timeout=self._timeout_seconds) as resp:
                    raw = resp.read().decode("utf-8", errors="replace")
                data = json.loads(raw)
                if isinstance(data, dict) and int(data.get("errcode", -1)) == 0:
                    return
                raise RuntimeError(str(data))
            except (HTTPError, URLError, RuntimeError) as exc:
                last_error = exc
                if attempt < self._max_retries - 1:
                    time.sleep(float(2**attempt))
                    continue
                break
        raise RuntimeError(f"wecom webhook send failed: {last_error}")

    @staticmethod
    def _truncate_utf8(value: str, max_bytes: int) -> str:
        text = str(value or "")
        encoded = text.encode("utf-8")
        if len(encoded) <= int(max_bytes):
            return text
        out: list[str] = []
        size = 0
        for ch in text:
            ch_bytes = ch.encode("utf-8")
            if size + len(ch_bytes) > int(max_bytes):
                break
            out.append(ch)
            size += len(ch_bytes)
        return "".join(out) + "\n…[truncated]"


def build_wecom_webhook_sender(
    *,
    timeout_seconds: float = 10.0,
    max_retries: int = 3,
) -> Callable[[str, str], None]:
    sender = WecomWebhookSender(
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
    )

    def send_func(target: str, message: str) -> None:
        sender.send(target, message)

    return send_func
