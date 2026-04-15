from __future__ import annotations

from collections.abc import Awaitable, Callable

from logdog.notify.base import (
    BaseNotifier,
    format_message_for_mode,
    normalize_message_mode,
    split_message,
)


SendFunc = Callable[[str, str], Awaitable[None]]


class WeixinNotifier(BaseNotifier):
    def __init__(
        self,
        send_func: SendFunc,
        *,
        max_message_chars: int = 4096,
        message_mode_getter: Callable[[], str] | None = None,
    ) -> None:
        super().__init__(name="weixin", max_message_chars=max_message_chars)
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
