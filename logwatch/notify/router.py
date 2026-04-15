from __future__ import annotations

import asyncio
import inspect
import logging
import re
from collections.abc import Awaitable, Callable, Mapping, Sequence
from datetime import datetime, timezone
from typing import Any

from logwatch.notify.base import BaseNotifier


FailureRecorder = Callable[[dict[str, Any]], Awaitable[None] | None]
RouteSelector = Callable[
    [str, str, dict[str, Any] | None], Sequence[BaseNotifier] | None
]
_REDACTED = "***REDACTED***"
_SENSITIVE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"(?i)bearer\s+\S+"),
    re.compile(r"(?i)\bapi[_-]?key\b\s*[:=]\s*\S+"),
    re.compile(r"(?i)\btoken\b\s*[:=]\s*\S+"),
    re.compile(r"(?i)\bpassword\b\s*[:=]\s*\S+"),
    re.compile(r'(?i)"(?:api[_-]?key|token|password)"\s*:\s*"[^"]*"'),
    re.compile(r"(?i)'(?:api[_-]?key|token|password)'\s*:\s*'[^']*'"),
)
logger = logging.getLogger(__name__)


def _sanitize_failure_message(message: str, max_chars: int) -> str:
    out = str(message or "")
    for pattern in _SENSITIVE_PATTERNS:
        out = pattern.sub(_REDACTED, out)
    if max_chars <= 0:
        return ""
    if len(out) > max_chars:
        return out[:max_chars]
    return out


class NotifyRouter:
    def __init__(
        self,
        notifiers: Sequence[BaseNotifier],
        *,
        host_notifiers: Mapping[str, Sequence[BaseNotifier]] | None = None,
        max_retries: int = 1,
        retry_backoff_seconds: float = 0.1,
        failure_message_max_chars: int = 512,
        failure_recorder: FailureRecorder | None = None,
        route_selector: RouteSelector | None = None,
    ) -> None:
        if max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        if retry_backoff_seconds < 0:
            raise ValueError("retry_backoff_seconds must be >= 0")
        if failure_message_max_chars <= 0:
            raise ValueError("failure_message_max_chars must be > 0")

        self._notifiers = list(notifiers)
        self._host_notifiers = {
            str(host): list(items)
            for host, items in (host_notifiers or {}).items()
            if str(host).strip() != ""
        }
        self._max_retries = int(max_retries)
        self._retry_backoff_seconds = float(retry_backoff_seconds)
        self._failure_message_max_chars = int(failure_message_max_chars)
        self._failure_recorder = failure_recorder
        self._route_selector = route_selector

    async def send(
        self,
        host: str,
        message: str,
        category: str,
        context: dict[str, Any] | None = None,
    ) -> bool:
        selected_notifiers: list[BaseNotifier] | None = None
        if self._route_selector is not None:
            selected = self._route_selector(host, category, dict(context or {}))
            if selected is not None:
                selected_notifiers = list(selected)
        if selected_notifiers is None:
            selected_notifiers = list(self._host_notifiers.get(host, self._notifiers))
        if not selected_notifiers:
            return False

        results = await asyncio.gather(
            *[
                self._send_with_retry(
                    notifier, host=host, message=message, category=category
                )
                for notifier in selected_notifiers
            ],
            return_exceptions=False,
        )
        return any(results)

    async def _send_with_retry(
        self,
        notifier: BaseNotifier,
        *,
        host: str,
        message: str,
        category: str,
    ) -> bool:
        last_exc: Exception | None = None
        for attempt in range(self._max_retries + 1):
            try:
                await notifier.send(host, message, category)
                return True
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                if attempt < self._max_retries:
                    if self._retry_backoff_seconds > 0:
                        await asyncio.sleep(self._retry_backoff_seconds * (2**attempt))
                    continue

        await self._record_failure(
            host=host,
            channel=notifier.name,
            category=category,
            message=message,
            error=str(last_exc) if last_exc else "unknown error",
        )
        return False

    async def _record_failure(
        self,
        *,
        host: str,
        channel: str,
        category: str,
        message: str,
        error: str,
    ) -> None:
        if self._failure_recorder is None:
            return

        payload = {
            "host": host,
            "channel": channel,
            "category": category,
            "message": _sanitize_failure_message(
                message, self._failure_message_max_chars
            ),
            "error": _sanitize_failure_message(error, self._failure_message_max_chars),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

        try:
            maybe_awaitable = self._failure_recorder(payload)
            if inspect.isawaitable(maybe_awaitable):
                await maybe_awaitable
        except Exception:  # noqa: BLE001
            # Recording failure is best-effort and must not break the main send path.
            logger.exception("notify failure recorder failed")
            return
