from __future__ import annotations

import asyncio
import inspect
import logging
from typing import Any

from logdog.pipeline.cooldown import CooldownStore


_ALERT_MESSAGES = {
    "restart": "ERROR docker event: container {container_name} restarted",
    "die": "ERROR docker event: container {container_name} exited unexpectedly",
    "oom": "OOM docker event: container {container_name} exceeded memory limits",
}
_REFRESH_ACTIONS = {"start", "restart", "die", "destroy", "stop"}
_DEFAULT_RECONNECT_BACKOFF_SECONDS = 1.0
_MAX_RECONNECT_BACKOFF_SECONDS = 10.0
logger = logging.getLogger(__name__)


class EventStreamWatcher:
    def __init__(
        self,
        *,
        host: dict[str, Any],
        stream_events: Any,
        run_alert: Any,
        refresh_host: Any | None = None,
        notifier_send: Any | None = None,
        save_alert: Any | None = None,
        save_storm_event: Any | None = None,
        mute_checker: Any | None = None,
        prompt_template: str = "default_alert",
        output_template: str = "standard",
        storm_controller: Any | None = None,
        config: dict[str, Any] | None = None,
        cooldown_store: CooldownStore | None = None,
        reconnect_backoff_seconds: float = _DEFAULT_RECONNECT_BACKOFF_SECONDS,
    ) -> None:
        self._host = dict(host)
        self._stream_events = stream_events
        self._run_alert = run_alert
        self._refresh_host = refresh_host
        self._notifier_send = notifier_send
        self._save_alert = save_alert
        self._save_storm_event = save_storm_event
        self._mute_checker = mute_checker
        self._prompt_template = str(
            prompt_template or self._host.get("prompt_template") or "default_alert"
        )
        self._output_template = str(
            output_template or self._host.get("output_template") or "standard"
        )
        self._storm_controller = storm_controller
        self._config = dict(config) if config is not None else dict(self._host)
        self._cooldown_store = cooldown_store
        self._task: asyncio.Task[None] | None = None
        self._reconnect_backoff_seconds = max(
            0.0,
            float(reconnect_backoff_seconds),
        )

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._task = asyncio.create_task(self.watch_forever())

    async def shutdown(self) -> None:
        task = self._task
        self._task = None
        if task is None:
            return
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    async def watch_forever(self) -> None:
        host_name = str(self._host.get("name") or "default")
        managed_mode = (
            self._task is not None and asyncio.current_task() is self._task
        )
        while True:
            try:
                async for event in self._stream_events(
                    self._host, filters={"type": "container"}
                ):
                    try:
                        action = str(event.get("action") or "").strip().lower()
                        container_id = str(event.get("container_id") or "").strip()
                        container_name = str(
                            event.get("container_name") or container_id or "container"
                        )
                        timestamp = str(event.get("time") or "")

                        if (
                            action in _REFRESH_ACTIONS
                            and self._refresh_host is not None
                        ):
                            await _maybe_await(self._refresh_host(host_name))

                        message = _ALERT_MESSAGES.get(action)
                        if message is None:
                            continue
                        alert_kwargs = {
                            "host": host_name,
                            "container_id": container_id,
                            "container_name": container_name,
                            "timestamp": timestamp,
                            "notifier_send": self._notifier_send,
                            "save_alert": self._save_alert,
                            "save_storm_event": self._save_storm_event,
                            "mute_checker": self._mute_checker,
                            "prompt_template": self._prompt_template,
                            "output_template": self._output_template,
                            "config": self._config,
                        }
                        if self._storm_controller is not None:
                            alert_kwargs["storm_controller"] = self._storm_controller
                        if self._cooldown_store is not None:
                            alert_kwargs["cooldown_store"] = self._cooldown_store
                        await _maybe_await(
                            self._run_alert(
                                message.format(container_name=container_name),
                                **alert_kwargs,
                            )
                        )
                    except asyncio.CancelledError:
                        raise
                    except Exception:  # noqa: BLE001
                        logger.exception(
                            "event stream handler failed host=%s",
                            host_name,
                        )
                if not managed_mode:
                    return
                delay = min(
                    max(self._reconnect_backoff_seconds, 0.0),
                    _MAX_RECONNECT_BACKOFF_SECONDS,
                )
                if delay > 0:
                    await asyncio.sleep(delay)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                logger.exception("event stream watcher crashed host=%s", host_name)
                delay = min(
                    max(self._reconnect_backoff_seconds, 0.0),
                    _MAX_RECONNECT_BACKOFF_SECONDS,
                )
                if delay > 0:
                    await asyncio.sleep(delay)


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value
