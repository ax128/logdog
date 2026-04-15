from __future__ import annotations

import inspect
import logging
from typing import Any


class WatchManager:
    def __init__(
        self,
        *,
        host_manager: Any,
        log_watcher_factory: Any,
        event_watcher_factory: Any,
    ) -> None:
        self._host_manager = host_manager
        self._log_watcher_factory = log_watcher_factory
        self._event_watcher_factory = event_watcher_factory
        self._log_watchers: dict[str, Any] = {}
        self._event_watchers: dict[str, Any] = {}
        self._started = False
        self._logger = logging.getLogger(__name__)

    async def start(self) -> None:
        self._started = True
        for status in self._host_manager.list_host_statuses():
            if status.get("status") != "connected":
                continue
            host_name = str(status.get("name") or "")
            if host_name == "":
                continue
            await self._ensure_host_watchers(host_name)
            await self.refresh_host(host_name)

    async def refresh_host(self, host_name: str) -> None:
        if not self._started:
            return
        status = self._host_status(host_name)
        if status is None or status.get("status") != "connected":
            return
        await self._ensure_host_watchers(host_name)
        log_watcher = self._log_watchers.get(host_name)
        if log_watcher is not None:
            await log_watcher.refresh_containers()

    async def shutdown(self) -> None:
        self._started = False
        event_watchers = list(self._event_watchers.values())
        log_watchers = list(self._log_watchers.values())
        self._event_watchers = {}
        self._log_watchers = {}

        for watcher in event_watchers:
            await self._safe_shutdown(watcher)
        for watcher in log_watchers:
            await self._safe_shutdown(watcher)

    async def handle_host_status_change(self, payload: dict[str, Any]) -> None:
        if not self._started:
            return
        host_name = str(payload.get("name") or "").strip()
        if host_name == "":
            return
        old_status = str(payload.get("old_status") or "")
        new_status = str(payload.get("new_status") or "")

        if old_status != "connected" and new_status == "connected":
            await self.refresh_host(host_name)
            return
        if old_status == "connected" and new_status != "connected":
            await self._shutdown_host(host_name)
            return
        if new_status == "connected":
            await self.refresh_host(host_name)

    async def reload_host_configs(self, host_names: list[str]) -> None:
        if not self._started:
            return
        ordered_names: list[str] = []
        seen: set[str] = set()
        for raw_name in host_names:
            host_name = str(raw_name or "").strip()
            if host_name == "" or host_name in seen:
                continue
            seen.add(host_name)
            ordered_names.append(host_name)

        replacements: dict[str, tuple[Any, Any]] = {}
        disconnect_hosts: list[str] = []
        try:
            for host_name in ordered_names:
                status = self._host_status(host_name)
                if status is None or status.get("status") != "connected":
                    disconnect_hosts.append(host_name)
                    continue
                new_log, new_event = await self._build_started_watchers(host_name)
                try:
                    await new_log.refresh_containers()
                except Exception:
                    await self._safe_shutdown(new_event)
                    await self._safe_shutdown(new_log)
                    raise
                replacements[host_name] = (new_log, new_event)
        except Exception:
            for new_log, new_event in replacements.values():
                await self._safe_shutdown(new_event)
                await self._safe_shutdown(new_log)
            raise

        stale: list[Any] = []
        for host_name in disconnect_hosts:
            event_watcher = self._event_watchers.pop(host_name, None)
            log_watcher = self._log_watchers.pop(host_name, None)
            if event_watcher is not None:
                stale.append(event_watcher)
            if log_watcher is not None:
                stale.append(log_watcher)

        for host_name, (new_log, new_event) in replacements.items():
            old_event = self._event_watchers.get(host_name)
            old_log = self._log_watchers.get(host_name)
            self._event_watchers[host_name] = new_event
            self._log_watchers[host_name] = new_log
            if old_event is not None and old_event is not new_event:
                stale.append(old_event)
            if old_log is not None and old_log is not new_log:
                stale.append(old_log)

        for watcher in stale:
            await self._safe_shutdown(watcher)

    async def _ensure_host_watchers(self, host_name: str) -> None:
        status = self._host_status(host_name)
        if status is None or status.get("status") != "connected":
            return

        if host_name in self._log_watchers and host_name in self._event_watchers:
            return

        new_log, new_event = await self._build_started_watchers(host_name)
        old_event = self._event_watchers.get(host_name)
        old_log = self._log_watchers.get(host_name)
        self._event_watchers[host_name] = new_event
        self._log_watchers[host_name] = new_log
        if old_event is not None and old_event is not new_event:
            await self._safe_shutdown(old_event)
        if old_log is not None and old_log is not new_log:
            await self._safe_shutdown(old_log)

    async def _build_started_watchers(self, host_name: str) -> tuple[Any, Any]:
        host = self._host_manager.get_host_config(host_name)
        if host is None:
            raise ValueError(f"unknown host: {host_name}")

        log_watcher = self._log_watcher_factory(host)
        event_watcher = self._event_watcher_factory(host)
        try:
            await log_watcher.start()
            await event_watcher.start()
        except Exception:
            await self._safe_shutdown(event_watcher)
            await self._safe_shutdown(log_watcher)
            raise
        return log_watcher, event_watcher

    async def _shutdown_host(self, host_name: str) -> None:
        event_watcher = self._event_watchers.pop(host_name, None)
        log_watcher = self._log_watchers.pop(host_name, None)
        if event_watcher is not None:
            await self._safe_shutdown(event_watcher)
        if log_watcher is not None:
            await self._safe_shutdown(log_watcher)

    async def _safe_shutdown(self, watcher: Any) -> None:
        shutdown = getattr(watcher, "shutdown", None)
        if not callable(shutdown):
            return
        try:
            maybe_awaitable = shutdown()
            if inspect.isawaitable(maybe_awaitable):
                await maybe_awaitable
        except Exception:  # noqa: BLE001
            self._logger.exception("watcher shutdown failed")

    def _host_status(self, host_name: str) -> dict[str, Any] | None:
        for status in self._host_manager.list_host_statuses():
            if str(status.get("name") or "") == host_name:
                return dict(status)
        return None

    def snapshot_state(self) -> dict[str, Any]:
        return {
            "started": self._started,
            "log_watchers": dict(self._log_watchers),
            "event_watchers": dict(self._event_watchers),
        }

    async def restore_state(self, snapshot: dict[str, Any]) -> None:
        if not isinstance(snapshot, dict):
            raise ValueError("invalid watch manager snapshot")
        target_log = snapshot.get("log_watchers")
        target_event = snapshot.get("event_watchers")
        if not isinstance(target_log, dict) or not isinstance(target_event, dict):
            raise ValueError("invalid watch manager snapshot")

        current_pairs = {
            *self._log_watchers.values(),
            *self._event_watchers.values(),
        }
        target_pairs = {*target_log.values(), *target_event.values()}
        for watcher in current_pairs:
            if watcher not in target_pairs:
                await self._safe_shutdown(watcher)

        self._log_watchers = dict(target_log)
        self._event_watchers = dict(target_event)
        self._started = bool(snapshot.get("started", self._started))
