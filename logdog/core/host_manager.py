from __future__ import annotations

import asyncio
import inspect
import logging
import os
import re
import stat
import time
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlsplit


HostConnector = Any
SleepFn = Any
StatusChangeHandler = Any
TimeFn = Any
ErrorClassifier = Any

_REDACTED = "***REDACTED***"
_SENSITIVE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"(?i)bearer\s+\S+"),
    re.compile(r"(?i)\bapi[_-]?key\b\s*[:=]\s*\S+"),
    re.compile(r"(?i)\btoken\b\s*[:=]\s*\S+"),
    re.compile(r"(?i)\bpassword\b\s*[:=]\s*\S+"),
    re.compile(r'(?i)"(?:api[_-]?key|token|password)"\s*:\s*"[^"]*"'),
    re.compile(r"(?i)'(?:api[_-]?key|token|password)'\s*:\s*'[^']*'"),
)
_MAX_ERROR_DISPLAY_CHARS = 256


@dataclass(slots=True)
class HostState:
    name: str
    url: str
    status: str = "disconnected"
    last_connected_at: str | None = None
    last_error: str | None = None
    last_error_kind: str | None = None
    failure_count: int = 0
    consecutive_failures: int = 0
    circuit_open_until_ts: float | None = None
    meta: dict[str, Any] = field(default_factory=dict)


class HostManager:
    def __init__(
        self,
        hosts: list[dict[str, Any]],
        *,
        connector: HostConnector | None = None,
        sleep_fn: SleepFn | None = None,
        max_retries: int = 3,
        connect_timeout: float = 30.0,
        circuit_break_threshold: int = 3,
        circuit_break_seconds: int = 60,
        time_fn: TimeFn | None = None,
        error_classifier: ErrorClassifier | None = None,
        on_status_change: StatusChangeHandler | None = None,
    ) -> None:
        if max_retries <= 0:
            raise ValueError("max_retries must be > 0")
        if connect_timeout <= 0:
            raise ValueError("connect_timeout must be > 0")
        if circuit_break_threshold <= 0:
            raise ValueError("circuit_break_threshold must be > 0")
        if circuit_break_seconds <= 0:
            raise ValueError("circuit_break_seconds must be > 0")

        self._logger = logging.getLogger(__name__)
        self._connector = connector
        self._sleep_fn = sleep_fn or asyncio.sleep
        self._max_retries = int(max_retries)
        self._connect_timeout = float(connect_timeout)
        self._circuit_break_threshold = int(circuit_break_threshold)
        self._circuit_break_seconds = int(circuit_break_seconds)
        self._time_fn = time_fn or time.time
        self._error_classifier = error_classifier or _classify_connect_error
        self._on_status_change = on_status_change

        self._hosts: dict[str, dict[str, Any]] = {}
        self._states: dict[str, HostState] = {}
        for host in hosts:
            normalized = _normalize_host(host)
            name = normalized["name"]
            if name in self._hosts:
                raise ValueError(f"duplicate host name: {name!r}")
            self._hosts[name] = normalized
            self._states[name] = HostState(name=name, url=normalized["url"])

    async def startup_check(self) -> list[dict[str, Any]]:
        for name in list(self._hosts.keys()):
            await self._connect_host(name)
        return self.list_host_statuses()

    async def reload_hosts(
        self, new_hosts: list[dict[str, Any]]
    ) -> dict[str, list[str]]:
        normalized_hosts: dict[str, dict[str, Any]] = {}
        ordered_host_names: list[str] = []
        for host in new_hosts:
            normalized = _normalize_host(host)
            host_name = normalized["name"]
            if host_name in normalized_hosts:
                raise ValueError(f"duplicate host name: {host_name!r}")
            normalized_hosts[host_name] = normalized
            ordered_host_names.append(host_name)

        existing_names = set(self._hosts.keys())
        incoming_names = set(normalized_hosts.keys())

        added: list[str] = []
        updated: list[str] = []

        for name in ordered_host_names:
            normalized = normalized_hosts[name]
            if name in self._hosts:
                self._hosts[name] = normalized
                self._states[name].url = normalized["url"]
                await self._connect_host(name)
                updated.append(name)
                continue

            self._hosts[name] = normalized
            self._states[name] = HostState(name=name, url=normalized["url"])
            await self._connect_host(name)
            added.append(name)

        removed = sorted(existing_names - incoming_names)
        for name in removed:
            state = self._states.get(name)
            if state is not None:
                old_status = state.status
                state.status = "removed"
                state.last_error = None
                state.last_error_kind = None
                await self._finalize_connect_result(state, old_status)
            self._hosts.pop(name, None)
            self._states.pop(name, None)

        return {
            "added": added,
            "updated": updated,
            "removed": removed,
            "removed_requires_restart": [],
        }

    def snapshot_state(self) -> dict[str, Any]:
        return {
            "hosts": deepcopy(self._hosts),
            "states": deepcopy(self._states),
        }

    def restore_state(self, snapshot: dict[str, Any]) -> None:
        hosts = snapshot.get("hosts") if isinstance(snapshot, dict) else None
        states = snapshot.get("states") if isinstance(snapshot, dict) else None
        if not isinstance(hosts, dict) or not isinstance(states, dict):
            raise ValueError("invalid host manager snapshot")
        self._hosts = deepcopy(hosts)
        self._states = deepcopy(states)

    def list_host_statuses(self) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for name in self._hosts.keys():
            state = self._states[name]
            out.append(
                {
                    "name": state.name,
                    "url": _sanitize_host_url(state.url),
                    "status": state.status,
                    "last_connected_at": state.last_connected_at,
                    "last_error": _sanitize_error_message(state.last_error),
                    "last_error_kind": state.last_error_kind,
                    "failure_count": state.failure_count,
                    "consecutive_failures": state.consecutive_failures,
                    "circuit_open_until": _ts_or_none_iso(state.circuit_open_until_ts),
                }
            )
        return out

    def get_host_config(self, name: str) -> dict[str, Any] | None:
        host = self._hosts.get(name)
        if host is None:
            return None
        return deepcopy(host)

    def get_host_state(self, name: str) -> dict[str, Any] | None:
        state = self._states.get(name)
        if state is None:
            return None
        return {
            "name": state.name,
            "url": _sanitize_host_url(state.url),
            "status": state.status,
            "last_connected_at": state.last_connected_at,
            "last_error": _sanitize_error_message(state.last_error),
            "last_error_kind": state.last_error_kind,
            "failure_count": state.failure_count,
            "consecutive_failures": state.consecutive_failures,
            "circuit_open_until": _ts_or_none_iso(state.circuit_open_until_ts),
        }

    async def _connect_host(self, name: str) -> bool:
        host = self._hosts[name]
        state = self._states[name]
        old_status = state.status
        now_ts = float(self._time_fn())

        if (
            state.circuit_open_until_ts is not None
            and now_ts < state.circuit_open_until_ts
        ):
            state.status = "disconnected"
            state.last_error_kind = "circuit_open"
            state.last_error = (
                "circuit open until "
                + datetime.fromtimestamp(
                    state.circuit_open_until_ts,
                    tz=timezone.utc,
                ).isoformat()
            )
            return await self._finalize_connect_result(state, old_status)

        if (
            state.circuit_open_until_ts is not None
            and now_ts >= state.circuit_open_until_ts
        ):
            state.circuit_open_until_ts = None
            state.meta["circuit_half_open"] = True

        precheck_error = self._precheck_ssh_key(host)
        if precheck_error is not None:
            state.status = "disconnected"
            state.last_error = precheck_error
            state.last_error_kind = "config"
            state.consecutive_failures += 1
            state.failure_count += 1
            return await self._finalize_connect_result(state, old_status)

        for attempt in range(self._max_retries):
            try:
                info = await self._run_connector(host)
                state.status = "connected"
                state.last_connected_at = _utcnow_iso()
                state.last_error = None
                state.last_error_kind = None
                state.failure_count = 0
                state.consecutive_failures = 0
                state.circuit_open_until_ts = None
                state.meta = info
                return await self._finalize_connect_result(state, old_status)
            except Exception as exc:  # noqa: BLE001
                state.failure_count += 1
                state.consecutive_failures += 1
                state.status = "disconnected"
                state.last_error = str(exc)
                state.last_error_kind = self._error_classifier(exc)
                if state.consecutive_failures >= self._circuit_break_threshold:
                    state.circuit_open_until_ts = float(self._time_fn()) + float(
                        self._circuit_break_seconds
                    )
                    state.meta["circuit_opened"] = True
                    break
                if attempt < self._max_retries - 1:
                    await self._sleep_fn(_backoff_seconds(attempt))

        return await self._finalize_connect_result(state, old_status)

    async def _run_connector(self, host: dict[str, Any]) -> dict[str, Any]:
        if self._connector is None:
            return {}
        result = self._connector(deepcopy(host))
        if inspect.isawaitable(result):
            result = await asyncio.wait_for(result, timeout=self._connect_timeout)
        if result is None:
            return {}
        if not isinstance(result, dict):
            raise TypeError("connector must return dict or None")
        return result

    def _precheck_ssh_key(self, host: dict[str, Any]) -> str | None:
        url = str(host.get("url") or "")
        if not url.startswith("ssh://"):
            return None

        ssh_key = str(host.get("ssh_key") or "").strip()
        if ssh_key == "":
            return "SSH key missing for ssh host"
        if not os.path.exists(ssh_key):
            return f"SSH key not found: {ssh_key}"

        mode = stat.S_IMODE(os.stat(ssh_key).st_mode)
        if mode > 0o600:
            return (
                "SSH key permissions too open: "
                f"{mode:#o}; require <= 0o600 for ssh host"
            )
        return None

    async def _finalize_connect_result(self, state: HostState, old_status: str) -> bool:
        if old_status != state.status:
            await self._emit_status_change(state, old_status=old_status)
        return state.status == "connected"

    async def _emit_status_change(self, state: HostState, *, old_status: str) -> None:
        handler = self._on_status_change
        if handler is None:
            return

        payload = {
            "name": state.name,
            "url": state.url,
            "old_status": old_status,
            "new_status": state.status,
            "last_connected_at": state.last_connected_at,
            "last_error": _sanitize_error_message(state.last_error),
        }
        try:
            result = handler(payload)
            if inspect.isawaitable(result):
                await result
        except Exception:  # noqa: BLE001
            self._logger.exception(
                "host status change handler failed host=%s", state.name
            )


def _normalize_host(host: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(host, dict):
        raise TypeError(f"host must be dict, got {type(host).__name__}")
    name = str(host.get("name") or "").strip()
    url = str(host.get("url") or "").strip()
    if name == "":
        raise ValueError("host.name must not be empty")
    if url == "":
        raise ValueError(f"host[{name}].url must not be empty")
    out = deepcopy(host)
    out["name"] = name
    out["url"] = url
    return out


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _backoff_seconds(attempt: int) -> float:
    return float(min(60, 2**attempt))


def _classify_connect_error(exc: Exception) -> str:
    if isinstance(exc, (asyncio.TimeoutError, TimeoutError)):
        return "network"
    text = str(exc).lower()
    if any(token in text for token in ("auth", "permission denied", "publickey")):
        return "auth"
    if any(token in text for token in ("ssh", "handshake", "key")):
        return "ssh"
    if any(
        token in text
        for token in (
            "timeout",
            "timed out",
            "connection refused",
            "unreachable",
            "network",
            "reset by peer",
        )
    ):
        return "network"
    if any(token in text for token in ("docker", "daemon", "api")):
        return "docker"
    return "unknown"


def _sanitize_error_message(
    message: str | None,
    *,
    max_chars: int = _MAX_ERROR_DISPLAY_CHARS,
) -> str | None:
    if message is None:
        return None
    out = str(message)
    for pattern in _SENSITIVE_PATTERNS:
        out = pattern.sub(_REDACTED, out)
    if max_chars <= 0:
        return ""
    if len(out) > max_chars:
        return out[:max_chars]
    return out


def _strip_url_query_and_fragment(url: str) -> str:
    text = str(url or "")
    cut_at = len(text)
    query_idx = text.find("?")
    frag_idx = text.find("#")
    if query_idx >= 0:
        cut_at = min(cut_at, query_idx)
    if frag_idx >= 0:
        cut_at = min(cut_at, frag_idx)
    return text[:cut_at]


def _sanitize_host_url(url: str | None, *, max_chars: int = 512) -> str:
    text = str(url or "").strip()
    if text == "":
        return ""
    stripped = _strip_url_query_and_fragment(text)
    if stripped.startswith("unix://"):
        out = stripped
    else:
        parsed = urlsplit(stripped)
        netloc = parsed.netloc
        if parsed.hostname is not None:
            host_text = str(parsed.hostname)
            if ":" in host_text and not host_text.startswith("["):
                host_text = f"[{host_text}]"
            if parsed.port is not None:
                host_text = f"{host_text}:{parsed.port}"
            if parsed.username is not None or parsed.password is not None:
                netloc = f"{_REDACTED}@{host_text}"
            else:
                netloc = host_text
        elif "@" in netloc:
            netloc = f"{_REDACTED}@{netloc.split('@', 1)[1]}"
        if parsed.scheme != "":
            out = f"{parsed.scheme}://{netloc}{parsed.path}"
        else:
            out = stripped

    if max_chars <= 0:
        return ""
    if len(out) > max_chars:
        return out[:max_chars]
    return out


def _ts_or_none_iso(value: float | None) -> str | None:
    if value is None:
        return None
    return datetime.fromtimestamp(float(value), tz=timezone.utc).isoformat()
