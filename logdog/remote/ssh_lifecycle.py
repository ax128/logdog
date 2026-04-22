from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable
from uuid import uuid4

__all__ = [
    "SSHSessionLifecycle",
    "SSHSessionStatus",
    "TransitionOutcome",
]


class SSHSessionStatus(str, Enum):
    NEW = "new"
    CONNECTING = "connecting"
    HANDSHAKING = "handshaking"
    RUNNING = "running"
    DRAINING = "draining"
    CLOSED = "closed"


@dataclass(slots=True, frozen=True)
class TransitionOutcome:
    created_new: bool
    state: dict[str, Any]


@dataclass(slots=True)
class _SessionRecord:
    host: str
    status: SSHSessionStatus = SSHSessionStatus.NEW
    session_token: str | None = None
    session_started_at: datetime | None = None
    last_heartbeat_at: datetime | None = None
    disconnect_reason: str | None = None
    closed_at: datetime | None = None
    tombstone_at: datetime | None = None


class SSHSessionLifecycle:
    def __init__(
        self,
        *,
        now_fn: Callable[[], datetime] | None = None,
    ) -> None:
        self._now_fn = now_fn or (lambda: datetime.now(timezone.utc))
        self._states: dict[str, _SessionRecord] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    async def request_connect(self, host: str) -> TransitionOutcome:
        record = self._record_for_mutation(host)
        async with self._lock_for(host):
            if record.status in {
                SSHSessionStatus.CONNECTING,
                SSHSessionStatus.HANDSHAKING,
                SSHSessionStatus.RUNNING,
                SSHSessionStatus.DRAINING,
            }:
                return TransitionOutcome(
                    created_new=False,
                    state=self._snapshot(record),
                )

            if record.status is SSHSessionStatus.CLOSED:
                if record.tombstone_at is None:
                    raise RuntimeError("previous SSH session has not been tombstoned")

            now = self._now()
            record.status = SSHSessionStatus.CONNECTING
            record.session_token = uuid4().hex
            record.session_started_at = now
            record.last_heartbeat_at = None
            record.disconnect_reason = None
            record.closed_at = None
            record.tombstone_at = None
            return TransitionOutcome(created_new=True, state=self._snapshot(record))

    async def begin_handshake(self, host: str, session_token: str) -> dict[str, Any]:
        record = self._record_for_mutation(host)
        async with self._lock_for(host):
            self._require_token(record, session_token)
            self._require_status(
                record,
                {SSHSessionStatus.CONNECTING},
                "begin_handshake",
            )
            record.status = SSHSessionStatus.HANDSHAKING
            return self._snapshot(record)

    async def mark_running(self, host: str, session_token: str) -> dict[str, Any]:
        record = self._record_for_mutation(host)
        async with self._lock_for(host):
            self._require_token(record, session_token)
            self._require_status(
                record,
                {SSHSessionStatus.CONNECTING, SSHSessionStatus.HANDSHAKING},
                "mark_running",
            )
            record.status = SSHSessionStatus.RUNNING
            return self._snapshot(record)

    async def record_heartbeat(
        self,
        host: str,
        session_token: str,
        *,
        at: datetime | None = None,
    ) -> dict[str, Any]:
        record = self._record_for_mutation(host)
        async with self._lock_for(host):
            self._require_token(record, session_token)
            self._require_status(record, {SSHSessionStatus.RUNNING}, "record_heartbeat")
            record.last_heartbeat_at = self._normalize_dt(at)
            return self._snapshot(record)

    async def begin_shutdown(
        self,
        host: str,
        *,
        reason: str | None = None,
        session_token: str | None = None,
    ) -> dict[str, Any]:
        record = self._record_for_mutation(host)
        async with self._lock_for(host):
            if session_token is not None:
                self._require_token(record, session_token)
            self._require_status(
                record,
                {
                    SSHSessionStatus.CONNECTING,
                    SSHSessionStatus.HANDSHAKING,
                    SSHSessionStatus.RUNNING,
                },
                "begin_shutdown",
            )
            record.status = SSHSessionStatus.DRAINING
            record.disconnect_reason = reason or "shutdown requested"
            return self._snapshot(record)

    async def mark_closed(
        self,
        host: str,
        *,
        reason: str | None = None,
        session_token: str | None = None,
    ) -> dict[str, Any]:
        record = self._record_for_mutation(host)
        async with self._lock_for(host):
            if session_token is not None:
                self._require_token(record, session_token)
            if record.status is SSHSessionStatus.CLOSED:
                if reason is not None:
                    record.disconnect_reason = reason
                return self._snapshot(record)
            self._require_status(
                record,
                {
                    SSHSessionStatus.CONNECTING,
                    SSHSessionStatus.HANDSHAKING,
                    SSHSessionStatus.RUNNING,
                    SSHSessionStatus.DRAINING,
                },
                "mark_closed",
            )
            now = self._now()
            record.status = SSHSessionStatus.CLOSED
            record.disconnect_reason = reason or record.disconnect_reason or "closed"
            record.closed_at = now
            record.tombstone_at = now
            return self._snapshot(record)

    def can_retry(self, host: str) -> bool:
        record = self._states.get(self._normalize_host(host))
        if record is None:
            return False
        return record.status is SSHSessionStatus.CLOSED and record.tombstone_at is not None

    def get_state(self, host: str) -> dict[str, Any]:
        record = self._states.get(self._normalize_host(host))
        if record is None:
            record = _SessionRecord(host=self._normalize_host(host))
        return self._snapshot(record)

    def _record_for_mutation(self, host: str) -> _SessionRecord:
        normalized = self._normalize_host(host)
        record = self._states.get(normalized)
        if record is None:
            record = _SessionRecord(host=normalized)
            self._states[normalized] = record
        return record

    def _lock_for(self, host: str) -> asyncio.Lock:
        normalized = self._normalize_host(host)
        lock = self._locks.get(normalized)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[normalized] = lock
        return lock

    def _snapshot(self, record: _SessionRecord) -> dict[str, Any]:
        return {
            "host": record.host,
            "status": record.status.value,
            "session_token": record.session_token,
            "session_started_at": self._dt_to_iso(record.session_started_at),
            "last_heartbeat_at": self._dt_to_iso(record.last_heartbeat_at),
            "disconnect_reason": record.disconnect_reason,
            "closed_at": self._dt_to_iso(record.closed_at),
            "tombstone_at": self._dt_to_iso(record.tombstone_at),
        }

    def _require_token(self, record: _SessionRecord, session_token: str) -> None:
        token = str(session_token or "").strip()
        if token == "":
            raise ValueError("session token must not be empty")
        if record.session_token != token:
            raise ValueError("session token does not match current session")

    def _require_status(
        self,
        record: _SessionRecord,
        allowed: set[SSHSessionStatus],
        operation: str,
    ) -> None:
        if record.status not in allowed:
            allowed_text = ", ".join(sorted(status.value for status in allowed))
            raise RuntimeError(
                f"{operation} requires status in {{{allowed_text}}}, "
                f"got {record.status.value}"
            )

    def _normalize_dt(self, value: datetime | None) -> datetime:
        if value is None:
            value = self._now()
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    def _now(self) -> datetime:
        return self._normalize_dt(self._now_fn())

    def _dt_to_iso(self, value: datetime | None) -> str | None:
        if value is None:
            return None
        return self._normalize_dt(value).isoformat()

    def _normalize_host(self, host: str) -> str:
        value = str(host or "").strip()
        if value == "":
            raise ValueError("host must not be empty")
        return value
