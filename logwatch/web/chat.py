from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import hashlib
import hmac
import inspect
import logging
import secrets
import time
import uuid
from collections.abc import Callable, Mapping
from typing import Any
from urllib.parse import urlparse

from fastapi import APIRouter, FastAPI, WebSocket
from logwatch.llm.agent_runtime import (
    DEFAULT_CHAT_FALLBACK_MESSAGE,
    build_chat_runtime,
)
from starlette.exceptions import WebSocketException
from starlette.websockets import WebSocketDisconnect


logger = logging.getLogger(__name__)
DEFAULT_WS_TICKET_TTL_SECONDS = 60
DEFAULT_WS_TICKET_MAX_ENTRIES = 10_000


class WsTicketStore:
    """Short-lived one-time tickets for browser WebSocket upgrades."""

    def __init__(
        self,
        *,
        ttl_seconds: int = DEFAULT_WS_TICKET_TTL_SECONDS,
        max_entries: int = DEFAULT_WS_TICKET_MAX_ENTRIES,
        time_fn: Callable[[], float] | None = None,
    ) -> None:
        normalized_ttl = int(ttl_seconds)
        normalized_max_entries = int(max_entries)
        if normalized_ttl <= 0:
            raise ValueError("ttl_seconds must be > 0")
        if normalized_max_entries <= 0:
            raise ValueError("max_entries must be > 0")
        self._ttl_seconds = normalized_ttl
        self._max_entries = normalized_max_entries
        self._time_fn = time_fn or time.time
        self._tickets: dict[str, tuple[float, str]] = {}

    def issue(self, subject: str) -> dict[str, Any]:
        normalized_subject = str(subject or "").strip()
        if normalized_subject == "":
            raise ValueError("subject must not be empty")

        now = float(self._time_fn())
        self._prune_expired(now)
        if len(self._tickets) >= self._max_entries:
            evict_ticket = min(
                self._tickets.items(),
                key=lambda item: item[1][0],
            )[0]
            self._tickets.pop(evict_ticket, None)

        ticket = secrets.token_urlsafe(24)
        expires_ts = now + float(self._ttl_seconds)
        self._tickets[ticket] = (expires_ts, normalized_subject)
        return {
            "ticket": ticket,
            "expires_at": _format_utc_timestamp(expires_ts),
            "ttl_seconds": self._ttl_seconds,
        }

    def consume(self, ticket: str) -> str | None:
        normalized_ticket = str(ticket or "").strip()
        if normalized_ticket == "":
            return None

        now = float(self._time_fn())
        self._prune_expired(now)
        stored = self._tickets.pop(normalized_ticket, None)
        if stored is None:
            return None

        expires_ts, subject = stored
        if expires_ts < now:
            return None
        return subject

    def _prune_expired(self, now: float) -> None:
        expired = [ticket for ticket, item in self._tickets.items() if item[0] < now]
        for ticket in expired:
            self._tickets.pop(ticket, None)


def _format_utc_timestamp(value: float) -> str:
    return datetime.fromtimestamp(float(value), tz=timezone.utc).isoformat()


def _extract_bearer_token(authorization: str | None) -> str | None:
    if not authorization:
        return None

    parts = authorization.split(" ", 1)
    if len(parts) != 2:
        return None

    scheme, token = parts
    if scheme.lower() != "bearer":
        return None

    token = token.strip()
    return token or None


def authorize_ws(
    query_params: Mapping[str, str],
    authorization: str | None,
    expected_token: str,
    sec_protocol: str | None = None,
    ws_ticket_consumer: Callable[[str], str | None] | None = None,
) -> bool:
    return (
        _resolve_authenticated_token(
            query_params,
            authorization,
            expected_token,
            sec_protocol,
            ws_ticket_consumer=ws_ticket_consumer,
        )
        is not None
    )


def _resolve_authenticated_token(
    query_params: Mapping[str, str],
    authorization: str | None,
    expected_token: str,
    sec_protocol: str | None = None,
    ws_ticket_consumer: Callable[[str], str | None] | None = None,
) -> str | None:
    if any(str(key).strip().lower() == "token" for key in query_params.keys()):
        return None

    token = _extract_bearer_token(authorization)
    if token:
        if hmac.compare_digest(token, expected_token):
            return token
        return None

    # Deliberately ignore Sec-WebSocket-Protocol for auth:
    # upstream proxies commonly log protocol headers and may leak credentials.
    _ = sec_protocol

    if ws_ticket_consumer is None:
        return None

    ticket = str(query_params.get("ticket") or "").strip()
    if ticket == "":
        return None

    consumed = ws_ticket_consumer(ticket)
    if inspect.isawaitable(consumed):
        raise TypeError("ws_ticket_consumer must be synchronous")
    if consumed is None:
        return None
    consumed_subject = str(consumed).strip()
    if consumed_subject == "":
        return None
    if hmac.compare_digest(consumed_subject, expected_token):
        return consumed_subject
    return None


def _build_ws_handler(
    *,
    web_auth_token: str,
    max_messages_per_connection: int,
    max_connections: int | None,
    max_connections_per_user: int | None,
    receive_timeout_seconds: float | None,
    chat_runtime: Any,
    chat_runtime_getter: Any | None = None,
    ws_ticket_consumer: Callable[[str], str | None] | None = None,
):
    connection_lock = asyncio.Lock()
    active_connections = 0
    active_connections_by_user: dict[str, int] = {}

    async def _try_acquire_connection(user_id: str) -> bool:
        nonlocal active_connections
        async with connection_lock:
            if max_connections is not None and active_connections >= max_connections:
                return False
            current_user_connections = int(active_connections_by_user.get(user_id, 0))
            if (
                max_connections_per_user is not None
                and current_user_connections >= max_connections_per_user
            ):
                return False
            active_connections += 1
            active_connections_by_user[user_id] = current_user_connections + 1
            return True

    async def _release_connection(user_id: str) -> None:
        nonlocal active_connections
        async with connection_lock:
            if active_connections > 0:
                active_connections -= 1
            current_user_connections = int(active_connections_by_user.get(user_id, 0))
            if current_user_connections <= 1:
                active_connections_by_user.pop(user_id, None)
            else:
                active_connections_by_user[user_id] = current_user_connections - 1

    def _origin_allowed(websocket: WebSocket) -> bool:
        origin = str(websocket.headers.get("origin") or "").strip()
        if origin == "":
            return True
        parsed = urlparse(origin)
        origin_host = str(parsed.netloc or "").strip().lower()
        ws_url = getattr(websocket, "url", None)
        expected_host = str(getattr(ws_url, "netloc", "") or "").strip().lower()
        return origin_host != "" and origin_host == expected_host

    async def ws_chat(websocket: WebSocket) -> None:
        if not _origin_allowed(websocket):
            raise WebSocketException(code=1008)
        authenticated_token = _resolve_authenticated_token(
            websocket.query_params,
            websocket.headers.get("authorization"),
            web_auth_token,
            websocket.headers.get("sec-websocket-protocol"),
            ws_ticket_consumer=ws_ticket_consumer,
        )
        if authenticated_token is None:
            raise WebSocketException(code=1008)

        runtime_user_id = _derive_runtime_user_id(authenticated_token)
        acquired = await _try_acquire_connection(runtime_user_id)
        if not acquired:
            raise WebSocketException(code=1008)

        try:
            await websocket.accept()
            session_key = _resolve_session_key(websocket, authenticated_token)
            processed = 0
            while processed < max_messages_per_connection:
                try:
                    if receive_timeout_seconds is not None:
                        msg = await asyncio.wait_for(
                            websocket.receive_text(),
                            timeout=receive_timeout_seconds,
                        )
                    else:
                        msg = await websocket.receive_text()
                except asyncio.TimeoutError:
                    logger.warning("web chat idle timeout user_id=%s", runtime_user_id)
                    break
                try:
                    runtime: Any = (
                        chat_runtime_getter()
                        if callable(chat_runtime_getter)
                        else chat_runtime
                    )
                    ainvoke_text = getattr(runtime, "ainvoke_text", None)
                    if callable(ainvoke_text):
                        reply = await ainvoke_text(
                            msg,
                            user_id=runtime_user_id,
                            session_key=session_key,
                            fallback=DEFAULT_CHAT_FALLBACK_MESSAGE,
                        )
                    else:
                        invoke_text = getattr(runtime, "invoke_text", None)
                        if not callable(invoke_text):
                            raise RuntimeError("chat runtime missing invoke_text")
                        reply = invoke_text(
                            msg,
                            user_id=runtime_user_id,
                            session_key=session_key,
                            fallback=DEFAULT_CHAT_FALLBACK_MESSAGE,
                        )
                except Exception:  # noqa: BLE001 - websocket must degrade safely
                    logger.warning("web chat runtime invoke failed", exc_info=True)
                    reply = DEFAULT_CHAT_FALLBACK_MESSAGE
                await websocket.send_text(str(reply))
                processed += 1
        except WebSocketDisconnect:
            return
        finally:
            await _release_connection(runtime_user_id)

        if hasattr(websocket, "close"):
            await websocket.close(code=1008)
        return

    return ws_chat


def create_chat_router(
    *,
    web_auth_token: str,
    max_messages_per_connection: int = 1000,
    max_connections: int | None = 200,
    max_connections_per_user: int | None = 20,
    receive_timeout_seconds: float | None = 60.0,
    chat_runtime: Any | None = None,
    chat_runtime_getter: Any | None = None,
    tool_registry: dict[str, Any] | None = None,
    runtime_factory: Any | None = None,
    ws_ticket_consumer: Callable[[str], str | None] | None = None,
) -> APIRouter:
    if max_messages_per_connection <= 0:
        raise ValueError("max_messages_per_connection must be > 0")
    if max_connections is not None and int(max_connections) <= 0:
        raise ValueError("max_connections must be > 0")
    if max_connections_per_user is not None and int(max_connections_per_user) <= 0:
        raise ValueError("max_connections_per_user must be > 0")
    if receive_timeout_seconds is not None and float(receive_timeout_seconds) <= 0:
        raise ValueError("receive_timeout_seconds must be > 0")

    resolved_chat_runtime = chat_runtime or build_chat_runtime(
        tool_registry=tool_registry or {},
        runtime_factory=runtime_factory,
    )

    router = APIRouter()
    ws_chat = _build_ws_handler(
        web_auth_token=web_auth_token,
        max_messages_per_connection=max_messages_per_connection,
        max_connections=(None if max_connections is None else int(max_connections)),
        max_connections_per_user=(
            None
            if max_connections_per_user is None
            else int(max_connections_per_user)
        ),
        receive_timeout_seconds=(
            None
            if receive_timeout_seconds is None
            else float(receive_timeout_seconds)
        ),
        chat_runtime=resolved_chat_runtime,
        chat_runtime_getter=chat_runtime_getter,
        ws_ticket_consumer=ws_ticket_consumer,
    )
    router.add_api_websocket_route("/ws/chat", ws_chat)
    setattr(router, "ws_handler", ws_chat)
    setattr(router, "chat_runtime", resolved_chat_runtime)
    return router


def create_chat_app(
    web_auth_token: str,
    *,
    max_messages_per_connection: int = 1000,
    max_connections: int | None = 200,
    max_connections_per_user: int | None = 20,
    receive_timeout_seconds: float | None = 60.0,
    chat_runtime: Any | None = None,
    tool_registry: dict[str, Any] | None = None,
    runtime_factory: Any | None = None,
    ws_ticket_consumer: Callable[[str], str | None] | None = None,
) -> FastAPI:
    app = FastAPI(title="LogWatch Chat")
    router = create_chat_router(
        web_auth_token=web_auth_token,
        max_messages_per_connection=max_messages_per_connection,
        max_connections=max_connections,
        max_connections_per_user=max_connections_per_user,
        receive_timeout_seconds=receive_timeout_seconds,
        chat_runtime=chat_runtime,
        tool_registry=tool_registry,
        runtime_factory=runtime_factory,
        ws_ticket_consumer=ws_ticket_consumer,
    )
    app.include_router(router)
    app.state.ws_handler = getattr(router, "ws_handler")
    app.state.chat_runtime = getattr(router, "chat_runtime")
    return app


def _resolve_session_key(websocket: WebSocket, authenticated_token: str) -> str:
    header_session = websocket.headers.get("x-session-id")
    if header_session and header_session.strip() != "":
        return header_session.strip()

    query_session = websocket.query_params.get("session")
    if query_session and query_session.strip() != "":
        return query_session.strip()

    return f"ws:{uuid.uuid4().hex}"


def _derive_runtime_user_id(authenticated_token: str) -> str:
    token_text = str(authenticated_token or "")
    digest = hashlib.sha256(token_text.encode("utf-8")).hexdigest()[:16]
    return f"auth:{digest}"
