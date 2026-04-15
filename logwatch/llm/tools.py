from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
import logging
import time
from collections import defaultdict, deque
from collections.abc import Callable
from typing import Any

from logwatch.llm.tool_types import ToolResult

from logwatch.core.db import query_metrics
from logwatch.core.docker_connector import (
    list_containers_for_host,
    query_container_logs,
    restart_container_for_host,
)
from logwatch.llm.permissions import ensure_tool_allowed, load_permission_policy


logger = logging.getLogger(__name__)


DEFAULT_AUDIT_REDACT_PATTERNS: tuple[str, ...] = (
    r"(?i)bearer\s+\S+",
    r"(?i)\bapi[_-]?key\b\s*[:=]\s*\S+",
    r"(?i)\bpassword\b\s*[:=]\s*\S+",
    r"(?i)\btoken\b\s*[:=]\s*\S+",
    r'(?i)"(?:api[_-]?key|token|password)"\s*:\s*"[^"]*"',
    r"(?i)'(?:api[_-]?key|token|password)'\s*:\s*'[^']*'",
)
MAX_QUERY_LOGS_HOURS = 24 * 7
MAX_QUERY_LOGS_LINES = 10_000
MAX_TOOL_METRICS_LIMIT = 20_000
MAX_TOOL_ALERTS_LIMIT = 2_000
MAX_MUTE_HOURS = 24 * 30
MAX_RESTART_TIMEOUT_SECONDS = 120


def validate_query_logs_args(hours: int, max_hours: int = 24) -> int:
    """Validate query_logs range arguments and return normalized hours."""

    try:
        normalized_hours = int(hours)
        normalized_max = int(max_hours)
    except (TypeError, ValueError) as exc:
        raise ValueError("hours and max_hours must be integers") from exc

    if normalized_max <= 0:
        raise ValueError("max_hours must be > 0")
    if normalized_hours <= 0:
        raise ValueError("hours must be > 0")
    if normalized_hours > normalized_max:
        raise ValueError(f"hours must be <= {normalized_max}")

    return normalized_hours


class RateLimiter:
    """Simple per-user sliding window limiter."""

    def __init__(
        self,
        limit: int,
        window_seconds: int,
        *,
        time_fn: Callable[[], float] | None = None,
        bucket_ttl_seconds: float | None = None,
        max_buckets: int = 10_000,
        prune_interval_seconds: float = 30.0,
    ) -> None:
        self.limit = int(limit)
        self.window_seconds = float(window_seconds)
        self._time_fn = time_fn or time.time
        self.bucket_ttl_seconds = (
            float(bucket_ttl_seconds)
            if bucket_ttl_seconds is not None
            else self.window_seconds * 10
        )
        self.max_buckets = int(max_buckets)
        self.prune_interval_seconds = float(prune_interval_seconds)
        self._buckets: dict[str, deque[float]] = defaultdict(deque)
        self._last_seen: dict[str, float] = {}
        self._last_prune_at = 0.0

        if self.limit <= 0:
            raise ValueError("limit must be > 0")
        if self.window_seconds <= 0:
            raise ValueError("window_seconds must be > 0")
        if self.bucket_ttl_seconds < 0:
            raise ValueError("bucket_ttl_seconds must be >= 0")
        if self.max_buckets <= 0:
            raise ValueError("max_buckets must be > 0")
        if self.prune_interval_seconds <= 0:
            raise ValueError("prune_interval_seconds must be > 0")

    def allow(self, user_id: str) -> bool:
        if not user_id:
            raise ValueError("user_id must not be empty")

        now = float(self._time_fn())
        self._prune(now)
        queue = self._buckets[user_id]
        while queue and (now - queue[0]) >= self.window_seconds:
            queue.popleft()

        self._last_seen[user_id] = now
        if len(queue) >= self.limit:
            return False

        queue.append(now)
        if len(self._buckets) > self.max_buckets:
            self._prune(now, force=True)
        return True

    def _prune(self, now: float, *, force: bool = False) -> None:
        should_prune = len(self._buckets) > self.max_buckets
        if (
            not force
            and not should_prune
            and (now - self._last_prune_at) < self.prune_interval_seconds
        ):
            return

        if self.bucket_ttl_seconds > 0:
            cutoff = now - self.bucket_ttl_seconds
            for user_id, last_seen in list(self._last_seen.items()):
                if last_seen < cutoff:
                    self._last_seen.pop(user_id, None)
                    self._buckets.pop(user_id, None)

        if len(self._buckets) > self.max_buckets:
            for user_id, _ in sorted(self._last_seen.items(), key=lambda item: item[1])[
                : len(self._buckets) - self.max_buckets
            ]:
                self._last_seen.pop(user_id, None)
                self._buckets.pop(user_id, None)

        self._last_prune_at = now


class ToolRateLimitError(RuntimeError):
    pass


@dataclass(slots=True)
class AgentTool:
    name: str
    description: str
    read_only: bool
    _invoke: Callable[[str, dict[str, Any]], Any]

    async def invoke(
        self,
        *,
        user_id: str,
        arguments: dict[str, Any] | None,
    ) -> Any:
        normalized_user_id = str(user_id or "").strip()
        if normalized_user_id == "":
            raise ValueError("user_id must not be empty")
        normalized_arguments = dict(arguments or {})
        return await _maybe_await(
            self._invoke(normalized_user_id, normalized_arguments)
        )


def create_tool_registry(
    *,
    host_manager: Any,
    metrics_writer_factory: Callable[[], Any],
    app_config: dict[str, Any] | None = None,
    docker_client_pool: Any | None = None,
    list_containers_fn: Any | None = None,
    query_logs_fn: Any | None = None,
    query_metrics_fn: Any | None = None,
    restart_container_fn: Any | None = None,
    time_fn: Callable[[], float] | None = None,
) -> dict[str, AgentTool]:
    tools_cfg = _resolve_llm_tools_config(app_config)
    rate_limit_cfg = _resolve_nested_dict(tools_cfg.get("rate_limit"))
    query_logs_cfg = _resolve_nested_dict(tools_cfg.get("query_logs"))
    permission_policy = load_permission_policy(app_config)
    clock = time_fn or time.time
    rate_limiter = RateLimiter(
        limit=_coerce_positive_int(rate_limit_cfg.get("limit"), default=20),
        window_seconds=_coerce_positive_int(
            rate_limit_cfg.get("window_seconds"),
            default=60,
        ),
        time_fn=clock,
    )
    configured_redact_patterns = tuple(
        str(item)
        for item in _coerce_sequence(tools_cfg.get("audit_redact_patterns"))
        if str(item).strip() != ""
    )
    audit_redact_patterns = tuple(
        dict.fromkeys((*DEFAULT_AUDIT_REDACT_PATTERNS, *configured_redact_patterns))
    )
    audit_max_chars = _coerce_positive_int(
        tools_cfg.get("audit_max_chars"), default=10_000
    )
    query_logs_max_hours = _coerce_bounded_int(
        query_logs_cfg.get("max_hours"),
        default=24,
        max_value=MAX_QUERY_LOGS_HOURS,
        field_name="query_logs.max_hours",
    )
    query_logs_max_lines = _coerce_bounded_int(
        query_logs_cfg.get("max_lines"),
        default=2000,
        max_value=MAX_QUERY_LOGS_LINES,
        field_name="query_logs.max_lines",
    )

    async def resolve_writer() -> Any:
        writer = await _maybe_await(metrics_writer_factory())
        if writer is None:
            raise RuntimeError("metrics writer factory returned None")
        return writer

    async def list_containers_op(host: dict[str, Any]) -> list[dict[str, Any]]:
        if list_containers_fn is not None:
            return list(await _maybe_await(list_containers_fn(host)))
        if docker_client_pool is not None and callable(
            getattr(docker_client_pool, "list_containers_for_host", None)
        ):
            return list(
                await _maybe_await(docker_client_pool.list_containers_for_host(host))
            )
        return list(await _maybe_await(list_containers_for_host(host)))

    async def query_logs_op(
        host: dict[str, Any],
        container: dict[str, Any],
        *,
        since: str,
        until: str | None,
        max_lines: int,
    ) -> list[dict[str, Any]]:
        if query_logs_fn is not None:
            return list(
                await _maybe_await(
                    query_logs_fn(
                        host,
                        container,
                        since=since,
                        until=until,
                        max_lines=max_lines,
                    )
                )
            )
        if docker_client_pool is not None and callable(
            getattr(docker_client_pool, "query_container_logs", None)
        ):
            return list(
                await _maybe_await(
                    docker_client_pool.query_container_logs(
                        host,
                        container,
                        since=since,
                        until=until,
                        max_lines=max_lines,
                    )
                )
            )
        return list(
            await _maybe_await(
                query_container_logs(
                    host,
                    container,
                    since=since,
                    until=until,
                    max_lines=max_lines,
                )
            )
        )

    async def query_metrics_op(
        *,
        writer: Any,
        host_name: str,
        container_id: str,
        start_time: str,
        end_time: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        if query_metrics_fn is not None:
            return list(
                await _maybe_await(
                    query_metrics_fn(
                        writer=writer,
                        host_name=host_name,
                        container_id=container_id,
                        start_time=start_time,
                        end_time=end_time,
                        limit=limit,
                    )
                )
            )

        ensure_conn = getattr(writer, "_ensure_conn", None)
        if not callable(ensure_conn):
            raise TypeError("metrics writer does not support metric queries")
        conn = await _maybe_await(ensure_conn())
        return list(
            await _maybe_await(
                query_metrics(
                    conn,
                    host_name=host_name,
                    container_id=container_id,
                    start_time=start_time,
                    end_time=end_time,
                    limit=limit,
                )
            )
        )

    async def restart_container_op(
        host: dict[str, Any],
        container: dict[str, Any],
        *,
        timeout: int,
    ) -> dict[str, Any]:
        if restart_container_fn is not None:
            return dict(
                await _maybe_await(
                    restart_container_fn(host, container, timeout=timeout)
                )
            )
        if docker_client_pool is not None and callable(
            getattr(docker_client_pool, "restart_container_for_host", None)
        ):
            return dict(
                await _maybe_await(
                    docker_client_pool.restart_container_for_host(
                        host,
                        container,
                        timeout=timeout,
                    )
                )
            )
        return dict(
            await _maybe_await(
                restart_container_for_host(host, container, timeout=timeout)
            )
        )

    async def guarded_invoke(
        tool_name: str,
        *,
        read_only: bool,
        user_id: str,
        arguments: dict[str, Any],
        handler: Callable[[dict[str, Any], Any], Any],
    ) -> Any:
        audit_payload = {
            "event": "llm_tool_call",
            "tool": tool_name,
            "user_id": user_id,
            "read_only": read_only,
            "arguments": dict(arguments),
        }

        try:
            writer = await resolve_writer()
        except Exception:
            logger.warning(
                "tool audit fallback (writer unavailable): %s",
                audit_payload,
                exc_info=True,
            )
            raise

        async def write_audit_safe(payload: dict[str, Any]) -> None:
            try:
                await writer.write_audit(
                    payload,
                    redact_patterns=audit_redact_patterns,
                    max_chars=audit_max_chars,
                )
            except Exception:  # noqa: BLE001
                logger.warning(
                    "tool audit write failed tool=%s user_id=%s",
                    tool_name,
                    user_id,
                    exc_info=True,
                )

        try:
            if not rate_limiter.allow(user_id):
                raise ToolRateLimitError("rate limit exceeded")
            ensure_tool_allowed(tool_name, arguments, policy=permission_policy)
            result = await _maybe_await(handler(arguments, writer))
        except Exception as exc:  # noqa: BLE001
            if isinstance(exc, ToolRateLimitError):
                status = "rate_limited"
            elif isinstance(exc, PermissionError):
                status = "denied"
            else:
                status = "error"
            await write_audit_safe(
                {**audit_payload, "status": status, "error": str(exc)}
            )
            raise

        await write_audit_safe({**audit_payload, "status": "ok"})
        return result

    async def list_hosts_handler(
        arguments: dict[str, Any], _writer: Any
    ) -> ToolResult:
        _ensure_no_unknown_required_arguments(arguments, allowed_keys=())
        return ToolResult.ok(json.dumps({"hosts": list(host_manager.list_host_statuses())}))

    async def list_containers_handler(
        arguments: dict[str, Any],
        _writer: Any,
    ) -> ToolResult:
        host = _require_host_config(host_manager, arguments)
        containers = await list_containers_op(host)
        return ToolResult.ok(json.dumps({"host": host["name"], "containers": containers}))

    async def query_logs_handler(
        arguments: dict[str, Any], _writer: Any
    ) -> ToolResult:
        host = _require_host_config(host_manager, arguments)
        container_id = _require_non_empty_str(
            arguments.get("container_id"), field_name="container_id"
        )
        hours = validate_query_logs_args(
            arguments.get("hours", 1),
            max_hours=query_logs_max_hours,
        )
        max_lines = _coerce_bounded_int(
            arguments.get("max_lines"),
            default=query_logs_max_lines,
            max_value=query_logs_max_lines,
            field_name="max_lines",
        )
        end_time = _format_utc_timestamp(clock())
        start_time = _format_utc_timestamp(clock() - (hours * 3600))
        container = await _resolve_container(
            host, container_id=container_id, list_containers_op=list_containers_op
        )
        lines = await query_logs_op(
            host,
            container,
            since=start_time,
            until=end_time,
            max_lines=max_lines,
        )
        return ToolResult.ok(json.dumps({
            "host": host["name"],
            "container_id": container["id"],
            "lines": lines,
        }))

    async def get_metrics_handler(
        arguments: dict[str, Any], writer: Any
    ) -> ToolResult:
        host = _require_host_config(host_manager, arguments)
        container_id = _require_non_empty_str(
            arguments.get("container_id"), field_name="container_id"
        )
        hours = validate_query_logs_args(
            arguments.get("hours", 1), max_hours=query_logs_max_hours
        )
        limit = _coerce_bounded_int(
            arguments.get("limit"),
            default=5000,
            max_value=MAX_TOOL_METRICS_LIMIT,
            field_name="limit",
        )
        end_time = _format_utc_timestamp(clock())
        start_time = _format_utc_timestamp(clock() - (hours * 3600))
        points = await query_metrics_op(
            writer=writer,
            host_name=host["name"],
            container_id=container_id,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
        )
        return ToolResult.ok(json.dumps({
            "host": host["name"],
            "container_id": container_id,
            "points": points,
        }))

    async def get_alerts_handler(
        arguments: dict[str, Any], writer: Any
    ) -> ToolResult:
        limit = _coerce_bounded_int(
            arguments.get("limit"),
            default=100,
            max_value=MAX_TOOL_ALERTS_LIMIT,
            field_name="limit",
        )
        return ToolResult.ok(json.dumps({"alerts": list(await _maybe_await(writer.list_alerts(limit=limit)))}))

    async def mute_alert_handler(
        arguments: dict[str, Any], writer: Any
    ) -> ToolResult:
        host = _require_host_config(host_manager, arguments)
        container_id = _require_non_empty_str(
            arguments.get("container_id"), field_name="container_id"
        )
        category = _require_non_empty_str(
            arguments.get("category"), field_name="category"
        )
        hours = _coerce_bounded_int(
            arguments.get("hours"),
            default=1,
            max_value=MAX_MUTE_HOURS,
            field_name="hours",
        )
        reason = str(arguments.get("reason") or "").strip()
        expires_at = _format_utc_timestamp(clock() + (hours * 3600))
        payload = {
            "host": host["name"],
            "container_id": container_id,
            "category": category,
            "reason": reason,
            "expires_at": expires_at,
        }
        await _maybe_await(writer.write_mute(payload))
        return ToolResult.ok(json.dumps({"ok": True, "mute": payload}))

    async def unmute_alert_handler(
        arguments: dict[str, Any], writer: Any
    ) -> ToolResult:
        host = _require_host_config(host_manager, arguments)
        container_id = _require_non_empty_str(
            arguments.get("container_id"), field_name="container_id"
        )
        category = _require_non_empty_str(
            arguments.get("category"), field_name="category"
        )
        deleted = int(
            await _maybe_await(
                writer.delete_mute(
                    host=host["name"],
                    container_id=container_id,
                    category=category,
                )
            )
        )
        return ToolResult.ok(json.dumps({"ok": True, "deleted": deleted}))

    async def restart_container_handler(
        arguments: dict[str, Any], _writer: Any
    ) -> ToolResult:
        host = _require_host_config(host_manager, arguments)
        container_id = _require_non_empty_str(
            arguments.get("container_id"), field_name="container_id"
        )
        timeout = _coerce_bounded_int(
            arguments.get("timeout"),
            default=10,
            max_value=MAX_RESTART_TIMEOUT_SECONDS,
            field_name="timeout",
        )
        container = await _resolve_container(
            host, container_id=container_id, list_containers_op=list_containers_op
        )
        result = await restart_container_op(host, container, timeout=timeout)
        return ToolResult.ok(json.dumps({"ok": True, "result": result}))

    return {
        "list_hosts": AgentTool(
            name="list_hosts",
            description="List configured hosts and statuses. No parameters required.",
            read_only=True,
            _invoke=lambda user_id, arguments: guarded_invoke(
                "list_hosts",
                read_only=True,
                user_id=user_id,
                arguments=arguments,
                handler=list_hosts_handler,
            ),
        ),
        "list_containers": AgentTool(
            name="list_containers",
            description="List containers for a host.",
            read_only=True,
            _invoke=lambda user_id, arguments: guarded_invoke(
                "list_containers",
                read_only=True,
                user_id=user_id,
                arguments=arguments,
                handler=list_containers_handler,
            ),
        ),
        "query_logs": AgentTool(
            name="query_logs",
            description="Query recent container logs.",
            read_only=True,
            _invoke=lambda user_id, arguments: guarded_invoke(
                "query_logs",
                read_only=True,
                user_id=user_id,
                arguments=arguments,
                handler=query_logs_handler,
            ),
        ),
        "get_metrics": AgentTool(
            name="get_metrics",
            description="Query container metrics from storage.",
            read_only=True,
            _invoke=lambda user_id, arguments: guarded_invoke(
                "get_metrics",
                read_only=True,
                user_id=user_id,
                arguments=arguments,
                handler=get_metrics_handler,
            ),
        ),
        "get_alerts": AgentTool(
            name="get_alerts",
            description="List recent stored alerts.",
            read_only=True,
            _invoke=lambda user_id, arguments: guarded_invoke(
                "get_alerts",
                read_only=True,
                user_id=user_id,
                arguments=arguments,
                handler=get_alerts_handler,
            ),
        ),
        "mute_alert": AgentTool(
            name="mute_alert",
            description="Create an alert mute entry.",
            read_only=False,
            _invoke=lambda user_id, arguments: guarded_invoke(
                "mute_alert",
                read_only=False,
                user_id=user_id,
                arguments=arguments,
                handler=mute_alert_handler,
            ),
        ),
        "unmute_alert": AgentTool(
            name="unmute_alert",
            description="Remove an alert mute entry.",
            read_only=False,
            _invoke=lambda user_id, arguments: guarded_invoke(
                "unmute_alert",
                read_only=False,
                user_id=user_id,
                arguments=arguments,
                handler=unmute_alert_handler,
            ),
        ),
        "restart_container": AgentTool(
            name="restart_container",
            description="Restart a container on an allowlisted host.",
            read_only=False,
            _invoke=lambda user_id, arguments: guarded_invoke(
                "restart_container",
                read_only=False,
                user_id=user_id,
                arguments=arguments,
                handler=restart_container_handler,
            ),
        ),
    }


def _resolve_llm_tools_config(app_config: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(app_config, dict):
        return {}
    llm_cfg = app_config.get("llm")
    if not isinstance(llm_cfg, dict):
        return {}
    tools_cfg = llm_cfg.get("tools")
    if not isinstance(tools_cfg, dict):
        return {}
    return tools_cfg


def _resolve_nested_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    return {}


def _coerce_sequence(value: Any) -> tuple[Any, ...]:
    if value is None:
        return ()
    if isinstance(value, (list, tuple, set, frozenset)):
        return tuple(value)
    return (value,)


def _coerce_positive_int(value: Any, *, default: int) -> int:
    if value is None:
        return int(default)
    try:
        normalized = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("value must be an integer") from exc
    if normalized <= 0:
        raise ValueError("value must be > 0")
    return normalized


def _coerce_bounded_int(
    value: Any,
    *,
    default: int,
    max_value: int,
    field_name: str,
) -> int:
    normalized = _coerce_positive_int(value, default=default)
    if normalized > int(max_value):
        raise ValueError(f"{field_name} must be <= {int(max_value)}")
    return normalized


def _require_non_empty_str(value: Any, *, field_name: str) -> str:
    normalized = str(value or "").strip()
    if normalized == "":
        raise ValueError(f"{field_name} must not be empty")
    return normalized


def _require_host_config(
    host_manager: Any, arguments: dict[str, Any]
) -> dict[str, Any]:
    host_name = _require_non_empty_str(arguments.get("host"), field_name="host")
    host = host_manager.get_host_config(host_name)
    if host is None:
        raise ValueError(f"unknown host: {host_name}")
    return host


async def _resolve_container(
    host: dict[str, Any],
    *,
    container_id: str,
    list_containers_op: Callable[[dict[str, Any]], Any],
) -> dict[str, Any]:
    containers = await _maybe_await(list_containers_op(host))
    for container in containers:
        if _container_matches(container, container_id):
            return dict(container)
    raise ValueError(f"unknown container: {container_id}")


def _container_matches(container: dict[str, Any], container_id: str) -> bool:
    return (
        str(container.get("id") or "") == container_id
        or str(container.get("name") or "") == container_id
    )


def _ensure_no_unknown_required_arguments(
    arguments: dict[str, Any],
    *,
    allowed_keys: tuple[str, ...],
) -> None:
    if not arguments:
        return
    unknown = sorted(set(arguments) - set(allowed_keys))
    if unknown:
        raise ValueError(f"unexpected arguments: {unknown}")


def _format_utc_timestamp(value: float) -> str:
    return datetime.fromtimestamp(float(value), tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


async def _maybe_await(value: Any) -> Any:
    if hasattr(value, "__await__"):
        return await value
    return value
