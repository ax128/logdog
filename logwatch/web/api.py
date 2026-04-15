from __future__ import annotations

import hmac
import inspect
from collections.abc import Awaitable, Callable
from typing import Any

from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, Query


ReloadAction = Callable[[], Awaitable[dict[str, bool] | None] | dict[str, bool] | None]
ListSendFailedAction = Callable[
    [int], Awaitable[list[dict[str, Any]] | None] | list[dict[str, Any]] | None
]
ListHostsAction = Callable[
    [], Awaitable[list[dict[str, Any]] | None] | list[dict[str, Any]] | None
]
ListContainersAction = Callable[
    [str], Awaitable[list[dict[str, Any]] | None] | list[dict[str, Any]] | None
]
ListAlertsAction = Callable[
    [int, str | None, str | None, str | None],
    Awaitable[list[dict[str, Any]] | None] | list[dict[str, Any]] | None,
]
QueryMetricsAction = Callable[
    [str, str, str | None, str | None, int],
    Awaitable[list[dict[str, Any]] | None] | list[dict[str, Any]] | None,
]
QueryHostSystemMetricsAction = Callable[
    [str, str | None, str | None, int],
    Awaitable[list[dict[str, Any]] | None] | list[dict[str, Any]] | None,
]
ListMutesAction = Callable[
    [int], Awaitable[list[dict[str, Any]] | None] | list[dict[str, Any]] | None
]
ListStormEventsAction = Callable[
    [int, str | None, str | None],
    Awaitable[list[dict[str, Any]] | None] | list[dict[str, Any]] | None,
]
StormEventStatsAction = Callable[
    [str | None, str | None],
    Awaitable[dict[str, Any] | None] | dict[str, Any] | None,
]
IssueWsTicketAction = Callable[
    [str], Awaitable[dict[str, Any] | None] | dict[str, Any] | None
]


def _extract_bearer_token(auth_header: str | None) -> str | None:
    if not auth_header:
        return None

    parts = auth_header.split(" ", 1)
    if len(parts) != 2:
        return None

    scheme, token = parts
    if scheme.lower() != "bearer":
        return None

    token = token.strip()
    return token or None


def verify_admin_token(auth_header: str | None, admin_token: str) -> None:
    """Raise PermissionError when the Authorization header is not the expected Bearer token."""

    if not auth_header:
        raise PermissionError("missing authorization header")

    token = _extract_bearer_token(auth_header)
    if token is None:
        raise PermissionError("invalid authorization scheme")

    if not hmac.compare_digest(token, admin_token):
        raise PermissionError("invalid admin token")


def verify_web_token(auth_header: str | None, web_auth_token: str) -> None:
    if not auth_header:
        raise PermissionError("missing authorization header")

    token = _extract_bearer_token(auth_header)
    if token is None:
        raise PermissionError("invalid authorization scheme")

    if not hmac.compare_digest(token, web_auth_token):
        raise PermissionError("invalid web token")


def _reload_payload() -> dict[str, bool]:
    return {"ok": True}


async def _run_reload_action(reload_action: ReloadAction | None) -> dict[str, bool]:
    if reload_action is None:
        return _reload_payload()

    result = reload_action()
    if inspect.isawaitable(result):
        result = await result

    if result is None:
        return _reload_payload()
    if isinstance(result, dict):
        return result
    raise RuntimeError("reload_action must return dict[str, bool] or None")


async def _run_list_send_failed_action(
    list_send_failed_action: ListSendFailedAction | None,
    limit: int,
) -> list[dict[str, Any]]:
    if list_send_failed_action is None:
        return []

    result = list_send_failed_action(int(limit))
    if inspect.isawaitable(result):
        result = await result

    if result is None:
        return []
    if not isinstance(result, list):
        raise RuntimeError("list_send_failed_action must return list[dict] or None")
    return result


async def _run_list_hosts_action(
    list_hosts_action: ListHostsAction | None,
) -> list[dict[str, Any]]:
    if list_hosts_action is None:
        return []
    result = list_hosts_action()
    if inspect.isawaitable(result):
        result = await result
    if result is None:
        return []
    if not isinstance(result, list):
        raise RuntimeError("list_hosts_action must return list[dict] or None")
    return result


async def _run_list_containers_action(
    list_containers_action: ListContainersAction | None,
    host: str,
) -> list[dict[str, Any]]:
    if list_containers_action is None:
        return []
    result = list_containers_action(str(host))
    if inspect.isawaitable(result):
        result = await result
    if result is None:
        return []
    if not isinstance(result, list):
        raise RuntimeError("list_containers_action must return list[dict] or None")
    return result


async def _run_list_alerts_action(
    list_alerts_action: ListAlertsAction | None,
    *,
    limit: int,
    host: str | None,
    container: str | None,
    category: str | None,
) -> list[dict[str, Any]]:
    if list_alerts_action is None:
        return []
    result = list_alerts_action(int(limit), host, container, category)
    if inspect.isawaitable(result):
        result = await result
    if result is None:
        return []
    if not isinstance(result, list):
        raise RuntimeError("list_alerts_action must return list[dict] or None")
    return result


async def _run_query_metrics_action(
    query_metrics_action: QueryMetricsAction | None,
    *,
    host: str,
    container: str,
    start: str | None,
    end: str | None,
    limit: int,
) -> list[dict[str, Any]]:
    if query_metrics_action is None:
        return []
    result = query_metrics_action(str(host), str(container), start, end, int(limit))
    if inspect.isawaitable(result):
        result = await result
    if result is None:
        return []
    if not isinstance(result, list):
        raise RuntimeError("query_metrics_action must return list[dict] or None")
    return result


async def _run_query_host_system_metrics_action(
    query_host_system_metrics_action: QueryHostSystemMetricsAction | None,
    *,
    host: str,
    start: str | None,
    end: str | None,
    limit: int,
) -> list[dict[str, Any]]:
    if query_host_system_metrics_action is None:
        return []
    result = query_host_system_metrics_action(str(host), start, end, int(limit))
    if inspect.isawaitable(result):
        result = await result
    if result is None:
        return []
    if not isinstance(result, list):
        raise RuntimeError(
            "query_host_system_metrics_action must return list[dict] or None"
        )
    return result


async def _run_list_mutes_action(
    list_mutes_action: ListMutesAction | None,
    *,
    limit: int,
) -> list[dict[str, Any]]:
    if list_mutes_action is None:
        return []
    result = list_mutes_action(int(limit))
    if inspect.isawaitable(result):
        result = await result
    if result is None:
        return []
    if not isinstance(result, list):
        raise RuntimeError("list_mutes_action must return list[dict] or None")
    return result


async def _run_list_storm_events_action(
    list_storm_events_action: ListStormEventsAction | None,
    *,
    limit: int,
    phase: str | None,
    category: str | None,
) -> list[dict[str, Any]]:
    if list_storm_events_action is None:
        return []
    result = list_storm_events_action(int(limit), phase, category)
    if inspect.isawaitable(result):
        result = await result
    if result is None:
        return []
    if not isinstance(result, list):
        raise RuntimeError("list_storm_events_action must return list[dict] or None")
    return result


async def _run_storm_event_stats_action(
    storm_event_stats_action: StormEventStatsAction | None,
    *,
    start: str | None,
    end: str | None,
) -> dict[str, Any]:
    if storm_event_stats_action is None:
        return {
            "total_events": 0,
            "host_count": 0,
            "by_category": [],
            "by_phase": [],
        }
    result = storm_event_stats_action(start, end)
    if inspect.isawaitable(result):
        result = await result
    if result is None:
        return {
            "total_events": 0,
            "host_count": 0,
            "by_category": [],
            "by_phase": [],
        }
    if not isinstance(result, dict):
        raise RuntimeError("storm_event_stats_action must return dict or None")
    return result


async def _run_issue_ws_ticket_action(
    issue_ws_ticket_action: IssueWsTicketAction | None,
    *,
    token_subject: str,
) -> dict[str, Any]:
    if issue_ws_ticket_action is None:
        raise RuntimeError("issue_ws_ticket_action is not configured")

    result = issue_ws_ticket_action(str(token_subject))
    if inspect.isawaitable(result):
        result = await result
    if result is None:
        raise RuntimeError("issue_ws_ticket_action returned None")
    if not isinstance(result, dict):
        raise RuntimeError("issue_ws_ticket_action must return dict or None")
    return result


def _build_auth_handlers(
    *,
    web_auth_token: str,
    web_admin_token: str,
    reload_action: ReloadAction | None,
) -> tuple[
    Callable[[str | None], Awaitable[None]],
    Callable[[str | None], Awaitable[None]],
    Callable[[], Awaitable[dict[str, bool]]],
]:
    async def require_web_user(
        authorization: str | None = Header(default=None),
    ) -> None:
        try:
            verify_web_token(authorization, web_auth_token)
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail="forbidden") from exc

    async def require_admin(authorization: str | None = Header(default=None)) -> None:
        try:
            verify_admin_token(authorization, web_admin_token)
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail="forbidden") from exc

    async def reload_config(_: None = Depends(require_admin)) -> dict[str, bool]:
        try:
            return await _run_reload_action(reload_action)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=503, detail="reload failed") from exc

    return require_web_user, require_admin, reload_config


def create_api_router(
    *,
    web_auth_token: str,
    web_admin_token: str,
    reload_action: ReloadAction | None = None,
    issue_ws_ticket_action: IssueWsTicketAction | None = None,
    list_send_failed_action: ListSendFailedAction | None = None,
    list_hosts_action: ListHostsAction | None = None,
    list_containers_action: ListContainersAction | None = None,
    list_alerts_action: ListAlertsAction | None = None,
    query_metrics_action: QueryMetricsAction | None = None,
    query_host_system_metrics_action: QueryHostSystemMetricsAction | None = None,
    list_mutes_action: ListMutesAction | None = None,
    list_storm_events_action: ListStormEventsAction | None = None,
    storm_event_stats_action: StormEventStatsAction | None = None,
) -> APIRouter:
    router = APIRouter()
    require_web_user, require_admin, reload_config = _build_auth_handlers(
        web_auth_token=web_auth_token,
        web_admin_token=web_admin_token,
        reload_action=reload_action,
    )

    async def health() -> dict[str, bool]:
        return {"ok": True}

    async def list_hosts(
        _: None = Depends(require_web_user),
    ) -> dict[str, list[dict[str, Any]]]:
        try:
            items = await _run_list_hosts_action(list_hosts_action)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=503, detail="list hosts failed") from exc
        return {"hosts": items}

    async def list_containers(
        host: str,
        _: None = Depends(require_web_user),
    ) -> dict[str, Any]:
        try:
            items = await _run_list_containers_action(list_containers_action, host)
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=404, detail="host not found") from exc
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(
                status_code=503, detail="list containers failed"
            ) from exc
        return {"host": str(host), "containers": items}

    async def list_alerts(
        host: str | None = Query(default=None),
        container: str | None = Query(default=None),
        category: str | None = Query(default=None),
        limit: int = Query(100, ge=1, le=1000),
        _: None = Depends(require_web_user),
    ) -> dict[str, list[dict[str, Any]]]:
        try:
            items = await _run_list_alerts_action(
                list_alerts_action,
                limit=limit,
                host=host,
                container=container,
                category=category,
            )
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=503, detail="list alerts failed") from exc
        return {"alerts": items}

    async def get_metrics(
        host: str,
        container: str,
        start: str | None = Query(default=None),
        end: str | None = Query(default=None),
        limit: int = Query(5000, ge=1, le=10000),
        _: None = Depends(require_web_user),
    ) -> dict[str, Any]:
        try:
            points = await _run_query_metrics_action(
                query_metrics_action,
                host=host,
                container=container,
                start=start,
                end=end,
                limit=limit,
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="host not found") from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="invalid request") from exc
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=503, detail="query metrics failed") from exc
        return {"host": str(host), "container": str(container), "points": points}

    async def get_host_system_metrics(
        host: str,
        start: str | None = Query(default=None),
        end: str | None = Query(default=None),
        limit: int = Query(5000, ge=1, le=10000),
        _: None = Depends(require_web_user),
    ) -> dict[str, Any]:
        try:
            points = await _run_query_host_system_metrics_action(
                query_host_system_metrics_action,
                host=host,
                start=start,
                end=end,
                limit=limit,
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="host not found") from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="invalid request") from exc
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(
                status_code=503,
                detail="query host system metrics failed",
            ) from exc
        return {"host": str(host), "points": points}

    async def list_mutes(
        limit: int = Query(100, ge=1, le=1000),
        _: None = Depends(require_web_user),
    ) -> dict[str, list[dict[str, Any]]]:
        try:
            items = await _run_list_mutes_action(list_mutes_action, limit=limit)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=503, detail="list mutes failed") from exc
        return {"mutes": items}

    async def list_send_failed(
        limit: int = Query(100, ge=1, le=1000),
        _: None = Depends(require_admin),
    ) -> dict[str, list[dict[str, Any]]]:
        try:
            items = await _run_list_send_failed_action(list_send_failed_action, limit)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(
                status_code=503, detail="list send_failed failed"
            ) from exc
        return {"items": items}

    async def issue_ws_ticket(
        authorization: str | None = Header(default=None),
        _: None = Depends(require_web_user),
    ) -> dict[str, Any]:
        token = _extract_bearer_token(authorization)
        if token is None:
            raise HTTPException(status_code=403, detail="forbidden")
        try:
            return await _run_issue_ws_ticket_action(
                issue_ws_ticket_action,
                token_subject=token,
            )
        except HTTPException:
            raise
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=503, detail="issue ws ticket failed") from exc

    async def list_storm_events(
        phase: str | None = Query(default=None),
        category: str | None = Query(default=None),
        limit: int = Query(100, ge=1, le=1000),
        _: None = Depends(require_web_user),
    ) -> dict[str, list[dict[str, Any]]]:
        try:
            items = await _run_list_storm_events_action(
                list_storm_events_action,
                limit=limit,
                phase=phase,
                category=category,
            )
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(
                status_code=503, detail="list storm events failed"
            ) from exc
        return {"items": items}

    async def storm_event_stats(
        start: str | None = Query(default=None),
        end: str | None = Query(default=None),
        _: None = Depends(require_web_user),
    ) -> dict[str, Any]:
        try:
            return await _run_storm_event_stats_action(
                storm_event_stats_action,
                start=start,
                end=end,
            )
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=503, detail="storm stats failed") from exc

    router.add_api_route("/api/hosts", list_hosts, methods=["GET"])
    router.add_api_route(
        "/api/hosts/{host}/containers", list_containers, methods=["GET"]
    )
    router.add_api_route("/api/alerts", list_alerts, methods=["GET"])
    router.add_api_route(
        "/api/hosts/{host}/metrics/{container}",
        get_metrics,
        methods=["GET"],
    )
    router.add_api_route(
        "/api/hosts/{host}/system-metrics",
        get_host_system_metrics,
        methods=["GET"],
    )
    router.add_api_route("/api/ws-ticket", issue_ws_ticket, methods=["POST"])
    router.add_api_route("/api/mutes", list_mutes, methods=["GET"])
    router.add_api_route("/api/reload", reload_config, methods=["POST"])
    router.add_api_route("/api/send-failed", list_send_failed, methods=["GET"])
    router.add_api_route("/api/storm-events", list_storm_events, methods=["GET"])
    router.add_api_route("/api/storm-events/stats", storm_event_stats, methods=["GET"])
    router.add_api_route("/health", health, methods=["GET"])
    setattr(router, "reload_handler", reload_config)
    return router


def create_api_app(
    web_auth_token: str,
    web_admin_token: str,
    *,
    reload_action: ReloadAction | None = None,
    issue_ws_ticket_action: IssueWsTicketAction | None = None,
    list_send_failed_action: ListSendFailedAction | None = None,
    list_hosts_action: ListHostsAction | None = None,
    list_containers_action: ListContainersAction | None = None,
    list_alerts_action: ListAlertsAction | None = None,
    query_metrics_action: QueryMetricsAction | None = None,
    query_host_system_metrics_action: QueryHostSystemMetricsAction | None = None,
    list_mutes_action: ListMutesAction | None = None,
    list_storm_events_action: ListStormEventsAction | None = None,
    storm_event_stats_action: StormEventStatsAction | None = None,
) -> FastAPI:
    """Build the HTTP API that exposes LogWatch routes."""

    app = FastAPI(title="LogWatch API")
    router = create_api_router(
        web_auth_token=web_auth_token,
        web_admin_token=web_admin_token,
        reload_action=reload_action,
        issue_ws_ticket_action=issue_ws_ticket_action,
        list_send_failed_action=list_send_failed_action,
        list_hosts_action=list_hosts_action,
        list_containers_action=list_containers_action,
        list_alerts_action=list_alerts_action,
        query_metrics_action=query_metrics_action,
        query_host_system_metrics_action=query_host_system_metrics_action,
        list_mutes_action=list_mutes_action,
        list_storm_events_action=list_storm_events_action,
        storm_event_stats_action=storm_event_stats_action,
    )

    app.state.reload_payload = _reload_payload
    app.state.reload_action = reload_action
    app.include_router(router)
    app.state.reload_handler = getattr(router, "reload_handler", None)

    return app
