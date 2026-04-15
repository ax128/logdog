from __future__ import annotations

import inspect
import importlib
import json
import logging
import os
import re
import time
from pathlib import Path
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from typing import Any, Callable

from fastapi import FastAPI

from logdog.collector.event_stream import EventStreamWatcher
from logdog.collector.host_metrics_sampler import HostMetricsSampler
from logdog.collector.sampler import MetricsSampler
from logdog.collector.watch_manager import WatchManager
from logdog.collector.scheduler import (
    HostMetricsSamplingScheduler,
    MetricsSamplingScheduler,
    ReportScheduler,
)
from logdog.collector.log_stream import (
    LogStreamWatcher,
    cancel_pending_dedup_tasks,
    run_alert_once,
)
from logdog.pipeline.cooldown import CooldownStore
from logdog.core.config import (
    expand_effective_hosts,
    load_app_config,
    resolve_config_path,
    resolve_runtime_settings,
)
from logdog.core.docker_connector import (
    DockerClientPool,
    connect_docker_host,
    fetch_container_stats,
    list_containers_for_host,
    stream_container_logs,
    stream_docker_events,
)
from logdog.core.host_manager import HostManager
from logdog.core.metrics_writer import MetricsSqliteWriter
from logdog.llm.agent_runtime import build_chat_runtime
from logdog.llm.provider import resolve_llm_params
from logdog.llm.tools import create_tool_registry
from logdog.core.retention_scheduler import RetentionCleanupScheduler
from logdog.notify.base import BaseNotifier, normalize_message_mode
from logdog.notify.policy import build_notify_routing_policy
from logdog.notify.router import NotifyRouter
from logdog.notify.telegram import (
    TELEGRAM_AUTO_TARGET,
    TelegramBotTokenSender,
    TelegramNotifier,
    build_telegram_bot_runtime,
    build_telegram_bot_token_sender,
    supports_auto_telegram_target,
)
from logdog.notify.wechat import WechatNotifier
from logdog.notify.weixin import WeixinNotifier
from logdog.notify.wecom import WecomNotifier, build_wecom_webhook_sender
from logdog.web.api import ReloadAction, create_api_router
from logdog.web.chat import WsTicketStore, create_chat_router
from logdog.web.frontend import mount_frontend


logger = logging.getLogger(__name__)


StatusNotifySender = Any
DEFAULT_METRICS_DB_PATH = "data/logdog.db"
DEFAULT_RETENTION_CONFIG: dict[str, int] = {
    "alerts_days": 30,
    "audit_days": 30,
    "send_failed_days": 14,
    "metrics_days": 7,
    "host_metrics_days": 7,
    "storm_days": 14,
}


def create_app(
    *,
    web_auth_token: str | None = None,
    web_admin_token: str | None = None,
    allow_insecure_default_tokens: bool = False,
    reload_action: ReloadAction | None = None,
    hosts: list[dict[str, Any]] | None = None,
    host_manager: HostManager | None = None,
    host_connector: Any | None = None,
    enable_metrics_scheduler: bool = True,
    metrics_interval_seconds: int | None = None,
    metrics_scheduler: MetricsSamplingScheduler | None = None,
    list_containers_fn: Any | None = None,
    fetch_stats_fn: Any | None = None,
    save_metric_fn: Any | None = None,
    collect_host_metrics_fn: Any | None = None,
    save_host_metric_fn: Any | None = None,
    host_metrics_scheduler: HostMetricsSamplingScheduler | None = None,
    docker_client_pool: DockerClientPool | None = None,
    docker_pool_max_clients: int | None = None,
    docker_pool_max_idle_seconds: float | None = None,
    metrics_db_path: str | None = None,
    metrics_db_connect: Any | None = None,
    enable_retention_cleanup: bool = True,
    retention_interval_seconds: int | None = None,
    retention_config: dict[str, int] | None = None,
    retention_scheduler: RetentionCleanupScheduler | None = None,
    app_config: dict[str, Any] | None = None,
    config_path: str | None = None,
    status_notify_sender: StatusNotifySender | None = None,
    notify_router: Any | None = None,
    telegram_send_func: Any | None = None,
    wechat_send_func: Any | None = None,
    weixin_send_func: Any | None = None,
    wecom_send_func: Any | None = None,
    telegram_application_factory: Any | None = None,
    telegram_handler_binder: Any | None = None,
    report_scheduler: Any | None = None,
    watch_manager: Any | None = None,
    enable_watch_manager: bool = True,
) -> FastAPI:
    resolved_web_auth_token = _resolve_web_auth_token(
        explicit_token=web_auth_token,
        allow_insecure_default_tokens=allow_insecure_default_tokens,
    )
    resolved_web_admin_token = _resolve_web_admin_token(
        explicit_token=web_admin_token,
        allow_insecure_default_tokens=allow_insecure_default_tokens,
    )
    explicit_hosts_provided = hosts is not None
    explicit_notify_router_provided = notify_router is not None
    explicit_status_notify_sender_provided = status_notify_sender is not None
    resolved_host_manager = host_manager
    resolved_app_config = _resolve_app_config(
        app_config=app_config,
        config_path=config_path,
        explicit_hosts_provided=explicit_hosts_provided,
    )
    current_app_config = resolved_app_config
    reload_config_path = _resolve_reload_config_path(
        app_config=app_config,
        config_path=config_path,
        explicit_hosts_provided=explicit_hosts_provided,
    )
    runtime_settings = resolve_runtime_settings(resolved_app_config)
    current_host_system_settings = dict(runtime_settings["host_system"])
    host_security_issue_open_state: dict[str, bool] = {}
    resolved_metrics_interval_seconds = _resolve_metrics_interval_seconds(
        metrics_interval_seconds,
        runtime_settings["metrics_interval_seconds"],
    )
    resolved_retention_interval_seconds = _resolve_retention_interval_seconds(
        retention_interval_seconds,
        runtime_settings["retention_interval_seconds"],
    )
    resolved_metrics_db_path = _resolve_metrics_db_path(
        metrics_db_path,
        runtime_settings["metrics_db_path"],
    )
    resolved_retention_config = _resolve_retention_config(
        runtime_settings["retention_config"],
        retention_config,
    )
    resolved_docker_client_pool = _resolve_docker_client_pool(
        docker_client_pool=docker_client_pool,
        host_manager=host_manager,
        host_connector=host_connector,
        enable_metrics_scheduler=enable_metrics_scheduler,
        metrics_scheduler=metrics_scheduler,
        list_containers_fn=list_containers_fn,
        fetch_stats_fn=fetch_stats_fn,
        docker_pool_max_clients=docker_pool_max_clients,
        docker_pool_max_idle_seconds=docker_pool_max_idle_seconds,
        config_docker_pool_max_clients=runtime_settings["docker_pool_max_clients"],
        config_docker_pool_max_idle_seconds=runtime_settings[
            "docker_pool_max_idle_seconds"
        ],
    )

    resolved_host_connector = host_connector
    if resolved_host_connector is None and resolved_docker_client_pool is not None:
        resolved_host_connector = resolved_docker_client_pool.connect_host
    if resolved_host_connector is None:
        resolved_host_connector = connect_docker_host

    resolved_wechat_send_func = (
        wechat_send_func if wechat_send_func is not None else wecom_send_func
    )
    resolved_wecom_send_func = (
        wecom_send_func if wecom_send_func is not None else wechat_send_func
    )
    resolved_weixin_send_func = weixin_send_func
    resolved_telegram_send_func = telegram_send_func
    if resolved_telegram_send_func is None:
        resolved_telegram_send_func = build_telegram_bot_token_sender(
            _resolve_telegram_bot_token(current_app_config)
        )
    if resolved_wecom_send_func is None:
        resolved_wecom_send_func = build_wecom_webhook_sender()

    metrics_writer: MetricsSqliteWriter | None = None

    def ensure_metrics_writer() -> MetricsSqliteWriter:
        nonlocal metrics_writer
        if metrics_writer is None:
            metrics_writer = MetricsSqliteWriter(
                db_path=resolved_metrics_db_path,
                db_connect=metrics_db_connect,
                sqlite_journal_mode=runtime_settings["sqlite"]["journal_mode"],
                sqlite_synchronous=runtime_settings["sqlite"]["synchronous"],
                sqlite_busy_timeout_ms=runtime_settings["sqlite"]["busy_timeout_ms"],
            )
        return metrics_writer

    if resolved_host_manager is None:
        resolved_hosts = hosts
        if resolved_hosts is None:
            resolved_hosts = expand_effective_hosts(resolved_app_config)
        if not resolved_hosts:
            resolved_hosts = [{"name": "local", "url": "unix:///var/run/docker.sock"}]
        resolved_host_manager = HostManager(
            resolved_hosts,
            connector=resolved_host_connector,
            max_retries=1,
            on_status_change=None,
        )

    current_message_modes: dict[str, str] = {
        "telegram": normalize_message_mode(None),
        "wechat": normalize_message_mode(None),
        "weixin": normalize_message_mode(None),
        "wecom": normalize_message_mode(None),
    }
    current_message_modes.update(
        _resolve_explicit_notify_message_modes(resolved_app_config)
    )

    def get_notify_message_mode(channel: str) -> str:
        normalized_channel = str(channel or "").strip().lower()
        return current_message_modes.get(
            normalized_channel,
            normalize_message_mode(None),
        )

    def set_notify_message_mode(channel: str, mode: str) -> str:
        normalized_channel = str(channel or "").strip().lower()
        if normalized_channel == "":
            raise ValueError("channel must not be empty")
        normalized_mode = normalize_message_mode(mode)
        current_message_modes[normalized_channel] = normalized_mode
        return normalized_mode

    resolved_notify_router = _resolve_notify_router(
        notify_router=notify_router,
        host_manager=resolved_host_manager,
        app_config=resolved_app_config,
        telegram_send_func=resolved_telegram_send_func,
        wechat_send_func=resolved_wechat_send_func,
        weixin_send_func=resolved_weixin_send_func,
        wecom_send_func=resolved_wecom_send_func,
        message_mode_getter=get_notify_message_mode,
        failure_recorder=_build_default_notify_failure_recorder(ensure_metrics_writer),
    )
    resolved_status_notify_sender = _resolve_status_notify_sender(
        status_notify_sender=status_notify_sender,
        notify_router=resolved_notify_router,
    )

    async def list_containers(host_name: str) -> list[dict[str, Any]]:
        host = resolved_host_manager.get_host_config(host_name)
        if host is None:
            raise KeyError(host_name)
        if list_containers_fn is not None:
            return await _maybe_await(list_containers_fn(host_name))
        if resolved_docker_client_pool is not None:
            return await resolved_docker_client_pool.list_containers_for_host(host)
        return await list_containers_for_host(host)

    async def list_alert_rows(
        limit: int,
        host: str | None = None,
        container: str | None = None,
        category: str | None = None,
    ) -> list[dict[str, Any]]:
        writer = ensure_metrics_writer()
        return await writer.list_alerts(
            limit=int(limit), host=host, container=container, category=category
        )

    async def query_metric_points(
        host_name: str,
        container_id: str,
        start_time: str | None,
        end_time: str | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        host = resolved_host_manager.get_host_config(host_name)
        if host is None:
            raise KeyError(host_name)
        writer = ensure_metrics_writer()
        return await writer.query_metrics(
            host_name=host_name,
            container_id=container_id,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
        )

    async def query_host_metric_points(
        host_name: str,
        start_time: str | None,
        end_time: str | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        host = resolved_host_manager.get_host_config(host_name)
        if host is None:
            raise KeyError(host_name)
        writer = ensure_metrics_writer()
        return await writer.query_host_metrics(
            host_name=host_name,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
        )

    async def query_host_container_latest_metric_points(
        host_name: str,
        start_time: str,
        end_time: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        host = resolved_host_manager.get_host_config(host_name)
        if host is None:
            raise KeyError(host_name)
        writer = ensure_metrics_writer()
        return await writer.query_host_container_latest_metrics(
            host_name=host_name,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
        )

    async def list_mute_rows(limit: int) -> list[dict[str, Any]]:
        writer = ensure_metrics_writer()
        return await writer.list_mutes(limit=int(limit))

    async def list_storm_rows(
        limit: int,
        phase: str | None = None,
        category: str | None = None,
    ) -> list[dict[str, Any]]:
        writer = ensure_metrics_writer()
        return await writer.list_storm_events(
            limit=int(limit),
            phase=phase,
            category=category,
        )

    async def storm_stats_rows(
        start: str | None = None,
        end: str | None = None,
    ) -> dict[str, Any]:
        writer = ensure_metrics_writer()
        return await writer.storm_event_stats(start_time=start, end_time=end)

    async def send_global_schedule_notification(
        schedule: dict[str, Any],
        message: str,
        category: str,
    ) -> bool:
        notifiers = _build_global_schedule_notifiers(
            schedule=schedule,
            app_config=current_app_config,
            telegram_send_func=resolved_telegram_send_func,
            wechat_send_func=resolved_wechat_send_func,
            weixin_send_func=resolved_weixin_send_func,
            wecom_send_func=resolved_wecom_send_func,
            message_mode_getter=get_notify_message_mode,
        )
        if notifiers:
            router = NotifyRouter(
                notifiers,
                failure_recorder=_build_default_notify_failure_recorder(
                    ensure_metrics_writer
                ),
            )
            return bool(await router.send("__global__", message, category))
        if resolved_notify_router is None:
            return False
        return bool(
            await _maybe_await(
                resolved_notify_router.send("__global__", message, category)
            )
        )

    ScheduleReportRunner = importlib.import_module(
        "logdog.collector.reports"
    ).ScheduleReportRunner
    AlertStormController = importlib.import_module(
        "logdog.collector.storm"
    ).AlertStormController
    schedule_report_runner = ScheduleReportRunner(
        host_manager=resolved_host_manager,
        list_containers=list_containers,
        list_alerts=list_alert_rows,
        query_metrics=query_metric_points,
        query_host_container_metrics=query_host_container_latest_metric_points,
        query_host_metrics=query_host_metric_points,
        host_system_settings_getter=lambda: current_host_system_settings,
        send_host_notification=getattr(resolved_notify_router, "send", None),
        send_global_notification=send_global_schedule_notification,
    )
    storm_controller = _build_alert_storm_controller(
        app_config=resolved_app_config,
        controller_cls=AlertStormController,
    )

    async def stream_logs(
        host: dict[str, Any], container: dict[str, Any], **kwargs: Any
    ):
        if resolved_docker_client_pool is not None:
            async for item in resolved_docker_client_pool.stream_container_logs(
                host,
                container,
                **kwargs,
            ):
                yield item
            return
        async for item in stream_container_logs(host, container, **kwargs):
            yield item

    async def stream_events(host: dict[str, Any], **kwargs: Any):
        if resolved_docker_client_pool is not None:
            async for item in resolved_docker_client_pool.stream_docker_events(
                host,
                **kwargs,
            ):
                yield item
            return
        async for item in stream_docker_events(host, **kwargs):
            yield item

    if metrics_scheduler is None and enable_metrics_scheduler:
        if save_metric_fn is None:
            ensure_metrics_writer()

        async def fetch_stats(
            host_name: str, container: dict[str, Any]
        ) -> dict[str, Any]:
            host = resolved_host_manager.get_host_config(host_name)
            if host is None:
                raise ValueError(f"unknown host: {host_name}")
            if fetch_stats_fn is not None:
                return await _maybe_await(fetch_stats_fn(host_name, container))
            if resolved_docker_client_pool is not None:
                return await resolved_docker_client_pool.fetch_container_stats(
                    host, container
                )
            return await fetch_container_stats(host, container)

        async def save_metric(sample: dict[str, Any]) -> None:
            if save_metric_fn is not None:
                await _maybe_await(save_metric_fn(sample))
                return
            if metrics_writer is None:
                return
            await metrics_writer.write(sample)

        sampler = MetricsSampler(fetch_stats=fetch_stats, save_metric=save_metric)
        metrics_scheduler = MetricsSamplingScheduler(
            host_manager=resolved_host_manager,
            sampler=sampler,
            list_containers=list_containers,
            interval_seconds=resolved_metrics_interval_seconds,
            max_instances=runtime_settings["scheduler"]["max_instances"],
            coalesce=runtime_settings["scheduler"]["coalesce"],
            misfire_grace_time=runtime_settings["scheduler"]["misfire_grace_time"],
        )

    def build_host_metrics_scheduler_for_settings(
        host_system_settings: dict[str, Any] | None,
    ) -> HostMetricsSamplingScheduler | None:
        settings = dict(host_system_settings or {})
        if not bool(settings.get("enabled", False)):
            return None
        security_settings = dict(settings.get("security") or {})
        security_enabled = bool(security_settings.get("enabled", False))
        security_push_on_issue = bool(security_settings.get("push_on_issue", True))
        if save_host_metric_fn is None or security_enabled:
            ensure_metrics_writer()

        async def collect_host_metrics(
            host_cfg: dict[str, Any],
            *,
            collect_load: bool,
            collect_network: bool,
            timeout_seconds: int,
        ) -> dict[str, Any]:
            sample: dict[str, Any]
            if collect_host_metrics_fn is not None:
                sample = dict(
                    await _maybe_await(
                        collect_host_metrics_fn(
                            host_cfg,
                            collect_load=bool(collect_load),
                            collect_network=bool(collect_network),
                            timeout_seconds=int(timeout_seconds),
                        )
                    )
                    or {}
                )
            else:
                collect_host_metrics_module = importlib.import_module(
                    "logdog.collector.host_metrics_probe"
                )
                collect_host_metrics_impl = getattr(
                    collect_host_metrics_module,
                    "collect_host_metrics_for_host",
                )
                sample = dict(
                    await _maybe_await(
                        collect_host_metrics_impl(
                            host_cfg,
                            collect_load=bool(collect_load),
                            collect_network=bool(collect_network),
                            timeout_seconds=int(timeout_seconds),
                        )
                    )
                    or {}
                )

            if security_enabled:
                assess_host_security_impl = getattr(
                    importlib.import_module("logdog.collector.host_metrics_probe"),
                    "assess_host_security",
                    None,
                )
                if callable(assess_host_security_impl):
                    security_report = await _maybe_await(
                        assess_host_security_impl(
                            host_cfg,
                            metric_sample=dict(sample),
                        )
                    )
                    if isinstance(security_report, dict):
                        sample["security_check"] = dict(security_report)
            return sample

        async def save_host_metric(sample: dict[str, Any]) -> None:
            if save_host_metric_fn is not None:
                await _maybe_await(save_host_metric_fn(sample))
            elif metrics_writer is not None:
                await metrics_writer.write_host_metric(sample)
            else:
                return

            if not security_enabled:
                return

            if metrics_writer is None:
                return

            host_name = str(sample.get("host_name") or "").strip()
            if host_name == "":
                return

            security_report = sample.get("security_check")
            if not isinstance(security_report, dict):
                return
            issues = security_report.get("issues")
            has_issues = isinstance(issues, list) and len(issues) > 0
            previous_has_issues = bool(
                host_security_issue_open_state.get(host_name, False)
            )
            host_security_issue_open_state[host_name] = has_issues

            if not has_issues or previous_has_issues:
                return

            analysis_text = str(
                security_report.get("analysis")
                or f"Host {host_name} SSH/系统安全检查发现风险，请尽快处理。"
            )

            pushed = False
            if security_push_on_issue and resolved_notify_router is not None:
                notify_sender = getattr(resolved_notify_router, "send", None)
                if callable(notify_sender):
                    try:
                        pushed = await _send_security_issue_notification(
                            notify_sender=notify_sender,
                            host_name=host_name,
                            message=analysis_text,
                            security_report=security_report,
                        )
                    except Exception:  # noqa: BLE001
                        logger.exception(
                            "security check notification failed host=%s",
                            host_name,
                        )

            await metrics_writer.write_alert(
                {
                    "host": host_name,
                    "container_id": "",
                    "container_name": "system",
                    "category": "SECURITY_CHECK",
                    "line": analysis_text,
                    "analysis": analysis_text,
                    "pushed": pushed,
                    "security_check": security_report,
                }
            )

        host_metrics_sampler = HostMetricsSampler(
            host_manager=resolved_host_manager,
            collect_host_metrics=collect_host_metrics,
            save_metric=save_host_metric,
            collect_load=bool(settings.get("collect_load", True)),
            collect_network=bool(settings.get("collect_network", True)),
            timeout_seconds=8,
        )
        return HostMetricsSamplingScheduler(
            sampler=host_metrics_sampler,
            interval_seconds=int(settings.get("sample_interval_seconds", 30)),
        )

    resolved_host_metrics_scheduler = host_metrics_scheduler
    if resolved_host_metrics_scheduler is None:
        resolved_host_metrics_scheduler = build_host_metrics_scheduler_for_settings(
            current_host_system_settings
        )

    if retention_scheduler is None and enable_retention_cleanup:
        if metrics_writer is None:
            ensure_metrics_writer()

        async def run_cleanup() -> None:
            if metrics_writer is None:
                return
            await metrics_writer.cleanup(resolved_retention_config)

        retention_scheduler = RetentionCleanupScheduler(
            run_cleanup=run_cleanup,
            interval_seconds=resolved_retention_interval_seconds,
        )

    def build_report_scheduler_for_config(app_cfg: dict[str, Any] | None):
        global_schedules = []
        if isinstance(app_cfg, dict):
            raw_global_schedules = app_cfg.get("global_schedules")
            if isinstance(raw_global_schedules, list):
                global_schedules = [
                    dict(item)
                    for item in raw_global_schedules
                    if isinstance(item, dict)
                ]

        has_host_schedules = False
        for status in resolved_host_manager.list_host_statuses():
            host_cfg = resolved_host_manager.get_host_config(
                str(status.get("name") or "")
            )
            if not isinstance(host_cfg, dict):
                continue
            host_schedules = host_cfg.get("schedules")
            if isinstance(host_schedules, list) and host_schedules:
                has_host_schedules = True
                break
        if not has_host_schedules and not global_schedules:
            return None
        return ReportScheduler(
            host_manager=resolved_host_manager,
            run_host_schedule=schedule_report_runner.run_host_schedule,
            run_global_schedule=schedule_report_runner.run_global_schedule,
            global_schedules=global_schedules,
        )

    resolved_report_scheduler = report_scheduler
    if resolved_report_scheduler is None:
        resolved_report_scheduler = build_report_scheduler_for_config(
            resolved_app_config
        )

    watch_primitives_available = resolved_docker_client_pool is None or all(
        callable(getattr(resolved_docker_client_pool, attr, None))
        for attr in (
            "list_containers_for_host",
            "stream_container_logs",
            "stream_docker_events",
        )
    )

    resolved_watch_manager = watch_manager
    if (
        resolved_watch_manager is None
        and enable_watch_manager
        and watch_primitives_available
    ):
        watcher_metrics_writer = ensure_metrics_writer()
        manager_ref: WatchManager | None = None

        async def save_alert(payload: dict[str, Any]) -> bool:
            await watcher_metrics_writer.write_alert(payload)
            return True

        async def save_storm_event(payload: dict[str, Any]) -> bool:
            await watcher_metrics_writer.write_storm_event(payload)
            return True

        async def mute_checker(
            *,
            host: str,
            container_id: str,
            category: str,
            at_time: str | None = None,
        ):
            return await watcher_metrics_writer.find_active_mute(
                host=host,
                container_id=container_id,
                category=category,
                at_time=at_time,
            )

        def build_log_watcher(host: dict[str, Any]) -> LogStreamWatcher:
            watcher_kwargs: dict[str, Any] = {
                "host": host,
                "list_containers": list_containers,
                "stream_logs": stream_logs,
                "notifier_send": getattr(resolved_notify_router, "send", None),
                "save_alert": save_alert,
                "metrics_writer": watcher_metrics_writer,
                "prompt_template": str(host.get("prompt_template") or "default_alert"),
                "output_template": str(host.get("output_template") or "standard"),
                "config": host,
            }
            if storm_controller is not None:
                watcher_kwargs["storm_controller"] = storm_controller
            return LogStreamWatcher(**watcher_kwargs)

        def build_event_watcher(host: dict[str, Any]) -> EventStreamWatcher:
            watcher_kwargs: dict[str, Any] = {
                "host": host,
                "stream_events": stream_events,
                "run_alert": run_alert_once,
                "notifier_send": getattr(resolved_notify_router, "send", None),
                "save_alert": save_alert,
                "save_storm_event": save_storm_event,
                "mute_checker": mute_checker,
                "prompt_template": str(host.get("prompt_template") or "default_alert"),
                "output_template": str(host.get("output_template") or "standard"),
                "config": host,
                "cooldown_store": CooldownStore(),
                "refresh_host": (
                    None if manager_ref is None else manager_ref.refresh_host
                ),
            }
            if storm_controller is not None:
                watcher_kwargs["storm_controller"] = storm_controller
            return EventStreamWatcher(**watcher_kwargs)

        manager_ref = WatchManager(
            host_manager=resolved_host_manager,
            log_watcher_factory=build_log_watcher,
            event_watcher_factory=build_event_watcher,
        )
        resolved_watch_manager = manager_ref
    elif resolved_watch_manager is None and enable_watch_manager:
        logger.warning(
            "watch manager disabled: docker streaming primitives unavailable"
        )

    existing_status_handler = getattr(resolved_host_manager, "_on_status_change", None)
    combined_status_handler = _build_host_status_change_handler(
        resolved_status_notify_sender,
        extra_handlers=(
            existing_status_handler,
            getattr(resolved_watch_manager, "handle_host_status_change", None),
        ),
    )
    if hasattr(resolved_host_manager, "_on_status_change"):
        resolved_host_manager._on_status_change = combined_status_handler

    app_lifespan_running = False

    @asynccontextmanager
    async def app_lifespan(_app: FastAPI) -> AsyncIterator[None]:
        nonlocal app_lifespan_running
        await resolved_host_manager.startup_check()
        scheduler = metrics_scheduler
        host_metrics_job = resolved_host_metrics_scheduler
        telegram_runtime = getattr(_app.state, "telegram_runtime", None)
        report_job = resolved_report_scheduler
        app_lifespan_running = True
        if scheduler is not None:
            try:
                await scheduler.start()
            except RuntimeError as exc:
                logger.warning("metrics scheduler disabled: %s", exc)
        if host_metrics_job is not None:
            try:
                await host_metrics_job.start()
            except RuntimeError as exc:
                logger.warning("host metrics scheduler disabled: %s", exc)
        retention_job = retention_scheduler
        if retention_job is not None:
            try:
                await retention_job.start()
            except RuntimeError as exc:
                logger.warning("retention scheduler disabled: %s", exc)
        watcher_job = resolved_watch_manager
        if watcher_job is not None:
            await watcher_job.start()
        if report_job is not None:
            await report_job.start()
        if telegram_runtime is not None:
            await telegram_runtime.start()
        try:
            yield
        finally:
            app_lifespan_running = False
            current_telegram_runtime = getattr(_app.state, "telegram_runtime", None)
            if current_telegram_runtime is not None:
                await current_telegram_runtime.shutdown()
            if (
                telegram_runtime is not None
                and telegram_runtime is not current_telegram_runtime
            ):
                await telegram_runtime.shutdown()

            current_report_job = getattr(_app.state, "report_scheduler", None)
            if current_report_job is not None:
                await current_report_job.shutdown()
            if report_job is not None and report_job is not current_report_job:
                await report_job.shutdown()
            if watcher_job is not None:
                await watcher_job.shutdown()
            await cancel_pending_dedup_tasks()
            if retention_job is not None:
                await retention_job.shutdown()
            if host_metrics_job is not None:
                await host_metrics_job.shutdown()
            if scheduler is not None:
                await scheduler.shutdown()
            if resolved_docker_client_pool is not None:
                await resolved_docker_client_pool.close_all()
            if metrics_writer is not None:
                await metrics_writer.close()

    lifespan_handler = app_lifespan
    app = FastAPI(title="LogDog", lifespan=lifespan_handler)
    app.state.host_manager = resolved_host_manager
    app.state.run_alert_once = run_alert_once
    app.state.metrics_scheduler = metrics_scheduler
    app.state.host_metrics_scheduler = resolved_host_metrics_scheduler
    app.state.retention_scheduler = retention_scheduler
    app.state.docker_client_pool = resolved_docker_client_pool
    app.state.notify_router = resolved_notify_router
    app.state.watch_manager = resolved_watch_manager
    app.state.report_scheduler = resolved_report_scheduler
    app.state.metrics_writer = metrics_writer
    app.state.tool_registry = create_tool_registry(
        host_manager=resolved_host_manager,
        metrics_writer_factory=ensure_metrics_writer,
        app_config=current_app_config,
        docker_client_pool=resolved_docker_client_pool,
    )
    _llm_cfg = (current_app_config or {}).get("llm") or {}
    _llm_model = _llm_cfg.get("default_model") or _llm_cfg.get("model") or None
    _llm_params = resolve_llm_params(_llm_model, _llm_cfg if isinstance(_llm_cfg, dict) else None)
    app.state.chat_runtime = build_chat_runtime(
        tool_registry=app.state.tool_registry,
        model=_llm_params.model or None,
        api_base=_llm_params.api_base or None,
        api_key=_llm_params.api_key or None,
    )
    app.state.telegram_runtime = _build_telegram_runtime_from_config(
        app_config=current_app_config,
        chat_runtime=app.state.chat_runtime,
        message_mode_setter=lambda mode: set_notify_message_mode("telegram", mode),
        message_mode_getter=lambda: get_notify_message_mode("telegram"),
        telegram_application_factory=telegram_application_factory,
        telegram_handler_binder=telegram_handler_binder,
    )
    app.state.schedule_report_runner = schedule_report_runner
    app.state.storm_controller = storm_controller
    app.state.app_config = current_app_config
    app.state.notify_message_modes = current_message_modes
    app.state.get_notify_message_mode = get_notify_message_mode
    app.state.set_notify_message_mode = set_notify_message_mode
    app.state.reload_degraded = None

    async def default_reload_action() -> dict[str, Any]:
        nonlocal current_app_config
        nonlocal current_host_system_settings
        nonlocal resolved_notify_router
        nonlocal resolved_status_notify_sender
        nonlocal resolved_report_scheduler
        nonlocal resolved_host_metrics_scheduler
        nonlocal storm_controller

        if reload_config_path is None:
            return {"ok": True}

        old_notify_router = resolved_notify_router
        old_report_scheduler = resolved_report_scheduler
        old_storm_controller = storm_controller
        old_tool_registry = app.state.tool_registry
        old_chat_runtime = app.state.chat_runtime
        old_telegram_runtime = app.state.telegram_runtime
        old_status_sender = resolved_status_notify_sender
        old_app_config = current_app_config
        old_host_system_settings = dict(current_host_system_settings)
        old_host_metrics_scheduler = resolved_host_metrics_scheduler
        old_message_modes = dict(current_message_modes)
        old_host_security_issue_state = dict(host_security_issue_open_state)
        old_status_change_handler = getattr(
            resolved_host_manager, "_on_status_change", None
        )

        host_manager_snapshot: dict[str, Any] | None = None
        host_snapshot_fn = getattr(resolved_host_manager, "snapshot_state", None)
        if callable(host_snapshot_fn):
            snapshot_value = host_snapshot_fn()
            if isinstance(snapshot_value, dict):
                host_manager_snapshot = dict(snapshot_value)

        watch_manager_snapshot: dict[str, Any] | None = None
        watch_snapshot_fn = getattr(resolved_watch_manager, "snapshot_state", None)
        if callable(watch_snapshot_fn):
            snapshot_value = watch_snapshot_fn()
            if isinstance(snapshot_value, dict):
                watch_manager_snapshot = dict(snapshot_value)

        new_report_scheduler_started = False
        new_host_metrics_scheduler_started = False
        old_telegram_stopped = False
        candidate_report_scheduler = None
        candidate_host_metrics_scheduler = None
        candidate_telegram_runtime = None
        old_report_scheduler_running = _is_scheduler_running(old_report_scheduler)
        old_host_metrics_scheduler_running = _is_scheduler_running(
            old_host_metrics_scheduler
        )

        try:
            new_app_config = load_app_config(reload_config_path)
            new_runtime_settings = resolve_runtime_settings(new_app_config)
            candidate_host_system_settings = dict(new_runtime_settings["host_system"])
            new_hosts = expand_effective_hosts(new_app_config)
            summary = await resolved_host_manager.reload_hosts(new_hosts)
            active_host_names = {
                str(item.get("name") or "").strip()
                for item in resolved_host_manager.list_host_statuses()
                if str(item.get("name") or "").strip() != ""
            }
            for host_name in list(host_security_issue_open_state.keys()):
                if host_name not in active_host_names:
                    host_security_issue_open_state.pop(host_name, None)

            if explicit_notify_router_provided:
                candidate_notify_router = resolved_notify_router
            else:
                candidate_notify_router = _resolve_notify_router(
                    notify_router=None,
                    host_manager=resolved_host_manager,
                    app_config=new_app_config,
                    telegram_send_func=resolved_telegram_send_func,
                    wechat_send_func=resolved_wechat_send_func,
                    weixin_send_func=resolved_weixin_send_func,
                    wecom_send_func=resolved_wecom_send_func,
                    message_mode_getter=get_notify_message_mode,
                    failure_recorder=_build_default_notify_failure_recorder(
                        ensure_metrics_writer
                    ),
                )

            if explicit_status_notify_sender_provided:
                candidate_status_sender = status_notify_sender
            else:
                candidate_status_sender = _resolve_status_notify_sender(
                    status_notify_sender=None,
                    notify_router=candidate_notify_router,
                )

            candidate_storm_controller = _build_alert_storm_controller(
                app_config=new_app_config,
                controller_cls=AlertStormController,
            )

            candidate_tool_registry = create_tool_registry(
                host_manager=resolved_host_manager,
                metrics_writer_factory=ensure_metrics_writer,
                app_config=new_app_config,
                docker_client_pool=resolved_docker_client_pool,
            )
            _new_llm_cfg = new_app_config.get("llm") or {}
            _new_llm_model = _new_llm_cfg.get("default_model") or _new_llm_cfg.get("model") or None
            _new_llm_params = resolve_llm_params(_new_llm_model, _new_llm_cfg if isinstance(_new_llm_cfg, dict) else None)
            candidate_chat_runtime = build_chat_runtime(
                tool_registry=candidate_tool_registry,
                model=_new_llm_params.model or None,
                api_base=_new_llm_params.api_base or None,
                api_key=_new_llm_params.api_key or None,
            )
            candidate_telegram_runtime = _build_telegram_runtime_from_config(
                app_config=new_app_config,
                chat_runtime=candidate_chat_runtime,
                message_mode_setter=lambda mode: set_notify_message_mode(
                    "telegram", mode
                ),
                message_mode_getter=lambda: get_notify_message_mode("telegram"),
                telegram_application_factory=telegram_application_factory,
                telegram_handler_binder=telegram_handler_binder,
            )

            candidate_report_scheduler = build_report_scheduler_for_config(
                new_app_config
            )
            candidate_host_metrics_scheduler = (
                build_host_metrics_scheduler_for_settings(
                    candidate_host_system_settings
                )
            )

            if app_lifespan_running:
                if candidate_report_scheduler is None:
                    if (
                        old_report_scheduler_running
                        and old_report_scheduler is not None
                    ):
                        await old_report_scheduler.shutdown()
                else:
                    await candidate_report_scheduler.start()
                    new_report_scheduler_started = True
                    if (
                        old_report_scheduler_running
                        and old_report_scheduler is not None
                    ):
                        await old_report_scheduler.shutdown()

            if app_lifespan_running:
                if candidate_host_metrics_scheduler is None:
                    if (
                        old_host_metrics_scheduler is not None
                        and old_host_metrics_scheduler_running
                    ):
                        await old_host_metrics_scheduler.shutdown()
                else:
                    await candidate_host_metrics_scheduler.start()
                    new_host_metrics_scheduler_started = True
                    if (
                        old_host_metrics_scheduler is not None
                        and old_host_metrics_scheduler_running
                    ):
                        await old_host_metrics_scheduler.shutdown()

            if app_lifespan_running:
                if candidate_telegram_runtime is not None:
                    await candidate_telegram_runtime.start()
                if old_telegram_runtime is not None:
                    await old_telegram_runtime.shutdown()
                    old_telegram_stopped = True

            resolved_notify_router = candidate_notify_router
            resolved_status_notify_sender = candidate_status_sender
            storm_controller = candidate_storm_controller
            resolved_report_scheduler = candidate_report_scheduler
            resolved_host_metrics_scheduler = candidate_host_metrics_scheduler
            current_app_config = new_app_config
            current_host_system_settings = candidate_host_system_settings
            current_message_modes.update(
                _resolve_explicit_notify_message_modes(new_app_config)
            )

            app.state.notify_router = resolved_notify_router
            app.state.storm_controller = storm_controller
            app.state.report_scheduler = resolved_report_scheduler
            app.state.host_metrics_scheduler = resolved_host_metrics_scheduler
            app.state.tool_registry = candidate_tool_registry
            app.state.chat_runtime = candidate_chat_runtime
            app.state.telegram_runtime = candidate_telegram_runtime
            app.state.app_config = current_app_config
            app.state.notify_message_modes = current_message_modes

            if hasattr(resolved_host_manager, "_on_status_change"):
                resolved_host_manager._on_status_change = (
                    _build_host_status_change_handler(
                        resolved_status_notify_sender,
                        extra_handlers=(
                            existing_status_handler,
                            getattr(
                                resolved_watch_manager,
                                "handle_host_status_change",
                                None,
                            ),
                        ),
                    )
                )

            if resolved_watch_manager is not None and hasattr(
                resolved_watch_manager, "reload_host_configs"
            ):
                changed_hosts = list(summary.get("added", [])) + list(
                    summary.get("updated", [])
                )
                await _maybe_await(
                    resolved_watch_manager.reload_host_configs(changed_hosts)
                )

            app.state.reload_degraded = None

            return {"ok": True, **summary}
        except Exception as exc:  # noqa: BLE001
            recovery_errors: list[str] = []
            if candidate_telegram_runtime is not None:
                try:
                    await candidate_telegram_runtime.shutdown()
                except Exception:  # noqa: BLE001
                    logger.exception("failed to shutdown candidate telegram runtime")

            if new_report_scheduler_started and candidate_report_scheduler is not None:
                try:
                    await candidate_report_scheduler.shutdown()
                except Exception:  # noqa: BLE001
                    logger.exception("failed to shutdown candidate report scheduler")

            if (
                new_host_metrics_scheduler_started
                and candidate_host_metrics_scheduler is not None
            ):
                try:
                    await candidate_host_metrics_scheduler.shutdown()
                except Exception:  # noqa: BLE001
                    logger.exception(
                        "failed to shutdown candidate host metrics scheduler"
                    )

            if old_report_scheduler_running and old_report_scheduler is not None:
                try:
                    if not _is_scheduler_running(old_report_scheduler):
                        await old_report_scheduler.start()
                except Exception:  # noqa: BLE001
                    logger.exception("failed to restore previous report scheduler")

            if (
                app_lifespan_running
                and old_telegram_stopped
                and old_telegram_runtime is not None
            ):
                try:
                    await old_telegram_runtime.start()
                except Exception:  # noqa: BLE001
                    logger.exception("failed to restore previous telegram runtime")

            if (
                app_lifespan_running
                and old_host_metrics_scheduler_running
                and old_host_metrics_scheduler is not None
            ):
                try:
                    if not _is_scheduler_running(old_host_metrics_scheduler):
                        await old_host_metrics_scheduler.start()
                except Exception:  # noqa: BLE001
                    logger.exception(
                        "failed to restore previous host metrics scheduler"
                    )
                    recovery_errors.append("host_metrics_scheduler_restore")

            restore_host_state = getattr(resolved_host_manager, "restore_state", None)
            if host_manager_snapshot is not None and callable(restore_host_state):
                try:
                    restore_host_state(host_manager_snapshot)
                except Exception:  # noqa: BLE001
                    logger.exception("failed to restore previous host manager state")
                    recovery_errors.append("host_manager_restore")

            restore_watch_state = getattr(resolved_watch_manager, "restore_state", None)
            if watch_manager_snapshot is not None and callable(restore_watch_state):
                try:
                    maybe_awaitable = restore_watch_state(watch_manager_snapshot)
                    if inspect.isawaitable(maybe_awaitable):
                        await maybe_awaitable
                except Exception:  # noqa: BLE001
                    logger.exception("failed to restore previous watch manager state")
                    recovery_errors.append("watch_manager_restore")

            resolved_notify_router = old_notify_router
            resolved_report_scheduler = old_report_scheduler
            resolved_host_metrics_scheduler = old_host_metrics_scheduler
            storm_controller = old_storm_controller
            resolved_status_notify_sender = old_status_sender
            current_app_config = old_app_config
            current_host_system_settings = old_host_system_settings

            app.state.notify_router = old_notify_router
            app.state.report_scheduler = old_report_scheduler
            app.state.host_metrics_scheduler = old_host_metrics_scheduler
            app.state.storm_controller = old_storm_controller
            app.state.tool_registry = old_tool_registry
            app.state.chat_runtime = old_chat_runtime
            app.state.telegram_runtime = old_telegram_runtime
            app.state.app_config = old_app_config

            current_message_modes.clear()
            current_message_modes.update(old_message_modes)
            host_security_issue_open_state.clear()
            host_security_issue_open_state.update(old_host_security_issue_state)
            app.state.notify_message_modes = current_message_modes

            if hasattr(resolved_host_manager, "_on_status_change"):
                resolved_host_manager._on_status_change = old_status_change_handler

            if recovery_errors:
                app.state.reload_degraded = {
                    "active": True,
                    "errors": list(recovery_errors),
                }
            else:
                app.state.reload_degraded = None

            await _send_reload_failure_notice(old_notify_router, exc)
            raise

    effective_reload_action = reload_action or default_reload_action
    app.state.reload_action = effective_reload_action
    ws_ticket_store = WsTicketStore()
    app.state.ws_ticket_store = ws_ticket_store

    async def list_send_failed(limit: int) -> list[dict[str, Any]]:
        writer = ensure_metrics_writer()
        return await writer.list_send_failed(limit)

    def issue_ws_ticket(token_subject: str) -> dict[str, Any]:
        return ws_ticket_store.issue(token_subject)

    api_router = create_api_router(
        web_auth_token=resolved_web_auth_token,
        web_admin_token=resolved_web_admin_token,
        reload_action=effective_reload_action,
        issue_ws_ticket_action=issue_ws_ticket,
        list_send_failed_action=list_send_failed,
        list_hosts_action=resolved_host_manager.list_host_statuses,
        list_containers_action=list_containers,
        list_alerts_action=list_alert_rows,
        query_metrics_action=query_metric_points,
        query_host_system_metrics_action=query_host_metric_points,
        list_mutes_action=list_mute_rows,
        list_storm_events_action=list_storm_rows,
        storm_event_stats_action=storm_stats_rows,
    )
    chat_router = create_chat_router(
        web_auth_token=resolved_web_auth_token,
        chat_runtime=app.state.chat_runtime,
        chat_runtime_getter=lambda: app.state.chat_runtime,
        ws_ticket_consumer=ws_ticket_store.consume,
    )

    # Keep a single deployment entrypoint while reusing tested API/WS sub-app routes.
    app.include_router(api_router)
    app.include_router(chat_router)
    mount_frontend(app)

    return app


def _resolve_app_config(
    *,
    app_config: dict[str, Any] | None,
    config_path: str | None,
    explicit_hosts_provided: bool,
) -> dict[str, Any] | None:
    if app_config is not None:
        return app_config

    if explicit_hosts_provided and config_path is None:
        return None

    resolved_config_path = resolve_config_path(config_path)
    has_env_config = os.getenv("LOGDOG_CONFIG", "").strip() != ""
    if (
        config_path is None
        and not has_env_config
        and not os.path.exists(resolved_config_path)
    ):
        return None

    return load_app_config(resolved_config_path)


def _resolve_reload_config_path(
    *,
    app_config: dict[str, Any] | None,
    config_path: str | None,
    explicit_hosts_provided: bool,
) -> str | None:
    if app_config is not None and config_path is None:
        return None
    if explicit_hosts_provided and config_path is None:
        return None
    resolved_config_path = resolve_config_path(config_path)
    has_env_config = os.getenv("LOGDOG_CONFIG", "").strip() != ""
    if (
        config_path is None
        and not has_env_config
        and not os.path.exists(resolved_config_path)
    ):
        return None
    return resolved_config_path


def _build_alert_storm_controller(
    *,
    app_config: dict[str, Any] | None,
    controller_cls: Any,
) -> Any | None:
    if not isinstance(app_config, dict):
        return None
    storm_cfg = app_config.get("alert_storm")
    if storm_cfg is None:
        return None
    if not isinstance(storm_cfg, dict):
        raise TypeError("app_config.alert_storm must be dict")
    if not bool(storm_cfg.get("enabled", False)):
        return None
    return controller_cls(
        enabled=True,
        window_seconds=int(storm_cfg.get("window_seconds", 120)),
        threshold=int(storm_cfg.get("threshold", 5)),
        suppress_minutes=int(storm_cfg.get("suppress_minutes", 10)),
    )


async def _maybe_await(value):
    if inspect.isawaitable(value):
        return await value
    return value


def _is_scheduler_running(scheduler: Any | None) -> bool:
    if scheduler is None:
        return False
    return getattr(scheduler, "_scheduler", None) is not None


async def _send_reload_failure_notice(
    notify_router: Any | None, exc: Exception
) -> None:
    if notify_router is None:
        return
    sender = getattr(notify_router, "send", None)
    if not callable(sender):
        return
    message = _sanitize_reload_error_message(exc)
    try:
        result = sender("__global__", message, "CONFIG_RELOAD")
        if inspect.isawaitable(result):
            await result
    except Exception:  # noqa: BLE001
        logger.exception("failed to emit reload failure notice")


_RELOAD_ERROR_REDACT_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"(?i)bearer\s+\S+"),
    re.compile(r"(?i)\bapi[_-]?key\b\s*[:=]\s*\S+"),
    re.compile(r"(?i)\btoken\b\s*[:=]\s*\S+"),
    re.compile(r"(?i)\bpassword\b\s*[:=]\s*\S+"),
    re.compile(r'(?i)"(?:api[_-]?key|token|password)"\s*:\s*"[^"]*"'),
    re.compile(r"(?i)'(?:api[_-]?key|token|password)'\s*:\s*'[^']*'"),
)


def _sanitize_reload_error_message(exc: Exception, *, max_chars: int = 256) -> str:
    message = f"config reload failed: {type(exc).__name__}"
    detail = str(exc or "").strip()
    if detail != "":
        detail_text = detail
        for pattern in _RELOAD_ERROR_REDACT_PATTERNS:
            detail_text = pattern.sub("***REDACTED***", detail_text)
        if len(detail_text) > max_chars:
            detail_text = detail_text[:max_chars]
        message = f"{message}: {detail_text}"
    return message


async def _send_security_issue_notification(
    *,
    notify_sender: Any,
    host_name: str,
    message: str,
    security_report: dict[str, Any],
) -> bool:
    try:
        result = notify_sender(
            host_name,
            message,
            "SECURITY_CHECK",
            context={"security_check": dict(security_report)},
        )
    except TypeError:
        result = notify_sender(host_name, message, "SECURITY_CHECK")
    return bool(await _maybe_await(result))


def _build_host_status_change_handler(
    status_notify_sender: StatusNotifySender | None,
    *,
    extra_handlers: tuple[Any | None, ...] = (),
):
    handlers = [handler for handler in extra_handlers if handler is not None]
    if status_notify_sender is None and not handlers:
        return None

    async def on_status_change(payload: dict[str, Any]) -> None:
        host_name = str(payload.get("name") or "unknown")
        old_status = str(payload.get("old_status") or "unknown")
        new_status = str(payload.get("new_status") or "unknown")
        message = f"Host {host_name} status changed: {old_status} -> {new_status}"
        last_error = payload.get("last_error")
        if last_error:
            message += f" | error: {last_error}"
        if status_notify_sender is not None:
            try:
                result = status_notify_sender(host_name, message, "HOST_STATUS")
                if inspect.isawaitable(result):
                    await result
            except Exception:  # noqa: BLE001
                logger.exception(
                    "status notify sender failed host=%s old=%s new=%s",
                    host_name,
                    old_status,
                    new_status,
                )
        for handler in handlers:
            try:
                result = handler(payload)
                if inspect.isawaitable(result):
                    await result
            except Exception:  # noqa: BLE001
                logger.exception(
                    "status change extra handler failed host=%s old=%s new=%s",
                    host_name,
                    old_status,
                    new_status,
                )

    return on_status_change


def _resolve_status_notify_sender(
    *,
    status_notify_sender: StatusNotifySender | None,
    notify_router: Any | None,
) -> StatusNotifySender | None:
    if status_notify_sender is not None:
        return status_notify_sender
    if notify_router is None:
        return None
    sender = getattr(notify_router, "send", None)
    if sender is None:
        raise TypeError("notify_router must expose send(host, message, category)")
    return sender


def _resolve_notify_router(
    *,
    notify_router: Any | None,
    host_manager: HostManager | None,
    app_config: dict[str, Any] | None,
    telegram_send_func: Any | None,
    wechat_send_func: Any | None,
    weixin_send_func: Any | None = None,
    wecom_send_func: Any | None = None,
    message_mode_getter: Callable[[str], str] | None = None,
    failure_recorder: Any | None = None,
) -> Any | None:
    if notify_router is not None:
        return notify_router
    if host_manager is None:
        return None

    host_notifiers: dict[str, list[BaseNotifier]] = {}
    host_channel_notifiers: dict[str, dict[str, list[BaseNotifier]]] = {}
    global_notify_cfg = _resolve_global_notify_config(app_config)
    for status in host_manager.list_host_statuses():
        host_name = str(status.get("name") or "").strip()
        if host_name == "":
            continue
        host_cfg = host_manager.get_host_config(host_name)
        if host_cfg is None:
            continue
        per_host_notifiers, per_host_channel_notifiers = _build_host_notifiers(
            host_config=host_cfg,
            global_notify_cfg=global_notify_cfg,
            telegram_send_func=telegram_send_func,
            wechat_send_func=wechat_send_func,
            weixin_send_func=weixin_send_func,
            wecom_send_func=wecom_send_func,
            message_mode_getter=message_mode_getter,
        )
        if per_host_notifiers:
            host_notifiers[host_name] = per_host_notifiers
            host_channel_notifiers[host_name] = per_host_channel_notifiers

    global_notifiers = _build_global_schedule_notifiers(
        schedule={},
        app_config=app_config,
        telegram_send_func=telegram_send_func,
        wechat_send_func=wechat_send_func,
        weixin_send_func=weixin_send_func,
        wecom_send_func=wecom_send_func,
        message_mode_getter=message_mode_getter,
    )
    if global_notifiers:
        host_notifiers["__global__"] = list(global_notifiers)
        host_channel_notifiers["__global__"] = _group_host_notifiers_by_channel(
            {"__global__": list(global_notifiers)}
        ).get("__global__", {})

    if host_notifiers:
        routing_cfg = global_notify_cfg.get("routing")
        if routing_cfg is not None and not isinstance(routing_cfg, dict):
            raise TypeError("app_config.notify.routing must be dict")
        routing_policy = build_notify_routing_policy(
            dict(routing_cfg) if isinstance(routing_cfg, dict) else None
        )

        def route_selector(host: str, category: str, context: dict[str, Any] | None):
            host_name = str(host or "").strip()
            host_level_notifiers = host_notifiers.get(host_name, [])
            resolved_host = host_name
            if not host_level_notifiers:
                host_level_notifiers = host_notifiers.get("__global__", [])
                resolved_host = "__global__"
                if not host_level_notifiers:
                    return []
                logger.debug(
                    "notify route_selector falling back to __global__ for host=%s",
                    host_name,
                )
            plan = routing_policy.resolve(
                host=host_name,
                category=category,
                context=dict(context or {}),
            )
            selected_channels = [
                str(item).strip().lower() for item in list(plan.get("channels") or [])
            ]
            if not selected_channels:
                return list(host_level_notifiers)

            channel_map = host_channel_notifiers.get(resolved_host, {})
            selected_notifiers: list[BaseNotifier] = []
            seen_ids: set[int] = set()
            for channel in selected_channels:
                for notifier in channel_map.get(channel, []):
                    key = id(notifier)
                    if key in seen_ids:
                        continue
                    seen_ids.add(key)
                    selected_notifiers.append(notifier)
            if selected_notifiers:
                return selected_notifiers
            if str(plan.get("rule") or "") == "default":
                return list(host_level_notifiers)
            logger.warning(
                "notify routing selected unknown channels host=%s channels=%s rule=%s",
                host_name,
                selected_channels,
                plan.get("rule"),
            )
            return []

        return NotifyRouter(
            [],
            host_notifiers=host_notifiers,
            failure_recorder=failure_recorder,
            route_selector=route_selector,
        )
    return None


def _resolve_global_notify_config(app_config: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(app_config, dict):
        return {}
    notify_cfg = app_config.get("notify")
    if notify_cfg is None:
        return {}
    if not isinstance(notify_cfg, dict):
        raise TypeError("app_config.notify must be dict")
    return notify_cfg


def _build_host_notifiers(
    *,
    host_config: dict[str, Any],
    global_notify_cfg: dict[str, Any],
    telegram_send_func: Any | None,
    wechat_send_func: Any | None,
    weixin_send_func: Any | None = None,
    wecom_send_func: Any | None = None,
    message_mode_getter: Callable[[str], str] | None = None,
) -> tuple[list[BaseNotifier], dict[str, list[BaseNotifier]]]:
    notifiers: list[BaseNotifier] = []
    channel_notifiers: dict[str, list[BaseNotifier]] = {}
    host_allowed_channels = _resolve_host_allowed_channels(host_config)

    telegram_cfg = _resolve_host_channel_config(
        host_config=host_config,
        global_notify_cfg=global_notify_cfg,
        channel="telegram",
    )
    if _is_channel_enabled(telegram_cfg) and _channel_allowed(
        "telegram",
        host_allowed_channels,
    ):
        telegram_channel_send_func, telegram_auto_target_supported = (
            _resolve_telegram_channel_send_func(
                channel_cfg=telegram_cfg,
                default_send_func=telegram_send_func,
            )
        )
        telegram_targets = _resolve_notify_targets(
            channel_cfg=telegram_cfg,
            list_field="chat_ids",
            env_var="TELEGRAM_CHAT_ID",
        )
        if telegram_channel_send_func is not None and not telegram_targets:
            if _is_telegram_auto_chat_enabled(
                telegram_cfg
            ) and telegram_auto_target_supported:
                telegram_targets = [TELEGRAM_AUTO_TARGET]
        if telegram_channel_send_func is not None and telegram_targets:
            notifier = TelegramNotifier(
                send_func=_build_multi_target_send_func(
                    telegram_channel_send_func, telegram_targets
                ),
                message_mode_getter=_resolve_channel_message_mode_getter(
                    channel="telegram",
                    channel_cfg=telegram_cfg,
                    message_mode_getter=message_mode_getter,
                ),
            )
            notifiers.append(notifier)
            _add_channel_notifier(channel_notifiers, "telegram", notifier)

    wechat_cfg = _resolve_host_channel_config(
        host_config=host_config,
        global_notify_cfg=global_notify_cfg,
        channel="wechat",
    )
    if _is_channel_enabled(wechat_cfg) and _channel_allowed(
        "wechat",
        host_allowed_channels,
    ):
        wechat_targets = _resolve_notify_targets(
            channel_cfg=wechat_cfg,
            list_field="webhook_urls",
            env_var="WECHAT_WEBHOOK_URL",
        )
        if wechat_send_func is not None and wechat_targets:
            notifier = WechatNotifier(
                send_func=_build_multi_target_send_func(
                    wechat_send_func, wechat_targets
                ),
                message_mode_getter=_resolve_channel_message_mode_getter(
                    channel="wechat",
                    channel_cfg=wechat_cfg,
                    message_mode_getter=message_mode_getter,
                ),
            )
            notifiers.append(notifier)
            _add_channel_notifier(channel_notifiers, "wechat", notifier)

    weixin_cfg = _resolve_host_channel_config(
        host_config=host_config,
        global_notify_cfg=global_notify_cfg,
        channel="weixin",
    )
    if _is_channel_enabled(weixin_cfg) and _channel_allowed(
        "weixin",
        host_allowed_channels,
    ):
        weixin_targets = _resolve_notify_targets_with_aliases(
            channel_cfg=weixin_cfg,
            candidate_fields=(
                "targets",
                "home_channel",
                "home_channels",
                "chat_ids",
                "user_ids",
                "channel_ids",
            ),
            env_var="WEIXIN_TARGET",
        )
        if weixin_send_func is not None and weixin_targets:
            notifier = WeixinNotifier(
                send_func=_build_multi_target_send_func(
                    weixin_send_func,
                    weixin_targets,
                ),
                message_mode_getter=_resolve_channel_message_mode_getter(
                    channel="weixin",
                    channel_cfg=weixin_cfg,
                    message_mode_getter=message_mode_getter,
                ),
            )
            notifiers.append(notifier)
            _add_channel_notifier(channel_notifiers, "weixin", notifier)

    wecom_cfg = _resolve_host_channel_config(
        host_config=host_config,
        global_notify_cfg=global_notify_cfg,
        channel="wecom",
    )
    if _is_channel_enabled(wecom_cfg) and _channel_allowed(
        "wecom",
        host_allowed_channels,
    ):
        wecom_targets = _resolve_notify_targets_with_aliases(
            channel_cfg=wecom_cfg,
            candidate_fields=(
                "targets",
                "home_channel",
                "home_channels",
                "chat_ids",
                "webhook_urls",
                "channel_ids",
            ),
            env_var="WECOM_TARGET",
        )
        if wecom_send_func is not None and wecom_targets:
            notifier = WecomNotifier(
                send_func=_build_multi_target_send_func(wecom_send_func, wecom_targets),
                message_mode_getter=_resolve_channel_message_mode_getter(
                    channel="wecom",
                    channel_cfg=wecom_cfg,
                    message_mode_getter=message_mode_getter,
                ),
            )
            notifiers.append(notifier)
            _add_channel_notifier(channel_notifiers, "wecom", notifier)

    named_channels = _resolve_named_notify_channels(global_notify_cfg)
    for channel_name, channel_cfg in named_channels.items():
        normalized_name = str(channel_name).strip().lower()
        if not _channel_allowed(normalized_name, host_allowed_channels):
            continue
        channel_type = str(channel_cfg.get("type") or "").strip().lower()
        if channel_type == "telegram":
            if not _is_channel_enabled(channel_cfg):
                continue
            telegram_channel_send_func, telegram_auto_target_supported = (
                _resolve_telegram_channel_send_func(
                    channel_cfg=channel_cfg,
                    default_send_func=telegram_send_func,
                )
            )
            if telegram_channel_send_func is None:
                continue
            targets = _resolve_notify_targets(
                channel_cfg=channel_cfg,
                list_field="chat_ids",
                env_var="TELEGRAM_CHAT_ID",
            )
            if (
                not targets
                and _is_telegram_auto_chat_enabled(channel_cfg)
                and telegram_auto_target_supported
            ):
                targets = [TELEGRAM_AUTO_TARGET]
            if not targets:
                continue
            notifier = TelegramNotifier(
                send_func=_build_multi_target_send_func(
                    telegram_channel_send_func, targets
                ),
                message_mode_getter=_resolve_channel_message_mode_getter(
                    channel=normalized_name,
                    channel_cfg=channel_cfg,
                    message_mode_getter=message_mode_getter,
                ),
            )
            notifier.name = normalized_name
            notifiers.append(notifier)
            _add_channel_notifier(channel_notifiers, normalized_name, notifier)
            continue
        if channel_type == "wechat":
            if wechat_send_func is None or not _is_channel_enabled(channel_cfg):
                continue
            targets = _resolve_notify_targets(
                channel_cfg=channel_cfg,
                list_field="webhook_urls",
                env_var="WECHAT_WEBHOOK_URL",
            )
            if not targets:
                continue
            notifier = WechatNotifier(
                send_func=_build_multi_target_send_func(wechat_send_func, targets),
                message_mode_getter=_resolve_channel_message_mode_getter(
                    channel=normalized_name,
                    channel_cfg=channel_cfg,
                    message_mode_getter=message_mode_getter,
                ),
            )
            notifier.name = normalized_name
            notifiers.append(notifier)
            _add_channel_notifier(channel_notifiers, normalized_name, notifier)
            continue

        if channel_type == "weixin":
            if weixin_send_func is None or not _is_channel_enabled(channel_cfg):
                continue
            targets = _resolve_notify_targets_with_aliases(
                channel_cfg=channel_cfg,
                candidate_fields=(
                    "targets",
                    "home_channel",
                    "home_channels",
                    "chat_ids",
                    "user_ids",
                    "channel_ids",
                ),
                env_var="WEIXIN_TARGET",
            )
            if not targets:
                continue
            notifier = WeixinNotifier(
                send_func=_build_multi_target_send_func(weixin_send_func, targets),
                message_mode_getter=_resolve_channel_message_mode_getter(
                    channel=normalized_name,
                    channel_cfg=channel_cfg,
                    message_mode_getter=message_mode_getter,
                ),
            )
            notifier.name = normalized_name
            notifiers.append(notifier)
            _add_channel_notifier(channel_notifiers, normalized_name, notifier)
            continue

        if channel_type == "wecom":
            if wecom_send_func is None or not _is_channel_enabled(channel_cfg):
                continue
            targets = _resolve_notify_targets_with_aliases(
                channel_cfg=channel_cfg,
                candidate_fields=(
                    "targets",
                    "home_channel",
                    "home_channels",
                    "chat_ids",
                    "webhook_urls",
                    "channel_ids",
                ),
                env_var="WECOM_TARGET",
            )
            if not targets:
                continue
            notifier = WecomNotifier(
                send_func=_build_multi_target_send_func(wecom_send_func, targets),
                message_mode_getter=_resolve_channel_message_mode_getter(
                    channel=normalized_name,
                    channel_cfg=channel_cfg,
                    message_mode_getter=message_mode_getter,
                ),
            )
            notifier.name = normalized_name
            notifiers.append(notifier)
            _add_channel_notifier(channel_notifiers, normalized_name, notifier)
            continue

        raise ValueError(
            f"app_config.notify.channels.{normalized_name}.type must be 'telegram'|'wechat'|'weixin'|'wecom'"
        )

    return notifiers, channel_notifiers


def _group_host_notifiers_by_channel(
    host_notifiers: dict[str, list[BaseNotifier]],
) -> dict[str, dict[str, list[BaseNotifier]]]:
    grouped: dict[str, dict[str, list[BaseNotifier]]] = {}
    for host_name, notifiers in host_notifiers.items():
        per_channel: dict[str, list[BaseNotifier]] = {}
        for notifier in notifiers:
            channel = str(getattr(notifier, "name", "")).strip().lower()
            if channel == "":
                continue
            per_channel.setdefault(channel, []).append(notifier)
        grouped[str(host_name)] = per_channel
    return grouped


def _build_global_schedule_notifiers(
    *,
    schedule: dict[str, Any],
    app_config: dict[str, Any] | None,
    telegram_send_func: Any | None,
    wechat_send_func: Any | None,
    weixin_send_func: Any | None = None,
    wecom_send_func: Any | None = None,
    message_mode_getter: Callable[[str], str] | None = None,
) -> list[BaseNotifier]:
    global_notify_cfg = _resolve_global_notify_config(app_config)
    notify_target = schedule.get("notify_target")
    if notify_target is not None and not isinstance(notify_target, dict):
        raise TypeError("global schedule notify_target must be dict")
    target_cfg = dict(notify_target or {})

    notifiers: list[BaseNotifier] = []

    telegram_cfg = _resolve_global_target_channel_config(
        global_notify_cfg=global_notify_cfg,
        notify_target_cfg=target_cfg,
        channel="telegram",
    )
    if _is_channel_enabled(telegram_cfg):
        telegram_channel_send_func, telegram_auto_target_supported = (
            _resolve_telegram_channel_send_func(
                channel_cfg=telegram_cfg,
                default_send_func=telegram_send_func,
            )
        )
        telegram_targets = _resolve_notify_targets(
            channel_cfg=telegram_cfg,
            list_field="chat_ids",
            env_var="TELEGRAM_CHAT_ID",
        )
        if telegram_channel_send_func is not None and not telegram_targets:
            if _is_telegram_auto_chat_enabled(
                telegram_cfg
            ) and telegram_auto_target_supported:
                telegram_targets = [TELEGRAM_AUTO_TARGET]
        if telegram_channel_send_func is not None and telegram_targets:
            notifiers.append(
                TelegramNotifier(
                    send_func=_build_multi_target_send_func(
                        telegram_channel_send_func, telegram_targets
                    ),
                    message_mode_getter=_resolve_channel_message_mode_getter(
                        channel="telegram",
                        channel_cfg=telegram_cfg,
                        message_mode_getter=message_mode_getter,
                    ),
                )
            )

    wechat_cfg = _resolve_global_target_channel_config(
        global_notify_cfg=global_notify_cfg,
        notify_target_cfg=target_cfg,
        channel="wechat",
    )
    if _is_channel_enabled(wechat_cfg):
        wechat_targets = _resolve_notify_targets(
            channel_cfg=wechat_cfg,
            list_field="webhook_urls",
            env_var="WECHAT_WEBHOOK_URL",
        )
        if wechat_send_func is not None and wechat_targets:
            notifiers.append(
                WechatNotifier(
                    send_func=_build_multi_target_send_func(
                        wechat_send_func, wechat_targets
                    ),
                    message_mode_getter=_resolve_channel_message_mode_getter(
                        channel="wechat",
                        channel_cfg=wechat_cfg,
                        message_mode_getter=message_mode_getter,
                    ),
                )
            )

    weixin_cfg = _resolve_global_target_channel_config(
        global_notify_cfg=global_notify_cfg,
        notify_target_cfg=target_cfg,
        channel="weixin",
    )
    if _is_channel_enabled(weixin_cfg):
        weixin_targets = _resolve_notify_targets_with_aliases(
            channel_cfg=weixin_cfg,
            candidate_fields=(
                "targets",
                "home_channel",
                "home_channels",
                "chat_ids",
                "user_ids",
                "channel_ids",
            ),
            env_var="WEIXIN_TARGET",
        )
        if weixin_send_func is not None and weixin_targets:
            notifiers.append(
                WeixinNotifier(
                    send_func=_build_multi_target_send_func(
                        weixin_send_func, weixin_targets
                    ),
                    message_mode_getter=_resolve_channel_message_mode_getter(
                        channel="weixin",
                        channel_cfg=weixin_cfg,
                        message_mode_getter=message_mode_getter,
                    ),
                )
            )

    wecom_cfg = _resolve_global_target_channel_config(
        global_notify_cfg=global_notify_cfg,
        notify_target_cfg=target_cfg,
        channel="wecom",
    )
    if _is_channel_enabled(wecom_cfg):
        wecom_targets = _resolve_notify_targets_with_aliases(
            channel_cfg=wecom_cfg,
            candidate_fields=(
                "targets",
                "home_channel",
                "home_channels",
                "chat_ids",
                "webhook_urls",
                "channel_ids",
            ),
            env_var="WECOM_TARGET",
        )
        if wecom_send_func is not None and wecom_targets:
            notifiers.append(
                WecomNotifier(
                    send_func=_build_multi_target_send_func(
                        wecom_send_func, wecom_targets
                    ),
                    message_mode_getter=_resolve_channel_message_mode_getter(
                        channel="wecom",
                        channel_cfg=wecom_cfg,
                        message_mode_getter=message_mode_getter,
                    ),
                )
            )

    return notifiers


def _resolve_global_target_channel_config(
    *,
    global_notify_cfg: dict[str, Any],
    notify_target_cfg: dict[str, Any],
    channel: str,
) -> dict[str, Any] | None:
    merged: dict[str, Any] = {}
    global_channel = global_notify_cfg.get(channel)
    if global_channel is not None:
        if not isinstance(global_channel, dict):
            raise TypeError(f"app_config.notify.{channel} must be dict")
        merged.update(global_channel)

    target_channel = notify_target_cfg.get(channel)
    if target_channel is not None:
        if not isinstance(target_channel, dict):
            raise TypeError(f"global schedule notify_target.{channel} must be dict")
        for key, value in target_channel.items():
            if key in {
                "chat_ids",
                "webhook_urls",
                "targets",
                "home_channels",
                "user_ids",
                "channel_ids",
            }:
                if not isinstance(value, (list, tuple)):
                    raise TypeError(
                        f"global schedule notify_target.{channel}.{key} must be list"
                    )
                if list(value):
                    merged[key] = list(value)
                elif key not in merged:
                    merged[key] = []
            else:
                merged[key] = value

    if not merged:
        return None
    return merged


def _resolve_host_channel_config(
    *,
    host_config: dict[str, Any],
    global_notify_cfg: dict[str, Any],
    channel: str,
) -> dict[str, Any] | None:
    merged: dict[str, Any] = {}
    global_channel = global_notify_cfg.get(channel)
    if global_channel is not None:
        if not isinstance(global_channel, dict):
            raise TypeError(f"app_config.notify.{channel} must be dict")
        merged.update(global_channel)

    host_notify = host_config.get("notify")
    if host_notify is not None and not isinstance(host_notify, dict):
        raise TypeError(f"host.notify must be dict, got {type(host_notify).__name__}")
    if isinstance(host_notify, dict):
        host_channel = host_notify.get(channel)
        if host_channel is not None:
            if not isinstance(host_channel, dict):
                raise TypeError(f"host.notify.{channel} must be dict")
            merged.update(host_channel)

    if not merged:
        return None
    return merged


def _resolve_named_notify_channels(
    global_notify_cfg: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    raw_channels = global_notify_cfg.get("channels")
    if raw_channels is None:
        return {}
    if not isinstance(raw_channels, dict):
        raise TypeError("app_config.notify.channels must be dict")

    out: dict[str, dict[str, Any]] = {}
    for raw_name, raw_cfg in raw_channels.items():
        name = str(raw_name or "").strip().lower()
        if name == "":
            continue
        if not isinstance(raw_cfg, dict):
            raise TypeError(f"app_config.notify.channels.{name} must be dict")
        channel_type = str(raw_cfg.get("type") or "").strip().lower()
        if channel_type == "":
            raise ValueError(f"app_config.notify.channels.{name}.type is required")
        cfg = dict(raw_cfg)
        cfg["type"] = channel_type
        out[name] = cfg
    return out


def _resolve_host_allowed_channels(host_config: dict[str, Any]) -> set[str] | None:
    host_notify = host_config.get("notify")
    if host_notify is None:
        return None
    if not isinstance(host_notify, dict):
        raise TypeError(f"host.notify must be dict, got {type(host_notify).__name__}")
    raw_channels = host_notify.get("channels")
    if raw_channels is None:
        return None
    if not isinstance(raw_channels, (list, tuple)):
        raise TypeError("host.notify.channels must be list")
    out = {
        str(item).strip().lower() for item in raw_channels if str(item).strip() != ""
    }
    return out


def _channel_allowed(name: str, allowed: set[str] | None) -> bool:
    if allowed is None:
        return True
    return str(name).strip().lower() in allowed


def _add_channel_notifier(
    channel_notifiers: dict[str, list[BaseNotifier]],
    channel_name: str,
    notifier: BaseNotifier,
) -> None:
    normalized = str(channel_name).strip().lower()
    if normalized == "":
        return
    channel_notifiers.setdefault(normalized, []).append(notifier)


def _build_default_notify_failure_recorder(writer_factory: Any):
    async def recorder(payload: dict[str, Any]) -> None:
        writer = writer_factory()
        await writer.write_send_failed(payload)

    return recorder


def _is_channel_enabled(channel_cfg: dict[str, Any] | None) -> bool:
    if channel_cfg is None:
        return True
    enabled = channel_cfg.get("enabled", True)
    if not isinstance(enabled, bool):
        raise TypeError("notify channel enabled must be bool")
    return enabled


def _is_telegram_auto_chat_enabled(channel_cfg: dict[str, Any] | None) -> bool:
    if channel_cfg is None:
        return False
    raw = channel_cfg.get("auto_chat_id", False)
    if not isinstance(raw, bool):
        raise TypeError("notify channel auto_chat_id must be bool")
    return raw


def _resolve_telegram_channel_send_func(
    *,
    channel_cfg: dict[str, Any] | None,
    default_send_func: Any | None,
) -> tuple[Any | None, bool]:
    tokens = _resolve_telegram_bot_tokens(channel_cfg)
    if not tokens:
        if default_send_func is None:
            return None, False
        return default_send_func, supports_auto_telegram_target(default_send_func)

    token_senders: list[Any] = []
    for token in tokens:
        sender = build_telegram_bot_token_sender(token)
        if sender is not None:
            token_senders.append(sender)
    if not token_senders:
        return None, False
    return _merge_send_funcs(token_senders), True


def _resolve_telegram_bot_tokens(channel_cfg: dict[str, Any] | None) -> list[str]:
    if channel_cfg is None:
        return []

    out: list[str] = []
    if "bot_token" in channel_cfg:
        raw_single = channel_cfg.get("bot_token")
        if raw_single is not None:
            if not isinstance(raw_single, str):
                raise TypeError("notify.telegram.bot_token must be string")
            value = raw_single.strip()
            if value != "":
                out.append(value)

    if "bot_tokens" in channel_cfg:
        raw_multi = channel_cfg.get("bot_tokens")
        if raw_multi is not None:
            if isinstance(raw_multi, str):
                out.extend(
                    item.strip() for item in raw_multi.split(",") if item.strip() != ""
                )
            elif isinstance(raw_multi, (list, tuple, set, frozenset)):
                out.extend(
                    str(item).strip() for item in raw_multi if str(item).strip() != ""
                )
            else:
                raise TypeError("notify.telegram.bot_tokens must be string or list")

    deduped: list[str] = []
    seen: set[str] = set()
    for item in out:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped


def _merge_send_funcs(send_funcs: list[Any]) -> Any:
    async def wrapped(target: str, message: str, parse_mode: str = "") -> None:
        first_exc: Exception | None = None
        for send_func in send_funcs:
            try:
                try:
                    result = send_func(target, message, parse_mode)
                except TypeError:
                    result = send_func(target, message)
                if inspect.isawaitable(result):
                    await result
            except Exception as exc:  # noqa: BLE001
                if first_exc is None:
                    first_exc = exc
        if first_exc is not None:
            raise first_exc

    return wrapped


def _resolve_notify_targets(
    *,
    channel_cfg: dict[str, Any] | None,
    list_field: str,
    env_var: str,
) -> list[str]:
    raw_targets = []
    if channel_cfg is not None:
        raw_targets = channel_cfg.get(list_field, [])
    if raw_targets is None:
        raw_targets = []
    if not isinstance(raw_targets, (list, tuple)):
        raise TypeError(f"notify channel field {list_field} must be list or tuple")

    targets = [str(item).strip() for item in raw_targets if str(item).strip()]
    if targets:
        return targets

    env_value = os.getenv(env_var, "")
    if env_value == "":
        return []
    return [item.strip() for item in env_value.split(",") if item.strip()]


def _resolve_channel_message_mode_getter(
    *,
    channel: str,
    channel_cfg: dict[str, Any] | None,
    message_mode_getter: Callable[[str], str] | None,
) -> Callable[[], str] | None:
    if message_mode_getter is not None:
        return lambda: normalize_message_mode(message_mode_getter(channel))
    if channel_cfg is None or "message_mode" not in channel_cfg:
        return None
    explicit_mode = normalize_message_mode(str(channel_cfg.get("message_mode")))
    return lambda: explicit_mode


def _resolve_explicit_notify_message_modes(
    app_config: dict[str, Any] | None,
) -> dict[str, str]:
    global_notify_cfg = _resolve_global_notify_config(app_config)
    resolved: dict[str, str] = {}
    for channel in ("telegram", "wechat", "weixin", "wecom"):
        channel_cfg = global_notify_cfg.get(channel)
        if not isinstance(channel_cfg, dict):
            continue
        if "message_mode" not in channel_cfg:
            continue
        resolved[channel] = normalize_message_mode(str(channel_cfg.get("message_mode")))

    if "wecom" not in resolved and "wechat" in resolved:
        resolved["wecom"] = resolved["wechat"]
    if "wechat" not in resolved and "wecom" in resolved:
        resolved["wechat"] = resolved["wecom"]

    named_channels = _resolve_named_notify_channels(global_notify_cfg)
    for channel_name, channel_cfg in named_channels.items():
        if "message_mode" not in channel_cfg:
            continue
        resolved[channel_name] = normalize_message_mode(
            str(channel_cfg.get("message_mode"))
        )
    return resolved


def _build_multi_target_send_func(send_func: Any, targets: list[str]):
    retry_targets_by_key: dict[tuple[str, str], tuple[list[str], float]] = {}
    retry_state_ttl_seconds = 2.0
    retry_state_max_entries = 2048

    def prune_retry_state(now: float) -> None:
        expired_keys = [
            key
            for key, (_, started_at) in retry_targets_by_key.items()
            if (now - started_at) > retry_state_ttl_seconds
        ]
        for key in expired_keys:
            retry_targets_by_key.pop(key, None)

        overflow = len(retry_targets_by_key) - retry_state_max_entries
        if overflow <= 0:
            return
        for key, _ in sorted(
            retry_targets_by_key.items(),
            key=lambda item: item[1][1],
        )[:overflow]:
            retry_targets_by_key.pop(key, None)

    async def wrapped(_host: str, message: str, parse_mode: str = "") -> None:
        now = time.monotonic()
        prune_retry_state(now)
        key = (str(_host), str(message))
        retry_state = retry_targets_by_key.get(key)
        pending_targets = list(targets)
        if retry_state is not None:
            retry_targets, retry_started_at = retry_state
            if (now - retry_started_at) <= retry_state_ttl_seconds:
                pending_targets = list(retry_targets)

        failed_targets: list[str] = []
        first_exc: Exception | None = None
        for target in pending_targets:
            try:
                try:
                    result = send_func(target, message, parse_mode)
                except TypeError:
                    result = send_func(target, message)
                if inspect.isawaitable(result):
                    await result
            except Exception as exc:  # noqa: BLE001
                failed_targets.append(target)
                if first_exc is None:
                    first_exc = exc

        if failed_targets:
            retry_targets_by_key[key] = (failed_targets, now)
            prune_retry_state(now)
            if first_exc is not None:
                raise first_exc
            raise RuntimeError("notify send failed")

        retry_targets_by_key.pop(key, None)

    return wrapped


def _resolve_notify_targets_with_aliases(
    *,
    channel_cfg: dict[str, Any] | None,
    candidate_fields: tuple[str, ...],
    env_var: str,
) -> list[str]:
    if channel_cfg is None:
        channel_cfg = {}

    out: list[str] = []
    for field in candidate_fields:
        if field not in channel_cfg:
            continue
        raw = channel_cfg.get(field)
        if raw is None:
            continue
        if isinstance(raw, str):
            value = raw.strip()
            if value != "":
                out.append(value)
            continue
        if isinstance(raw, (list, tuple, set, frozenset)):
            out.extend(str(item).strip() for item in raw if str(item).strip() != "")
            continue
        raise TypeError(f"notify channel field {field} must be string or list")

    deduped: list[str] = []
    seen: set[str] = set()
    for item in out:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    if deduped:
        return deduped

    env_value = os.getenv(env_var, "")
    if env_value == "" and env_var == "WECOM_TARGET":
        env_value = os.getenv("WECOM_WEBHOOK_URL", "")
    if env_value == "":
        return []
    return [item.strip() for item in env_value.split(",") if item.strip()]


def _resolve_telegram_bot_token(app_config: dict[str, Any] | None = None) -> str:
    """Resolve the Telegram bot token.

    Priority: TELEGRAM_BOT_TOKEN env var > first bot_token from notify channel config.
    """
    env_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if env_token:
        return env_token
    if not isinstance(app_config, dict):
        return ""
    notify_cfg = app_config.get("notify")
    if not isinstance(notify_cfg, dict):
        return ""
    # Check built-in telegram config
    telegram_cfg = notify_cfg.get("telegram")
    if isinstance(telegram_cfg, dict):
        tokens = _resolve_telegram_bot_tokens(telegram_cfg)
        if tokens:
            return tokens[0]
    # Check named channels for the first telegram channel with a token
    channels_cfg = notify_cfg.get("channels")
    if isinstance(channels_cfg, dict):
        for _name, ch_cfg in channels_cfg.items():
            if not isinstance(ch_cfg, dict):
                continue
            if str(ch_cfg.get("type") or "").strip().lower() != "telegram":
                continue
            tokens = _resolve_telegram_bot_tokens(ch_cfg)
            if tokens:
                return tokens[0]
    return ""


def _resolve_authorized_telegram_users(
    app_config: dict[str, Any] | None,
) -> set[str]:
    if not isinstance(app_config, dict):
        return set()
    agent_cfg = app_config.get("agent")
    if not isinstance(agent_cfg, dict):
        return set()
    authorized_users = agent_cfg.get("authorized_users")
    if not isinstance(authorized_users, dict):
        return set()
    telegram_users = authorized_users.get("telegram")
    if telegram_users is None:
        return set()
    if not isinstance(telegram_users, (list, tuple, set, frozenset)):
        raise TypeError("app_config.agent.authorized_users.telegram must be a list")
    return {str(item).strip() for item in telegram_users if str(item).strip() != ""}


_AUTHORIZED_USERS_STATE_FILE = "data/logdog_authorized.json"


def _load_persisted_authorized_users() -> set[str]:
    try:
        path = Path(_AUTHORIZED_USERS_STATE_FILE)
        if path.exists():
            data = json.loads(path.read_text())
            return {str(uid).strip() for uid in data.get("telegram", []) if str(uid).strip()}
    except Exception:  # noqa: BLE001
        logger.warning("failed to load persisted authorized users", exc_info=True)
    return set()


def _persist_authorized_user(user_id: str) -> None:
    try:
        path = Path(_AUTHORIZED_USERS_STATE_FILE)
        path.parent.mkdir(parents=True, exist_ok=True)
        existing: list[str] = []
        if path.exists():
            existing = json.loads(path.read_text()).get("telegram", [])
        if user_id not in existing:
            existing.append(user_id)
        path.write_text(json.dumps({"telegram": existing}, indent=2))
        logger.info("persisted authorized Telegram user %s to %s", user_id, path)
    except Exception:  # noqa: BLE001
        logger.warning("failed to persist authorized user", exc_info=True)


def _build_telegram_runtime_from_config(
    *,
    app_config: dict[str, Any] | None,
    chat_runtime: Any,
    message_mode_setter: Callable[[str], str] | None = None,
    message_mode_getter: Callable[[], str] | None = None,
    telegram_application_factory: Any | None = None,
    telegram_handler_binder: Any | None = None,
):
    authorized_telegram_users = _resolve_authorized_telegram_users(app_config)
    # Merge with persisted state from previous runs
    authorized_telegram_users |= _load_persisted_authorized_users()
    telegram_bot_token = _resolve_telegram_bot_token(app_config)
    if telegram_bot_token and not authorized_telegram_users:
        logger.info(
            "Telegram Bot: no authorized users configured — "
            "first user to send a message will be auto-authorized"
        )
    runtime = build_telegram_bot_runtime(
        bot_token=telegram_bot_token,
        chat_runtime=chat_runtime,
        authorized_user_ids=authorized_telegram_users,
        message_mode_setter=message_mode_setter,
        message_mode_getter=message_mode_getter,
        application_factory=telegram_application_factory,
        handler_binder=telegram_handler_binder,
    )
    if runtime is not None and telegram_bot_token:
        runtime.set_authorized_user_persist_callback(_persist_authorized_user)
        try:
            sender = TelegramBotTokenSender(telegram_bot_token)
            runtime.set_sender(sender)
        except Exception:
            logger.warning(
                "failed to create TelegramBotTokenSender for runtime", exc_info=True
            )
    return runtime


def _resolve_metrics_db_path(
    metrics_db_path: str | None, config_db_path: str | None
) -> str:
    if metrics_db_path:
        return metrics_db_path
    env_path = os.getenv("LOGDOG_METRICS_DB_PATH")
    if env_path:
        return env_path
    if config_db_path:
        return config_db_path
    return DEFAULT_METRICS_DB_PATH


def _resolve_retention_config(
    config_retention: dict[str, int],
    retention_config: dict[str, int] | None,
) -> dict[str, int]:
    out = dict(DEFAULT_RETENTION_CONFIG)
    out.update(config_retention)
    if retention_config:
        out.update(retention_config)
    return out


def _resolve_metrics_interval_seconds(
    metrics_interval_seconds: int | None,
    config_interval_seconds: int,
) -> int:
    if metrics_interval_seconds is None:
        return int(config_interval_seconds)
    return int(metrics_interval_seconds)


def _resolve_retention_interval_seconds(
    retention_interval_seconds: int | None,
    config_interval_seconds: int,
) -> int:
    if retention_interval_seconds is None:
        return int(config_interval_seconds)
    return int(retention_interval_seconds)


_INSECURE_DEFAULT_WEB_AUTH_TOKEN = "web-token"
_INSECURE_DEFAULT_WEB_ADMIN_TOKEN = "admin-token"


def _resolve_web_auth_token(
    *,
    explicit_token: str | None,
    allow_insecure_default_tokens: bool,
) -> str:
    return _resolve_auth_token(
        explicit_token=explicit_token,
        env_var="WEB_AUTH_TOKEN",
        token_name="web auth token",
        insecure_default=_INSECURE_DEFAULT_WEB_AUTH_TOKEN,
        allow_insecure_default_tokens=allow_insecure_default_tokens,
    )


def _resolve_web_admin_token(
    *,
    explicit_token: str | None,
    allow_insecure_default_tokens: bool,
) -> str:
    return _resolve_auth_token(
        explicit_token=explicit_token,
        env_var="WEB_ADMIN_TOKEN",
        token_name="web admin token",
        insecure_default=_INSECURE_DEFAULT_WEB_ADMIN_TOKEN,
        allow_insecure_default_tokens=allow_insecure_default_tokens,
    )


def _resolve_auth_token(
    *,
    explicit_token: str | None,
    env_var: str,
    token_name: str,
    insecure_default: str,
    allow_insecure_default_tokens: bool,
) -> str:
    explicit = str(explicit_token or "").strip()
    if explicit != "":
        return explicit

    env_value = str(os.getenv(env_var, "")).strip()
    if env_value != "":
        return env_value

    if allow_insecure_default_tokens:
        logger.warning(
            "%s is using insecure default value because allow_insecure_default_tokens=True",
            token_name,
        )
        return insecure_default

    raise ValueError(
        f"{env_var} is required (or pass explicit token to create_app). "
        "Insecure defaults are disabled."
    )


def _resolve_docker_client_pool(
    *,
    docker_client_pool: DockerClientPool | None,
    host_manager: HostManager | None,
    host_connector: Any | None,
    enable_metrics_scheduler: bool,
    metrics_scheduler: MetricsSamplingScheduler | None,
    list_containers_fn: Any | None,
    fetch_stats_fn: Any | None,
    docker_pool_max_clients: int | None,
    docker_pool_max_idle_seconds: float | None,
    config_docker_pool_max_clients: int | None,
    config_docker_pool_max_idle_seconds: float | None,
) -> DockerClientPool | None:
    if docker_client_pool is not None:
        return docker_client_pool
    resolved_pool_max_clients = docker_pool_max_clients
    if resolved_pool_max_clients is None:
        resolved_pool_max_clients = config_docker_pool_max_clients
    resolved_pool_max_idle_seconds = docker_pool_max_idle_seconds
    if resolved_pool_max_idle_seconds is None:
        resolved_pool_max_idle_seconds = config_docker_pool_max_idle_seconds
    if host_manager is None and host_connector is None:
        return DockerClientPool(
            max_clients=resolved_pool_max_clients,
            max_idle_seconds=resolved_pool_max_idle_seconds,
        )
    if metrics_scheduler is None and enable_metrics_scheduler:
        if list_containers_fn is None or fetch_stats_fn is None:
            return DockerClientPool(
                max_clients=resolved_pool_max_clients,
                max_idle_seconds=resolved_pool_max_idle_seconds,
            )
    return None
