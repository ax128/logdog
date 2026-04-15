from __future__ import annotations

import asyncio
import inspect
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from logdog.llm.analyzer import analyze_with_template
from logdog.notify.render import render_output


class ScheduleReportRunner:
    def __init__(
        self,
        *,
        host_manager: Any,
        list_containers: Any,
        list_alerts: Any,
        query_metrics: Any,
        query_host_container_metrics: Any | None = None,
        query_host_metrics: Any | None = None,
        host_system_settings_getter: Any | None = None,
        send_host_notification: Any | None,
        send_global_notification: Any | None = None,
        analyze: Any = analyze_with_template,
        time_fn: Any | None = None,
    ) -> None:
        self._host_manager = host_manager
        self._list_containers = list_containers
        self._list_alerts = list_alerts
        self._query_metrics = query_metrics
        self._query_host_container_metrics = query_host_container_metrics
        self._query_host_metrics = query_host_metrics
        self._host_system_settings_getter = host_system_settings_getter
        self._send_host_notification = send_host_notification
        self._send_global_notification = send_global_notification
        self._analyze = analyze
        self._time_fn = time_fn or time.time
        self._last_host_emit_at: dict[str, float] = {}

    async def run_host_schedule(
        self,
        host_name: str,
        schedule: dict[str, Any],
    ) -> dict[str, Any]:
        host_config = self._host_manager.get_host_config(host_name)
        if not isinstance(host_config, dict):
            return {"sent": False, "reason": "unknown_host", "scene": None}

        host_state = self._host_manager.get_host_state(host_name) or {}
        now_ts = float(self._time_fn())
        now_dt = datetime.fromtimestamp(now_ts, tz=timezone.utc)
        scene = _normalize_scene(schedule.get("template"))
        window_seconds = _schedule_window_seconds(schedule, scene)
        since_dt = now_dt - timedelta(seconds=window_seconds)

        containers = []
        if str(host_state.get("status") or "") == "connected":
            containers = list(
                await _maybe_await(self._list_containers(host_name)) or []
            )
        alerts = list(
            await _maybe_await(
                self._list_alerts(limit=500, host=host_name, container=None)
            )
            or []
        )
        recent_alerts = _filter_recent_rows(alerts, since_dt=since_dt)
        metrics_text = await self._build_host_metrics_text(
            host_name=host_name,
            containers=containers,
            since_dt=since_dt,
            now_dt=now_dt,
        )
        (
            host_metrics_text,
            host_resource_warnings,
        ) = await self._build_host_system_metrics_text(
            host_name=host_name,
            since_dt=since_dt,
            now_dt=now_dt,
        )
        combined_metrics_text = _merge_metrics_text(
            metrics_text=metrics_text,
            host_metrics_text=host_metrics_text,
        )
        abnormal = _has_host_abnormality(
            host_state=host_state,
            containers=containers,
            recent_alerts=recent_alerts,
        )

        raw_notify_cfg = host_config.get("notify")
        notify_cfg: dict[str, Any] = (
            dict(raw_notify_cfg) if isinstance(raw_notify_cfg, dict) else {}
        )
        push_on_normal = bool(notify_cfg.get("push_on_normal", False))
        heartbeat_interval = _to_int(
            notify_cfg.get("heartbeat_interval_seconds"), default=0
        )

        emit_scene = scene
        if scene != "heartbeat" and not abnormal and not push_on_normal:
            if heartbeat_interval <= 0 or not _heartbeat_due(
                self._last_host_emit_at.get(host_name),
                now_ts,
                heartbeat_interval,
            ):
                return {"sent": False, "reason": "normal_suppressed", "scene": scene}
            emit_scene = "heartbeat"

        context = self._build_host_context(
            host_name=host_name,
            host_state=host_state,
            containers=containers,
            alerts=recent_alerts,
            metrics_text=combined_metrics_text,
            host_metrics_text=host_metrics_text,
            host_resource_warnings=host_resource_warnings,
            timestamp=now_dt.strftime("%Y-%m-%d %H:%M:%S"),
            scene=emit_scene,
        )
        analysis_mode = _resolve_schedule_analysis_mode(host_config)
        if analysis_mode == "script":
            analysis = _build_script_schedule_analysis(
                scene=emit_scene, context=context
            )
            message = analysis
        elif analysis_mode == "llm":
            analysis = await _invoke_analyze(
                self._analyze,
                emit_scene,
                context,
                emit_scene,
                enable_agent=True,
            )
            message = analysis
        else:
            analysis = await _invoke_analyze(
                self._analyze,
                emit_scene,
                context,
                emit_scene,
                enable_agent=False,
            )
            message = _render_host_message(
                scene=emit_scene,
                host_config=host_config,
                context=context,
                analysis=analysis,
            )
        category = emit_scene.upper()
        pushed = True
        if self._send_host_notification is not None:
            pushed = bool(
                await _maybe_await(
                    self._send_host_notification(host_name, message, category)
                )
            )
        if pushed:
            self._last_host_emit_at[host_name] = now_ts
        return {
            "sent": pushed,
            "reason": "pushed" if pushed else "notify_failed",
            "scene": emit_scene,
        }

    async def run_global_schedule(self, schedule: dict[str, Any]) -> dict[str, Any]:
        now_dt = datetime.fromtimestamp(float(self._time_fn()), tz=timezone.utc)
        max_host_summary_chars = _to_int(
            schedule.get("max_host_summary_chars"),
            default=320,
        )
        max_global_summary_chars = _to_int(
            schedule.get("max_global_summary_chars"),
            default=2200,
        )
        top_hosts_on_overflow = _to_int(
            schedule.get("top_hosts_on_overflow"),
            default=3,
        )

        statuses = self._host_manager.list_host_statuses()
        tasks = [
            self._collect_global_host_summary(
                status=status,
                max_host_summary_chars=max_host_summary_chars,
            )
            for status in statuses
        ]
        host_summaries = [
            item for item in (await _gather_safe(tasks)) if item is not None
        ]
        summaries = _compress_global_summaries(
            host_summaries,
            max_total_chars=max_global_summary_chars,
            top_hosts=max(1, top_hosts_on_overflow),
        )

        context = {
            "host_name": "all-hosts",
            "timestamp": now_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "alert_history": summaries,
            "metrics": summaries,
        }
        scene = _normalize_scene(schedule.get("template") or "daily")
        analysis = self._analyze(scene, context, scene)
        pushed = True
        if self._send_global_notification is not None:
            pushed = bool(
                await _maybe_await(
                    self._send_global_notification(schedule, analysis, scene.upper())
                )
            )
        elif self._send_host_notification is not None:
            pushed = bool(
                await _maybe_await(
                    self._send_host_notification("__global__", analysis, scene.upper())
                )
            )
        return {
            "sent": pushed,
            "reason": "pushed" if pushed else "notify_failed",
            "scene": scene,
        }

    async def _collect_global_host_summary(
        self,
        *,
        status: dict[str, Any],
        max_host_summary_chars: int,
    ) -> dict[str, Any] | None:
        host_name = str(status.get("name") or "").strip()
        if host_name == "":
            return None

        host_alerts = list(
            await _maybe_await(
                self._list_alerts(limit=100, host=host_name, container=None)
            )
            or []
        )
        alert_count = len(host_alerts)

        host_status = str(status.get("status") or "")
        if host_status != "connected":
            summary = (
                f"⚠️ [{host_name}] unreachable "
                f"(last online: {status.get('last_connected_at') or 'unknown'}) "
                f"alerts={alert_count}"
            )
            return {
                "host": host_name,
                "status": host_status,
                "alert_count": alert_count,
                "summary": _truncate_text(summary, max_host_summary_chars),
                "compact_summary": f"[compact][{host_name}] status=disconnected alerts={alert_count}",
            }

        containers = list(await _maybe_await(self._list_containers(host_name)) or [])
        top_lines = _extract_top_alert_lines(host_alerts, limit=3)
        top_text = " | ".join(top_lines) if top_lines else "none"
        summary = (
            f"[{host_name}] status=connected containers={len(containers)} alerts={alert_count} "
            f"top_alerts={top_text}"
        )
        return {
            "host": host_name,
            "status": host_status,
            "alert_count": alert_count,
            "summary": _truncate_text(summary, max_host_summary_chars),
            "compact_summary": f"[compact][{host_name}] status=connected alerts={alert_count}",
        }

    async def _build_host_metrics_text(
        self,
        *,
        host_name: str,
        containers: list[dict[str, Any]],
        since_dt: datetime,
        now_dt: datetime,
    ) -> str:
        if self._query_host_container_metrics is not None:
            rows = list(
                await _maybe_await(
                    self._query_host_container_metrics(
                        host_name,
                        since_dt.strftime("%Y-%m-%d %H:%M:%S"),
                        now_dt.strftime("%Y-%m-%d %H:%M:%S"),
                        500,
                    )
                )
                or []
            )
            if rows:
                return "\n".join(
                    f"{row.get('container_name') or row.get('container_id')}: "
                    f"cpu={row.get('cpu', '-')} mem={row.get('mem_used', '-')}"
                    for row in rows
                )

        lines: list[str] = []
        for container in containers:
            container_id = str(
                container.get("id") or container.get("container_id") or ""
            ).strip()
            if container_id == "":
                continue
            points = list(
                await _maybe_await(
                    self._query_metrics(
                        host_name,
                        container_id,
                        since_dt.strftime("%Y-%m-%d %H:%M:%S"),
                        now_dt.strftime("%Y-%m-%d %H:%M:%S"),
                        200,
                    )
                )
                or []
            )
            if not points:
                continue
            latest = points[-1]
            lines.append(
                f"{container.get('name') or container_id}: cpu={latest.get('cpu', '-')} mem={latest.get('mem_used', '-')}"
            )
        return "\n".join(lines).strip()

    async def _build_host_system_metrics_text(
        self,
        *,
        host_name: str,
        since_dt: datetime,
        now_dt: datetime,
    ) -> tuple[str, list[str]]:
        if self._query_host_metrics is None:
            return "", []

        report_cfg = await self._resolve_host_system_report_settings()
        if not bool(report_cfg.get("include_in_schedule", True)):
            return "", []

        points = list(
            await _maybe_await(
                self._query_host_metrics(
                    host_name,
                    since_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    now_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    500,
                )
            )
            or []
        )
        if not points:
            return "", []

        latest = points[-1]
        cpu_values = [_to_float(row.get("cpu_percent")) for row in points]
        cpu_avg = sum(cpu_values) / max(1, len(cpu_values))
        cpu_max = max(cpu_values) if cpu_values else 0.0
        mem_used = _to_int(latest.get("mem_used"), default=0)
        mem_total = _to_int(latest.get("mem_total"), default=0)
        disk_used = _to_int(latest.get("disk_root_used"), default=0)
        disk_total = _to_int(latest.get("disk_root_total"), default=0)
        mem_used_percent = _percent(mem_used, mem_total)
        disk_used_percent = _percent(disk_used, disk_total)
        cpu_percent = _to_float(latest.get("cpu_percent"))

        summary = (
            f"latest cpu={cpu_percent:.1f}% "
            f"load=({_to_float(latest.get('load_1')):.2f},{_to_float(latest.get('load_5')):.2f},{_to_float(latest.get('load_15')):.2f}) "
            f"mem={mem_used_percent:.1f}%({mem_used}/{mem_total}) "
            f"disk={disk_used_percent:.1f}%({disk_used}/{disk_total}) "
            f"net=rx{_to_int(latest.get('net_rx'), default=0)} tx{_to_int(latest.get('net_tx'), default=0)} "
            f"source={latest.get('source') or 'unknown'} "
            f"cpu_avg={cpu_avg:.1f}% cpu_max={cpu_max:.1f}%"
        )

        warn_thresholds = report_cfg.get("warn_thresholds")
        if not isinstance(warn_thresholds, dict):
            warn_thresholds = {}
        warnings: list[str] = []
        cpu_threshold = _to_float(warn_thresholds.get("cpu_percent"))
        mem_threshold = _to_float(warn_thresholds.get("mem_used_percent"))
        disk_threshold = _to_float(warn_thresholds.get("disk_used_percent"))
        if cpu_threshold > 0 and cpu_percent >= cpu_threshold:
            warnings.append(f"cpu {cpu_percent:.1f}% >= {cpu_threshold:.1f}%")
        if mem_threshold > 0 and mem_used_percent >= mem_threshold:
            warnings.append(f"mem_used {mem_used_percent:.1f}% >= {mem_threshold:.1f}%")
        if disk_threshold > 0 and disk_used_percent >= disk_threshold:
            warnings.append(
                f"disk_used {disk_used_percent:.1f}% >= {disk_threshold:.1f}%"
            )
        return summary, warnings

    async def _resolve_host_system_report_settings(self) -> dict[str, Any]:
        defaults = {
            "include_in_schedule": True,
            "warn_thresholds": {
                "cpu_percent": 85.0,
                "mem_used_percent": 90.0,
                "disk_used_percent": 90.0,
            },
        }
        getter = self._host_system_settings_getter
        if getter is None:
            return defaults

        resolved = getter()
        if inspect.isawaitable(resolved):
            resolved = await resolved
        if not isinstance(resolved, dict):
            return defaults
        report_cfg = resolved.get("report")
        if not isinstance(report_cfg, dict):
            return defaults

        merged = {
            "include_in_schedule": bool(
                report_cfg.get("include_in_schedule", defaults["include_in_schedule"])
            ),
            "warn_thresholds": dict(defaults["warn_thresholds"]),
        }
        raw_warn = report_cfg.get("warn_thresholds")
        if isinstance(raw_warn, dict):
            merged_warn = dict(merged["warn_thresholds"])
            for key in ("cpu_percent", "mem_used_percent", "disk_used_percent"):
                if key in raw_warn:
                    merged_warn[key] = _to_float(raw_warn.get(key))
            merged["warn_thresholds"] = merged_warn
        return merged

    def _build_host_context(
        self,
        *,
        host_name: str,
        host_state: dict[str, Any],
        containers: list[dict[str, Any]],
        alerts: list[dict[str, Any]],
        metrics_text: str,
        host_metrics_text: str,
        host_resource_warnings: list[str],
        timestamp: str,
        scene: str,
    ) -> dict[str, Any]:
        logs = [
            str(
                row.get("payload", {}).get("line")
                or row.get("payload", {}).get("analysis")
                or ""
            )
            for row in alerts
        ]
        container_status_lines = [
            f"- {container.get('name') or container.get('id')}: {container.get('status', 'unknown')}"
            for container in containers
        ]
        context = {
            "host_name": host_name,
            "container_name": "fleet",
            "timestamp": timestamp,
            "logs": [line for line in logs if line],
            "metrics": metrics_text,
            "host_metrics": host_metrics_text,
            "host_resource_warnings": list(host_resource_warnings),
            "alert_history": [line for line in logs if line],
            "container_status": container_status_lines,
            "container_status_table": "\n".join(container_status_lines),
            "total_containers": len(containers),
            "last_alert_time": alerts[0].get("created_at") if alerts else "none",
            "last_alert_summary": str(
                alerts[0].get("payload", {}).get("analysis")
                or alerts[0].get("payload", {}).get("line")
                or "none"
            )
            if alerts
            else "none",
        }
        if scene == "heartbeat" and not container_status_lines:
            context["container_status"] = ["- no containers reported"]
            context["container_status_table"] = "- no containers reported"
        return context


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


async def _invoke_analyze(
    analyze: Any,
    scene: str,
    context: dict[str, Any],
    template: str,
    *,
    enable_agent: bool,
) -> str:
    if _analyze_callable_accepts_enable_agent(analyze):
        result = analyze(scene, context, template, enable_agent=enable_agent)
    else:
        result = analyze(scene, context, template)
    return str(await _maybe_await(result))


def _analyze_callable_accepts_enable_agent(analyze: Any) -> bool:
    try:
        signature = inspect.signature(analyze)
    except (TypeError, ValueError):
        return True
    for param in signature.parameters.values():
        if param.kind is inspect.Parameter.VAR_KEYWORD:
            return True
    return "enable_agent" in signature.parameters


def _normalize_scene(value: Any) -> str:
    text = str(value or "interval").strip().lower()
    if text in {"interval", "hourly", "daily", "heartbeat"}:
        return text
    return "interval"


def _schedule_window_seconds(schedule: dict[str, Any], scene: str) -> int:
    if "interval_seconds" in schedule:
        return max(1, int(schedule["interval_seconds"]))
    if scene == "hourly":
        return 3600
    if scene == "daily":
        return 86400
    if scene == "heartbeat":
        return 3600
    return 300


def _has_host_abnormality(
    *,
    host_state: dict[str, Any],
    containers: list[dict[str, Any]],
    recent_alerts: list[dict[str, Any]],
) -> bool:
    if str(host_state.get("status") or "") != "connected":
        return True
    if recent_alerts:
        return True
    for container in containers:
        status = str(container.get("status") or "").lower()
        if status not in {"running", "healthy", "connected"}:
            return True
    return False


def _heartbeat_due(
    last_emit_at: float | None, now_ts: float, interval_seconds: int
) -> bool:
    if interval_seconds <= 0:
        return False
    if last_emit_at is None:
        return True
    return (now_ts - float(last_emit_at)) >= float(interval_seconds)


def _filter_recent_rows(
    rows: list[dict[str, Any]], *, since_dt: datetime
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        created_at = str(row.get("created_at") or "").strip()
        if created_at == "":
            out.append(row)
            continue
        try:
            normalized = created_at.replace("Z", "+00:00")
            parsed = datetime.fromisoformat(normalized)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            else:
                parsed = parsed.astimezone(timezone.utc)
        except ValueError:
            out.append(row)
            continue
        if parsed >= since_dt:
            out.append(row)
    return out


def _render_host_message(
    *,
    scene: str,
    host_config: dict[str, Any],
    context: dict[str, Any],
    analysis: str,
) -> str:
    if scene == "heartbeat":
        return render_output("heartbeat", context)
    return analysis


def _resolve_schedule_analysis_mode(host_config: dict[str, Any]) -> str:
    llm_cfg = host_config.get("llm")
    if not isinstance(llm_cfg, dict):
        return "legacy"
    if "enabled" not in llm_cfg:
        return "legacy"
    enabled = llm_cfg.get("enabled")
    if not isinstance(enabled, bool):
        return "legacy"
    if enabled:
        return "llm"
    return "script"


def _build_script_schedule_analysis(*, scene: str, context: dict[str, Any]) -> str:
    host_name = str(context.get("host_name") or "unknown")
    timestamp = str(context.get("timestamp") or "-")
    metrics = str(context.get("metrics") or "-")
    alerts = str(context.get("alert_history") or "-")
    containers = str(context.get("container_status") or "-")
    warnings = str(context.get("host_resource_warnings") or "-")
    return "\n".join(
        [
            f"[SCRIPT] {str(scene or 'report').upper()}",
            f"host={host_name}",
            f"timestamp={timestamp}",
            "containers:",
            containers,
            "alerts:",
            alerts,
            "metrics:",
            metrics,
            "warnings:",
            warnings,
        ]
    ).strip()


def _to_int(value: Any, *, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _to_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _percent(used: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return (float(used) / float(total)) * 100.0


def _merge_metrics_text(*, metrics_text: str, host_metrics_text: str) -> str:
    container_text = str(metrics_text or "").strip()
    host_text = str(host_metrics_text or "").strip()
    if container_text and host_text:
        return f"{container_text}\n\nHost system:\n{host_text}"
    if host_text:
        return f"Host system:\n{host_text}"
    return container_text


def _truncate_text(text: str, max_chars: int) -> str:
    if max_chars <= 0:
        return ""
    if len(text) <= max_chars:
        return text
    if max_chars <= 3:
        return text[:max_chars]
    return text[: max_chars - 3] + "..."


def _extract_top_alert_lines(alerts: list[dict[str, Any]], *, limit: int) -> list[str]:
    out: list[str] = []
    for row in alerts:
        payload = row.get("payload")
        if not isinstance(payload, dict):
            continue
        line = str(payload.get("line") or payload.get("analysis") or "").strip()
        if line == "":
            continue
        out.append(line)
        if len(out) >= limit:
            break
    return out


def _compress_global_summaries(
    items: list[dict[str, Any]],
    *,
    max_total_chars: int,
    top_hosts: int,
) -> list[str]:
    full = [str(item.get("summary") or "") for item in items if item is not None]
    total_chars = sum(len(text) for text in full)
    if total_chars <= max_total_chars:
        return full

    ranked = sorted(
        [item for item in items if item is not None],
        key=lambda item: int(item.get("alert_count") or 0),
        reverse=True,
    )
    keep_hosts = {
        str(item.get("host") or "") for item in ranked[: max(1, int(top_hosts))]
    }

    compressed: list[str] = []
    for item in items:
        host_name = str(item.get("host") or "")
        if host_name in keep_hosts:
            compressed.append(str(item.get("summary") or ""))
            continue
        compressed.append(str(item.get("compact_summary") or ""))
    return compressed


async def _gather_safe(tasks: list[Any]) -> list[Any]:
    if not tasks:
        return []
    results = await asyncio.gather(*tasks, return_exceptions=True)
    out: list[Any] = []
    for result in results:
        if isinstance(result, Exception):
            continue
        out.append(result)
    return out
