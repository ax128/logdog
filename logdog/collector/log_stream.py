from __future__ import annotations

import time
import logging
import asyncio
import inspect
import threading
from copy import deepcopy
from fnmatch import fnmatch
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from logdog.llm.analyzer import analyze_with_template
from logdog.llm.provider import resolve_llm_params
from logdog.notify.render import render_output
from logdog.pipeline.cooldown import CooldownStore
from logdog.pipeline.filter import apply_rules
from logdog.pipeline.preprocessor.base import LogLine
from logdog.pipeline.preprocessor.loader import load_preprocessors


logger = logging.getLogger(__name__)

_DEFAULT_RUNTIME_CONFIG: dict[str, Any] = {
    "rules": {
        "ignore": [],
        "redact": [{"pattern": r"Bearer\s+\S+", "replace": "Bearer ***REDACTED***"}],
        "custom_alerts": [],
    },
    "alert_keywords": ["ERROR", "FATAL", "panic", "OOM", "timeout", "failed"],
}
_DEFAULT_COOLDOWN_STORE = CooldownStore()
_DEDUP_LOCK = threading.Lock()
_PENDING_DEDUP: dict[tuple[str, str, str], "_DedupWindow"] = {}
_PENDING_DEDUP_TASKS: dict[tuple[str, str, str], asyncio.Task[None]] = {}
_STORM_TASK_LOCK = threading.Lock()
_PENDING_STORM_END_TASKS: dict[tuple[int, str], asyncio.Task[None]] = {}
_DEFAULT_RECONNECT_BACKOFF_SECONDS = 1.0
_MAX_RECONNECT_BACKOFF_SECONDS = 10.0


@dataclass(frozen=True, slots=True)
class AlertRunResult:
    triggered: bool
    pushed: bool
    saved: bool
    category: str | None
    analysis: str


@dataclass(frozen=True, slots=True)
class _AlertItem:
    line: str
    kwargs: dict[str, Any]


@dataclass(slots=True)
class _DedupWindow:
    host: str
    container_id: str
    container_name: str
    category: str
    first_seen: float
    last_seen: float
    repeat_count: int
    sample_line: str
    prompt_template: str
    output_template: str
    analysis_mode: str
    llm_model: str | None
    llm_config: dict[str, Any] | None
    notifier_send: Any
    save_alert: Any
    mute_checker: Any


async def _default_notify(_host: str, _message: str, _category: str) -> bool:
    return True


async def _default_save(_payload: dict[str, Any]) -> bool:
    return True


async def _default_save_storm_event(_payload: dict[str, Any]) -> bool:
    return True


def _parse_log_timestamp(raw: str) -> float | None:
    """Parse a Docker log timestamp string to epoch seconds."""
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        # Docker timestamps: "2024-01-15T10:30:00.123456789Z"
        # Python can't parse nanoseconds, truncate to microseconds
        if "." in text:
            base, frac = text.split(".", 1)
            # Remove trailing Z or timezone
            frac_clean = frac.rstrip("Z")
            if len(frac_clean) > 6:
                frac_clean = frac_clean[:6]
            text = f"{base}.{frac_clean}+00:00"
        elif text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        return dt.timestamp()
    except (ValueError, TypeError, OSError):
        return None


def _format_ts(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def _mark_dedup_task_done(key: tuple[str, str, str]) -> None:
    with _DEDUP_LOCK:
        _PENDING_DEDUP_TASKS.pop(key, None)


def _mark_storm_task_done(key: tuple[int, str]) -> None:
    with _STORM_TASK_LOCK:
        _PENDING_STORM_END_TASKS.pop(key, None)


def _schedule_dedup_summary(
    *,
    host: str,
    container_id: str,
    container_name: str,
    category: str,
    check_time: float,
    redacted_line: str,
    prompt_template: str,
    output_template: str,
    analysis_mode: str,
    llm_model: str | None = None,
    llm_config: dict[str, Any] | None = None,
    notifier_send: Any,
    save_alert: Any,
    mute_checker: Any,
    window_seconds: float,
    window_end_at: float,
) -> None:
    key = (host, container_id, category)
    with _DEDUP_LOCK:
        existing = _PENDING_DEDUP.get(key)
        if existing is None:
            _PENDING_DEDUP[key] = _DedupWindow(
                host=host,
                container_id=container_id,
                container_name=container_name,
                category=category,
                first_seen=check_time,
                last_seen=check_time,
                repeat_count=1,
                sample_line=redacted_line,
                prompt_template=prompt_template,
                output_template=output_template,
                analysis_mode=analysis_mode,
                llm_model=llm_model,
                llm_config=llm_config,
                notifier_send=notifier_send,
                save_alert=save_alert,
                mute_checker=mute_checker,
            )
        else:
            existing.repeat_count += 1
            existing.last_seen = check_time
            existing.sample_line = redacted_line
            existing.prompt_template = prompt_template
            existing.output_template = output_template
            existing.analysis_mode = analysis_mode
            existing.llm_model = llm_model
            existing.llm_config = llm_config
            existing.notifier_send = notifier_send
            existing.save_alert = save_alert
            existing.mute_checker = mute_checker

        if key in _PENDING_DEDUP_TASKS:
            return

    delay = max(0.0, float(window_end_at - check_time))
    task = asyncio.create_task(
        _flush_dedup_summary_after_delay(
            key=key, delay=delay, window_seconds=window_seconds
        )
    )
    task.add_done_callback(lambda _: _mark_dedup_task_done(key))
    with _DEDUP_LOCK:
        _PENDING_DEDUP_TASKS[key] = task


async def _flush_dedup_summary_after_delay(
    *,
    key: tuple[str, str, str],
    delay: float,
    window_seconds: float,
) -> None:
    if delay > 0:
        await asyncio.sleep(delay)

    with _DEDUP_LOCK:
        window = _PENDING_DEDUP.pop(key, None)
    if window is None:
        return

    mute_impl = window.mute_checker
    if mute_impl is not None:
        muted = None
        try:
            muted = mute_impl(
                host=window.host,
                container_id=window.container_id,
                category=window.category,
                at_time=_format_ts(window.last_seen),
            )
            if inspect.isawaitable(muted):
                muted = await muted
        except Exception:  # noqa: BLE001
            logger.exception(
                "dedup mute check failed host=%s container_id=%s category=%s",
                window.host,
                window.container_id,
                window.category,
            )
            muted = None
        if muted is not None:
            return

    context = {
        "host_name": window.host,
        "container_name": window.container_name,
        "timestamp": _format_ts(window.last_seen),
        "logs": [
            f"{window.category} DEDUP_SUMMARY",
            f"repeat_count={window.repeat_count}",
            f"first_seen={_format_ts(window.first_seen)}",
            f"last_seen={_format_ts(window.last_seen)}",
            f"window_seconds={window_seconds:.3f}",
            window.sample_line,
        ],
    }
    if window.analysis_mode == "script":
        analysis = _build_script_alert_analysis(
            host=window.host,
            container_name=window.container_name,
            category=window.category,
            timestamp=_format_ts(window.last_seen),
            line=window.sample_line,
        )
    elif window.analysis_mode == "llm":
        _dedup_params = resolve_llm_params(window.llm_model, window.llm_config)
        analysis = analyze_with_template(
            "alert",
            context,
            window.prompt_template,
            enable_agent=True,
            model=_dedup_params.model or None,
            api_base=_dedup_params.api_base or None,
            api_key=_dedup_params.api_key or None,
            provider_type=_dedup_params.provider_type or None,
        )
    else:
        _dedup_params = resolve_llm_params(window.llm_model, window.llm_config)
        analysis = analyze_with_template(
            "alert",
            context,
            window.prompt_template,
            enable_agent=False,
            model=_dedup_params.model or None,
            api_base=_dedup_params.api_base or None,
            api_key=_dedup_params.api_key or None,
            provider_type=_dedup_params.provider_type or None,
        )
    message = _render_notification_message(
        output_template=window.output_template,
        host=window.host,
        container_name=window.container_name,
        category=window.category,
        analysis=analysis,
        line=window.sample_line,
        timestamp=_format_ts(window.last_seen),
        direct_analysis_output=window.analysis_mode in {"llm", "script"},
    )

    notify_impl = window.notifier_send or _default_notify
    save_impl = window.save_alert or _default_save

    try:
        pushed = bool(
            await _call_notify(
                notify_impl,
                host=window.host,
                message=message,
                category=window.category,
                context={
                    "container_id": window.container_id,
                    "container_name": window.container_name,
                },
            )
        )
    except Exception:  # noqa: BLE001
        logger.exception(
            "notify dedup summary failed host=%s container_id=%s category=%s",
            window.host,
            window.container_id,
            window.category,
        )
        pushed = False

    payload = {
        "host": window.host,
        "container_id": window.container_id,
        "container_name": window.container_name,
        "category": window.category,
        "line": window.sample_line,
        "analysis": analysis,
        "pushed": pushed,
        "dedup_summary": True,
        "dedup_repeat_count": window.repeat_count,
        "dedup_first_seen": window.first_seen,
        "dedup_last_seen": window.last_seen,
        "dedup_window_seconds": window_seconds,
    }

    try:
        await save_impl(payload)
    except Exception:  # noqa: BLE001
        logger.exception(
            "save dedup summary failed host=%s container_id=%s category=%s",
            window.host,
            window.container_id,
            window.category,
        )


def load_runtime_config(config: dict[str, Any] | None = None) -> dict[str, Any]:
    if config is None:
        return deepcopy(_DEFAULT_RUNTIME_CONFIG)

    merged = deepcopy(_DEFAULT_RUNTIME_CONFIG)
    incoming = deepcopy(config)
    for key, value in incoming.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key].update(value)
        else:
            merged[key] = value

    return merged


async def run_alert_once(
    line: str,
    *,
    host: str = "default",
    container_id: str = "container-unknown",
    container_name: str | None = None,
    timestamp: str = "",
    now: float | None = None,
    config: dict[str, Any] | None = None,
    cooldown_store: CooldownStore | None = None,
    notifier_send=None,
    save_alert=None,
    save_storm_event=None,
    mute_checker=None,
    prompt_template: str = "default_alert",
    output_template: str = "standard",
    storm_controller=None,
) -> AlertRunResult:
    cfg = load_runtime_config(config)
    rule = apply_rules(line, config=cfg)
    if not rule.triggered or not rule.matched_category:
        return AlertRunResult(
            triggered=False,
            pushed=False,
            saved=False,
            category=None,
            analysis="",
        )

    cooldown = cooldown_store or _DEFAULT_COOLDOWN_STORE
    check_time = float(time.time() if now is None else now)
    dedup_key = (host, container_id, rule.matched_category)
    analysis_mode = _resolve_alert_analysis_mode(cfg)
    _llm_cfg = cfg.get("llm") if isinstance(cfg.get("llm"), dict) else None
    _llm_model = (_llm_cfg or {}).get("default_model") or (_llm_cfg or {}).get("model") or None
    _llm_params = resolve_llm_params(_llm_model, _llm_cfg)

    if mute_checker is not None:
        try:
            mute_result = mute_checker(
                host=host,
                container_id=container_id,
                category=rule.matched_category,
                at_time=timestamp or None,
            )
            if inspect.isawaitable(mute_result):
                mute_result = await mute_result
        except Exception:  # noqa: BLE001
            logger.exception(
                "mute check failed host=%s container_id=%s category=%s",
                host,
                container_id,
                rule.matched_category,
            )
            mute_result = None
        if mute_result is not None:
            return AlertRunResult(
                triggered=True,
                pushed=False,
                saved=False,
                category=rule.matched_category,
                analysis="",
            )

    notify_impl = notifier_send or _default_notify
    save_impl = save_alert or _default_save
    save_storm_event_impl = save_storm_event or _default_save_storm_event

    extra_pushes = await _emit_storm_due_messages(
        storm_controller=storm_controller,
        now=check_time,
        notify_impl=notify_impl,
        save_impl=save_impl,
        save_storm_event_impl=save_storm_event_impl,
    )

    storm_action = None
    if storm_controller is not None:
        storm_action = storm_controller.record_event(
            host=host,
            category=rule.matched_category,
            line=rule.redacted_line,
            now=check_time,
        )

    if storm_action is not None and storm_action.get("mode") == "suppressed":
        payload = {
            "host": host,
            "container_id": container_id,
            "container_name": container_name or container_id,
            "category": rule.matched_category,
            "line": rule.redacted_line,
            "analysis": "",
            "pushed": False,
            "storm_suppressed": True,
        }
        try:
            saved = bool(await save_impl(payload))
        except Exception:  # noqa: BLE001
            logger.exception(
                "save_alert failed host=%s container_id=%s category=%s",
                host,
                container_id,
                rule.matched_category,
            )
            saved = False
        return AlertRunResult(
            triggered=True,
            pushed=bool(extra_pushes),
            saved=saved,
            category=rule.matched_category,
            analysis="",
        )

    allowed = cooldown.allow(host, container_id, rule.matched_category, now=check_time)
    if not allowed:
        if now is None:
            last_allowed_at = cooldown.last_allowed_at(
                host, container_id, rule.matched_category
            )
            window_seconds = cooldown.window_seconds(rule.matched_category)
            window_end_at = (
                (last_allowed_at + window_seconds)
                if last_allowed_at is not None
                else (check_time + window_seconds)
            )
            _schedule_dedup_summary(
                host=host,
                container_id=container_id,
                container_name=container_name or container_id,
                category=rule.matched_category,
                check_time=check_time,
                redacted_line=rule.redacted_line,
                prompt_template=prompt_template,
                output_template=output_template,
                analysis_mode=analysis_mode,
                llm_model=_llm_model,
                llm_config=_llm_cfg,
                notifier_send=notifier_send,
                save_alert=save_alert,
                mute_checker=mute_checker,
                window_seconds=window_seconds,
                window_end_at=window_end_at,
            )
        return AlertRunResult(
            triggered=True,
            pushed=False,
            saved=False,
            category=rule.matched_category,
            analysis="",
        )
    with _DEDUP_LOCK:
        _PENDING_DEDUP.pop(dedup_key, None)

    if storm_action is not None and storm_action.get("mode") == "storm_start":
        pushed, saved = await _emit_storm_message(
            storm_action,
            notify_impl=notify_impl,
            save_impl=save_impl,
            save_storm_event_impl=save_storm_event_impl,
            fallback_host=host,
            container_id=container_id,
        )
        _schedule_storm_end_flush(
            storm_controller=storm_controller,
            category=rule.matched_category,
            suppress_minutes=float(getattr(storm_controller, "suppress_minutes", 0.0)),
            notify_impl=notify_impl,
            save_impl=save_impl,
            save_storm_event_impl=save_storm_event_impl,
        )
        return AlertRunResult(
            triggered=True,
            pushed=pushed or bool(extra_pushes),
            saved=saved,
            category=rule.matched_category,
            analysis="",
        )

    context = {
        "host_name": host,
        "container_name": container_name or container_id,
        "timestamp": timestamp,
        "logs": [rule.redacted_line],
    }
    if analysis_mode == "script":
        analysis = _build_script_alert_analysis(
            host=host,
            container_name=container_name or container_id,
            category=rule.matched_category,
            timestamp=timestamp,
            line=rule.redacted_line,
        )
    elif analysis_mode == "llm":
        analysis = analyze_with_template(
            "alert",
            context,
            prompt_template,
            enable_agent=True,
            model=_llm_params.model or None,
            api_base=_llm_params.api_base or None,
            api_key=_llm_params.api_key or None,
            provider_type=_llm_params.provider_type or None,
        )
    else:
        analysis = analyze_with_template(
            "alert",
            context,
            prompt_template,
            enable_agent=False,
            model=_llm_params.model or None,
            api_base=_llm_params.api_base or None,
            api_key=_llm_params.api_key or None,
            provider_type=_llm_params.provider_type or None,
        )
    message = _render_notification_message(
        output_template=output_template,
        host=host,
        container_name=container_name or container_id,
        category=rule.matched_category,
        analysis=analysis,
        line=rule.redacted_line,
        timestamp=timestamp,
        direct_analysis_output=analysis_mode in {"llm", "script"},
    )

    try:
        pushed = bool(
            await _call_notify(
                notify_impl,
                host=host,
                message=message,
                category=rule.matched_category,
                context={
                    "container_id": container_id,
                    "container_name": container_name or container_id,
                },
            )
        )
    except Exception:  # noqa: BLE001
        logger.exception(
            "notify failed host=%s container_id=%s category=%s",
            host,
            container_id,
            rule.matched_category,
        )
        pushed = False

    payload = {
        "host": host,
        "container_id": container_id,
        "category": rule.matched_category,
        "line": rule.redacted_line,
        "analysis": analysis,
        "pushed": pushed,
    }
    try:
        saved = bool(await save_impl(payload))
    except Exception:  # noqa: BLE001
        logger.exception(
            "save_alert failed host=%s container_id=%s category=%s",
            host,
            container_id,
            rule.matched_category,
        )
        saved = False

    return AlertRunResult(
        triggered=True,
        pushed=pushed,
        saved=saved,
        category=rule.matched_category,
        analysis=analysis,
    )


class LogStreamWatcher:
    def __init__(
        self,
        *,
        host: dict[str, Any],
        stream_logs: Any,
        list_containers: Any | None = None,
        run_alert: Any | None = None,
        notifier_send: Any | None = None,
        save_alert: Any | None = None,
        metrics_writer: Any | None = None,
        preprocessors: list[Any] | None = None,
        cooldown_store: CooldownStore | None = None,
        prompt_template: str | None = None,
        output_template: str | None = None,
        storm_controller: Any | None = None,
        config: dict[str, Any] | None = None,
    ) -> None:
        self._host = dict(host)
        self._stream_logs = stream_logs
        self._list_containers = list_containers
        self._run_alert = run_alert or run_alert_once
        self._notifier_send = notifier_send
        self._save_alert = save_alert
        self._metrics_writer = metrics_writer
        self._preprocessors = (
            list(preprocessors) if preprocessors is not None else load_preprocessors()
        )
        # Keep cooldown state scoped to watcher/app instance instead of process-global default.
        self._cooldown_store = cooldown_store or CooldownStore()
        self._prompt_template = str(
            prompt_template or self._host.get("prompt_template") or "default_alert"
        )
        self._output_template = str(
            output_template or self._host.get("output_template") or "standard"
        )
        self._storm_controller = storm_controller
        self._alert_config = deepcopy(config if config is not None else self._host)
        self._container_tasks: dict[str, asyncio.Task[None]] = {}

        watch_cfg = self._host.get("watch")
        watch_settings = dict(watch_cfg) if isinstance(watch_cfg, dict) else {}
        self._queue_maxsize = max(1, int(watch_settings.get("queue_maxsize", 1024)))
        self._worker_count = max(1, int(watch_settings.get("worker_count", 2)))
        self._drop_when_full = bool(watch_settings.get("drop_when_full", True))
        self._watch_lookback_seconds = max(
            0,
            int(watch_settings.get("lookback_seconds", 300)),
        )
        self._reconnect_backoff_seconds = min(
            max(
                0.0,
                float(
                    watch_settings.get(
                        "reconnect_backoff_seconds",
                        _DEFAULT_RECONNECT_BACKOFF_SECONDS,
                    )
                ),
            ),
            _MAX_RECONNECT_BACKOFF_SECONDS,
        )
        self._queue: asyncio.Queue[_AlertItem | None] = asyncio.Queue(
            maxsize=self._queue_maxsize
        )
        self._worker_tasks: list[asyncio.Task[None]] = []
        self._dropped_events = 0

    @property
    def dropped_events(self) -> int:
        return int(self._dropped_events)

    async def start(self) -> None:
        await self.refresh_containers()

    async def refresh_containers(self) -> None:
        if self._list_containers is None:
            return

        host_name = str(self._host.get("name") or "")
        containers = await _maybe_await(self._list_containers(host_name))
        current: dict[str, dict[str, Any]] = {}
        for container in list(containers or []):
            container_id = str(
                container.get("id") or container.get("container_id") or ""
            ).strip()
            if container_id == "":
                continue
            if not self._should_watch_container(container):
                continue
            current[container_id] = dict(container)
            if container_id in self._container_tasks:
                continue
            task = asyncio.create_task(self.watch_container(dict(container)))
            task.add_done_callback(
                lambda _task, cid=container_id: self._container_tasks.pop(cid, None)
            )
            self._container_tasks[container_id] = task

        stale_ids = [
            container_id
            for container_id in self._container_tasks
            if container_id not in current
        ]
        for container_id in stale_ids:
            task = self._container_tasks.pop(container_id)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def shutdown(self) -> None:
        tasks = list(self._container_tasks.values())
        self._container_tasks.clear()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await self._stop_workers()

    async def watch_container(self, container: dict[str, Any]) -> None:
        await self._ensure_workers()

        host_name = str(self._host.get("name") or "default")
        container_id = str(
            container.get("id") or container.get("container_id") or "container-unknown"
        )
        container_name = str(container.get("name") or container_id)
        c_prompt, c_output, c_config = self._resolve_container_config(container_name)
        current_task = asyncio.current_task()
        managed_mode = (
            current_task is not None and current_task in self._container_tasks.values()
        )

        # Determine initial lookback window
        lookback = self._watch_lookback_seconds
        last_log_ts: float | None = None

        while True:
            try:
                # Calculate 'since' for this stream iteration
                stream_kwargs: dict[str, Any] = {}
                if last_log_ts is not None:
                    # Reconnect: resume from last processed timestamp
                    stream_kwargs["since"] = int(last_log_ts)
                elif lookback > 0:
                    # First connect: only look back N seconds
                    stream_kwargs["since"] = int(time.time() - lookback)

                async for record in self._stream_logs(self._host, container, **stream_kwargs):
                    # Track timestamp for reconnect resume
                    ts_raw = record.get("timestamp") or ""
                    if ts_raw:
                        parsed_ts = _parse_log_timestamp(ts_raw)
                        if parsed_ts is not None:
                            last_log_ts = parsed_ts

                    lines = self._apply_preprocessors(
                        LogLine(
                            host_name=host_name,
                            container_id=container_id,
                            container_name=container_name,
                            timestamp=str(record.get("timestamp") or ""),
                            content=str(record.get("line") or ""),
                            metadata=dict(record),
                        )
                    )
                    for line in lines:
                        alert_kwargs = {
                            "host": host_name,
                            "container_id": container_id,
                            "container_name": container_name,
                            "timestamp": line.timestamp,
                            "cooldown_store": self._cooldown_store,
                            "notifier_send": self._notifier_send,
                            "save_alert": self._build_save_alert(),
                            "save_storm_event": self._build_save_storm_event(),
                            "mute_checker": self._build_mute_checker(),
                            "prompt_template": c_prompt,
                            "output_template": c_output,
                            "config": c_config,
                        }
                        if self._storm_controller is not None:
                            alert_kwargs["storm_controller"] = self._storm_controller
                        await self._enqueue_alert_item(
                            _AlertItem(
                                line=line.content,
                                kwargs=alert_kwargs,
                            )
                        )
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                if _is_container_gone(exc):
                    logger.warning(
                        "container gone, stopping watcher host=%s container=%s",
                        host_name,
                        container_name,
                    )
                    return
                if not managed_mode:
                    raise
                logger.exception(
                    "log stream watcher crashed host=%s container=%s",
                    host_name,
                    container_name,
                )
            await self._queue.join()
            if not managed_mode:
                return
            if self._reconnect_backoff_seconds > 0:
                await asyncio.sleep(self._reconnect_backoff_seconds)

    def _apply_preprocessors(self, line: LogLine) -> list[LogLine]:
        current = [line]
        for preprocessor in self._preprocessors:
            current = list(preprocessor.process(current))
        return current

    def _should_watch_container(self, container: dict[str, Any]) -> bool:
        container_cfg = self._host.get("containers")
        if not isinstance(container_cfg, dict):
            return True

        include = container_cfg.get("include")
        exclude = container_cfg.get("exclude")
        name = str(container.get("name") or "")
        container_id = str(container.get("id") or container.get("container_id") or "")

        if isinstance(include, list) and include:
            if not any(
                _container_pattern_matches(item, name=name, container_id=container_id)
                for item in include
            ):
                return False

        if isinstance(exclude, list) and exclude:
            if any(
                _container_pattern_matches(item, name=name, container_id=container_id)
                for item in exclude
            ):
                return False

        return True

    def _resolve_container_config(
        self, container_name: str
    ) -> tuple[str, str, dict[str, Any]]:
        """Resolve prompt_template, output_template, and alert config for a container.

        Checks containers.overrides for matching patterns (fnmatch).
        Falls back to host-level defaults.
        """
        prompt = self._prompt_template
        output = self._output_template
        config = self._alert_config

        containers_cfg = self._host.get("containers")
        if not isinstance(containers_cfg, dict):
            return prompt, output, config
        overrides = containers_cfg.get("overrides")
        if not isinstance(overrides, dict):
            return prompt, output, config

        # Find first matching override (fnmatch pattern)
        matched: dict[str, Any] | None = None
        for pattern, override_cfg in overrides.items():
            if not isinstance(override_cfg, dict):
                continue
            if fnmatch(container_name, str(pattern)):
                matched = override_cfg
                break

        if matched is None:
            return prompt, output, config

        if "prompt_template" in matched:
            prompt = str(matched["prompt_template"])
        if "output_template" in matched:
            output = str(matched["output_template"])
        needs_config_merge = "rules" in matched or "llm" in matched
        if needs_config_merge:
            merged_config = deepcopy(config)

            if "rules" in matched:
                # Merge: host rules as base, override rules on top
                override_rules = matched["rules"]
                if isinstance(override_rules, dict):
                    existing_rules = merged_config.get("rules") or {}
                    if not isinstance(existing_rules, dict):
                        existing_rules = {}
                    merged_rules = dict(existing_rules)
                    # For list fields, override replaces
                    for key in ("ignore", "redact", "custom_alerts", "alert_keywords"):
                        if key in override_rules:
                            merged_rules[key] = override_rules[key]
                    merged_config["rules"] = merged_rules

            if "llm" in matched:
                override_llm = matched["llm"]
                if isinstance(override_llm, dict):
                    existing_llm = merged_config.get("llm") or {}
                    if not isinstance(existing_llm, dict):
                        existing_llm = {}
                    merged_llm = dict(existing_llm)
                    merged_llm.update(override_llm)
                    # Ensure providers are inherited from host config if not overridden
                    if "providers" not in override_llm and "providers" in existing_llm:
                        merged_llm["providers"] = existing_llm["providers"]
                    merged_config["llm"] = merged_llm

            config = merged_config

        return prompt, output, config

    def _build_save_alert(self):
        if self._save_alert is not None:
            return self._save_alert
        writer = self._metrics_writer
        if writer is None or not hasattr(writer, "write_alert"):
            return None

        async def save_alert(payload: dict[str, Any]) -> bool:
            await _maybe_await(writer.write_alert(payload))
            return True

        return save_alert

    def _build_mute_checker(self):
        writer = self._metrics_writer
        if writer is None or not hasattr(writer, "find_active_mute"):
            return None

        async def mute_checker(
            *,
            host: str,
            container_id: str,
            category: str,
            at_time: str | None = None,
        ):
            return await _maybe_await(
                writer.find_active_mute(
                    host=host,
                    container_id=container_id,
                    category=category,
                    at_time=at_time,
                )
            )

        return mute_checker

    def _build_save_storm_event(self):
        writer = self._metrics_writer
        if writer is None or not hasattr(writer, "write_storm_event"):
            return None

        async def save_storm_event(payload: dict[str, Any]) -> bool:
            await _maybe_await(writer.write_storm_event(payload))
            return True

        return save_storm_event

    async def _ensure_workers(self) -> None:
        if self._worker_tasks:
            return
        for idx in range(self._worker_count):
            self._worker_tasks.append(asyncio.create_task(self._alert_worker(idx)))

    async def _enqueue_alert_item(self, item: _AlertItem) -> None:
        try:
            self._queue.put_nowait(item)
        except asyncio.QueueFull:
            if self._drop_when_full:
                self._dropped_events += 1
            else:
                await self._queue.put(item)

    async def _alert_worker(self, worker_idx: int) -> None:
        while True:
            item = await self._queue.get()
            try:
                if item is None:
                    return
                await _maybe_await(self._run_alert(item.line, **item.kwargs))
            except Exception:  # noqa: BLE001
                logger.exception("log alert worker failed worker=%s", worker_idx)
            finally:
                self._queue.task_done()

    async def _stop_workers(self) -> None:
        if not self._worker_tasks:
            return

        await self._queue.join()
        for _ in self._worker_tasks:
            await self._queue.put(None)
        workers = list(self._worker_tasks)
        self._worker_tasks.clear()
        await asyncio.gather(*workers, return_exceptions=True)


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


def _schedule_storm_end_flush(
    *,
    storm_controller: Any,
    category: str,
    suppress_minutes: float,
    notify_impl: Any,
    save_impl: Any,
    save_storm_event_impl: Any,
) -> None:
    if suppress_minutes <= 0:
        return
    key = (id(storm_controller), str(category))
    delay = float(suppress_minutes) * 60.0
    task = asyncio.create_task(
        _flush_storm_end_after_delay(
            key=key,
            delay=delay,
            storm_controller=storm_controller,
            notify_impl=notify_impl,
            save_impl=save_impl,
            save_storm_event_impl=save_storm_event_impl,
        )
    )
    with _STORM_TASK_LOCK:
        previous = _PENDING_STORM_END_TASKS.get(key)
        _PENDING_STORM_END_TASKS[key] = task
    if previous is not None and not previous.done():
        previous.cancel()
    task.add_done_callback(lambda _: _mark_storm_task_done(key))


async def _flush_storm_end_after_delay(
    *,
    key: tuple[int, str],
    delay: float,
    storm_controller: Any,
    notify_impl: Any,
    save_impl: Any,
    save_storm_event_impl: Any,
) -> None:
    try:
        await asyncio.sleep(max(0.0, delay))
        await _emit_storm_due_messages(
            storm_controller=storm_controller,
            now=float(time.time()),
            notify_impl=notify_impl,
            save_impl=save_impl,
            save_storm_event_impl=save_storm_event_impl,
        )
    except asyncio.CancelledError:
        return
    finally:
        _mark_storm_task_done(key)


async def _emit_storm_due_messages(
    *,
    storm_controller: Any,
    now: float,
    notify_impl: Any,
    save_impl: Any,
    save_storm_event_impl: Any,
) -> int:
    if storm_controller is None:
        return 0
    count = 0
    for item in list(storm_controller.flush_due(now)):
        pushed, _saved = await _emit_storm_message(
            item,
            notify_impl=notify_impl,
            save_impl=save_impl,
            save_storm_event_impl=save_storm_event_impl,
            fallback_host=str(item.get("host") or "__storm__"),
            container_id="__storm__",
        )
        if pushed:
            count += 1
    return count


async def _emit_storm_message(
    item: dict[str, Any],
    *,
    notify_impl: Any,
    save_impl: Any,
    save_storm_event_impl: Any,
    fallback_host: str,
    container_id: str,
) -> tuple[bool, bool]:
    host = str(item.get("host") or fallback_host)
    category = str(item.get("category") or "STORM")
    message = str(item.get("message") or "")
    pushed = False
    try:
        pushed = bool(
            await _call_notify(
                notify_impl,
                host=host,
                message=message,
                category=category,
                context={"container_id": container_id, "container_name": container_id},
            )
        )
    except Exception:  # noqa: BLE001
        logger.exception("storm notify failed host=%s category=%s", host, category)
    payload = {
        "host": host,
        "container_id": container_id,
        "category": category,
        "line": message,
        "analysis": message,
        "pushed": pushed,
    }
    payload.update(dict(item.get("payload") or {}))
    saved = True
    try:
        save_result = await save_impl(payload)
        if save_result is False:
            saved = False
    except Exception:  # noqa: BLE001
        logger.exception("storm save failed host=%s category=%s", host, category)
        saved = False

    storm_event_payload = {
        "host": host,
        "category": str(item.get("payload", {}).get("category") or category),
        "storm_phase": str(item.get("payload", {}).get("storm_phase") or "unknown"),
        "storm_total": item.get("payload", {}).get("storm_total"),
        "storm_hosts": item.get("payload", {}).get("storm_hosts"),
        "message": message,
        "pushed": pushed,
    }
    try:
        await save_storm_event_impl(storm_event_payload)
    except Exception:  # noqa: BLE001
        logger.exception("storm event save failed host=%s category=%s", host, category)

    return pushed, saved


async def cancel_pending_dedup_tasks() -> None:
    with _DEDUP_LOCK:
        tasks = list(_PENDING_DEDUP_TASKS.values())
        _PENDING_DEDUP_TASKS.clear()
        _PENDING_DEDUP.clear()
    with _STORM_TASK_LOCK:
        storm_tasks = list(_PENDING_STORM_END_TASKS.values())
        _PENDING_STORM_END_TASKS.clear()
    for task in tasks:
        task.cancel()
    for task in storm_tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    if storm_tasks:
        await asyncio.gather(*storm_tasks, return_exceptions=True)


async def reset_alert_runtime_state_for_tests() -> None:
    await cancel_pending_dedup_tasks()
    _DEFAULT_COOLDOWN_STORE.reset()


async def _call_notify(
    notify_impl: Any,
    *,
    host: str,
    message: str,
    category: str,
    context: dict[str, Any] | None = None,
) -> Any:
    if _notify_callable_accepts_context(notify_impl):
        return await _maybe_await(
            notify_impl(host, message, category, context=dict(context or {}))
        )
    return await _maybe_await(notify_impl(host, message, category))


def _notify_callable_accepts_context(notify_impl: Any) -> bool:
    try:
        signature = inspect.signature(notify_impl)
    except (TypeError, ValueError):
        return True

    for param in signature.parameters.values():
        if param.kind is inspect.Parameter.VAR_KEYWORD:
            return True
    return "context" in signature.parameters


def _render_notification_message(
    *,
    output_template: str,
    host: str,
    container_name: str,
    category: str,
    analysis: str,
    line: str,
    timestamp: str,
    direct_analysis_output: bool = False,
) -> str:
    if direct_analysis_output:
        return str(analysis)

    render_context = {
        "severity": category,
        "category": category,
        "host_name": host,
        "container_name": container_name,
        "timestamp": timestamp,
        "llm_summary": analysis,
        "llm_causes": "-",
        "llm_actions": "-",
        "llm_timeline": "-",
        "llm_impact": "-",
        "llm_business_metrics": "-",
        "llm_recovery_time": "-",
        "log_snippet": line,
        "metrics_detail": "-",
        "cpu": "-",
        "mem_used": "-",
        "mem_limit": "-",
        "restart_count": "-",
        "total_containers": "-",
        "container_status_table": "-",
        "last_alert_time": timestamp,
        "last_alert_summary": analysis,
    }
    try:
        return render_output(output_template, render_context)
    except Exception:  # noqa: BLE001
        logger.warning(
            "output render failed host=%s container=%s template=%s",
            host,
            container_name,
            output_template,
            exc_info=True,
        )
        return analysis


def _resolve_alert_analysis_mode(config: dict[str, Any]) -> str:
    llm_cfg = config.get("llm")
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


def _build_script_alert_analysis(
    *,
    host: str,
    container_name: str,
    category: str,
    timestamp: str,
    line: str,
) -> str:
    return "\n".join(
        [
            "[SCRIPT] ALERT",
            f"host={host}",
            f"container={container_name}",
            f"category={category}",
            f"timestamp={timestamp or '-'}",
            "log:",
            str(line or ""),
        ]
    ).strip()


def _container_pattern_matches(pattern: Any, *, name: str, container_id: str) -> bool:
    text = str(pattern or "").strip()
    if text == "":
        return False
    return text in name or text in container_id


def _is_container_gone(exc: BaseException) -> bool:
    """Check if the exception indicates the container no longer exists."""
    # docker.errors.NotFound (404)
    cls_name = type(exc).__name__
    if cls_name == "NotFound":
        return True
    # Walk the exception chain (e.g. raised ... from NotFound)
    cause = exc.__cause__
    if cause is not None and type(cause).__name__ == "NotFound":
        return True
    return False
