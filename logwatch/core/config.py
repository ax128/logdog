from __future__ import annotations

import os
import re
from copy import deepcopy
from typing import Any, cast

import yaml


RULE_FIELDS = ("ignore", "redact", "custom_alerts")
MODE_REPLACE = "replace"
ALLOWED_MODES = {"append", MODE_REPLACE}
NOTIFY_LIST_FIELDS = {"chat_ids", "webhook_urls"}
NOTIFY_ROOT_LIST_FIELDS = {"channels"}
DEFAULT_METRICS_INTERVAL_SECONDS = 30
DEFAULT_HOST_SYSTEM_SAMPLE_INTERVAL_SECONDS = 30
DEFAULT_HOST_SYSTEM_SECURITY_ENABLED = False
DEFAULT_HOST_SYSTEM_SECURITY_PUSH_ON_ISSUE = True
DEFAULT_RETENTION_INTERVAL_SECONDS = 86400
DEFAULT_HOST_SYSTEM_WARN_THRESHOLDS = {
    "cpu_percent": 85.0,
    "mem_used_percent": 90.0,
    "disk_used_percent": 90.0,
}
RUNTIME_RETENTION_KEYS = (
    "alerts_days",
    "audit_days",
    "send_failed_days",
    "metrics_days",
    "host_metrics_days",
    "storm_days",
)
DEFAULT_CONFIG_PATH = "config/logwatch.yaml"
DEFAULT_SQLITE_JOURNAL_MODE = "wal"
DEFAULT_SQLITE_SYNCHRONOUS = "normal"
DEFAULT_SQLITE_BUSY_TIMEOUT_MS = 5000
DEFAULT_SCHEDULER_MAX_INSTANCES = 1
DEFAULT_SCHEDULER_COALESCE = True
DEFAULT_SCHEDULER_MISFIRE_GRACE_TIME = 30
_SQLITE_JOURNAL_MODES = {"delete", "truncate", "persist", "memory", "wal", "off"}
_SQLITE_SYNCHRONOUS_MODES = {"off", "normal", "full", "extra"}
_INT_LIKE_PATTERN = re.compile(r"^[+-]?\d+$")
_ENV_VAR_PATTERN = re.compile(r"\$\{([^}]+)\}")


def _expand_env_vars(value: Any) -> Any:
    """Recursively expand ``${VAR}`` references in string values."""
    if isinstance(value, str):
        return _ENV_VAR_PATTERN.sub(
            lambda m: os.environ.get(m.group(1), ""), value
        )
    if isinstance(value, dict):
        return {k: _expand_env_vars(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand_env_vars(item) for item in value]
    return value


def resolve_config_path(config_path: str | os.PathLike[str] | None) -> str:
    if config_path is not None:
        explicit_path = str(config_path).strip()
        if explicit_path:
            return explicit_path

    env_path = os.getenv("LOGWATCH_CONFIG", "").strip()
    if env_path:
        return env_path

    return DEFAULT_CONFIG_PATH


def load_app_config(config_path: str | os.PathLike[str] | None) -> dict[str, Any]:
    resolved_path = resolve_config_path(config_path)
    with open(resolved_path, encoding="utf-8") as handle:
        loaded = yaml.safe_load(handle)

    if not isinstance(loaded, dict):
        raise TypeError("config root must be a mapping")

    return _expand_env_vars(loaded)


def expand_effective_hosts(app_config: dict | None) -> list[dict[str, Any]]:
    cfg = _ensure_dict(app_config, "root", "app_config")
    defaults = _ensure_dict(cfg.get("defaults"), "defaults", "app_config")
    raw_hosts = cfg.get("hosts")
    if raw_hosts is None:
        return []
    if not isinstance(raw_hosts, (list, tuple)):
        raise TypeError(
            f"app_config.hosts must be a list or tuple, got {type(raw_hosts).__name__}"
        )

    expanded: list[dict[str, Any]] = []
    for idx, host in enumerate(raw_hosts):
        if not isinstance(host, dict):
            raise TypeError(
                f"app_config.hosts[{idx}] must be a dict, got {type(host).__name__}"
            )
        expanded.append(merge_host_config(defaults, host))

    _ensure_unique_host_names(expanded, source="app_config.hosts")
    return expanded


def merge_host_config(defaults: dict | None, host: dict | None) -> dict:
    defaults = _ensure_dict(defaults, "root", "defaults")
    host = _ensure_dict(host, "root", "host")
    merged = deepcopy(defaults)

    schedules_mode = _resolve_schedules_mode(
        defaults.get("schedules_mode"), host.get("schedules_mode")
    )
    default_schedules = _ensure_list(defaults.get("schedules"), "schedules", "defaults")
    host_schedules = _ensure_list(host.get("schedules"), "schedules", "host")
    _ensure_schedule_items(default_schedules, "defaults.schedules")
    _ensure_schedule_items(host_schedules, "host.schedules")

    merged["schedules_mode"] = schedules_mode
    merged["schedules"] = _merge_schedules(
        default_schedules, host_schedules, schedules_mode
    )

    containers = _merge_containers(defaults.get("containers"), host.get("containers"))
    if containers:
        merged["containers"] = containers
    else:
        merged.pop("containers", None)

    rules = _merge_rules(defaults.get("rules"), host.get("rules"))
    if rules:
        merged["rules"] = rules
    else:
        merged.pop("rules", None)

    notify = _merge_notify(defaults.get("notify"), host.get("notify"))
    if notify:
        merged["notify"] = notify
    else:
        merged.pop("notify", None)

    for key, value in host.items():
        if key in {"containers", "rules", "notify", "schedules", "schedules_mode"}:
            continue
        merged[key] = deepcopy(value)

    return merged


def resolve_runtime_settings(app_config: dict | None) -> dict[str, Any]:
    cfg = _ensure_dict(app_config, "root", "app_config")
    metrics_cfg = _ensure_dict(cfg.get("metrics"), "metrics", "app_config")
    host_system_cfg = _ensure_dict(
        metrics_cfg.get("host_system"),
        "metrics.host_system",
        "app_config",
    )
    host_system_security_cfg = _ensure_dict(
        host_system_cfg.get("security"),
        "metrics.host_system.security",
        "app_config",
    )
    host_system_report_cfg = _ensure_dict(
        host_system_cfg.get("report"),
        "metrics.host_system.report",
        "app_config",
    )
    host_system_warn_cfg = _ensure_dict(
        host_system_report_cfg.get("warn_thresholds"),
        "metrics.host_system.report.warn_thresholds",
        "app_config",
    )
    storage_cfg = _ensure_dict(cfg.get("storage"), "storage", "app_config")
    sqlite_cfg = _ensure_dict(storage_cfg.get("sqlite"), "storage.sqlite", "app_config")
    scheduler_cfg = _ensure_dict(cfg.get("scheduler"), "scheduler", "app_config")
    retention_cfg = _ensure_dict(
        storage_cfg.get("retention"), "storage.retention", "app_config"
    )
    docker_cfg = _ensure_dict(cfg.get("docker"), "docker", "app_config")
    docker_pool_cfg = _ensure_dict(docker_cfg.get("pool"), "docker.pool", "app_config")

    metrics_interval_seconds = _coerce_positive_int(
        metrics_cfg.get("sample_interval_seconds"),
        default=DEFAULT_METRICS_INTERVAL_SECONDS,
        field_name="app_config.metrics.sample_interval_seconds",
    )
    retention_interval_seconds = _coerce_positive_int(
        retention_cfg.get("cleanup_interval_seconds"),
        default=DEFAULT_RETENTION_INTERVAL_SECONDS,
        field_name="app_config.storage.retention.cleanup_interval_seconds",
    )
    metrics_db_path = _coerce_optional_str(
        storage_cfg.get("db_path"),
        field_name="app_config.storage.db_path",
    )
    sqlite_journal_mode = _coerce_sqlite_journal_mode(
        sqlite_cfg.get("journal_mode"),
        field_name="app_config.storage.sqlite.journal_mode",
        default=DEFAULT_SQLITE_JOURNAL_MODE,
    )
    sqlite_synchronous = _coerce_sqlite_synchronous(
        sqlite_cfg.get("synchronous"),
        field_name="app_config.storage.sqlite.synchronous",
        default=DEFAULT_SQLITE_SYNCHRONOUS,
    )
    sqlite_busy_timeout_ms = _coerce_positive_int(
        sqlite_cfg.get("busy_timeout_ms"),
        default=DEFAULT_SQLITE_BUSY_TIMEOUT_MS,
        field_name="app_config.storage.sqlite.busy_timeout_ms",
    )
    scheduler_max_instances = _coerce_positive_int(
        scheduler_cfg.get("max_instances"),
        default=DEFAULT_SCHEDULER_MAX_INSTANCES,
        field_name="app_config.scheduler.max_instances",
    )
    scheduler_coalesce = _coerce_bool(
        scheduler_cfg.get("coalesce"),
        default=DEFAULT_SCHEDULER_COALESCE,
        field_name="app_config.scheduler.coalesce",
    )
    scheduler_misfire_grace_time = _coerce_positive_int(
        scheduler_cfg.get("misfire_grace_time"),
        default=DEFAULT_SCHEDULER_MISFIRE_GRACE_TIME,
        field_name="app_config.scheduler.misfire_grace_time",
    )
    host_system_enabled = _coerce_bool(
        host_system_cfg.get("enabled"),
        default=False,
        field_name="app_config.metrics.host_system.enabled",
    )
    host_system_sample_interval_seconds = _coerce_positive_int(
        host_system_cfg.get("sample_interval_seconds"),
        default=DEFAULT_HOST_SYSTEM_SAMPLE_INTERVAL_SECONDS,
        field_name="app_config.metrics.host_system.sample_interval_seconds",
    )
    host_system_collect_load = _coerce_bool(
        host_system_cfg.get("collect_load"),
        default=True,
        field_name="app_config.metrics.host_system.collect_load",
    )
    host_system_collect_network = _coerce_bool(
        host_system_cfg.get("collect_network"),
        default=True,
        field_name="app_config.metrics.host_system.collect_network",
    )
    host_system_security_enabled = _coerce_bool(
        host_system_security_cfg.get("enabled"),
        default=DEFAULT_HOST_SYSTEM_SECURITY_ENABLED,
        field_name="app_config.metrics.host_system.security.enabled",
    )
    host_system_security_push_on_issue = _coerce_bool(
        host_system_security_cfg.get("push_on_issue"),
        default=DEFAULT_HOST_SYSTEM_SECURITY_PUSH_ON_ISSUE,
        field_name="app_config.metrics.host_system.security.push_on_issue",
    )
    host_system_include_in_schedule = _coerce_bool(
        host_system_report_cfg.get("include_in_schedule"),
        default=True,
        field_name="app_config.metrics.host_system.report.include_in_schedule",
    )
    host_system_warn_thresholds = {
        "cpu_percent": _coerce_percentage(
            host_system_warn_cfg.get("cpu_percent"),
            default=DEFAULT_HOST_SYSTEM_WARN_THRESHOLDS["cpu_percent"],
            field_name="app_config.metrics.host_system.report.warn_thresholds.cpu_percent",
        ),
        "mem_used_percent": _coerce_percentage(
            host_system_warn_cfg.get("mem_used_percent"),
            default=DEFAULT_HOST_SYSTEM_WARN_THRESHOLDS["mem_used_percent"],
            field_name="app_config.metrics.host_system.report.warn_thresholds.mem_used_percent",
        ),
        "disk_used_percent": _coerce_percentage(
            host_system_warn_cfg.get("disk_used_percent"),
            default=DEFAULT_HOST_SYSTEM_WARN_THRESHOLDS["disk_used_percent"],
            field_name="app_config.metrics.host_system.report.warn_thresholds.disk_used_percent",
        ),
    }
    host_system_settings = {
        "enabled": host_system_enabled,
        "sample_interval_seconds": host_system_sample_interval_seconds,
        "collect_load": host_system_collect_load,
        "collect_network": host_system_collect_network,
        "security": {
            "enabled": host_system_security_enabled,
            "push_on_issue": host_system_security_push_on_issue,
        },
        "report": {
            "include_in_schedule": host_system_include_in_schedule,
            "warn_thresholds": host_system_warn_thresholds,
        },
    }

    retention_days: dict[str, int] = {}
    for key in RUNTIME_RETENTION_KEYS:
        if key not in retention_cfg:
            continue
        value = retention_cfg.get(key)
        days = _coerce_int_like(
            value,
            field_name=f"app_config.storage.retention.{key}",
        )
        if days < 0:
            raise ValueError(f"app_config.storage.retention.{key} must be >= 0")
        retention_days[key] = days

    docker_pool_max_clients = _coerce_optional_positive_int(
        docker_pool_cfg.get("max_clients"),
        field_name="app_config.docker.pool.max_clients",
    )
    docker_pool_max_idle_seconds = _coerce_optional_positive_float(
        docker_pool_cfg.get("max_idle_seconds"),
        field_name="app_config.docker.pool.max_idle_seconds",
    )

    return {
        "metrics_interval_seconds": metrics_interval_seconds,
        "retention_interval_seconds": retention_interval_seconds,
        "metrics_db_path": metrics_db_path,
        "sqlite": {
            "journal_mode": sqlite_journal_mode,
            "synchronous": sqlite_synchronous,
            "busy_timeout_ms": sqlite_busy_timeout_ms,
        },
        "scheduler": {
            "max_instances": scheduler_max_instances,
            "coalesce": scheduler_coalesce,
            "misfire_grace_time": scheduler_misfire_grace_time,
        },
        "retention_config": retention_days,
        "host_system": host_system_settings,
        "docker_pool_max_clients": docker_pool_max_clients,
        "docker_pool_max_idle_seconds": docker_pool_max_idle_seconds,
    }


def _resolve_schedules_mode(default_mode, host_mode):
    if host_mode is not None:
        _validate_mode(host_mode, "schedules_mode", "host")
        return host_mode
    if default_mode is not None:
        _validate_mode(default_mode, "schedules_mode", "defaults")
        return default_mode
    return "append"


def _validate_mode(value: object | None, field_name: str, source: str) -> None:
    if value is None:
        return
    if not isinstance(value, str):
        raise TypeError(
            f"{source}.{field_name} must be a string, got {type(value).__name__}"
        )
    if value not in ALLOWED_MODES:
        raise ValueError(
            f"{source}.{field_name} must be one of {sorted(ALLOWED_MODES)}, got {value!r}"
        )


def _merge_schedules(
    default_schedules: list[dict], host_schedules: list[dict], mode: str
) -> list[dict]:
    default_copies = [deepcopy(item) for item in default_schedules]
    host_copies = [deepcopy(item) for item in host_schedules]

    if mode == MODE_REPLACE:
        return host_copies

    merged: list[dict] = []
    seen: dict[str, int] = {}

    for entry in default_copies + host_copies:
        name = entry.get("name")
        if name is None:
            merged.append(entry)
            continue

        if name in seen:
            previous_index = seen.pop(name)
            merged.pop(previous_index)
            for key, idx in list(seen.items()):
                if idx > previous_index:
                    seen[key] = idx - 1

        merged.append(entry)
        seen[name] = len(merged) - 1

    return merged


def _merge_containers(
    default_containers: dict | None, host_containers: dict | None
) -> dict:
    default_containers = _ensure_dict(default_containers, "containers", "defaults")
    host_containers = _ensure_dict(host_containers, "containers", "host")

    include = None
    if "include" in host_containers:
        include = _ensure_list(
            host_containers.get("include"), "containers.include", "host"
        )
    elif "include" in default_containers:
        include = _ensure_list(
            default_containers.get("include"), "containers.include", "defaults"
        )

    exclude = _ensure_list(
        default_containers.get("exclude"), "containers.exclude", "defaults"
    )
    if "exclude" in host_containers:
        exclude.extend(
            _ensure_list(host_containers.get("exclude"), "containers.exclude", "host")
        )

    result: dict = {}
    if include is not None:
        result["include"] = _copy_sequence(include)
    if exclude or "exclude" in default_containers or "exclude" in host_containers:
        result["exclude"] = _copy_sequence(exclude)

    return result


def _copy_sequence(values: list[Any]) -> list[Any]:
    return [deepcopy(entry) for entry in values]


def _merge_rules(default_rules: dict | None, host_rules: dict | None) -> dict:
    default_rules = _ensure_dict(default_rules, "rules", "defaults")
    host_rules = _ensure_dict(host_rules, "rules", "host")

    merged: dict = {}
    for field in RULE_FIELDS:
        default_values = _ensure_list(
            default_rules.get(field), f"rules.{field}", "defaults"
        )
        host_values = _ensure_list(host_rules.get(field), f"rules.{field}", "host")
        mode_key = f"{field}_mode"
        default_mode_value = default_rules.get(mode_key)
        host_mode_value = host_rules.get(mode_key)

        _validate_mode(default_mode_value, mode_key, "defaults")
        _validate_mode(host_mode_value, mode_key, "host")

        mode_to_use = (
            host_mode_value if host_mode_value is not None else default_mode_value
        )

        combined = _combine_rule_items(default_values, host_values, mode_to_use)
        if combined or host_values or mode_to_use == MODE_REPLACE:
            merged[field] = combined

    return merged


def _combine_rule_items(
    default_values: list[Any], host_values: list[Any], mode: str | None
) -> list[Any]:
    if mode == MODE_REPLACE:
        base_items: list[Any] = []
    else:
        base_items = default_values
    return _dedupe_rule_items(base_items, host_values)


def _dedupe_rule_items(base_items: list[Any], host_items: list[Any]) -> list[Any]:
    result: list[Any] = []
    seen: dict[Any, int] = {}

    for entry in base_items:
        key = _rule_pattern_key(entry)
        if key is None:
            result.append(deepcopy(entry))
            continue
        if key in seen:
            continue
        seen[key] = len(result)
        result.append(deepcopy(entry))

    for entry in host_items:
        key = _rule_pattern_key(entry)
        if key is None:
            result.append(deepcopy(entry))
            continue
        if key in seen:
            index = seen[key]
            result[index] = deepcopy(entry)
        else:
            seen[key] = len(result)
            result.append(deepcopy(entry))

    return result


def _rule_pattern_key(entry: Any) -> Any:
    if isinstance(entry, dict):
        return entry.get("pattern")
    return entry


def _merge_notify(default_notify: dict | None, host_notify: dict | None) -> dict:
    default_notify = _ensure_dict(default_notify, "notify", "defaults")
    host_notify = _ensure_dict(host_notify, "notify", "host")

    if not default_notify and not host_notify:
        return {}

    merged: dict = {}
    for channel, section in default_notify.items():
        if channel in NOTIFY_ROOT_LIST_FIELDS:
            merged[channel] = _copy_sequence(
                _ensure_list(section, f"notify.{channel}", "defaults")
            )
            continue
        section_dict = _ensure_dict(section, f"notify.{channel}", "defaults")
        merged[channel] = _copy_notify_section(section_dict, "defaults", channel)

    for channel, section in host_notify.items():
        if channel in NOTIFY_ROOT_LIST_FIELDS:
            host_values = _copy_sequence(
                _ensure_list(section, f"notify.{channel}", "host")
            )
            if host_values:
                merged[channel] = host_values
            elif channel not in merged:
                merged[channel] = []
            continue
        host_section = _copy_notify_section(
            _ensure_dict(section, f"notify.{channel}", "host"), "host", channel
        )
        merged_channel = merged.setdefault(channel, {})
        merged_channel.update(host_section)

    return merged


def _copy_notify_section(section_data: dict, source: str, channel: str) -> dict:
    result: dict = {}
    for key, value in section_data.items():
        if key in NOTIFY_LIST_FIELDS:
            result[key] = _copy_sequence(
                _ensure_list(value, f"notify.{channel}.{key}", source)
            )
        else:
            result[key] = deepcopy(value)
    return result


def _ensure_dict(value: object | None, name: str, source: str) -> dict:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise TypeError(f"{source}.{name} must be a dict, got {type(value).__name__}")
    return value


def _ensure_list(value: object | None, name: str, source: str) -> list:
    if value is None:
        return []
    if isinstance(value, list):
        return list(value)
    if isinstance(value, tuple):
        return list(value)
    raise TypeError(
        f"{source}.{name} must be a list or tuple, got {type(value).__name__}"
    )


def _ensure_schedule_items(entries: list[Any], source: str) -> None:
    for idx, entry in enumerate(entries):
        if not isinstance(entry, dict):
            raise TypeError(
                f"{source}[{idx}] must be a dict, got {type(entry).__name__}"
            )


def _ensure_unique_host_names(hosts: list[dict[str, Any]], *, source: str) -> None:
    seen: set[str] = set()
    for idx, host in enumerate(hosts):
        name = str(host.get("name") or "").strip()
        if name == "":
            continue
        if name in seen:
            raise ValueError(f"{source} contains duplicate host name: {name!r}")
        seen.add(name)


def _coerce_positive_int(value: object | None, *, default: int, field_name: str) -> int:
    if value is None:
        return int(default)
    out = _coerce_int_like(value, field_name=field_name)
    if out <= 0:
        raise ValueError(f"{field_name} must be > 0")
    return out


def _coerce_optional_str(value: object | None, *, field_name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    out = value.strip()
    if out == "":
        return None
    return out


def _coerce_optional_positive_int(
    value: object | None, *, field_name: str
) -> int | None:
    if value is None:
        return None
    out = _coerce_int_like(value, field_name=field_name)
    if out <= 0:
        raise ValueError(f"{field_name} must be > 0")
    return out


def _coerce_optional_positive_float(
    value: object | None, *, field_name: str
) -> float | None:
    if value is None:
        return None
    out = _coerce_float_like(value, field_name=field_name)
    if out <= 0:
        raise ValueError(f"{field_name} must be > 0")
    return out


def _coerce_int_like(value: object | None, *, field_name: str) -> int:
    if isinstance(value, bool):
        raise TypeError(f"{field_name} must be int-like")
    if isinstance(value, int):
        return int(value)
    if isinstance(value, float):
        if not value.is_integer():
            raise TypeError(f"{field_name} must be int-like")
        return int(value)
    if isinstance(value, str):
        normalized = value.strip()
        if not _INT_LIKE_PATTERN.fullmatch(normalized):
            raise TypeError(f"{field_name} must be int-like")
        return int(normalized)
    raise TypeError(f"{field_name} must be int-like")


def _coerce_float_like(value: object | None, *, field_name: str) -> float:
    if isinstance(value, bool):
        raise TypeError(f"{field_name} must be float-like")
    if not isinstance(value, (int, float, str)):
        raise TypeError(f"{field_name} must be float-like")
    try:
        return float(cast(int | float | str, value))
    except (TypeError, ValueError) as exc:
        raise TypeError(f"{field_name} must be float-like") from exc


def _coerce_bool(value: object | None, *, default: bool, field_name: str) -> bool:
    if value is None:
        return bool(default)
    if not isinstance(value, bool):
        raise TypeError(f"{field_name} must be bool")
    return value


def _coerce_percentage(
    value: object | None, *, default: float, field_name: str
) -> float:
    if value is None:
        return float(default)
    out = _coerce_float_like(value, field_name=field_name)
    if out < 0 or out > 100:
        raise ValueError(f"{field_name} must be between 0 and 100")
    return float(out)


def _coerce_sqlite_journal_mode(
    value: object | None, *, field_name: str, default: str
) -> str:
    out = _coerce_non_empty_lower_str(value, field_name=field_name, default=default)
    if out not in _SQLITE_JOURNAL_MODES:
        raise ValueError(
            f"{field_name} must be one of {sorted(_SQLITE_JOURNAL_MODES)}, got {out!r}"
        )
    return out


def _coerce_sqlite_synchronous(
    value: object | None, *, field_name: str, default: str
) -> str:
    out = _coerce_non_empty_lower_str(value, field_name=field_name, default=default)
    if out not in _SQLITE_SYNCHRONOUS_MODES:
        raise ValueError(
            f"{field_name} must be one of {sorted(_SQLITE_SYNCHRONOUS_MODES)}, got {out!r}"
        )
    return out


def _coerce_non_empty_lower_str(
    value: object | None, *, field_name: str, default: str
) -> str:
    if value is None:
        return str(default).strip().lower()
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    out = value.strip().lower()
    if out == "":
        raise ValueError(f"{field_name} must not be empty")
    return out
