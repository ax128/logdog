from __future__ import annotations

import asyncio
import json
import logging
import os
import stat
import shutil
import socket
import threading
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

import paramiko


logger = logging.getLogger(__name__)

DEFAULT_SSH_TIMEOUT_SECONDS = 8
HOST_METRICS_REMOTE_COMMAND = """python3 - <<'PY'
import json
import os
import shutil
import time
from datetime import datetime, timezone


def _read_meminfo():
    values = {}
    try:
        with open("/proc/meminfo", "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                parts = line.split(":", 1)
                if len(parts) != 2:
                    continue
                key = parts[0].strip()
                val = parts[1].strip().split()[0]
                try:
                    values[key] = int(val) * 1024
                except Exception:
                    continue
    except Exception:
        return {"mem_total": 0, "mem_used": 0, "mem_available": 0}
    total = int(values.get("MemTotal", 0))
    available = int(values.get("MemAvailable", values.get("MemFree", 0)))
    used = max(0, total - available)
    return {
        "mem_total": total,
        "mem_used": used,
        "mem_available": available,
    }


def _read_net():
    rx = 0
    tx = 0
    try:
        with open("/proc/net/dev", "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if ":" not in line:
                    continue
                iface, rest = line.split(":", 1)
                iface = iface.strip()
                if iface == "lo":
                    continue
                fields = rest.split()
                if len(fields) < 16:
                    continue
                try:
                    rx += int(fields[0])
                    tx += int(fields[8])
                except Exception:
                    continue
    except Exception:
        return {"net_rx": 0, "net_tx": 0}
    return {"net_rx": rx, "net_tx": tx}


def _read_load():
    try:
        load_1, load_5, load_15 = os.getloadavg()
        return {
            "load_1": float(load_1),
            "load_5": float(load_5),
            "load_15": float(load_15),
        }
    except Exception:
        return {"load_1": 0.0, "load_5": 0.0, "load_15": 0.0}


def _read_cpu_ticks():
    try:
        with open("/proc/stat", "r", encoding="utf-8", errors="ignore") as f:
            line = f.readline().strip()
    except Exception:
        return None
    if not line.startswith("cpu "):
        return None
    parts = line.split()[1:]
    if len(parts) < 4:
        return None
    numbers = []
    for item in parts:
        try:
            numbers.append(int(item))
        except Exception:
            numbers.append(0)
    total = sum(numbers)
    idle = numbers[3] + (numbers[4] if len(numbers) > 4 else 0)
    return total, idle


def _read_cpu_percent(sample_seconds=0.1):
    first = _read_cpu_ticks()
    if first is None:
        return 0.0
    time.sleep(max(0.0, float(sample_seconds)))
    second = _read_cpu_ticks()
    if second is None:
        return 0.0
    total_delta = second[0] - first[0]
    idle_delta = second[1] - first[1]
    if total_delta <= 0:
        return 0.0
    active = total_delta - idle_delta
    if active <= 0:
        return 0.0
    return round((active / total_delta) * 100.0, 2)


def _read_disk():
    try:
        usage = shutil.disk_usage("/")
        return {
            "disk_root_total": int(usage.total),
            "disk_root_used": int(usage.used),
            "disk_root_free": int(usage.free),
        }
    except Exception:
        return {
            "disk_root_total": 0,
            "disk_root_used": 0,
            "disk_root_free": 0,
        }


payload = {
    "timestamp": datetime.now(timezone.utc).isoformat(),
    "cpu_percent": _read_cpu_percent(),
}
payload.update(_read_load())
payload.update(_read_meminfo())
payload.update(_read_disk())
payload.update(_read_net())
print(json.dumps(payload, separators=(",", ":"), ensure_ascii=True))
PY"""
_ALLOWED_REMOTE_COMMANDS = {HOST_METRICS_REMOTE_COMMAND}


class HostMetricsProbeError(RuntimeError):
    """Base exception for host metrics probe failures."""


class HostMetricsProbeTimeoutError(HostMetricsProbeError):
    """Raised when probe execution times out."""


class HostMetricsProbeAuthError(HostMetricsProbeError):
    """Raised when SSH authentication fails."""


class HostMetricsProbeParseError(HostMetricsProbeError):
    """Raised when probe output cannot be parsed."""


def assess_host_security(
    host_cfg: dict[str, Any],
    *,
    metric_sample: dict[str, Any] | None = None,
    mem_warn_percent: float = 98.0,
    disk_warn_percent: float = 98.0,
) -> dict[str, Any]:
    host_name = str(host_cfg.get("name") or "unknown").strip() or "unknown"
    url_text = str(host_cfg.get("url") or "").strip()
    parsed = urlparse(url_text)
    scheme = str(parsed.scheme or "").strip().lower() or "unknown"
    checked_at = _utc_now_iso()
    issues: list[str] = []

    if scheme == "ssh":
        username = (
            str(parsed.username or host_cfg.get("ssh_user") or "").strip().lower()
        )
        if username == "root":
            issues.append("SSH login uses root account")

        ssh_key = str(host_cfg.get("ssh_key") or "").strip()
        strict_ssh_key = bool(host_cfg.get("strict_ssh_key", False))
        ssh_key_label = os.path.basename(ssh_key) if ssh_key else "ssh_key"
        if ssh_key == "" and strict_ssh_key:
            issues.append("SSH key is missing")
        elif not os.path.exists(ssh_key):
            issues.append(f"SSH key file not found: {ssh_key_label}")
        else:
            try:
                mode = stat.S_IMODE(os.stat(ssh_key).st_mode)
                if mode > 0o600:
                    issues.append(
                        f"SSH key permissions too open: {mode:#o} (expected <= 0o600)"
                    )
            except OSError:
                issues.append(f"SSH key file stat failed: {ssh_key_label}")

        if str(host_cfg.get("ssh_password") or "").strip() != "":
            issues.append("SSH password is configured; prefer key-only auth")
        if str(host_cfg.get("password") or "").strip() != "":
            issues.append("Password field is configured; prefer key-only auth")

    sample = dict(metric_sample or {})
    mem_total = _to_int(sample.get("mem_total"))
    mem_used = _to_int(sample.get("mem_used"))
    disk_total = _to_int(sample.get("disk_root_total"))
    disk_used = _to_int(sample.get("disk_root_used"))
    mem_percent = (float(mem_used) / float(mem_total) * 100.0) if mem_total > 0 else 0.0
    disk_percent = (
        (float(disk_used) / float(disk_total) * 100.0) if disk_total > 0 else 0.0
    )
    if mem_total > 0 and mem_percent >= float(mem_warn_percent):
        issues.append(
            f"System mem usage is high: {mem_percent:.1f}% (threshold {float(mem_warn_percent):.1f}%)"
        )
    if disk_total > 0 and disk_percent >= float(disk_warn_percent):
        issues.append(
            f"System disk usage is high: {disk_percent:.1f}% (threshold {float(disk_warn_percent):.1f}%)"
        )

    return {
        "checked_at": checked_at,
        "host": host_name,
        "scheme": scheme,
        "ok": len(issues) == 0,
        "issues": issues,
        "analysis": _build_security_analysis(host_name=host_name, issues=issues),
    }


def collect_local_host_metrics(
    *,
    collect_load: bool = True,
    collect_network: bool = True,
) -> dict[str, Any]:
    raw: dict[str, Any] = {
        "timestamp": _utc_now_iso(),
        "cpu_percent": _read_cpu_percent(),
    }
    if collect_load:
        load_1, load_5, load_15 = _read_load_averages()
        raw.update(
            {
                "load_1": load_1,
                "load_5": load_5,
                "load_15": load_15,
            }
        )
    raw.update(_read_memory_info())
    raw.update(_read_disk_usage())
    if collect_network:
        raw.update(_read_network_totals())
    return _build_host_metric_sample(
        raw,
        source="local",
        collect_load=collect_load,
        collect_network=collect_network,
    )


async def collect_remote_host_metrics(
    host_cfg: dict[str, Any],
    *,
    collect_load: bool = True,
    collect_network: bool = True,
    timeout_seconds: int = DEFAULT_SSH_TIMEOUT_SECONDS,
    ssh_client_factory: Any | None = None,
) -> dict[str, Any]:
    to_thread = getattr(asyncio, "to_thread", None)
    if callable(to_thread) and getattr(to_thread, "__module__", "") != "asyncio.threads":
        return await to_thread(
            _collect_remote_host_metrics_sync,
            host_cfg,
            collect_load=collect_load,
            collect_network=collect_network,
            timeout_seconds=timeout_seconds,
            ssh_client_factory=ssh_client_factory,
        )

    return await _run_sync_in_daemon_thread(
        _collect_remote_host_metrics_sync,
        host_cfg,
        collect_load=collect_load,
        collect_network=collect_network,
        timeout_seconds=timeout_seconds,
        ssh_client_factory=ssh_client_factory,
    )


async def collect_host_metrics_for_host(
    host_cfg: dict[str, Any],
    *,
    collect_load: bool = True,
    collect_network: bool = True,
    timeout_seconds: int = DEFAULT_SSH_TIMEOUT_SECONDS,
    ssh_client_factory: Any | None = None,
) -> dict[str, Any]:
    url_text = str(host_cfg.get("url") or "").strip()
    parsed = urlparse(url_text)
    if parsed.scheme == "unix":
        return collect_local_host_metrics(
            collect_load=collect_load,
            collect_network=collect_network,
        )
    if parsed.scheme == "ssh":
        return await collect_remote_host_metrics(
            host_cfg,
            collect_load=collect_load,
            collect_network=collect_network,
            timeout_seconds=timeout_seconds,
            ssh_client_factory=ssh_client_factory,
        )
    raise HostMetricsProbeParseError(f"unsupported host URL scheme: {parsed.scheme!r}")


def _collect_remote_host_metrics_sync(
    host_cfg: dict[str, Any],
    *,
    collect_load: bool,
    collect_network: bool,
    timeout_seconds: int,
    ssh_client_factory: Any | None,
) -> dict[str, Any]:
    url_text = str(host_cfg.get("url") or "").strip()
    parsed = urlparse(url_text)
    if parsed.scheme != "ssh":
        raise HostMetricsProbeParseError(f"unsupported host URL: {url_text!r}")
    hostname = parsed.hostname
    if not hostname:
        raise HostMetricsProbeParseError("ssh host missing hostname")

    username = parsed.username or str(host_cfg.get("ssh_user") or "").strip()
    if str(username).strip() == "":
        raise HostMetricsProbeParseError("ssh host missing username")
    port = int(parsed.port or 22)
    timeout = max(1.0, float(timeout_seconds))
    ssh_key = str(host_cfg.get("ssh_key") or "").strip()
    strict_host_key = bool(host_cfg.get("strict_host_key", True))
    command = HOST_METRICS_REMOTE_COMMAND
    _assert_command_allowed(command)

    client = (ssh_client_factory or paramiko.SSHClient)()
    try:
        _set_host_key_policy(client, strict_host_key=strict_host_key)
        connect_kwargs: dict[str, Any] = {
            "hostname": hostname,
            "port": port,
            "username": username,
            "timeout": timeout,
            "banner_timeout": timeout,
            "auth_timeout": timeout,
            "look_for_keys": True,
        }
        if ssh_key:
            connect_kwargs["key_filename"] = ssh_key

        client.connect(**connect_kwargs)
        _, stdout, stderr = client.exec_command(command, timeout=timeout)
        stdout_text = _read_stream_text(stdout)
        stderr_text = _read_stream_text(stderr)
        exit_status = _read_exit_status(stdout)
        if exit_status != 0:
            snippet = _clip_error(stderr_text or stdout_text)
            raise HostMetricsProbeParseError(
                f"remote probe failed status={exit_status}: {snippet}"
            )
        try:
            raw = json.loads(stdout_text or "{}")
        except json.JSONDecodeError as exc:
            raise HostMetricsProbeParseError(
                "remote probe output is not valid JSON"
            ) from exc
        if not isinstance(raw, dict):
            raise HostMetricsProbeParseError("remote probe output must be JSON object")
        return _build_host_metric_sample(
            raw,
            source="ssh",
            collect_load=collect_load,
            collect_network=collect_network,
        )
    except HostMetricsProbeError:
        raise
    except paramiko.AuthenticationException as exc:
        raise HostMetricsProbeAuthError(
            f"ssh authentication failed for {username}@{hostname}"
        ) from exc
    except (socket.timeout, TimeoutError) as exc:
        raise HostMetricsProbeTimeoutError(
            f"ssh probe timed out for {username}@{hostname}"
        ) from exc
    except paramiko.SSHException as exc:
        message = str(exc).lower()
        if "timed out" in message or "timeout" in message:
            raise HostMetricsProbeTimeoutError(
                f"ssh probe timed out for {username}@{hostname}"
            ) from exc
        raise HostMetricsProbeError(
            f"ssh probe failed for {username}@{hostname}"
        ) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("host metrics ssh probe failed host=%s", hostname)
        raise HostMetricsProbeError(
            f"ssh probe failed for {username}@{hostname}"
        ) from exc
    finally:
        close_fn = getattr(client, "close", None)
        if callable(close_fn):
            close_fn()


def _set_host_key_policy(client: Any, *, strict_host_key: bool = True) -> None:
    loader = getattr(client, "load_system_host_keys", None)
    if callable(loader):
        loader()

    setter = getattr(client, "set_missing_host_key_policy", None)
    if not callable(setter):
        return
    policy_name = "RejectPolicy" if strict_host_key else "AutoAddPolicy"
    policy_factory = getattr(paramiko, policy_name, None)
    if policy_factory is None:
        return
    setter(policy_factory())


def _assert_command_allowed(command: str) -> None:
    if command not in _ALLOWED_REMOTE_COMMANDS:
        raise ValueError("remote probe command is not in whitelist")


async def _run_sync_in_daemon_thread(func: Any, /, *args: Any, **kwargs: Any) -> Any:
    loop = asyncio.get_running_loop()
    future: asyncio.Future[Any] = loop.create_future()

    def _set_result(value: Any) -> None:
        if not future.done():
            future.set_result(value)

    def _set_error(exc: Exception) -> None:
        if not future.done():
            future.set_exception(exc)

    def _worker() -> None:
        try:
            result = func(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001
            loop.call_soon_threadsafe(_set_error, exc)
            return
        loop.call_soon_threadsafe(_set_result, result)

    thread = threading.Thread(target=_worker, daemon=True, name="host-metrics-probe")
    thread.start()
    return await future


def _read_stream_text(stream: Any) -> str:
    if stream is None:
        return ""
    reader = getattr(stream, "read", None)
    if not callable(reader):
        return ""
    data = reader()
    if isinstance(data, bytes):
        return data.decode("utf-8", errors="replace").strip()
    return str(data or "").strip()


def _read_exit_status(stdout: Any) -> int:
    channel = getattr(stdout, "channel", None)
    if channel is None:
        return 0
    getter = getattr(channel, "recv_exit_status", None)
    if not callable(getter):
        return 0
    try:
        return _to_int(getter())
    except Exception:  # noqa: BLE001
        return 0


def _build_host_metric_sample(
    raw: dict[str, Any],
    *,
    source: str,
    collect_load: bool,
    collect_network: bool,
) -> dict[str, Any]:
    timestamp = _normalize_timestamp(raw.get("timestamp"))
    mem_total = _to_int(raw.get("mem_total"))
    mem_available = _to_int(raw.get("mem_available"))
    mem_used_value = raw.get("mem_used")
    if mem_used_value is None and mem_total > 0 and mem_available > 0:
        mem_used = max(0, mem_total - mem_available)
    else:
        mem_used = _to_int(mem_used_value)

    sample = {
        "timestamp": timestamp,
        "cpu_percent": _to_float(raw.get("cpu_percent")),
        "load_1": _to_float(raw.get("load_1")) if collect_load else 0.0,
        "load_5": _to_float(raw.get("load_5")) if collect_load else 0.0,
        "load_15": _to_float(raw.get("load_15")) if collect_load else 0.0,
        "mem_total": mem_total,
        "mem_used": mem_used,
        "mem_available": mem_available,
        "disk_root_total": _to_int(raw.get("disk_root_total")),
        "disk_root_used": _to_int(raw.get("disk_root_used")),
        "disk_root_free": _to_int(raw.get("disk_root_free")),
        "net_rx": _to_int(raw.get("net_rx")) if collect_network else 0,
        "net_tx": _to_int(raw.get("net_tx")) if collect_network else 0,
        "source": source,
    }
    return sample


def _read_cpu_percent(*, sample_seconds: float = 0.1) -> float:
    first = _read_cpu_ticks()
    if first is None:
        return 0.0
    time.sleep(max(0.0, float(sample_seconds)))
    second = _read_cpu_ticks()
    if second is None:
        return 0.0
    first_total, first_idle = first
    second_total, second_idle = second
    total_delta = second_total - first_total
    idle_delta = second_idle - first_idle
    if total_delta <= 0:
        return 0.0
    active = total_delta - idle_delta
    if active <= 0:
        return 0.0
    return round((active / total_delta) * 100.0, 2)


def _read_cpu_ticks() -> tuple[int, int] | None:
    try:
        with open("/proc/stat", "r", encoding="utf-8", errors="ignore") as handle:
            line = handle.readline().strip()
    except OSError:
        return None
    if not line.startswith("cpu "):
        return None
    parts = line.split()[1:]
    if len(parts) < 4:
        return None
    numbers: list[int] = []
    for item in parts:
        try:
            numbers.append(int(item))
        except ValueError:
            numbers.append(0)
    total = sum(numbers)
    idle = numbers[3] + (numbers[4] if len(numbers) > 4 else 0)
    return total, idle


def _read_load_averages() -> tuple[float, float, float]:
    try:
        one, five, fifteen = os.getloadavg()
        return float(one), float(five), float(fifteen)
    except OSError:
        return 0.0, 0.0, 0.0


def _read_memory_info() -> dict[str, int]:
    mapping: dict[str, int] = {}
    try:
        with open("/proc/meminfo", "r", encoding="utf-8", errors="ignore") as handle:
            for line in handle:
                if ":" not in line:
                    continue
                key, raw_value = line.split(":", 1)
                number = raw_value.strip().split()[0]
                try:
                    mapping[key.strip()] = int(number) * 1024
                except ValueError:
                    continue
    except OSError:
        return {"mem_total": 0, "mem_used": 0, "mem_available": 0}
    total = int(mapping.get("MemTotal", 0))
    available = int(mapping.get("MemAvailable", mapping.get("MemFree", 0)))
    used = max(0, total - available)
    return {
        "mem_total": total,
        "mem_used": used,
        "mem_available": available,
    }


def _read_disk_usage() -> dict[str, int]:
    try:
        usage = shutil.disk_usage("/")
    except OSError:
        return {
            "disk_root_total": 0,
            "disk_root_used": 0,
            "disk_root_free": 0,
        }
    return {
        "disk_root_total": int(usage.total),
        "disk_root_used": int(usage.used),
        "disk_root_free": int(usage.free),
    }


def _read_network_totals() -> dict[str, int]:
    rx = 0
    tx = 0
    try:
        with open("/proc/net/dev", "r", encoding="utf-8", errors="ignore") as handle:
            for line in handle:
                if ":" not in line:
                    continue
                iface, rest = line.split(":", 1)
                if iface.strip() == "lo":
                    continue
                values = rest.split()
                if len(values) < 16:
                    continue
                rx += _to_int(values[0])
                tx += _to_int(values[8])
    except OSError:
        return {"net_rx": 0, "net_tx": 0}
    return {"net_rx": rx, "net_tx": tx}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_timestamp(value: Any) -> str:
    if value is None:
        return _utc_now_iso()
    text = str(value).strip()
    if text == "":
        return _utc_now_iso()
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return _utc_now_iso()
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)
    return parsed.isoformat()


def _clip_error(text: str, *, max_chars: int = 200) -> str:
    value = str(text or "").strip()
    if len(value) <= max_chars:
        return value
    return value[:max_chars]


def _to_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _to_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _build_security_analysis(*, host_name: str, issues: list[str]) -> str:
    if not issues:
        return f"Host {host_name} SSH/系统安全检查通过（未发现风险项）。"
    details = "\n".join(f"- {item}" for item in issues)
    return f"Host {host_name} SSH/系统安全检查发现风险：\n{details}"
