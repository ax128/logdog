from __future__ import annotations

import io
import asyncio
import json
import inspect
import os
import select
import shlex
import shutil
import socket
import subprocess
import time
import tempfile
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from logdog.pipeline.preprocessor.base import LogLine
from logdog.remote.worker_pipeline import RemoteWorkerPipeline, validate_remote_pipeline_config
from logdog.remote.worker_protocol import FrameReader, encode_frame


_DEFAULT_IDLE_TIMEOUT_SECONDS = 300.0
_DEFAULT_POLL_INTERVAL_SECONDS = 0.1
_DEFAULT_READ_SIZE = 65536
_STREAM_ACTIONS = frozenset(
    {"stream_container_logs", "stream_logs", "stream_docker_events", "stream_events"}
)


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


@dataclass(slots=True)
class RemoteWorkerContext:
    backend: Any
    cleanup_hook: Callable[[dict[str, Any]], Any] | None = None
    clock: Callable[[], float] = field(default_factory=lambda: time.monotonic)
    sleep: Callable[[float], Any] = asyncio.sleep
    idle_timeout_seconds: float = _DEFAULT_IDLE_TIMEOUT_SECONDS
    poll_interval_seconds: float = _DEFAULT_POLL_INTERVAL_SECONDS
    read_size: int = _DEFAULT_READ_SIZE
    workspace_root: str | Path | None = None
    log_pipeline: RemoteWorkerPipeline | None = None


class LocalWorkspaceManager:
    def __init__(self, *, root: str | Path | None = None) -> None:
        self._tempdir = tempfile.TemporaryDirectory(
            prefix="logdog-remote-worker-",
            dir=str(root) if root is not None else None,
        )
        self.path = Path(self._tempdir.name)
        self._closed = False

    async def cleanup(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._tempdir.cleanup()


class StdIOTransport:
    def __init__(
        self,
        *,
        stdin: Any | None = None,
        stdout: Any | None = None,
    ) -> None:
        import sys

        self._stdin = stdin if stdin is not None else sys.stdin
        self._stdout = stdout if stdout is not None else sys.stdout
        self._stdin_buffer = getattr(self._stdin, "buffer", self._stdin)
        self._stdout_buffer = getattr(self._stdout, "buffer", self._stdout)
        self._stdin_fd = self._fileno(self._stdin_buffer)
        self._stdout_fd = self._fileno(self._stdout_buffer)

    def read(
        self,
        size: int,
        *,
        timeout_seconds: float | None = None,
    ) -> bytes | None:
        if self._stdin_fd is None:
            data = self._stdin_buffer.read(size)
            return data if isinstance(data, bytes) else bytes(data or b"")

        timeout = None if timeout_seconds is None else max(0.0, float(timeout_seconds))
        ready, _, _ = select.select([self._stdin_fd], [], [], timeout)
        if not ready:
            return None
        data = os.read(self._stdin_fd, int(size))
        return data

    def write(self, data: bytes) -> int:
        if hasattr(self._stdout_buffer, "write"):
            written = self._stdout_buffer.write(data)
            flush = getattr(self._stdout_buffer, "flush", None)
            if callable(flush):
                flush()
            return len(data) if written is None else int(written)
        if self._stdout_fd is None:
            raise AttributeError("stdout does not implement write")
        return os.write(self._stdout_fd, data)

    def flush(self) -> None:
        flush = getattr(self._stdout_buffer, "flush", None)
        if callable(flush):
            flush()

    def close(self) -> None:
        self.flush()

    @staticmethod
    def _fileno(stream: Any) -> int | None:
        fileno = getattr(stream, "fileno", None)
        if not callable(fileno):
            return None
        with suppress(OSError, ValueError, io.UnsupportedOperation):  # type: ignore[name-defined]
            return int(fileno())
        return None


class RemoteWorkerBackend:
    def __init__(self, *, docker_command: str = "docker") -> None:
        self._docker_command = str(docker_command or "docker").strip() or "docker"

    def _docker_args(self, *args: Any) -> list[str]:
        return [self._docker_command, *[str(arg) for arg in args]]

    def _run_docker(
        self,
        *args: Any,
        timeout_seconds: float | None = None,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            self._docker_args(*args),
            check=True,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )

    def _normalize_docker_time_arg(self, value: Any | None) -> str | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            dt = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).isoformat(timespec="seconds").replace(
                "+00:00", "Z"
            )
        if isinstance(value, (int, float)):
            return str(value)
        raw = str(value).strip()
        if raw == "":
            return None
        try:
            float(raw)
            return raw
        except ValueError:
            pass
        candidate = raw
        if candidate.endswith("Z"):
            candidate = f"{candidate[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError:
            return raw
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).isoformat(timespec="seconds").replace(
            "+00:00", "Z"
        )

    def _read_text(self, path: str | Path) -> str:
        with open(path, "r", encoding="utf-8", errors="replace") as handle:
            return handle.read()

    def _load_json(self, text: str) -> Any:
        stripped = text.strip()
        if stripped == "":
            return {}
        return json.loads(stripped)

    def _load_json_lines(self, text: str) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for line in text.splitlines():
            stripped = line.strip()
            if stripped == "":
                continue
            parsed = json.loads(stripped)
            if isinstance(parsed, dict):
                items.append(dict(parsed))
        return items

    def _split_timestamped_line(self, line: str) -> dict[str, Any]:
        stripped = line.rstrip("\r\n")
        if stripped == "":
            return {}
        timestamp, sep, content = stripped.partition(" ")
        if sep == "":
            return {"line": stripped}
        if self._looks_like_timestamp(timestamp):
            return {"timestamp": timestamp, "line": content}
        return {"line": stripped}

    def _looks_like_timestamp(self, value: str) -> bool:
        return "T" in value and "-" in value

    def _normalize_container(self, item: dict[str, Any]) -> dict[str, Any]:
        container_id = str(item.get("ID") or item.get("Container") or item.get("Id") or "").strip()
        name = str(item.get("Names") or item.get("Name") or container_id).strip()
        return {
            "id": container_id,
            "container_id": container_id,
            "name": name,
            "image": item.get("Image"),
            "status": item.get("Status"),
            "restart_count": 0,
            "created_at": item.get("CreatedAt"),
            "ports": item.get("Ports"),
            "raw": dict(item),
        }

    def _normalize_stats(self, item: dict[str, Any]) -> dict[str, Any]:
        container_id = str(item.get("Container") or item.get("ID") or item.get("Id") or "").strip()
        mem_used, mem_limit = self._parse_usage_pair(str(item.get("MemUsage") or ""))
        net_rx, net_tx = self._parse_usage_pair(str(item.get("NetIO") or ""))
        block_read, block_write = self._parse_usage_pair(str(item.get("BlockIO") or ""))
        return {
            "container_id": container_id,
            "container_name": str(item.get("Name") or container_id),
            "cpu_percent": self._parse_percent(item.get("CPUPerc")),
            "memory_stats": {
                "usage": mem_used,
                "limit": mem_limit,
            },
            "networks": {
                "total": {
                    "rx_bytes": net_rx,
                    "tx_bytes": net_tx,
                }
            },
            "blkio_stats": {
                "io_service_bytes_recursive": [
                    {"op": "Read", "value": block_read},
                    {"op": "Write", "value": block_write},
                ]
            },
            "pids_stats": {"current": self._to_int(item.get("PIDs"))},
            "summary_source": "docker-cli",
            "raw": dict(item),
        }

    def _normalize_event(self, line: str) -> dict[str, Any]:
        stripped = line.strip()
        if stripped == "":
            return {}
        if stripped.startswith("{") and stripped.endswith("}"):
            parsed = json.loads(stripped)
            if isinstance(parsed, dict):
                actor = parsed.get("Actor")
                if not isinstance(actor, dict):
                    actor = {}
                actor_attributes = actor.get("Attributes")
                if not isinstance(actor_attributes, dict):
                    actor_attributes = {}
                container_id = str(actor.get("ID") or parsed.get("id") or "").strip()
                container_name = str(
                    actor_attributes.get("name")
                    or parsed.get("from")
                    or container_id
                )
                return {
                    "time": parsed.get("time", parsed.get("timeNano")),
                    "type": str(parsed.get("Type") or parsed.get("type") or ""),
                    "action": str(
                        parsed.get("Action")
                        or parsed.get("status")
                        or parsed.get("action")
                        or ""
                    ),
                    "container_id": container_id,
                    "container_name": container_name,
                    "actor_attributes": dict(actor_attributes),
                }
        return {"raw": stripped}

    def _normalize_command(self, command: Any) -> list[str]:
        if isinstance(command, (list, tuple)):
            argv = [str(item) for item in command if str(item) != ""]
        elif isinstance(command, str):
            argv = shlex.split(command)
        else:
            argv = [str(command)]
        if not argv:
            raise ValueError("command must not be empty")
        return argv

    def _parse_percent(self, value: Any) -> float:
        text = str(value or "").strip()
        if text.endswith("%"):
            text = text[:-1]
        try:
            return float(text)
        except ValueError:
            return 0.0

    def _parse_usage_pair(self, value: str) -> tuple[int, int]:
        left, sep, right = str(value or "").partition("/")
        if sep == "":
            size = self._parse_size_to_bytes(left)
            return size, 0
        return self._parse_size_to_bytes(left), self._parse_size_to_bytes(right)

    def _parse_size_to_bytes(self, value: str) -> int:
        text = str(value or "").strip()
        if text == "":
            return 0
        parts = text.split()
        if len(parts) == 1:
            raw_number = "".join(ch for ch in parts[0] if (ch.isdigit() or ch == "."))
            raw_unit = parts[0][len(raw_number):]
        else:
            raw_number = parts[0]
            raw_unit = parts[1]
        try:
            number = float(raw_number)
        except ValueError:
            return 0
        unit = raw_unit.strip().lower()
        factors = {
            "": 1,
            "b": 1,
            "kb": 1000,
            "mb": 1000**2,
            "gb": 1000**3,
            "tb": 1000**4,
            "kib": 1024,
            "mib": 1024**2,
            "gib": 1024**3,
            "tib": 1024**4,
        }
        return int(number * factors.get(unit, 1))

    def _to_int(self, value: Any) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    async def _run_stream(
        self,
        *,
        args: list[str],
        parse_line: Callable[[str], dict[str, Any]],
    ):
        proc = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            assert proc.stdout is not None
            while True:
                chunk = await proc.stdout.readline()
                if not chunk:
                    break
                item = parse_line(chunk.decode("utf-8", errors="replace"))
                if item:
                    yield item
            await self._raise_stream_error(proc, args)
        finally:
            await self._close_stream_process(proc)

    async def _raise_stream_error(
        self,
        proc: asyncio.subprocess.Process,
        args: list[str],
    ) -> None:
        if proc.returncode is None:
            await proc.wait()
        returncode = proc.returncode
        if returncode in (0, None):
            return
        stderr = ""
        stderr_pipe = proc.stderr
        if stderr_pipe is not None:
            with suppress(Exception):
                stderr = (await stderr_pipe.read()).decode("utf-8", errors="replace").strip()
        command = " ".join(args)
        message = stderr if stderr != "" else f"{command} exited with code {returncode}"
        raise RuntimeError(message)

    async def _close_stream_process(self, proc: asyncio.subprocess.Process) -> None:
        if proc.returncode is not None:
            return
        with suppress(ProcessLookupError):
            proc.terminate()
        with suppress(asyncio.TimeoutError, ProcessLookupError):
            await asyncio.wait_for(proc.wait(), timeout=0.5)
        if proc.returncode is None:
            with suppress(ProcessLookupError):
                proc.kill()
            with suppress(Exception):
                await proc.wait()

    async def connect_host(
        self,
        host: dict[str, Any],
        *,
        timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        payload = self._load_json(
            self._run_docker(
                "version",
                "--format",
                "{{json .}}",
                timeout_seconds=timeout_seconds,
            ).stdout
        )
        server = payload.get("Server", {}) if isinstance(payload, dict) else {}
        client = payload.get("Client", {}) if isinstance(payload, dict) else {}
        return {
            "backend": "docker-cli",
            "host_name": str(host.get("name") or ""),
            "server_version": str(server.get("Version") or ""),
            "api_version": str(server.get("APIVersion") or server.get("ApiVersion") or ""),
            "client_version": str(client.get("Version") or ""),
            "raw": payload,
        }

    async def list_containers_for_host(
        self,
        host: dict[str, Any],
        *,
        timeout_seconds: float | None = None,
    ) -> list[dict[str, Any]]:
        output = self._run_docker(
            "ps",
            "--all",
            "--no-trunc",
            "--format",
            "{{json .}}",
            timeout_seconds=timeout_seconds,
        ).stdout
        containers = [self._normalize_container(item) for item in self._load_json_lines(output)]
        container_ids = [str(item.get("id") or "") for item in containers if str(item.get("id") or "") != ""]
        details: dict[str, dict[str, Any]] = {}
        if container_ids:
            inspect_payload = self._load_json(
                self._run_docker(
                    "inspect",
                    *container_ids,
                    timeout_seconds=timeout_seconds,
                ).stdout
            )
            if isinstance(inspect_payload, list):
                for item in inspect_payload:
                    if not isinstance(item, dict):
                        continue
                    container_id = str(item.get("Id") or "").strip()
                    if container_id != "":
                        details[container_id] = dict(item)
        for item in containers:
            detail = details.get(str(item.get("id") or ""))
            if not isinstance(detail, dict):
                continue
            item["restart_count"] = self._to_int(detail.get("RestartCount"))
            state = detail.get("State")
            if isinstance(state, dict):
                status = str(state.get("Status") or "").strip()
                if status != "":
                    item["status"] = status
            detail_name = str(detail.get("Name") or "").lstrip("/")
            if detail_name != "":
                item["name"] = detail_name
        return containers

    async def fetch_container_stats(
        self,
        host: dict[str, Any],
        container: dict[str, Any],
        *,
        timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        container_id = str(container.get("id") or container.get("container_id") or "").strip()
        payload = self._run_docker(
            "stats",
            "--no-stream",
            "--format",
            "{{json .}}",
            container_id,
            timeout_seconds=timeout_seconds,
        ).stdout
        stats = self._load_json(payload)
        if not isinstance(stats, dict):
            raise RuntimeError("docker stats did not return a JSON object")
        normalized = self._normalize_stats(stats)
        normalized["host_name"] = str(host.get("name") or "")
        return normalized

    async def query_container_logs(
        self,
        host: dict[str, Any],
        container: dict[str, Any],
        *,
        since: Any | None = None,
        until: Any | None = None,
        max_lines: int = 2000,
        timeout_seconds: float | None = None,
    ) -> list[dict[str, Any]]:
        container_id = str(container.get("id") or container.get("container_id") or "").strip()
        args: list[Any] = ["logs", "--timestamps"]
        if max_lines > 0:
            args.extend(["--tail", str(int(max_lines))])
        normalized_since = self._normalize_docker_time_arg(since)
        if normalized_since is not None:
            args.extend(["--since", normalized_since])
        normalized_until = self._normalize_docker_time_arg(until)
        if normalized_until is not None:
            args.extend(["--until", normalized_until])
        args.append(container_id)
        output = self._run_docker(*args, timeout_seconds=timeout_seconds).stdout
        records: list[dict[str, Any]] = []
        for line in output.splitlines():
            parsed = self._split_timestamped_line(line)
            if not parsed:
                continue
            parsed["host_name"] = str(host.get("name") or "")
            parsed["container_id"] = container_id
            parsed["container_name"] = str(container.get("name") or container_id)
            records.append(parsed)
        return records

    async def stream_container_logs(
        self,
        host: dict[str, Any],
        container: dict[str, Any],
        *,
        since: Any | None = None,
        tail: Any | None = None,
    ):
        container_id = str(container.get("id") or container.get("container_id") or "").strip()
        args: list[Any] = self._docker_args("logs", "--follow", "--timestamps")
        if tail is not None:
            args.extend(["--tail", str(tail)])
        normalized_since = self._normalize_docker_time_arg(since)
        if normalized_since is not None:
            args.extend(["--since", normalized_since])
        args.append(container_id)
        async for item in self._run_stream(args=[str(arg) for arg in args], parse_line=self._parse_stream_log_line(host, container)):
            yield item

    def _parse_stream_log_line(
        self,
        host: dict[str, Any],
        container: dict[str, Any],
    ) -> Callable[[str], dict[str, Any]]:
        host_name = str(host.get("name") or "")
        container_id = str(container.get("id") or container.get("container_id") or "")
        container_name = str(container.get("name") or container_id)

        def _parse(line: str) -> dict[str, Any]:
            parsed = self._split_timestamped_line(line)
            if not parsed:
                return {}
            parsed["host_name"] = host_name
            parsed["container_id"] = container_id
            parsed["container_name"] = container_name
            return parsed

        return _parse

    async def stream_docker_events(
        self,
        host: dict[str, Any],
        *,
        filters: dict[str, Any] | None = None,
    ):
        args: list[Any] = self._docker_args("events", "--format", "{{json .}}")
        if isinstance(filters, dict):
            for key in sorted(filters.keys()):
                value = filters[key]
                if isinstance(value, (list, tuple, set)):
                    values = list(value)
                else:
                    values = [value]
                for item in values:
                    if item is None or str(item) == "":
                        continue
                    args.extend(["--filter", f"{key}={item}"])
        async for item in self._run_stream(
            args=[str(arg) for arg in args],
            parse_line=self._parse_stream_event_line(host),
        ):
            yield item

    def _parse_stream_event_line(self, host: dict[str, Any]) -> Callable[[str], dict[str, Any]]:
        host_name = str(host.get("name") or "")

        def _parse(line: str) -> dict[str, Any]:
            item = self._normalize_event(line)
            if not item:
                return {}
            item["host_name"] = host_name
            return item

        return _parse

    async def restart_container_for_host(
        self,
        host: dict[str, Any],
        container: dict[str, Any],
        *,
        timeout: int = 10,
    ) -> dict[str, Any]:
        container_id = str(container.get("id") or container.get("container_id") or "").strip()
        output = self._run_docker("restart", container_id, timeout_seconds=float(timeout)).stdout.strip()
        return {
            "host_name": str(host.get("name") or ""),
            "container_id": container_id,
            "container_name": str(container.get("name") or container_id),
            "timeout": int(timeout),
            "restarted": True,
            "output": output if output != "" else container_id,
        }

    async def exec_container_for_host(
        self,
        host: dict[str, Any],
        container: dict[str, Any],
        *,
        command: Any,
        timeout_seconds: int = 30,
        max_output_chars: int = 2000,
    ) -> dict[str, Any]:
        container_id = str(container.get("id") or container.get("container_id") or "").strip()
        command_text = str(command if command is not None else "").strip()
        argv = self._normalize_command(command)
        result = self._run_docker(
            "exec",
            container_id,
            *argv,
            timeout_seconds=float(timeout_seconds),
        )
        output = result.stdout.strip()
        stderr_output = result.stderr.strip()
        if stderr_output:
            output = output + ("" if output == "" else "\n") + stderr_output
        if max_output_chars > 0 and len(output) > max_output_chars:
            output = output[:max_output_chars] + f"\n[truncated at {int(max_output_chars)} chars]"
        return {
            "host_name": str(host.get("name") or ""),
            "container_id": container_id,
            "container_name": str(container.get("name") or container_id),
            "command": command_text if command_text != "" else " ".join(argv),
            "timeout_seconds": int(timeout_seconds),
            "exit_code": result.returncode,
            "output": output,
        }

    async def collect_host_metrics_for_host(
        self,
        host: dict[str, Any],
        *,
        collect_load: bool = True,
        collect_network: bool = True,
        timeout_seconds: int = 8,
    ) -> dict[str, Any]:
        del timeout_seconds
        disk = self._collect_disk_metrics()
        memory = self._collect_memory_metrics()
        mem_total = self._to_int(memory.get("MemTotal"))
        mem_available = self._to_int(
            memory.get("MemAvailable", memory.get("MemFree"))
        )
        mem_used = max(0, mem_total - mem_available)
        net_rx, net_tx = self._collect_network_totals() if collect_network else (0, 0)
        metrics: dict[str, Any] = {
            "host_name": str(host.get("name") or ""),
            "hostname": socket.gethostname(),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "cpu_percent": self._read_cpu_percent(),
            "mem_total": mem_total,
            "mem_used": mem_used,
            "mem_available": mem_available,
            "disk_root_total": int(disk.get("total_bytes") or 0),
            "disk_root_used": int(disk.get("used_bytes") or 0),
            "disk_root_free": int(disk.get("free_bytes") or 0),
            "net_rx": net_rx,
            "net_tx": net_tx,
            "source": "remote-worker",
        }
        if collect_load:
            loadavg = self._collect_loadavg()
            metrics["load_1"] = float(loadavg.get("1m") or 0.0)
            metrics["load_5"] = float(loadavg.get("5m") or 0.0)
            metrics["load_15"] = float(loadavg.get("15m") or 0.0)
        else:
            metrics["load_1"] = 0.0
            metrics["load_5"] = 0.0
            metrics["load_15"] = 0.0
        return metrics

    def _collect_disk_metrics(self) -> dict[str, Any]:
        usage = shutil.disk_usage("/")
        return {
            "path": "/",
            "total_bytes": int(usage.total),
            "used_bytes": int(usage.used),
            "free_bytes": int(usage.free),
        }

    def _collect_loadavg(self) -> dict[str, float]:
        raw = self._read_text("/proc/loadavg").strip()
        if raw == "":
            return {}
        parts = raw.split()
        if len(parts) < 3:
            return {}
        return {
            "1m": float(parts[0]),
            "5m": float(parts[1]),
            "15m": float(parts[2]),
        }

    def _collect_memory_metrics(self) -> dict[str, Any]:
        raw = self._read_text("/proc/meminfo")
        values: dict[str, int] = {}
        for line in raw.splitlines():
            key, sep, value = line.partition(":")
            if sep == "":
                continue
            normalized = value.strip().split()
            if not normalized:
                continue
            try:
                amount = int(normalized[0])
            except ValueError:
                continue
            unit = normalized[1] if len(normalized) > 1 else ""
            if unit.lower() == "kb":
                amount *= 1024
            values[key] = amount
        return values

    def _read_uptime_seconds(self) -> float | None:
        raw = self._read_text("/proc/uptime").strip()
        if raw == "":
            return None
        try:
            return float(raw.split()[0])
        except (IndexError, ValueError):
            return None

    def _collect_network_metrics(self) -> dict[str, Any]:
        raw = self._read_text("/proc/net/dev")
        interfaces: dict[str, dict[str, int]] = {}
        for line in raw.splitlines():
            if ":" not in line:
                continue
            name, payload = line.split(":", 1)
            interface = name.strip()
            if interface == "" or interface in {"Inter-|", "face"}:
                continue
            fields = payload.split()
            if len(fields) < 16:
                continue
            try:
                interfaces[interface] = {
                    "rx_bytes": int(fields[0]),
                    "rx_packets": int(fields[1]),
                    "tx_bytes": int(fields[8]),
                    "tx_packets": int(fields[9]),
                }
            except ValueError:
                continue
        return interfaces

    def _collect_network_totals(self) -> tuple[int, int]:
        network = self._collect_network_metrics()
        total_rx = 0
        total_tx = 0
        for name, item in network.items():
            if str(name) == "lo" or not isinstance(item, dict):
                continue
            total_rx += self._to_int(item.get("rx_bytes"))
            total_tx += self._to_int(item.get("tx_bytes"))
        return total_rx, total_tx

    def _read_cpu_percent(self, sample_seconds: float = 0.1) -> float:
        first = self._read_cpu_ticks()
        if first is None:
            return 0.0
        time.sleep(max(0.0, float(sample_seconds)))
        second = self._read_cpu_ticks()
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

    def _read_cpu_ticks(self) -> tuple[int, int] | None:
        raw = self._read_text("/proc/stat")
        first_line = raw.splitlines()[0].strip() if raw else ""
        if not first_line.startswith("cpu "):
            return None
        values = []
        for item in first_line.split()[1:]:
            values.append(self._to_int(item))
        if len(values) < 4:
            return None
        total = sum(values)
        idle = values[3] + (values[4] if len(values) > 4 else 0)
        return total, idle


DockerCLIWorkerBackend = RemoteWorkerBackend


class RemoteWorkerProcess:
    def __init__(
        self,
        *,
        transport: Any | None = None,
        context: RemoteWorkerContext | None = None,
        channel: Any | None = None,
        docker_backend: Any | None = None,
        cleanup: Callable[[str], Any] | None = None,
        heartbeat_timeout_seconds: float | None = None,
        heartbeat_poll_interval_seconds: float | None = None,
        workspace_root: str | Path | None = None,
        log_pipeline: RemoteWorkerPipeline | None = None,
    ) -> None:
        resolved_transport = transport if transport is not None else channel
        if resolved_transport is None:
            raise ValueError("transport/channel is required")
        if context is None:
            if docker_backend is None:
                raise ValueError("docker_backend is required when context is not provided")

            async def _cleanup_hook(payload: dict[str, Any]) -> Any:
                if cleanup is None:
                    return None
                return await _maybe_await(cleanup(str(payload.get("reason") or "")))

            context = RemoteWorkerContext(
                backend=docker_backend,
                cleanup_hook=_cleanup_hook,
                idle_timeout_seconds=(
                    float(heartbeat_timeout_seconds)
                    if heartbeat_timeout_seconds is not None
                    else _DEFAULT_IDLE_TIMEOUT_SECONDS
                ),
                poll_interval_seconds=(
                    float(heartbeat_poll_interval_seconds)
                    if heartbeat_poll_interval_seconds is not None
                    else _DEFAULT_POLL_INTERVAL_SECONDS
                ),
                workspace_root=workspace_root,
                log_pipeline=log_pipeline,
            )
        self._transport = resolved_transport
        self._context = context
        self._reader = FrameReader()
        self._workspace_manager = LocalWorkspaceManager(
            root=self._context.workspace_root
        )
        self._request_tasks: set[asyncio.Task[None]] = set()
        self._stream_tasks: dict[str, asyncio.Task[None]] = {}
        self._write_lock = asyncio.Lock()
        self._closing = False
        self._cleanup_finished = False
        self._last_activity_at = self._context.clock()
        self._shutdown_reason = "shutdown"
        self._stream_sequence = 0

    async def serve_forever(self) -> None:
        await self.run()

    async def run(self) -> None:
        try:
            while not self._closing:
                chunk = await self._read_chunk()
                if chunk is None:
                    if self._should_idle_shutdown():
                        await self._shutdown("heartbeat timeout", emit_ack=False)
                        break
                    await _maybe_await(self._context.sleep(self._context.poll_interval_seconds))
                    continue

                if chunk == b"":
                    await self._shutdown("connection closed", emit_ack=False)
                    break

                self._last_activity_at = self._context.clock()
                for message in self._reader.feed(chunk):
                    self._last_activity_at = self._context.clock()
                    await self._dispatch_message(message)
                    if self._closing:
                        break
        finally:
            await self._finish_shutdown()

    async def _dispatch_message(self, message: dict[str, Any]) -> None:
        message_type = str(message.get("type") or "").strip()
        if message_type == "heartbeat":
            self._last_activity_at = self._context.clock()
            return
        if message_type != "request":
            return

        request_id = str(message.get("request_id") or "").strip()
        action = str(message.get("action") or "").strip()
        if request_id == "" or action == "":
            await self._send_response(
                request_id or "unknown",
                ok=False,
                error={
                    "type": "MalformedRequest",
                    "message": "request_id and action are required",
                },
            )
            return

        payload = self._request_payload(message)
        if action == "shutdown":
            reason = str(payload.get("reason") or "shutdown requested")
            await self._send_response(
                request_id,
                ok=True,
                result={"accepted": True, "reason": reason},
            )
            await self._shutdown(reason, emit_ack=True, request_id=request_id)
            return
        if action == "cancel_stream":
            stream_id = str(payload.get("stream_id") or "").strip()
            if stream_id == "":
                await self._send_response(
                    request_id,
                    ok=False,
                    error={
                        "type": "ValueError",
                        "message": "payload.stream_id is required",
                    },
                )
                return
            cancelled = self._cancel_stream_task(stream_id)
            await self._send_response(
                request_id,
                ok=True,
                result={"stream_id": stream_id, "cancelled": cancelled},
            )
            return
        if action in _STREAM_ACTIONS:
            payload["stream_id"] = self._request_stream_id(request_id, payload)

        task = asyncio.create_task(
            self._run_request(request_id=request_id, action=action, payload=payload)
        )
        self._request_tasks.add(task)
        stream_id = (
            str(payload.get("stream_id") or "").strip()
            if action in _STREAM_ACTIONS
            else ""
        )
        if stream_id != "":
            self._stream_tasks[stream_id] = task

        def _on_done(done: asyncio.Task[None]) -> None:
            self._request_tasks.discard(done)
            if stream_id != "" and self._stream_tasks.get(stream_id) is done:
                self._stream_tasks.pop(stream_id, None)

        task.add_done_callback(_on_done)

    async def _run_request(
        self,
        *,
        request_id: str,
        action: str,
        payload: dict[str, Any],
    ) -> None:
        try:
            result = await self._dispatch_action(
                request_id=request_id,
                action=action,
                payload=payload,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            await self._send_response(
                request_id,
                ok=False,
                error={
                    "type": exc.__class__.__name__,
                    "message": str(exc),
                },
            )
            return

        if result is None:
            return
        if isinstance(result, dict) and result.get("_kind") == "stream":
            return
        await self._send_response(request_id, ok=True, result=result)

    async def _dispatch_action(
        self,
        *,
        request_id: str,
        action: str,
        payload: dict[str, Any],
    ) -> Any:
        backend = self._context.backend
        if action == "connect_host":
            host = self._require_host(payload)
            timeout_seconds = self._optional_timeout_seconds(payload)
            if timeout_seconds is None:
                return await self._call_backend("connect_host", host)
            return await self._call_backend(
                "connect_host",
                host,
                timeout_seconds=timeout_seconds,
            )
        if action == "list_containers":
            host = self._require_host(payload)
            timeout_seconds = self._optional_timeout_seconds(payload)
            if timeout_seconds is None:
                return await self._call_backend("list_containers_for_host", host)
            return await self._call_backend(
                "list_containers_for_host",
                host,
                timeout_seconds=timeout_seconds,
            )
        if action == "fetch_container_stats":
            host = self._require_host(payload)
            container = self._require_container(payload)
            timeout_seconds = self._optional_timeout_seconds(payload)
            if timeout_seconds is None:
                return await self._call_backend(
                    "fetch_container_stats",
                    host,
                    container,
                )
            return await self._call_backend(
                "fetch_container_stats",
                host,
                container,
                timeout_seconds=timeout_seconds,
            )
        if action == "query_container_logs":
            host = self._require_host(payload)
            container = self._require_container(payload)
            timeout_seconds = self._optional_timeout_seconds(payload)
            backend_kwargs: dict[str, Any] = {
                "since": payload.get("since"),
                "until": payload.get("until"),
                "max_lines": int(payload.get("max_lines", 2000)),
            }
            if timeout_seconds is not None:
                backend_kwargs["timeout_seconds"] = timeout_seconds
            records = await self._collect_records(
                await self._call_backend(
                    "query_container_logs",
                    host,
                    container,
                    **backend_kwargs,
                )
            )
            return self._normalize_log_records(host, container, records)
        if action in {"restart_container_for_host", "restart_container"}:
            host = self._require_host(payload)
            container = self._require_container(payload)
            return await self._call_backend(
                "restart_container_for_host",
                host,
                container,
                timeout=int(payload.get("timeout", 10)),
            )
        if action in {"exec_container_for_host", "exec_container"}:
            host = self._require_host(payload)
            container = self._require_container(payload)
            return await self._call_backend(
                "exec_container_for_host",
                host,
                container,
                command=str(payload.get("command") or ""),
                timeout_seconds=int(payload.get("timeout_seconds", 30)),
                max_output_chars=int(payload.get("max_output_chars", 2000)),
            )
        if action in {"collect_host_metrics_for_host", "collect_host_metrics"}:
            host = self._require_host(payload)
            method = getattr(backend, "collect_host_metrics_for_host", None)
            if not callable(method):
                raise AttributeError(
                    "backend does not implement collect_host_metrics_for_host"
                )
            return await _maybe_await(
                method(
                    host,
                    collect_load=bool(payload.get("collect_load", True)),
                    collect_network=bool(payload.get("collect_network", True)),
                    timeout_seconds=int(payload.get("timeout_seconds", 8)),
                )
            )
        if action in {"stream_container_logs", "stream_logs"}:
            host = self._require_host(payload)
            container = self._require_container(payload)
            raw_pipeline_config = payload.get("pipeline")
            pipeline_config: dict[str, Any] | None = None
            if raw_pipeline_config is not None:
                if not isinstance(raw_pipeline_config, dict):
                    raise TypeError("remote pipeline config must be an object")
                pipeline_config = validate_remote_pipeline_config(raw_pipeline_config)
            stream_id = self._request_stream_id(request_id, payload)
            await self._send_message(
                {
                    "type": "response",
                    "request_id": request_id,
                    "ok": True,
                    "stream_id": stream_id,
                }
            )
            stream = None
            try:
                stream = await self._call_backend(
                    "stream_container_logs",
                    host,
                    container,
                    since=payload.get("since"),
                    tail=payload.get("tail"),
                )
                async for record in self._iterate_stream(stream):
                    for item in self._normalize_log_records(
                        host,
                        container,
                        [record],
                        pipeline_config=pipeline_config,
                    ):
                        await self._send_frame("log", stream_id, item)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                await self._send_stream_error(stream_id, exc)
                return {"_kind": "stream"}
            finally:
                if stream is not None:
                    await self._close_stream_resource(stream)
            await self._send_stream_end(stream_id)
            return {"_kind": "stream"}
        if action in {"stream_docker_events", "stream_events"}:
            host = self._require_host(payload)
            stream_id = self._request_stream_id(request_id, payload)
            await self._send_message(
                {
                    "type": "response",
                    "request_id": request_id,
                    "ok": True,
                    "stream_id": stream_id,
                }
            )
            stream = None
            try:
                stream = await self._call_backend(
                    "stream_docker_events",
                    host,
                    filters=payload.get("filters"),
                )
                async for event in self._iterate_stream(stream):
                    await self._send_frame("event", stream_id, dict(event))
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                await self._send_stream_error(stream_id, exc)
                return {"_kind": "stream"}
            finally:
                if stream is not None:
                    await self._close_stream_resource(stream)
            await self._send_stream_end(stream_id)
            return {"_kind": "stream"}
        if action == "shutdown":
            return None
        raise ValueError(f"unsupported action: {action}")

    async def _shutdown(
        self,
        reason: str,
        *,
        emit_ack: bool,
        request_id: str | None = None,
    ) -> None:
        if self._closing:
            return
        self._closing = True
        self._shutdown_reason = reason
        if self._request_tasks:
            await asyncio.sleep(0)
        if emit_ack and request_id is not None:
            await self._send_message(
                {
                    "type": "shutdown_ack",
                    "reason": reason,
                }
            )

    async def _finish_shutdown(self) -> None:
        if not self._closing:
            self._closing = True
        if self._request_tasks:
            for task in list(self._request_tasks):
                task.cancel()
            await asyncio.gather(*self._request_tasks, return_exceptions=True)
        self._stream_tasks.clear()
        await self._run_cleanup(reason=self._shutdown_reason)
        close = getattr(self._transport, "close", None)
        if callable(close):
            await _maybe_await(close())

    async def _run_cleanup(self, *, reason: str) -> None:
        if self._cleanup_finished:
            return
        self._cleanup_finished = True
        payload = {
            "reason": reason,
            "workspace_dir": str(self._workspace_manager.path),
            "last_activity_at": self._last_activity_at,
        }
        try:
            if self._context.cleanup_hook is not None:
                await _maybe_await(self._context.cleanup_hook(payload))
        finally:
            await self._workspace_manager.cleanup()

    async def _send_response(
        self,
        request_id: str,
        *,
        ok: bool,
        result: Any | None = None,
        error: dict[str, Any] | None = None,
    ) -> None:
        message: dict[str, Any] = {
            "type": "response",
            "request_id": request_id,
            "ok": bool(ok),
        }
        if result is not None:
            message["result"] = result
        if error is not None:
            message["error"] = error
        await self._send_message(message)

    async def _send_frame(
        self,
        frame_type: str,
        stream_id: str,
        payload: dict[str, Any],
    ) -> None:
        message = {"type": frame_type, "stream_id": stream_id, **payload}
        await self._send_message(message)

    async def _send_stream_end(self, stream_id: str) -> None:
        await self._send_message({"type": "stream_end", "stream_id": stream_id})

    async def _send_stream_error(self, stream_id: str, exc: BaseException) -> None:
        await self._send_message(
            {
                "type": "error",
                "stream_id": stream_id,
                "message": str(exc),
                "error": {
                    "type": exc.__class__.__name__,
                    "message": str(exc),
                },
            }
        )

    async def _send_message(self, message: dict[str, Any]) -> None:
        encoded = encode_frame(message)
        async with self._write_lock:
            write = getattr(self._transport, "write", None)
            if not callable(write):
                raise AttributeError("transport does not implement write")
            await _maybe_await(write(encoded))
            flush = getattr(self._transport, "flush", None)
            if callable(flush):
                await _maybe_await(flush())

    async def _read_chunk(self) -> bytes | None:
        read = getattr(self._transport, "read", None)
        if not callable(read):
            raise AttributeError("transport does not implement read")
        try:
            return await _maybe_await(
                read(
                    self._context.read_size,
                    timeout_seconds=self._context.poll_interval_seconds,
                )
            )
        except TypeError:
            chunk = read(
                self._context.read_size,
            )
            return await _maybe_await(chunk)

    def _request_payload(self, message: dict[str, Any]) -> dict[str, Any]:
        payload = message.get("payload")
        if isinstance(payload, dict):
            result = dict(payload)
        else:
            result = {
                key: value
                for key, value in message.items()
                if key not in {"type", "request_id", "action", "stream_id"}
            }
        stream_id = str(message.get("stream_id") or "").strip()
        if stream_id != "":
            result.setdefault("stream_id", stream_id)
        return result

    def _require_host(self, payload: dict[str, Any]) -> dict[str, Any]:
        host = payload.get("host")
        if not isinstance(host, dict):
            raise ValueError("payload.host must be a dict")
        return dict(host)

    def _require_container(self, payload: dict[str, Any]) -> dict[str, Any]:
        container = payload.get("container")
        if isinstance(container, dict):
            return dict(container)
        container_id = str(payload.get("container_id") or "").strip()
        if container_id == "":
            raise ValueError("payload.container or payload.container_id is required")
        container_name = str(payload.get("container_name") or container_id)
        return {"id": container_id, "container_id": container_id, "name": container_name}

    def _optional_timeout_seconds(self, payload: dict[str, Any]) -> float | None:
        raw_timeout = payload.get("timeout_seconds")
        if raw_timeout is None:
            return None
        if isinstance(raw_timeout, bool):
            raise ValueError("payload.timeout_seconds must be a number")
        if isinstance(raw_timeout, (int, float)):
            timeout_seconds = float(raw_timeout)
        elif isinstance(raw_timeout, str):
            normalized = raw_timeout.strip()
            if normalized == "":
                return None
            timeout_seconds = float(normalized)
        else:
            timeout_seconds = float(raw_timeout)
        if timeout_seconds <= 0:
            return None
        return float(timeout_seconds)

    def _cancel_stream_task(self, stream_id: str) -> bool:
        task = self._stream_tasks.pop(stream_id, None)
        if task is None:
            return False
        task.cancel()
        return True

    async def _call_backend(self, method_name: str, *args: Any, **kwargs: Any) -> Any:
        method = getattr(self._context.backend, method_name, None)
        if not callable(method):
            raise AttributeError(f"backend does not implement {method_name}")
        return await _maybe_await(method(*args, **kwargs))

    async def _collect_records(self, records: Any) -> list[dict[str, Any]]:
        if records is None:
            return []
        if hasattr(records, "__aiter__"):
            collected: list[dict[str, Any]] = []
            async for item in records:
                if isinstance(item, dict):
                    collected.append(dict(item))
            return collected
        return [dict(item) for item in list(records) if isinstance(item, dict)]

    def _normalize_log_records(
        self,
        host: dict[str, Any],
        container: dict[str, Any],
        records: Any,
        *,
        pipeline_config: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        items = list(records or [])
        pipeline = self._context.log_pipeline
        if pipeline is None and isinstance(pipeline_config, dict):
            pipeline = RemoteWorkerPipeline(
                config=validate_remote_pipeline_config(pipeline_config)
            )
        if pipeline is None:
            return [dict(item) for item in items if isinstance(item, dict)]

        host_name = str(host.get("name") or "")
        container_id = str(container.get("id") or container.get("container_id") or "")
        container_name = str(container.get("name") or container_id)
        lines = [
            LogLine(
                host_name=host_name,
                container_id=container_id,
                container_name=container_name,
                timestamp=str(item.get("timestamp") or item.get("time") or ""),
                content=str(item.get("line") or item.get("content") or ""),
                metadata=dict(item),
            )
            for item in items
            if isinstance(item, dict)
        ]
        processed = pipeline.process(lines)
        return [
            {
                "host_name": line.host_name,
                "container_id": line.container_id,
                "container_name": line.container_name,
                "timestamp": line.timestamp,
                "line": line.content,
                "metadata": dict(line.metadata or {}),
            }
            for line in processed
        ]

    async def _iterate_stream(self, stream: Any):
        if hasattr(stream, "__aiter__"):
            async for item in stream:
                if isinstance(item, dict):
                    yield item
            return
        items = await _maybe_await(stream)
        for item in items or []:
            if isinstance(item, dict):
                yield item

    async def _close_stream_resource(self, stream: Any) -> None:
        aclose = getattr(stream, "aclose", None)
        if callable(aclose):
            try:
                await _maybe_await(aclose())
            except Exception:  # noqa: BLE001
                return
            return
        close = getattr(stream, "close", None)
        if callable(close):
            try:
                await _maybe_await(close())
            except Exception:  # noqa: BLE001
                return

    def _request_stream_id(self, request_id: str, payload: dict[str, Any]) -> str:
        stream_id = str(payload.get("stream_id") or "").strip()
        if stream_id:
            return stream_id
        self._stream_sequence += 1
        return f"{request_id}-stream-{self._stream_sequence}"

    def _should_idle_shutdown(self) -> bool:
        timeout = float(self._context.idle_timeout_seconds)
        if timeout <= 0:
            return False
        return (self._context.clock() - self._last_activity_at) >= timeout


async def run_remote_worker(
    *,
    transport: Any,
    backend: Any,
    cleanup_hook: Callable[[dict[str, Any]], Any] | None = None,
    clock: Callable[[], float] | None = None,
    sleep: Callable[[float], Any] | None = None,
    idle_timeout_seconds: float = _DEFAULT_IDLE_TIMEOUT_SECONDS,
    poll_interval_seconds: float = _DEFAULT_POLL_INTERVAL_SECONDS,
    read_size: int = _DEFAULT_READ_SIZE,
    workspace_root: str | Path | None = None,
    log_pipeline: RemoteWorkerPipeline | None = None,
) -> None:
    context = RemoteWorkerContext(
        backend=backend,
        cleanup_hook=cleanup_hook,
        clock=clock or __import__("time").monotonic,
        sleep=sleep or asyncio.sleep,
        idle_timeout_seconds=idle_timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
        read_size=read_size,
        workspace_root=workspace_root,
        log_pipeline=log_pipeline,
    )
    worker = RemoteWorkerProcess(transport=transport, context=context)
    await worker.run()


def main(argv: list[str] | None = None) -> int:
    del argv
    deploy_root = str(os.environ.get("LOGDOG_REMOTE_DEPLOY_ROOT") or "").strip()
    workspace_root = (
        str(os.environ.get("LOGDOG_REMOTE_TEMP_ROOT") or "").strip()
        or str(os.environ.get("LOGDOG_REMOTE_WORKER_WORKSPACE_ROOT") or "").strip()
        or deploy_root
    )
    raw_idle_timeout = str(
        os.environ.get("LOGDOG_REMOTE_HEARTBEAT_TIMEOUT_SECONDS") or ""
    ).strip()
    raw_poll_interval = str(
        os.environ.get("LOGDOG_REMOTE_HEARTBEAT_POLL_INTERVAL_SECONDS") or ""
    ).strip()

    async def cleanup_hook(_payload: dict[str, Any]) -> None:
        if deploy_root == "":
            return
        deploy_path = Path(deploy_root)
        if not deploy_path.name.startswith("logdog-remote-worker"):
            return
        shutil.rmtree(deploy_path, ignore_errors=True)

    transport = StdIOTransport()
    backend = DockerCLIWorkerBackend()
    asyncio.run(
        run_remote_worker(
            transport=transport,
            backend=backend,
            cleanup_hook=cleanup_hook,
            idle_timeout_seconds=(
                float(raw_idle_timeout)
                if raw_idle_timeout != ""
                else _DEFAULT_IDLE_TIMEOUT_SECONDS
            ),
            poll_interval_seconds=(
                float(raw_poll_interval)
                if raw_poll_interval != ""
                else _DEFAULT_POLL_INTERVAL_SECONDS
            ),
            workspace_root=workspace_root or None,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
