from __future__ import annotations

import asyncio
import inspect
import io
import posixpath
import queue
import shlex
import threading
import time
from dataclasses import dataclass
from itertools import count
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from logdog.collector.host_metrics_probe import _set_host_key_policy
from logdog.remote.ssh_lifecycle import SSHSessionLifecycle
from logdog.remote.worker_runtime import WorkerSession

try:
    import paramiko
except Exception:  # pragma: no cover - exercised where dependency is unavailable
    paramiko = None

@dataclass(slots=True)
class _HostSession:
    host: dict[str, Any]
    session_token: str
    interpreter: str
    channel: Any
    session: Any
    connect_result: dict[str, Any]
    heartbeat_task: asyncio.Task[None] | None = None


class _MissingLauncher:
    async def open_channel(self, host: dict[str, Any], *, interpreter: str) -> Any:
        raise RuntimeError(
            "remote worker launcher is not configured for host "
            f"{host.get('name')!r}"
        )


_FALSEY_STRINGS = frozenset({"", "0", "false", "no", "off"})
_TRUTHY_STRINGS = frozenset({"1", "true", "yes", "on"})


def _coerce_bool_like(value: Any, *, default: bool) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in _FALSEY_STRINGS:
            return False
        if normalized in _TRUTHY_STRINGS:
            return True
    return bool(value)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _run_sync_in_daemon_thread(func: Any, /, *args: Any, **kwargs: Any):
    async def _runner() -> Any:
        result_queue: queue.Queue[tuple[bool, Any]] = queue.Queue(maxsize=1)

        def _worker() -> None:
            try:
                result = func(*args, **kwargs)
            except BaseException as exc:  # noqa: BLE001
                result_queue.put((False, exc))
                return
            result_queue.put((True, result))

        thread = threading.Thread(
            target=_worker,
            daemon=True,
            name="remote-worker-launcher",
        )
        thread.start()

        while True:
            try:
                succeeded, value = result_queue.get_nowait()
            except queue.Empty:
                await asyncio.sleep(0.01)
                continue
            if succeeded:
                return value
            raise value

    return _runner()


class ManagedSSHChannel:
    def __init__(
        self,
        *,
        client: Any,
        channel: Any,
        poll_interval_seconds: float = 0.05,
    ) -> None:
        self._client = client
        self._channel = channel
        self._poll_interval_seconds = max(0.01, float(poll_interval_seconds))
        self._closed = False

    def read(
        self,
        size: int,
        *,
        timeout_seconds: float | None = None,
    ) -> bytes | None:
        normalized_size = max(1, int(size))
        deadline = None
        if timeout_seconds is not None:
            deadline = time.monotonic() + max(0.0, float(timeout_seconds))
        stderr_chunks: list[bytes] = []

        while True:
            if self._closed:
                return b""

            recv_ready = getattr(self._channel, "recv_ready", None)
            if callable(recv_ready) and recv_ready():
                return bytes(self._channel.recv(normalized_size))

            recv_stderr_ready = getattr(self._channel, "recv_stderr_ready", None)
            if callable(recv_stderr_ready) and recv_stderr_ready():
                stderr_chunks.append(bytes(self._channel.recv_stderr(normalized_size)))
                continue

            exit_ready = getattr(self._channel, "exit_status_ready", None)
            if callable(exit_ready) and exit_ready():
                stderr_text = b"".join(stderr_chunks).decode("utf-8", errors="replace")
                stderr_text = stderr_text.strip()
                if stderr_text != "":
                    raise RuntimeError(stderr_text)
                return b""

            if deadline is not None and time.monotonic() >= deadline:
                return None

            time.sleep(self._poll_interval_seconds)

    def write(self, data: bytes) -> int:
        if self._closed:
            raise RuntimeError("managed ssh channel is closed")
        payload = bytes(data)
        sender = getattr(self._channel, "sendall", None)
        if not callable(sender):
            raise AttributeError("ssh channel does not implement sendall")
        sender(payload)
        return len(payload)

    def flush(self) -> None:
        return None

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        channel_close = getattr(self._channel, "close", None)
        if callable(channel_close):
            channel_close()
        client_close = getattr(self._client, "close", None)
        if callable(client_close):
            client_close()


class ParamikoRemoteWorkerLauncher:
    def __init__(
        self,
        *,
        ssh_client_factory: Any | None = None,
        local_root: str | Path | None = None,
        upload_files: list[str] | tuple[str, ...] | None = None,
        connect_timeout_seconds: float = 30.0,
        channel_poll_interval_seconds: float = 0.05,
    ) -> None:
        if paramiko is None and ssh_client_factory is None:
            raise RuntimeError("paramiko is required for ParamikoRemoteWorkerLauncher")
        self._ssh_client_factory = ssh_client_factory or paramiko.SSHClient
        self._local_root = Path(local_root) if local_root is not None else _repo_root()
        self._upload_files = tuple(
            upload_files
            or (
                "logdog/remote/__init__.py",
                "logdog/remote/worker_main.py",
                "logdog/remote/worker_pipeline.py",
                "logdog/remote/worker_protocol.py",
                "logdog/pipeline/filter.py",
                "logdog/pipeline/preprocessor/base.py",
            )
        )
        self._connect_timeout_seconds = float(connect_timeout_seconds)
        self._channel_poll_interval_seconds = float(channel_poll_interval_seconds)

    async def prepare_session(self, host: dict[str, Any]) -> dict[str, Any]:
        return await _run_sync_in_daemon_thread(self._prepare_session_sync, dict(host))

    def _prepare_session_sync(self, host: dict[str, Any]) -> dict[str, Any]:
        connection = self._parse_host_connection(host)
        client = self._ssh_client_factory()
        remote_root = ""
        worker_channel = None
        try:
            _set_host_key_policy(
                client,
                strict_host_key=connection["strict_host_key"],
            )
            connect_kwargs = {
                "hostname": connection["hostname"],
                "port": connection["port"],
                "username": connection["username"],
                "timeout": connection["timeout_seconds"],
                "banner_timeout": connection["timeout_seconds"],
                "auth_timeout": connection["timeout_seconds"],
                "look_for_keys": True,
            }
            if connection["ssh_key"]:
                connect_kwargs["key_filename"] = connection["ssh_key"]
            if connection["password"]:
                connect_kwargs["password"] = connection["password"]
            client.connect(**connect_kwargs)

            interpreter = self._probe_remote_interpreter(
                client,
                timeout_seconds=connection["timeout_seconds"],
            )
            remote_root = self._create_remote_root(
                client,
                temp_root=connection["temp_root"],
                timeout_seconds=connection["timeout_seconds"],
            )
            self._upload_runtime_files(client, remote_root)
            worker_channel = self._start_worker_channel(
                client,
                interpreter=interpreter,
                remote_root=remote_root,
                timeout_seconds=connection["timeout_seconds"],
                heartbeat_timeout_seconds=connection["heartbeat_timeout_seconds"],
                heartbeat_poll_interval_seconds=connection[
                    "heartbeat_poll_interval_seconds"
                ],
            )
            return {
                "channel": ManagedSSHChannel(
                    client=client,
                    channel=worker_channel,
                    poll_interval_seconds=self._channel_poll_interval_seconds,
                ),
                "interpreter": interpreter,
                "remote_root": remote_root,
            }
        except Exception:
            if remote_root != "":
                try:
                    self._cleanup_remote_root(
                        client,
                        remote_root=remote_root,
                        timeout_seconds=connection.get("timeout_seconds", 30.0),
                    )
                except Exception:
                    pass
            if worker_channel is not None:
                worker_close = getattr(worker_channel, "close", None)
                if callable(worker_close):
                    worker_close()
            client_close = getattr(client, "close", None)
            if callable(client_close):
                client_close()
            raise

    def _parse_host_connection(self, host: dict[str, Any]) -> dict[str, Any]:
        host_name = str(host.get("name") or "").strip() or "unknown"
        url = str(host.get("url") or "").strip()
        parsed = urlparse(url)
        if parsed.scheme.lower() != "ssh":
            raise ValueError(f"host[{host_name}] must use ssh:// url")

        hostname = str(parsed.hostname or "").strip()
        username = (
            str(parsed.username or host.get("ssh_user") or "").strip()
        )
        if hostname == "":
            raise ValueError(f"host[{host_name}] ssh hostname is required")
        if username == "":
            raise ValueError(f"host[{host_name}] ssh username is required")

        remote_cfg = host.get("remote_worker")
        remote_worker = dict(remote_cfg) if isinstance(remote_cfg, dict) else {}
        temp_root = str(remote_worker.get("temp_root") or "/tmp").strip() or "/tmp"
        heartbeat_timeout_seconds = remote_worker.get("heartbeat_timeout_seconds")
        heartbeat_poll_interval_seconds = remote_worker.get(
            "heartbeat_poll_interval_seconds"
        )
        # NOTE: docker SDK's ssh:// path uses the system ssh client by default and
        # typically auto-accepts unknown host keys in non-interactive mode. To
        # keep remote worker default-on behavior compatible with existing setups,
        # we default to non-strict host key checking unless explicitly enabled.
        strict_host_key = _coerce_bool_like(
            host.get("strict_host_key", host.get("strict_ssh_key")),
            default=False,
        )
        ssh_key = str(host.get("ssh_key") or "").strip()
        password = str(parsed.password or host.get("ssh_password") or host.get("password") or "").strip()
        raw_timeout = host.get("docker_timeout_seconds")
        timeout_seconds = self._connect_timeout_seconds
        if raw_timeout is not None and str(raw_timeout).strip() != "":
            timeout_seconds = float(raw_timeout)
        return {
            "hostname": hostname,
            "port": int(parsed.port or 22),
            "username": username,
            "ssh_key": ssh_key,
            "password": password,
            "strict_host_key": strict_host_key,
            "timeout_seconds": timeout_seconds,
            "temp_root": temp_root,
            "heartbeat_timeout_seconds": heartbeat_timeout_seconds,
            "heartbeat_poll_interval_seconds": heartbeat_poll_interval_seconds,
        }

    def _probe_remote_interpreter(self, client: Any, *, timeout_seconds: float) -> str:
        for candidate in ("python3", "python"):
            command = (
                "/bin/sh -lc "
                + shlex.quote(
                    f"command -v {shlex.quote(candidate)} >/dev/null 2>&1 && printf %s {shlex.quote(candidate)}"
                )
            )
            exit_status, stdout_text, _stderr_text = self._exec_capture(
                client,
                command,
                timeout_seconds=timeout_seconds,
            )
            if exit_status == 0 and stdout_text.strip() != "":
                return stdout_text.strip()
        raise RuntimeError("no usable remote Python interpreter found")

    def _create_remote_root(
        self,
        client: Any,
        *,
        temp_root: str,
        timeout_seconds: float,
    ) -> str:
        shell_script = (
            f'base={shlex.quote(temp_root)}; '
            'mkdir -p "$base"; '
            'mktemp -d "$base/logdog-remote-worker.XXXXXX"'
        )
        exit_status, stdout_text, stderr_text = self._exec_capture(
            client,
            "/bin/sh -lc " + shlex.quote(shell_script),
            timeout_seconds=timeout_seconds,
        )
        remote_root = stdout_text.strip()
        if exit_status != 0 or remote_root == "":
            message = stderr_text or stdout_text or "failed to create remote temp dir"
            raise RuntimeError(message.strip())
        return remote_root

    def _upload_runtime_files(self, client: Any, remote_root: str) -> None:
        sftp = client.open_sftp()
        try:
            for relative_path in self._upload_files:
                normalized_relative_path = str(relative_path).strip().replace("\\", "/")
                if normalized_relative_path == "":
                    continue
                local_path = self._local_root / Path(normalized_relative_path)
                if not local_path.exists():
                    raise FileNotFoundError(local_path)
                remote_path = posixpath.join(remote_root, normalized_relative_path)
                self._ensure_remote_dir(sftp, posixpath.dirname(remote_path))
                with local_path.open("rb") as handle:
                    sftp.putfo(io.BytesIO(handle.read()), remote_path)
        finally:
            close = getattr(sftp, "close", None)
            if callable(close):
                close()

    def _ensure_remote_dir(self, sftp: Any, remote_dir: str) -> None:
        normalized = str(remote_dir or "").strip()
        if normalized == "":
            return
        current = ""
        for part in normalized.split("/"):
            if part == "":
                continue
            current = f"{current}/{part}" if current else f"/{part}"
            try:
                sftp.mkdir(current)
            except Exception:
                continue

    def _start_worker_channel(
        self,
        client: Any,
        *,
        interpreter: str,
        remote_root: str,
        timeout_seconds: float,
        heartbeat_timeout_seconds: Any | None = None,
        heartbeat_poll_interval_seconds: Any | None = None,
    ) -> Any:
        transport = client.get_transport()
        if transport is None:
            raise RuntimeError("ssh transport is unavailable")
        channel = transport.open_session(timeout=timeout_seconds)
        setter = getattr(channel, "settimeout", None)
        if callable(setter):
            setter(timeout_seconds)
        shell_script = (
            f"cd {shlex.quote(remote_root)} && "
            f"PYTHONPATH={shlex.quote(remote_root)} "
            f"LOGDOG_REMOTE_DEPLOY_ROOT={shlex.quote(remote_root)} "
            f"LOGDOG_REMOTE_TEMP_ROOT={shlex.quote(remote_root)} "
            f"{self._format_optional_env('LOGDOG_REMOTE_HEARTBEAT_TIMEOUT_SECONDS', heartbeat_timeout_seconds)}"
            f"{self._format_optional_env('LOGDOG_REMOTE_HEARTBEAT_POLL_INTERVAL_SECONDS', heartbeat_poll_interval_seconds)}"
            f"{shlex.quote(interpreter)} -u -m logdog.remote.worker_main"
        )
        channel.exec_command("/bin/sh -lc " + shlex.quote(shell_script))
        return channel

    def _format_optional_env(self, name: str, value: Any) -> str:
        normalized = "" if value is None else str(value).strip()
        if normalized == "":
            return ""
        return f"{name}={shlex.quote(normalized)} "

    def _cleanup_remote_root(
        self,
        client: Any,
        *,
        remote_root: str,
        timeout_seconds: float,
    ) -> None:
        shell_script = f"rm -rf {shlex.quote(remote_root)}"
        self._exec_capture(
            client,
            "/bin/sh -lc " + shlex.quote(shell_script),
            timeout_seconds=timeout_seconds,
        )

    def _exec_capture(
        self,
        client: Any,
        command: str,
        *,
        timeout_seconds: float,
    ) -> tuple[int, str, str]:
        transport = client.get_transport()
        if transport is None:
            raise RuntimeError("ssh transport is unavailable")
        channel = transport.open_session(timeout=timeout_seconds)
        stdout_chunks: list[bytes] = []
        stderr_chunks: list[bytes] = []
        try:
            setter = getattr(channel, "settimeout", None)
            if callable(setter):
                setter(timeout_seconds)
            channel.exec_command(command)
            deadline = time.monotonic() + max(1.0, float(timeout_seconds))
            while True:
                recv_ready = getattr(channel, "recv_ready", None)
                if callable(recv_ready) and recv_ready():
                    stdout_chunks.append(bytes(channel.recv(65536)))
                    continue

                recv_stderr_ready = getattr(channel, "recv_stderr_ready", None)
                if callable(recv_stderr_ready) and recv_stderr_ready():
                    stderr_chunks.append(bytes(channel.recv_stderr(65536)))
                    continue

                exit_ready = getattr(channel, "exit_status_ready", None)
                if callable(exit_ready) and exit_ready():
                    break

                if time.monotonic() >= deadline:
                    raise TimeoutError(f"remote command timed out: {command}")
                time.sleep(0.01)

            exit_status_getter = getattr(channel, "recv_exit_status", None)
            exit_status = (
                int(exit_status_getter()) if callable(exit_status_getter) else 0
            )
            return (
                exit_status,
                b"".join(stdout_chunks).decode("utf-8", errors="replace"),
                b"".join(stderr_chunks).decode("utf-8", errors="replace"),
            )
        finally:
            close = getattr(channel, "close", None)
            if callable(close):
                close()


class RemoteWorkerBackend:
    def __init__(
        self,
        *,
        lifecycle: SSHSessionLifecycle | None = None,
        launcher: Any | None = None,
        session_factory: Any | None = None,
        probe_interpreter: Any | None = None,
        request_timeout_seconds: float = 30.0,
        heartbeat_interval_seconds: float = 30.0,
    ) -> None:
        self._lifecycle = lifecycle or SSHSessionLifecycle()
        self._launcher = launcher or _MissingLauncher()
        self._session_factory = session_factory or self._default_session_factory
        self._probe_interpreter = probe_interpreter or self._default_probe_interpreter
        self._request_timeout_seconds = float(request_timeout_seconds)
        self._heartbeat_interval_seconds = float(heartbeat_interval_seconds)
        self._hosts: dict[str, _HostSession] = {}
        self._lock = asyncio.Lock()
        self._session_condition = asyncio.Condition(self._lock)
        self._request_sequence = count(1)
        self._stream_sequence = count(1)

    def _docker_command_timeout_seconds(self, host: dict[str, Any]) -> float:
        raw_timeout = host.get("docker_timeout_seconds")
        if raw_timeout is None or str(raw_timeout).strip() == "":
            timeout_seconds = 10.0
        else:
            try:
                timeout_seconds = float(raw_timeout)
            except (TypeError, ValueError):
                timeout_seconds = 10.0

        # Ensure the remote docker command times out before the session request does,
        # so we return a structured error instead of hanging until the client-side
        # request timeout.
        max_allowed = max(1.0, float(self._request_timeout_seconds) - 2.0)
        if timeout_seconds <= 0:
            timeout_seconds = 1.0
        return float(min(timeout_seconds, max_allowed))

    async def connect_host(self, host: dict[str, Any]) -> dict[str, Any]:
        entry = await self._ensure_host_session(host)
        return dict(entry.connect_result)

    async def list_containers_for_host(self, host: dict[str, Any]) -> list[dict[str, Any]]:
        response = await self._request(
            host,
            action="list_containers",
            payload={
                "host": dict(host),
                "timeout_seconds": self._docker_command_timeout_seconds(host),
            },
        )
        return list(response)

    async def fetch_container_stats(
        self,
        host: dict[str, Any],
        container: dict[str, Any],
    ) -> dict[str, Any]:
        response = await self._request(
            host,
            action="fetch_container_stats",
            payload={
                "host": dict(host),
                "container": dict(container),
                "timeout_seconds": self._docker_command_timeout_seconds(host),
            },
        )
        return dict(response)

    async def query_container_logs(
        self,
        host: dict[str, Any],
        container: dict[str, Any],
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        payload = {"host": dict(host), "container": dict(container)}
        payload.update(dict(kwargs))
        payload.setdefault(
            "timeout_seconds",
            self._docker_command_timeout_seconds(host),
        )
        response = await self._request(
            host,
            action="query_container_logs",
            payload=payload,
        )
        return list(response)

    async def stream_container_logs(
        self,
        host: dict[str, Any],
        container: dict[str, Any],
        **kwargs: Any,
    ):
        payload = {"host": dict(host), "container": dict(container)}
        payload.update(dict(kwargs))
        async for item in self._open_stream(
            host,
            action="stream_container_logs",
            payload=payload,
        ):
            yield dict(item)

    async def stream_docker_events(
        self,
        host: dict[str, Any],
        **kwargs: Any,
    ):
        payload = {"host": dict(host)}
        payload.update(dict(kwargs))
        async for item in self._open_stream(
            host,
            action="stream_docker_events",
            payload=payload,
        ):
            yield dict(item)

    async def restart_container_for_host(
        self,
        host: dict[str, Any],
        container: dict[str, Any],
        **kwargs: Any,
    ) -> dict[str, Any]:
        payload = {"host": dict(host), "container": dict(container)}
        payload.update(dict(kwargs))
        response = await self._request(
            host,
            action="restart_container_for_host",
            payload=payload,
        )
        return dict(response)

    async def exec_container_for_host(
        self,
        host: dict[str, Any],
        container: dict[str, Any],
        **kwargs: Any,
    ) -> dict[str, Any]:
        payload = {"host": dict(host), "container": dict(container)}
        payload.update(dict(kwargs))
        response = await self._request(
            host,
            action="exec_container_for_host",
            payload=payload,
        )
        return dict(response)

    async def collect_host_metrics_for_host(
        self,
        host: dict[str, Any],
        **kwargs: Any,
    ) -> dict[str, Any]:
        payload = {"host": dict(host)}
        payload.update(dict(kwargs))
        response = await self._request(
            host,
            action="collect_host_metrics_for_host",
            payload=payload,
        )
        return dict(response)

    async def close_host(self, host_name_or_host: Any) -> None:
        host_name = self._host_name(host_name_or_host)
        async with self._session_condition:
            entry = self._hosts.pop(host_name, None)
            self._session_condition.notify_all()
        if entry is None:
            return
        await self._cancel_heartbeat_task(entry.heartbeat_task)
        try:
            await self._lifecycle.begin_shutdown(
                host_name,
                reason="close_host requested",
                session_token=entry.session_token,
            )
        except Exception:
            pass
        close_errors: list[BaseException] = []
        try:
            await entry.session.request(
                action="shutdown",
                payload={
                    "reason": "close_host requested",
                    "host": dict(entry.host),
                },
                request_id=self._next_request_id(),
                timeout_seconds=min(5.0, self._request_timeout_seconds),
            )
        except Exception:
            pass
        session_close_error = await self._close_resource(entry.session)
        if session_close_error is not None:
            close_errors.append(session_close_error)
        channel_close_error = await self._close_resource(entry.channel)
        if channel_close_error is not None:
            close_errors.append(channel_close_error)
        await self._lifecycle.mark_closed(
            host_name,
            reason="close_host requested",
            session_token=entry.session_token,
        )
        async with self._session_condition:
            self._session_condition.notify_all()
        if close_errors:
            raise close_errors[0]

    async def close_all(self) -> None:
        async with self._lock:
            host_names = list(self._hosts.keys())
        for host_name in host_names:
            await self.close_host(host_name)

    async def _request(
        self,
        host: dict[str, Any],
        *,
        action: str,
        payload: dict[str, Any],
    ) -> Any:
        entry = await self._ensure_host_session(host)
        response = await entry.session.request(
            action=action,
            payload=payload,
            request_id=self._next_request_id(),
            timeout_seconds=self._request_timeout_seconds,
        )
        return self._unwrap_response(action, response)

    async def _open_stream(
        self,
        host: dict[str, Any],
        *,
        action: str,
        payload: dict[str, Any],
    ):
        entry = await self._ensure_host_session(host)
        stream = await entry.session.open_stream(
            action=action,
            payload=payload,
            request_id=self._next_request_id(),
            stream_id=self._next_stream_id(),
        )
        try:
            async for item in stream:
                yield dict(item)
        finally:
            aclose = getattr(stream, "aclose", None)
            if callable(aclose):
                await aclose()

    async def _ensure_host_session(self, host: dict[str, Any]) -> _HostSession:
        host_name = self._host_name(host)
        while True:
            async with self._session_condition:
                existing = self._hosts.get(host_name)
                if existing is not None:
                    return existing

            transition = await self._lifecycle.request_connect(host_name)
            if transition.created_new:
                session_token = str(transition.state.get("session_token") or "")
                break

            async with self._session_condition:
                existing = self._hosts.get(host_name)
                if existing is not None:
                    return existing
                try:
                    await asyncio.wait_for(self._session_condition.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue

        session: Any | None = None
        channel: Any | None = None
        try:
            await self._lifecycle.begin_handshake(host_name, session_token)
            prepare_session = getattr(self._launcher, "prepare_session", None)
            if callable(prepare_session):
                prepared = prepare_session(dict(host))
                if inspect.isawaitable(prepared):
                    prepared = await prepared
                channel, interpreter = self._resolve_prepared_session(
                    host=dict(host),
                    prepared=prepared,
                )
            else:
                interpreter = self._resolve_interpreter(host)
                channel = await self._launcher.open_channel(
                    dict(host),
                    interpreter=interpreter,
                )
            session = self._session_factory(channel=channel)
            start = getattr(session, "start", None)
            if callable(start):
                maybe_started = start()
                if inspect.isawaitable(maybe_started):
                    await maybe_started
            await self._lifecycle.mark_running(host_name, session_token)
            connect_result = dict(
                self._unwrap_response(
                    "connect_host",
                    await session.request(
                        action="connect_host",
                        payload={
                            "host": dict(host),
                            "timeout_seconds": self._docker_command_timeout_seconds(host),
                        },
                        request_id=self._next_request_id(),
                        timeout_seconds=self._request_timeout_seconds,
                    ),
                )
            )
        except Exception as exc:
            await self._close_resources_best_effort(session, channel)
            await self._lifecycle.mark_closed(
                host_name,
                reason=str(exc) or type(exc).__name__,
                session_token=session_token,
            )
            async with self._session_condition:
                self._session_condition.notify_all()
            raise

        entry = _HostSession(
            host=dict(host),
            session_token=session_token,
            interpreter=interpreter,
            channel=channel,
            session=session,
            connect_result=connect_result,
        )
        async with self._session_condition:
            current = self._hosts.get(host_name)
            if current is not None:
                return current
            heartbeat_task = self._start_heartbeat_task(
                host=dict(host),
                host_name=host_name,
                session_token=session_token,
                session=session,
            )
            entry.heartbeat_task = heartbeat_task
            self._hosts[host_name] = entry
            self._session_condition.notify_all()
        return entry

    def _start_heartbeat_task(
        self,
        *,
        host: dict[str, Any],
        host_name: str,
        session_token: str,
        session: Any,
    ) -> asyncio.Task[None] | None:
        interval_seconds = self._heartbeat_interval_for_host(host)
        if interval_seconds <= 0:
            return None
        return asyncio.create_task(
            self._heartbeat_loop(
                host_name=host_name,
                session_token=session_token,
                session=session,
                interval_seconds=interval_seconds,
            )
        )

    async def _heartbeat_loop(
        self,
        *,
        host_name: str,
        session_token: str,
        session: Any,
        interval_seconds: float,
    ) -> None:
        try:
            while True:
                await asyncio.sleep(interval_seconds)
                async with self._lock:
                    entry = self._hosts.get(host_name)
                if entry is None or entry.session_token != session_token:
                    return
                heartbeat = getattr(session, "send_heartbeat", None)
                if callable(heartbeat):
                    await heartbeat()
                await self._lifecycle.record_heartbeat(host_name, session_token)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            async with self._session_condition:
                current = self._hosts.get(host_name)
                channel = current.channel if current is not None and current.session_token == session_token else None
                if current is not None and current.session_token == session_token:
                    self._hosts.pop(host_name, None)
                self._session_condition.notify_all()
            try:
                await self._lifecycle.mark_closed(
                    host_name,
                    reason=str(exc) or "heartbeat failed",
                    session_token=session_token,
                )
            except Exception:
                pass
            await self._close_resources_best_effort(session, channel)

    async def _cancel_heartbeat_task(
        self,
        heartbeat_task: asyncio.Task[None] | None,
    ) -> None:
        if heartbeat_task is None:
            return
        heartbeat_task.cancel()
        try:
            await heartbeat_task
        except asyncio.CancelledError:
            return

    async def _close_resource(self, resource: Any) -> BaseException | None:
        close = getattr(resource, "close", None)
        if not callable(close):
            return None
        try:
            maybe_awaitable = close()
            if inspect.isawaitable(maybe_awaitable):
                await maybe_awaitable
        except BaseException as exc:  # noqa: BLE001
            return exc
        return None

    async def _close_resources_best_effort(self, *resources: Any) -> None:
        for resource in resources:
            try:
                await self._close_resource(resource)
            except Exception:
                pass

    def _heartbeat_interval_for_host(self, host: dict[str, Any]) -> float:
        remote_cfg = host.get("remote_worker")
        if isinstance(remote_cfg, dict):
            raw_interval = remote_cfg.get("heartbeat_interval_seconds")
            if raw_interval is not None and str(raw_interval).strip() != "":
                return float(raw_interval)
        return self._heartbeat_interval_seconds

    def _resolve_prepared_session(
        self,
        *,
        host: dict[str, Any],
        prepared: Any,
    ) -> tuple[Any, str]:
        if not isinstance(prepared, dict):
            raise TypeError("launcher.prepare_session must return dict")
        channel = prepared.get("channel")
        if channel is None:
            raise ValueError("launcher.prepare_session must return channel")
        interpreter = str(prepared.get("interpreter") or "").strip()
        if interpreter == "":
            interpreter = self._resolve_interpreter(host)
        return channel, interpreter

    def _resolve_interpreter(self, host: dict[str, Any]) -> str:
        for candidate in ("python3", "python"):
            resolved = self._probe_interpreter(dict(host), candidate)
            if inspect.isawaitable(resolved):
                raise TypeError("probe_interpreter must be synchronous")
            if isinstance(resolved, str) and resolved.strip() != "":
                return resolved.strip()
        raise RuntimeError("no usable remote Python interpreter found")

    def _unwrap_response(self, action: str, response: dict[str, Any]) -> Any:
        payload = dict(response or {})
        if not bool(payload.get("ok", False)):
            raise RuntimeError(
                str(payload.get("error") or payload.get("message") or f"{action} failed")
            )
        return payload.get("result")

    def _host_name(self, host_name_or_host: Any) -> str:
        if isinstance(host_name_or_host, dict):
            value = host_name_or_host.get("name")
        else:
            value = host_name_or_host
        normalized = str(value or "").strip()
        if normalized == "":
            raise ValueError("host name must not be empty")
        return normalized

    def _next_request_id(self) -> str:
        return f"req-{next(self._request_sequence)}"

    def _next_stream_id(self) -> str:
        return f"stream-{next(self._stream_sequence)}"

    def _default_probe_interpreter(
        self,
        _host: dict[str, Any],
        candidate: str,
    ) -> str | None:
        return candidate

    def _default_session_factory(self, *, channel: Any) -> WorkerSession:
        return WorkerSession(channel=channel)
