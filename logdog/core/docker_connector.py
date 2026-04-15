from __future__ import annotations

import asyncio
import importlib
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, AsyncIterator, cast


_IMPORT_MODULE = importlib.import_module
_STREAM_END = object()
_DEFAULT_STREAM_QUEUE_MAXSIZE = 1024


async def _run_in_daemon_thread(fn: Any) -> Any:
    to_thread = getattr(asyncio, "to_thread", None)
    if callable(to_thread) and getattr(to_thread, "__module__", "") != "asyncio.threads":
        return await to_thread(fn)

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
            result = fn()
        except Exception as exc:  # noqa: BLE001
            loop.call_soon_threadsafe(_set_error, exc)
            return
        loop.call_soon_threadsafe(_set_result, result)

    thread = threading.Thread(
        target=_worker,
        daemon=True,
        name="docker-to-thread",
    )
    thread.start()
    return await future


_TO_THREAD = _run_in_daemon_thread


@dataclass(slots=True)
class _PooledClient:
    kwargs: dict[str, Any]
    client: Any
    last_used_at: float


@dataclass(slots=True)
class _StreamFailure:
    error: BaseException


class _MappedStreamIterator:
    def __init__(self, source: Any, mapper: Any) -> None:
        self._source = source
        self._mapper = mapper
        self._buffer: list[dict[str, Any]] = []

    def __iter__(self) -> _MappedStreamIterator:
        return self

    def __next__(self) -> dict[str, Any]:
        while not self._buffer:
            self._buffer.extend(self._mapper(next(self._source)))
        return self._buffer.pop(0)

    def close(self) -> None:
        close_fn = getattr(self._source, "close", None)
        if callable(close_fn):
            close_fn()


class _BufferedLogStreamIterator:
    def __init__(self, source: Any) -> None:
        self._source = source
        self._buffer = ""
        self._records: list[dict[str, Any]] = []
        self._source_exhausted = False

    def __iter__(self) -> _BufferedLogStreamIterator:
        return self

    def __next__(self) -> dict[str, Any]:
        while not self._records:
            if self._source_exhausted:
                raise StopIteration
            self._read_next_chunk()
        return self._records.pop(0)

    def close(self) -> None:
        close_fn = getattr(self._source, "close", None)
        if callable(close_fn):
            close_fn()

    def _read_next_chunk(self) -> None:
        try:
            self._buffer += _coerce_text(next(self._source))
        except StopIteration:
            self._source_exhausted = True
            final_line = self._consume_final_remainder()
            if final_line is not None:
                self._records.append(_normalize_log_line(final_line))
            return

        while "\n" in self._buffer:
            line, self._buffer = self._buffer.split("\n", 1)
            self._records.append(_normalize_log_line(_rstrip_carriage_return(line)))

    def _consume_final_remainder(self) -> str | None:
        remainder = _rstrip_carriage_return(self._buffer)
        self._buffer = ""
        if remainder == "":
            return None
        return remainder


class _StreamHandle:
    def __init__(
        self,
        *,
        client: Any,
        operation: Any,
        to_thread: Any,
        loop: asyncio.AbstractEventLoop,
        queue_maxsize: int = _DEFAULT_STREAM_QUEUE_MAXSIZE,
    ) -> None:
        normalized_queue_maxsize = int(queue_maxsize)
        if normalized_queue_maxsize <= 0:
            raise ValueError("queue_maxsize must be > 0")
        self._client = client
        self._operation = operation
        self._to_thread = to_thread
        self._loop = loop
        self._queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=normalized_queue_maxsize)
        self._finished = asyncio.Event()
        self._close_lock = asyncio.Lock()
        self._resource_lock = threading.Lock()
        self._stop_requested = threading.Event()
        self._iterator: Any | None = None
        self._iterator_closed = False
        self._client_closed = False
        self._thread = threading.Thread(
            target=self._reader_loop,
            name="docker-stream-reader",
            daemon=True,
        )
        self._thread.start()

    def __aiter__(self) -> _StreamHandle:
        return self

    async def __anext__(self) -> dict[str, Any]:
        item = await self._queue.get()
        if item is _STREAM_END:
            raise StopAsyncIteration
        if isinstance(item, _StreamFailure):
            raise item.error
        return cast(dict[str, Any], item)

    async def aclose(self) -> None:
        async with self._close_lock:
            self._stop_requested.set()
            await self._to_thread(self._close_resources)
        if self._finished.is_set():
            return
        try:
            await asyncio.wait_for(self._finished.wait(), timeout=1.0)
        except asyncio.TimeoutError:
            self._finished.set()

    def _reader_loop(self) -> None:
        try:
            iterator = self._operation(self._client)
            self._iterator = iterator
            while not self._stop_requested.is_set():
                self._deliver(next(iterator))
        except StopIteration:
            pass
        except BaseException as exc:  # noqa: BLE001
            if not self._stop_requested.is_set():
                self._deliver(_StreamFailure(exc))
        finally:
            self._close_resources()
            self._deliver(_STREAM_END)
            self._loop.call_soon_threadsafe(self._finished.set)

    def _deliver(self, item: Any) -> None:
        if self._stop_requested.is_set():
            return

        def put_nowait() -> None:
            try:
                self._queue.put_nowait(item)
            except asyncio.QueueFull:
                # Keep memory bounded under burst load by evicting oldest item.
                try:
                    self._queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                try:
                    self._queue.put_nowait(item)
                except asyncio.QueueFull:
                    return

        try:
            self._loop.call_soon_threadsafe(put_nowait)
        except RuntimeError:
            return

    def _close_resources(self) -> None:
        with self._resource_lock:
            iterator = self._iterator
            if iterator is not None and not self._iterator_closed:
                close_fn = getattr(iterator, "close", None)
                if callable(close_fn):
                    try:
                        close_fn()
                    except Exception:  # noqa: BLE001
                        pass
                self._iterator_closed = True
            if not self._client_closed:
                close_fn = getattr(self._client, "close", None)
                if callable(close_fn):
                    try:
                        close_fn()
                    except Exception:  # noqa: BLE001
                        pass
                self._client_closed = True


class DockerClientPool:
    def __init__(
        self,
        *,
        module_loader: Any | None = None,
        to_thread: Any | None = None,
        monotonic_fn: Any | None = None,
        max_clients: int | None = None,
        max_idle_seconds: float | None = None,
        stream_queue_maxsize: int = _DEFAULT_STREAM_QUEUE_MAXSIZE,
    ) -> None:
        if max_clients is not None and int(max_clients) <= 0:
            raise ValueError("max_clients must be > 0")
        if max_idle_seconds is not None and float(max_idle_seconds) <= 0:
            raise ValueError("max_idle_seconds must be > 0")
        if int(stream_queue_maxsize) <= 0:
            raise ValueError("stream_queue_maxsize must be > 0")
        self._module_loader = module_loader or _load_docker_module
        self._to_thread = to_thread or _TO_THREAD
        self._monotonic_fn = monotonic_fn or time.monotonic
        self._max_clients = int(max_clients) if max_clients is not None else None
        self._max_idle_seconds = (
            float(max_idle_seconds) if max_idle_seconds is not None else None
        )
        self._stream_queue_maxsize = int(stream_queue_maxsize)
        self._clients: dict[str, _PooledClient] = {}
        self._active_streams: dict[int, _StreamHandle] = {}
        self._lock = asyncio.Lock()

    async def connect_host(self, host: dict[str, Any]) -> dict[str, Any]:
        return await self._run(host, _connect_operation())

    async def list_containers_for_host(
        self, host: dict[str, Any]
    ) -> list[dict[str, Any]]:
        return await self._run(host, _list_containers_operation())

    async def fetch_container_stats(
        self, host: dict[str, Any], container: dict[str, Any]
    ) -> dict[str, Any]:
        container_id = _container_id(container)
        return await self._run(host, _fetch_stats_operation(container_id))

    async def query_container_logs(
        self,
        host: dict[str, Any],
        container: dict[str, Any],
        *,
        since: Any | None = None,
        until: Any | None = None,
        max_lines: int = 2000,
    ) -> list[dict[str, Any]]:
        container_id = _container_id(container)
        return await self._run(
            host,
            _query_logs_operation(
                container_id, since=since, until=until, max_lines=max_lines
            ),
        )

    async def stream_container_logs(
        self,
        host: dict[str, Any],
        container: dict[str, Any],
        *,
        since: Any | None = None,
        tail: int | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        container_id = _container_id(container)
        stream = await self._open_stream(
            host,
            _stream_logs_operation(container_id, since=since, tail=tail),
        )
        try:
            async for item in stream:
                yield item
        finally:
            await stream.aclose()
            await self._discard_stream(stream)

    async def stream_docker_events(
        self,
        host: dict[str, Any],
        *,
        filters: dict[str, Any] | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        stream = await self._open_stream(
            host, _stream_events_operation(filters=filters)
        )
        try:
            async for item in stream:
                yield item
        finally:
            await stream.aclose()
            await self._discard_stream(stream)

    async def restart_container_for_host(
        self,
        host: dict[str, Any],
        container: dict[str, Any],
        *,
        timeout: int = 10,
    ) -> dict[str, Any]:
        container_id = _container_id(container)
        return await self._run(
            host, _restart_container_operation(container_id, timeout=timeout)
        )

    async def exec_container_for_host(
        self,
        host: dict[str, Any],
        container: dict[str, Any],
        *,
        command: str,
        max_output_chars: int = 2000,
    ) -> dict[str, Any]:
        container_id = _container_id(container)
        return await self._run(
            host,
            _exec_container_operation(
                container_id, command=command, max_output_chars=max_output_chars
            ),
        )

    async def close_all(self) -> None:
        async with self._lock:
            streams = list(self._active_streams.values())
            self._active_streams = {}
            entries = list(self._clients.values())
            self._clients = {}
        for stream in streams:
            await stream.aclose()
        for entry in entries:
            await self._close_client(entry.client)

    async def _run(self, host: dict[str, Any], operation: Any) -> Any:
        client = await self._get_client(host)
        return await self._to_thread(lambda: operation(client))

    async def _open_stream(self, host: dict[str, Any], operation: Any) -> _StreamHandle:
        stream = await _open_stream_handle(
            host,
            operation,
            module_loader=self._module_loader,
            to_thread=self._to_thread,
            queue_maxsize=self._stream_queue_maxsize,
        )
        async with self._lock:
            self._active_streams[id(stream)] = stream
        return stream

    async def _discard_stream(self, stream: _StreamHandle) -> None:
        async with self._lock:
            self._active_streams.pop(id(stream), None)

    async def _get_client(self, host: dict[str, Any]) -> Any:
        key = _client_key(host)
        kwargs = _docker_client_kwargs(host)
        now = self._now()

        async with self._lock:
            entry = await self._get_or_create_client_locked(key, kwargs, now)
            entry.last_used_at = now
            return entry.client

    async def _get_or_create_client_locked(
        self,
        key: str,
        kwargs: dict[str, Any],
        now: float,
    ) -> _PooledClient:
        await self._evict_idle_locked(now)
        entry = self._clients.get(key)
        if entry is not None and entry.kwargs == kwargs:
            if await self._is_client_healthy(entry.client):
                return entry
            await self._close_client(entry.client)
            self._clients.pop(key, None)
            entry = None

        if entry is not None:
            await self._close_client(entry.client)
            self._clients.pop(key, None)

        await self._enforce_capacity_locked()

        docker_module = self._module_loader()
        client = await self._to_thread(lambda: _make_docker_client(docker_module, kwargs))
        entry = _PooledClient(kwargs=kwargs, client=client, last_used_at=now)
        self._clients[key] = entry
        return entry

    async def _close_client(self, client: Any) -> None:
        close_fn = getattr(client, "close", None)
        if callable(close_fn):
            await self._to_thread(close_fn)

    async def _is_client_healthy(self, client: Any) -> bool:
        def check() -> bool:
            ping_fn = getattr(client, "ping", None)
            if callable(ping_fn):
                result = ping_fn()
                if isinstance(result, bool):
                    return result
                return True

            version_fn = getattr(client, "version", None)
            if callable(version_fn):
                version_fn()
            return True

        try:
            return bool(await self._to_thread(check))
        except Exception:  # noqa: BLE001
            return False

    def _now(self) -> float:
        return float(self._monotonic_fn())

    async def _evict_idle_locked(self, now: float) -> None:
        max_idle = self._max_idle_seconds
        if max_idle is None:
            return
        stale_keys = [
            key
            for key, entry in self._clients.items()
            if (now - entry.last_used_at) > max_idle
        ]
        for key in stale_keys:
            entry = self._clients.pop(key, None)
            if entry is None:
                continue
            await self._close_client(entry.client)

    async def _enforce_capacity_locked(self) -> None:
        max_clients = self._max_clients
        if max_clients is None:
            return
        while len(self._clients) >= max_clients:
            evict_key, evict_entry = min(
                self._clients.items(),
                key=lambda item: item[1].last_used_at,
            )
            self._clients.pop(evict_key, None)
            await self._close_client(evict_entry.client)


async def connect_docker_host(host: dict[str, Any]) -> dict[str, Any]:
    return await _run_with_client(host, _connect_operation())


async def list_containers_for_host(host: dict[str, Any]) -> list[dict[str, Any]]:
    return await _run_with_client(host, _list_containers_operation())


async def fetch_container_stats(
    host: dict[str, Any], container: dict[str, Any]
) -> dict[str, Any]:
    container_id = _container_id(container)
    return await _run_with_client(host, _fetch_stats_operation(container_id))


async def query_container_logs(
    host: dict[str, Any],
    container: dict[str, Any],
    *,
    since: Any | None = None,
    until: Any | None = None,
    max_lines: int = 2000,
) -> list[dict[str, Any]]:
    container_id = _container_id(container)
    return await _run_with_client(
        host,
        _query_logs_operation(
            container_id, since=since, until=until, max_lines=max_lines
        ),
    )


async def stream_container_logs(
    host: dict[str, Any],
    container: dict[str, Any],
    *,
    since: Any | None = None,
    tail: int | None = None,
) -> AsyncIterator[dict[str, Any]]:
    container_id = _container_id(container)
    stream = await _open_stream_handle(
        host,
        _stream_logs_operation(container_id, since=since, tail=tail),
        module_loader=_load_docker_module,
        to_thread=_TO_THREAD,
    )
    try:
        async for item in stream:
            yield item
    finally:
        await stream.aclose()


async def stream_docker_events(
    host: dict[str, Any],
    *,
    filters: dict[str, Any] | None = None,
) -> AsyncIterator[dict[str, Any]]:
    stream = await _open_stream_handle(
        host,
        _stream_events_operation(filters=filters),
        module_loader=_load_docker_module,
        to_thread=_TO_THREAD,
    )
    try:
        async for item in stream:
            yield item
    finally:
        await stream.aclose()


async def restart_container_for_host(
    host: dict[str, Any],
    container: dict[str, Any],
    *,
    timeout: int = 10,
) -> dict[str, Any]:
    container_id = _container_id(container)
    return await _run_with_client(
        host, _restart_container_operation(container_id, timeout=timeout)
    )


async def exec_container_for_host(
    host: dict[str, Any],
    container: dict[str, Any],
    *,
    command: str,
    max_output_chars: int = 2000,
) -> dict[str, Any]:
    container_id = _container_id(container)
    return await _run_with_client(
        host, _exec_container_operation(container_id, command=command, max_output_chars=max_output_chars)
    )


async def _run_with_client(host: dict[str, Any], operation) -> Any:
    docker_module = _load_docker_module()
    kwargs = _docker_client_kwargs(host)

    def task() -> Any:
        client = _make_docker_client(docker_module, kwargs)
        try:
            return operation(client)
        finally:
            close_fn = getattr(client, "close", None)
            if callable(close_fn):
                close_fn()

    return await _TO_THREAD(task)


async def _open_stream_handle(
    host: dict[str, Any],
    operation: Any,
    *,
    module_loader: Any,
    to_thread: Any,
    queue_maxsize: int = _DEFAULT_STREAM_QUEUE_MAXSIZE,
) -> _StreamHandle:
    docker_module = module_loader()
    kwargs = _docker_client_kwargs(host)
    client = await to_thread(lambda: _make_docker_client(docker_module, kwargs))
    return _StreamHandle(
        client=client,
        operation=operation,
        to_thread=to_thread,
        loop=asyncio.get_running_loop(),
        queue_maxsize=queue_maxsize,
    )


def _load_docker_module() -> Any:
    try:
        return _IMPORT_MODULE("docker")
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("docker SDK is not available") from exc


_DEFAULT_SSH_TIMEOUT = 15  # seconds


def _docker_client_kwargs(host: dict[str, Any]) -> dict[str, Any]:
    url = str(host.get("url") or "").strip()
    if url == "":
        raise ValueError("host url is required")
    kwargs: dict[str, Any] = {"base_url": url}
    if url.startswith("ssh://"):
        kwargs["use_ssh_client"] = True
        kwargs["timeout"] = _DEFAULT_SSH_TIMEOUT
        ssh_key = str(host.get("ssh_key") or "").strip()
        if ssh_key:
            # Private key: not forwarded to DockerClient directly; used by
            # _make_docker_client to inject into paramiko's ssh_params.
            kwargs["_ssh_key"] = ssh_key
    return kwargs


_SSH_CLIENT_CREATE_LOCK = threading.Lock()


def _make_docker_client(docker_module: Any, kwargs: dict[str, Any]) -> Any:
    """Create a DockerClient, injecting ssh_key into the paramiko connection if configured.

    kwargs may contain a private ``_ssh_key`` entry (not accepted by DockerClient).
    We strip all underscore-prefixed private entries before forwarding kwargs to
    DockerClient so callers can keep the full kwargs (including ``_ssh_key``) for
    pool-entry equality comparisons without mutating the stored dict.
    """
    ssh_key = str(kwargs.get("_ssh_key") or "").strip()
    client_kwargs = {k: v for k, v in kwargs.items() if not k.startswith("_")}

    if not ssh_key:
        return docker_module.DockerClient(**client_kwargs)

    # Inject key_filename into paramiko's SSHHTTPAdapter._create_paramiko_client.
    # The adapter builds ssh_params in that method; we wrap it to append
    # key_filename immediately after the original runs.  A module-level lock
    # serialises concurrent client creations so the temporary patch is safe.
    try:
        ssh_adapter_cls = _IMPORT_MODULE("docker.transport.sshconn").SSHHTTPAdapter
    except Exception:  # noqa: BLE001
        # docker SDK not available or pre-7 layout — create without key injection.
        return docker_module.DockerClient(**client_kwargs)

    original_create = ssh_adapter_cls._create_paramiko_client

    def _patched_create(self: Any, base_url: str) -> None:
        original_create(self, base_url)
        # Unconditionally override key_filename; SSH config entries are
        # still respected for proxy, hostname, port, and username.
        self.ssh_params["key_filename"] = ssh_key

    with _SSH_CLIENT_CREATE_LOCK:
        ssh_adapter_cls._create_paramiko_client = _patched_create
        try:
            return docker_module.DockerClient(**client_kwargs)
        finally:
            ssh_adapter_cls._create_paramiko_client = original_create


def _client_key(host: dict[str, Any]) -> str:
    name = str(host.get("name") or "").strip()
    if name != "":
        return name
    return str(host.get("url") or "").strip()


def _container_id(container: dict[str, Any]) -> str:
    container_id = str(
        container.get("id") or container.get("container_id") or ""
    ).strip()
    if container_id == "":
        raise ValueError("container id is required")
    return container_id


def _connect_operation():
    def operation(client: Any) -> dict[str, Any]:
        version = client.version() or {}
        if not isinstance(version, dict):
            version = {}
        return {
            "server_version": str(version.get("Version") or ""),
            "api_version": str(version.get("ApiVersion") or ""),
        }

    return operation


def _list_containers_operation():
    def operation(client: Any) -> list[dict[str, Any]]:
        containers = client.containers.list(all=True)
        result: list[dict[str, Any]] = []
        for container in containers:
            container_id = str(getattr(container, "id", "") or "").strip()
            if container_id == "":
                continue
            attrs = getattr(container, "attrs", {}) or {}
            restart_count = _extract_restart_count(attrs)
            status = str(
                getattr(container, "status", "")
                or _extract_state_status(attrs)
                or "unknown"
            )
            container_name = str(
                getattr(container, "name", "")
                or _extract_container_name(attrs)
                or container_id
            )
            result.append(
                {
                    "id": container_id,
                    "name": container_name,
                    "status": status,
                    "restart_count": restart_count,
                }
            )
        return result

    return operation


def _fetch_stats_operation(container_id: str):
    def operation(client: Any) -> dict[str, Any]:
        container_obj = client.containers.get(container_id)
        stats = container_obj.stats(stream=False)
        if not isinstance(stats, dict):
            raise TypeError("docker stats payload must be dict")
        return stats

    return operation


def _coerce_docker_time(value: Any) -> datetime | int | float | None:
    """Convert string timestamps to datetime for Docker SDK compatibility."""
    if value is None:
        return None
    if isinstance(value, (int, float, datetime)):
        return value
    s = str(value).strip()
    if not s:
        return None
    # Unix timestamp as string
    try:
        return float(s)
    except ValueError:
        pass
    # ISO 8601 string
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        pass
    return None


def _query_logs_operation(
    container_id: str,
    *,
    since: Any | None = None,
    until: Any | None = None,
    max_lines: int = 2000,
):
    tail = _require_positive_int(max_lines, field_name="max_lines")

    def operation(client: Any) -> list[dict[str, Any]]:
        container_obj = client.containers.get(container_id)
        kwargs: dict[str, Any] = {
            "stdout": True,
            "stderr": True,
            "timestamps": True,
            "tail": tail,
        }
        coerced_since = _coerce_docker_time(since)
        coerced_until = _coerce_docker_time(until)
        if coerced_since is not None:
            kwargs["since"] = coerced_since
        if coerced_until is not None:
            kwargs["until"] = coerced_until
        return _normalize_log_entries(container_obj.logs(**kwargs))

    return operation


def _stream_logs_operation(
    container_id: str,
    *,
    since: Any | None = None,
    tail: int | None = None,
):
    validated_tail = None
    if tail is not None:
        validated_tail = _require_positive_int(tail, field_name="tail")

    def operation(client: Any):
        container_obj = client.containers.get(container_id)
        kwargs: dict[str, Any] = {
            "stream": True,
            "follow": True,
            "stdout": True,
            "stderr": True,
            "timestamps": True,
        }
        coerced_since = _coerce_docker_time(since)
        if coerced_since is not None:
            kwargs["since"] = coerced_since
        if validated_tail is not None:
            kwargs["tail"] = validated_tail
        return _BufferedLogStreamIterator(container_obj.logs(**kwargs))

    return operation


def _stream_events_operation(*, filters: dict[str, Any] | None = None):
    def operation(client: Any):
        kwargs: dict[str, Any] = {"decode": True}
        if filters is not None:
            kwargs["filters"] = dict(filters)
        return _MappedStreamIterator(
            client.events(**kwargs),
            lambda item: [_normalize_event(item)],
        )

    return operation


def _restart_container_operation(container_id: str, *, timeout: int = 10):
    restart_timeout = _require_positive_int(timeout, field_name="timeout")

    def operation(client: Any) -> dict[str, Any]:
        container_obj = client.containers.get(container_id)
        container_obj.restart(timeout=restart_timeout)
        container_name = str(getattr(container_obj, "name", "") or container_id)
        return {
            "container_id": container_id,
            "container_name": container_name,
            "timeout": restart_timeout,
            "restarted": True,
        }

    return operation


def _exec_container_operation(
    container_id: str, *, command: str, max_output_chars: int = 2000
):
    """Return an operation that runs a command inside a container via exec_run."""
    if not str(command or "").strip():
        raise ValueError("command must not be empty")
    _max = int(max_output_chars)

    def operation(client: Any) -> dict[str, Any]:
        container_obj = client.containers.get(container_id)
        container_name = str(getattr(container_obj, "name", "") or container_id)
        exit_code, output = container_obj.exec_run(command, demux=True)
        exit_code_int = int(exit_code) if exit_code is not None else -1

        # output is a (stdout_bytes, stderr_bytes) tuple when demux=True.
        stdout_bytes, stderr_bytes = (
            output if isinstance(output, tuple) else (output, None)
        )

        def _decode(b: Any) -> str:
            if b is None:
                return ""
            if isinstance(b, bytes):
                return b.decode("utf-8", errors="replace")
            return str(b)

        stdout_text = _decode(stdout_bytes)
        stderr_text = _decode(stderr_bytes)

        # Truncate combined output to stay within caller-defined char limit.
        combined = stdout_text
        if stderr_text:
            combined = combined + ("" if not combined else "\n") + stderr_text
        if len(combined) > _max:
            combined = combined[:_max] + f"\n[truncated at {_max} chars]"

        return {
            "container_id": container_id,
            "container_name": container_name,
            "command": command,
            "exit_code": exit_code_int,
            "output": combined,
        }

    return operation


def _normalize_log_entries(payload: Any) -> list[dict[str, Any]]:
    text = _coerce_text(payload)
    return [_normalize_log_line(line) for line in text.splitlines()]


def _normalize_log_line(line: str) -> dict[str, Any]:
    timestamp = ""
    content = line
    maybe_timestamp, separator, remainder = line.partition(" ")
    if separator != "" and _looks_like_log_timestamp(maybe_timestamp):
        timestamp = maybe_timestamp
        content = remainder
    return {"timestamp": timestamp, "line": content}


def _rstrip_carriage_return(value: str) -> str:
    if value.endswith("\r"):
        return value[:-1]
    return value


def _normalize_event(raw_event: Any) -> dict[str, Any]:
    payload = raw_event if isinstance(raw_event, dict) else {}
    actor = payload.get("Actor") or {}
    if not isinstance(actor, dict):
        actor = {}
    actor_attributes = actor.get("Attributes") or {}
    if not isinstance(actor_attributes, dict):
        actor_attributes = {}
    container_id = str(actor.get("ID") or payload.get("id") or "").strip()
    container_name = str(
        actor_attributes.get("name") or payload.get("from") or container_id
    )
    return {
        "time": payload.get("time", payload.get("timeNano")),
        "type": str(payload.get("Type") or ""),
        "action": str(payload.get("Action") or payload.get("status") or ""),
        "container_id": container_id,
        "container_name": container_name,
        "actor_attributes": dict(actor_attributes),
    }


def _coerce_text(payload: Any) -> str:
    if payload is None:
        return ""
    if isinstance(payload, bytes):
        return payload.decode("utf-8", errors="replace")
    return str(payload)


def _looks_like_log_timestamp(value: str) -> bool:
    return len(value) >= 20 and value[4:5] == "-" and value[7:8] == "-" and "T" in value


def _next_stream_item(iterator: Any) -> Any:
    try:
        return next(iterator)
    except StopIteration:
        return _STREAM_END


def _require_positive_int(value: Any, *, field_name: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise ValueError(f"{field_name} must be > 0")
    return parsed


def _extract_restart_count(attrs: dict[str, Any]) -> int:
    raw = attrs.get("RestartCount")
    if raw is None:
        raw = (attrs.get("State") or {}).get("RestartCount")
    if raw is None:
        return 0
    try:
        return int(raw)
    except (TypeError, ValueError):
        return 0


def _extract_state_status(attrs: dict[str, Any]) -> str:
    status = (attrs.get("State") or {}).get("Status")
    return str(status or "")


def _extract_container_name(attrs: dict[str, Any]) -> str:
    name = str(attrs.get("Name") or "")
    if name.startswith("/"):
        name = name[1:]
    return name
