from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Any, cast

import pytest

import logdog.core.docker_connector as docker_connector


class _FakeClock:
    def __init__(self, start: float = 0.0) -> None:
        self.value = float(start)

    def now(self) -> float:
        return self.value

    def set(self, value: float) -> None:
        self.value = float(value)


class _FakeClosableIterator:
    def __init__(
        self,
        items: list[Any],
        *,
        label: str,
        call_trace: list[tuple[str, Any]],
        lifecycle_trace: list[tuple[str, str]],
        owner_label: str,
        block_after_first_item: bool = False,
    ) -> None:
        self._items = list(items)
        self._index = 0
        self._label = label
        self._call_trace = call_trace
        self._lifecycle_trace = lifecycle_trace
        self._owner_label = owner_label
        self._block_after_first_item = block_after_first_item
        self._release_event = threading.Event()
        if not block_after_first_item:
            self._release_event.set()
        self.closed = False
        self.close_calls = 0

    def __iter__(self):
        return self

    def __next__(self) -> Any:
        if self._block_after_first_item and self._index >= 1 and not self.closed:
            self._release_event.wait(timeout=5.0)
        if self._index >= len(self._items):
            raise StopIteration
        if self.closed:
            raise StopIteration
        item = self._items[self._index]
        self._index += 1
        return item

    def close(self) -> None:
        self._call_trace.append((f"{self._label}:close", None))
        self._lifecycle_trace.append((self._owner_label, f"{self._label}:close"))
        self.close_calls += 1
        self.closed = True
        self._release_event.set()

    def release(self) -> None:
        self._release_event.set()


class _FakeStreamContainer:
    def __init__(
        self,
        *,
        container_id: str,
        name: str,
        history_payload: bytes,
        stream_payloads: list[bytes],
        call_trace: list[tuple[str, Any]],
        lifecycle_trace: list[tuple[str, str]],
        owner_label: str,
        block_stream_after_first_item: bool,
    ) -> None:
        self.id = container_id
        self.name = name
        self._history_payload = history_payload
        self._stream_payloads = list(stream_payloads)
        self._call_trace = call_trace
        self._lifecycle_trace = lifecycle_trace
        self._owner_label = owner_label
        self._block_stream_after_first_item = block_stream_after_first_item
        self.log_iterators: list[_FakeClosableIterator] = []

    def logs(self, **kwargs: Any):
        self._call_trace.append(("logs", dict(kwargs)))
        if kwargs.get("stream"):
            iterator = _FakeClosableIterator(
                self._stream_payloads,
                label="logs_iterator",
                call_trace=self._call_trace,
                lifecycle_trace=self._lifecycle_trace,
                owner_label=self._owner_label,
                block_after_first_item=self._block_stream_after_first_item,
            )
            self.log_iterators.append(iterator)
            return iterator
        return self._history_payload

    def restart(self, *, timeout: int) -> None:
        self._call_trace.append(("restart", timeout))


class _FakeContainersApi:
    def __init__(
        self, containers: list[_FakeStreamContainer], call_trace: list[tuple[str, Any]]
    ) -> None:
        self._containers = {container.id: container for container in containers}
        self._call_trace = call_trace

    def get(self, container_id: str) -> _FakeStreamContainer:
        self._call_trace.append(("get", container_id))
        return self._containers[container_id]


class _FakeStreamsDockerClient:
    created: list["_FakeStreamsDockerClient"] = []
    lifecycle_trace: list[tuple[str, str]] = []
    history_payload: bytes = (
        b"2026-04-11T10:00:01.000000000Z first line\nplain second line\n"
    )
    stream_payloads: list[bytes] = [
        b"2026-04-11T10:00:01.000000000Z live one\n",
        b"2026-04-11T10:00:02.000000000Z live two\nplain live three\n",
    ]
    event_payloads: list[dict[str, Any]] = [
        {
            "time": 1712839200,
            "Type": "container",
            "Action": "start",
            "Actor": {
                "ID": "c1",
                "Attributes": {"name": "svc-1", "image": "demo:1"},
            },
        }
    ]
    block_log_stream_after_first_item: bool = False
    block_event_stream_after_first_item: bool = False

    def __init__(self, *, base_url: str, use_ssh_client: bool = False) -> None:
        self.base_url = base_url
        self.use_ssh_client = use_ssh_client
        self.label = f"client-{len(type(self).created)}"
        self.closed = False
        self.close_calls = 0
        self.call_trace: list[tuple[str, Any]] = []
        self.event_iterators: list[_FakeClosableIterator] = []
        self.containers = _FakeContainersApi(
            [
                _FakeStreamContainer(
                    container_id="c1",
                    name="svc-1",
                    history_payload=type(self).history_payload,
                    stream_payloads=type(self).stream_payloads,
                    call_trace=self.call_trace,
                    lifecycle_trace=type(self).lifecycle_trace,
                    owner_label=self.label,
                    block_stream_after_first_item=type(
                        self
                    ).block_log_stream_after_first_item,
                )
            ],
            self.call_trace,
        )
        type(self).created.append(self)

    def version(self) -> dict[str, str]:
        self.call_trace.append(("version", None))
        return {"Version": "24.0.7", "ApiVersion": "1.43"}

    def events(self, *, decode: bool = False, filters: dict[str, Any] | None = None):
        self.call_trace.append(("events", {"decode": decode, "filters": filters}))
        iterator = _FakeClosableIterator(
            type(self).event_payloads,
            label="events_iterator",
            call_trace=self.call_trace,
            lifecycle_trace=type(self).lifecycle_trace,
            owner_label=self.label,
            block_after_first_item=type(self).block_event_stream_after_first_item,
        )
        self.event_iterators.append(iterator)
        return iterator

    def close(self) -> None:
        self.call_trace.append(("close", None))
        type(self).lifecycle_trace.append((self.label, "close"))
        self.close_calls += 1
        self.closed = True


class _FakeStreamsDockerModule:
    DockerClient = _FakeStreamsDockerClient


def _reset_fake_client_payloads() -> None:
    _FakeStreamsDockerClient.created = []
    _FakeStreamsDockerClient.lifecycle_trace = []
    _FakeStreamsDockerClient.history_payload = (
        b"2026-04-11T10:00:01.000000000Z first line\nplain second line\n"
    )
    _FakeStreamsDockerClient.stream_payloads = [
        b"2026-04-11T10:00:01.000000000Z live one\n",
        b"2026-04-11T10:00:02.000000000Z live two\nplain live three\n",
    ]
    _FakeStreamsDockerClient.event_payloads = [
        {
            "time": 1712839200,
            "Type": "container",
            "Action": "start",
            "Actor": {
                "ID": "c1",
                "Attributes": {"name": "svc-1", "image": "demo:1"},
            },
        }
    ]
    _FakeStreamsDockerClient.block_log_stream_after_first_item = False
    _FakeStreamsDockerClient.block_event_stream_after_first_item = False


def _block_stream_iterators(*, logs: bool = False, events: bool = False) -> None:
    _FakeStreamsDockerClient.block_log_stream_after_first_item = logs
    _FakeStreamsDockerClient.block_event_stream_after_first_item = events


def _host() -> dict[str, str]:
    return {"name": "local", "url": "unix:///var/run/docker.sock"}


def _other_host(name: str) -> dict[str, str]:
    return {"name": name, "url": f"unix:///var/run/{name}.sock"}


async def _run_sync(fn):
    return fn()


async def _collect(async_iterable) -> list[Any]:
    return [item async for item in async_iterable]


@pytest.mark.asyncio
async def test_query_container_logs_returns_normalized_history_and_closes_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _reset_fake_client_payloads()
    monkeypatch.setattr(
        docker_connector, "_IMPORT_MODULE", lambda _name: _FakeStreamsDockerModule()
    )
    monkeypatch.setattr(docker_connector, "_TO_THREAD", lambda fn: _run_sync(fn))

    out = await docker_connector.query_container_logs(
        _host(),
        {"id": "c1"},
        since="2026-04-11T10:00:00Z",
        until="2026-04-11T10:05:00Z",
        max_lines=2,
    )

    assert out == [
        {"timestamp": "2026-04-11T10:00:01.000000000Z", "line": "first line"},
        {"timestamp": "", "line": "plain second line"},
    ]
    assert _FakeStreamsDockerClient.created[0].call_trace == [
        ("get", "c1"),
        (
            "logs",
            {
                "stdout": True,
                "stderr": True,
                "timestamps": True,
                "tail": 2,
                "since": datetime(2026, 4, 11, 10, 0, 0, tzinfo=timezone.utc),
                "until": datetime(2026, 4, 11, 10, 5, 0, tzinfo=timezone.utc),
            },
        ),
        ("close", None),
    ]
    assert _FakeStreamsDockerClient.created[0].closed is True


@pytest.mark.asyncio
async def test_stream_container_logs_yields_normalized_items_in_order_and_closes_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _reset_fake_client_payloads()
    monkeypatch.setattr(
        docker_connector, "_IMPORT_MODULE", lambda _name: _FakeStreamsDockerModule()
    )
    monkeypatch.setattr(docker_connector, "_TO_THREAD", lambda fn: _run_sync(fn))

    out = await _collect(
        docker_connector.stream_container_logs(
            _host(),
            {"id": "c1"},
            since="cursor-1",
            tail=5,
        )
    )

    assert out == [
        {"timestamp": "2026-04-11T10:00:01.000000000Z", "line": "live one"},
        {"timestamp": "2026-04-11T10:00:02.000000000Z", "line": "live two"},
        {"timestamp": "", "line": "plain live three"},
    ]
    assert _FakeStreamsDockerClient.created[0].call_trace == [
        ("get", "c1"),
        (
            "logs",
            {
                "stream": True,
                "follow": True,
                "stdout": True,
                "stderr": True,
                "timestamps": True,
                "since": "cursor-1",
                "tail": 5,
            },
        ),
        ("logs_iterator:close", None),
        ("close", None),
    ]
    assert _FakeStreamsDockerClient.created[0].closed is True


@pytest.mark.asyncio
async def test_stream_container_logs_reassembles_split_timestamped_lines(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _reset_fake_client_payloads()
    _FakeStreamsDockerClient.stream_payloads = [
        b"2026-04-11T10:00:01.000000000Z split ",
        b"line\n2026-04-11T10:00:02.000000000Z whole line\n",
    ]
    monkeypatch.setattr(
        docker_connector, "_IMPORT_MODULE", lambda _name: _FakeStreamsDockerModule()
    )
    monkeypatch.setattr(docker_connector, "_TO_THREAD", lambda fn: _run_sync(fn))

    out = await _collect(
        docker_connector.stream_container_logs(
            _host(),
            {"id": "c1"},
        )
    )

    assert out == [
        {"timestamp": "2026-04-11T10:00:01.000000000Z", "line": "split line"},
        {"timestamp": "2026-04-11T10:00:02.000000000Z", "line": "whole line"},
    ]


@pytest.mark.asyncio
async def test_stream_container_logs_flushes_final_trailing_line_without_newline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _reset_fake_client_payloads()
    _FakeStreamsDockerClient.stream_payloads = [
        b"2026-04-11T10:00:01.000000000Z complete line\n",
        b"2026-04-11T10:00:02.000000000Z trailing remainder",
    ]
    monkeypatch.setattr(
        docker_connector, "_IMPORT_MODULE", lambda _name: _FakeStreamsDockerModule()
    )
    monkeypatch.setattr(docker_connector, "_TO_THREAD", lambda fn: _run_sync(fn))

    out = await _collect(
        docker_connector.stream_container_logs(
            _host(),
            {"id": "c1"},
        )
    )

    assert out == [
        {"timestamp": "2026-04-11T10:00:01.000000000Z", "line": "complete line"},
        {
            "timestamp": "2026-04-11T10:00:02.000000000Z",
            "line": "trailing remainder",
        },
    ]


@pytest.mark.asyncio
async def test_stream_docker_events_normalizes_payload_and_closes_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _reset_fake_client_payloads()
    monkeypatch.setattr(
        docker_connector, "_IMPORT_MODULE", lambda _name: _FakeStreamsDockerModule()
    )
    monkeypatch.setattr(docker_connector, "_TO_THREAD", lambda fn: _run_sync(fn))

    out = await _collect(
        docker_connector.stream_docker_events(
            _host(),
            filters={"type": ["container"]},
        )
    )

    assert out == [
        {
            "time": 1712839200,
            "type": "container",
            "action": "start",
            "container_id": "c1",
            "container_name": "svc-1",
            "actor_attributes": {"name": "svc-1", "image": "demo:1"},
        }
    ]
    assert _FakeStreamsDockerClient.created[0].call_trace == [
        ("events", {"decode": True, "filters": {"type": ["container"]}}),
        ("events_iterator:close", None),
        ("close", None),
    ]
    assert _FakeStreamsDockerClient.created[0].closed is True


@pytest.mark.asyncio
async def test_stream_handle_uses_bounded_internal_queue() -> None:
    _reset_fake_client_payloads()
    stream = await docker_connector._open_stream_handle(
        _host(),
        docker_connector._stream_events_operation(),
        module_loader=lambda: _FakeStreamsDockerModule(),
        to_thread=_run_sync,
    )
    try:
        queue = getattr(stream, "_queue")
        assert getattr(queue, "maxsize", 0) > 0
    finally:
        await stream.aclose()


@pytest.mark.asyncio
async def test_docker_client_pool_query_logs_and_restart_reuse_client_and_preserve_call_order() -> (
    None
):
    _reset_fake_client_payloads()
    pool = docker_connector.DockerClientPool(
        module_loader=lambda: _FakeStreamsDockerModule(),
        to_thread=_run_sync,
    )

    logs = await pool.query_container_logs(_host(), {"id": "c1"}, max_lines=1)
    restarted = await pool.restart_container_for_host(
        _host(), {"container_id": "c1"}, timeout=7
    )

    assert logs == [
        {"timestamp": "2026-04-11T10:00:01.000000000Z", "line": "first line"},
        {"timestamp": "", "line": "plain second line"},
    ]
    assert restarted == {
        "container_id": "c1",
        "container_name": "svc-1",
        "timeout": 7,
        "restarted": True,
    }
    assert len(_FakeStreamsDockerClient.created) == 1
    assert _FakeStreamsDockerClient.created[0].call_trace == [
        ("get", "c1"),
        (
            "logs",
            {
                "stdout": True,
                "stderr": True,
                "timestamps": True,
                "tail": 1,
            },
        ),
        ("version", None),
        ("get", "c1"),
        ("restart", 7),
    ]
    assert _FakeStreamsDockerClient.created[0].closed is False

    await pool.close_all()

    assert _FakeStreamsDockerClient.created[0].closed is True
    assert _FakeStreamsDockerClient.created[0].close_calls == 1


@pytest.mark.asyncio
async def test_docker_client_pool_stream_uses_dedicated_client_instead_of_pool_client() -> (
    None
):
    _reset_fake_client_payloads()
    _block_stream_iterators(logs=True)
    pool = docker_connector.DockerClientPool(
        module_loader=lambda: _FakeStreamsDockerModule(),
        to_thread=_run_sync,
        max_clients=1,
    )

    log_stream = pool.stream_container_logs(_host(), {"id": "c1"}, tail=1)
    first_log = await anext(log_stream)
    short_op = await pool.connect_host(_host())

    assert first_log == {
        "timestamp": "2026-04-11T10:00:01.000000000Z",
        "line": "live one",
    }
    assert short_op == {"server_version": "24.0.7", "api_version": "1.43"}
    assert len(_FakeStreamsDockerClient.created) == 2
    assert _FakeStreamsDockerClient.created[0].closed is False
    assert _FakeStreamsDockerClient.created[1].closed is False
    assert _FakeStreamsDockerClient.created[1].call_trace == [("version", None)]

    await cast(Any, log_stream).aclose()
    await pool.close_all()


@pytest.mark.asyncio
async def test_docker_client_pool_short_operation_runs_while_stream_is_active() -> None:
    _reset_fake_client_payloads()
    _block_stream_iterators(logs=True)
    pool = docker_connector.DockerClientPool(
        module_loader=lambda: _FakeStreamsDockerModule(),
        to_thread=_run_sync,
        max_clients=1,
    )

    log_stream = pool.stream_container_logs(_host(), {"id": "c1"}, tail=1)
    first_log = await anext(log_stream)
    logs = await pool.query_container_logs(_host(), {"id": "c1"}, max_lines=1)

    assert first_log == {
        "timestamp": "2026-04-11T10:00:01.000000000Z",
        "line": "live one",
    }
    assert logs == [
        {"timestamp": "2026-04-11T10:00:01.000000000Z", "line": "first line"},
        {"timestamp": "", "line": "plain second line"},
    ]
    assert len(_FakeStreamsDockerClient.created) == 2
    assert _FakeStreamsDockerClient.created[0].closed is False
    assert _FakeStreamsDockerClient.created[1].closed is False

    await cast(Any, log_stream).aclose()
    await pool.close_all()


@pytest.mark.asyncio
async def test_docker_client_pool_stream_aclose_cleans_up_iterator_and_client() -> None:
    _reset_fake_client_payloads()
    _block_stream_iterators(logs=True)
    pool = docker_connector.DockerClientPool(
        module_loader=lambda: _FakeStreamsDockerModule(),
        to_thread=_run_sync,
        max_clients=1,
    )

    log_stream = pool.stream_container_logs(_host(), {"id": "c1"}, tail=1)
    await anext(log_stream)
    first_client = _FakeStreamsDockerClient.created[0]
    log_iterator = first_client.containers._containers["c1"].log_iterators[0]

    assert log_iterator.close_calls == 0
    assert first_client.close_calls == 0

    await cast(Any, log_stream).aclose()

    assert log_iterator.close_calls == 1
    assert first_client.close_calls == 1
    assert first_client.closed is True

    await pool.connect_host(_host())

    assert len(_FakeStreamsDockerClient.created) == 2
    assert _FakeStreamsDockerClient.created[1].closed is False

    await pool.close_all()


@pytest.mark.asyncio
async def test_docker_client_pool_close_all_stops_active_streams_then_closes_pooled_clients() -> (
    None
):
    _reset_fake_client_payloads()
    _block_stream_iterators(logs=True)
    pool = docker_connector.DockerClientPool(
        module_loader=lambda: _FakeStreamsDockerModule(),
        to_thread=_run_sync,
    )

    log_stream = pool.stream_container_logs(_host(), {"id": "c1"}, tail=1)
    first_log = await anext(log_stream)
    short_op = await pool.connect_host(_host())

    assert first_log == {
        "timestamp": "2026-04-11T10:00:01.000000000Z",
        "line": "live one",
    }
    assert short_op == {"server_version": "24.0.7", "api_version": "1.43"}
    assert len(_FakeStreamsDockerClient.created) == 2

    await pool.close_all()

    stream_client = _FakeStreamsDockerClient.created[0]
    pooled_client = _FakeStreamsDockerClient.created[1]
    log_iterator = stream_client.containers._containers["c1"].log_iterators[0]
    assert stream_client.closed is True
    assert pooled_client.closed is True
    assert log_iterator.close_calls == 1
    assert _FakeStreamsDockerClient.lifecycle_trace.index(
        (stream_client.label, "close")
    ) < _FakeStreamsDockerClient.lifecycle_trace.index((pooled_client.label, "close"))

    await cast(Any, log_stream).aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method_name", "kwargs"),
    [
        ("query_container_logs", {"max_lines": 0}),
        ("stream_container_logs", {"tail": 0}),
        ("restart_container_for_host", {"timeout": 0}),
    ],
)
async def test_core_primitives_reject_non_positive_bounds(
    monkeypatch: pytest.MonkeyPatch,
    method_name: str,
    kwargs: dict[str, int],
) -> None:
    _reset_fake_client_payloads()
    monkeypatch.setattr(
        docker_connector, "_IMPORT_MODULE", lambda _name: _FakeStreamsDockerModule()
    )
    monkeypatch.setattr(docker_connector, "_TO_THREAD", lambda fn: _run_sync(fn))

    method = getattr(docker_connector, method_name)
    with pytest.raises(ValueError):
        if method_name == "stream_container_logs":
            await cast(Any, method)(_host(), {"id": "c1"}, **kwargs).__anext__()
        else:
            await method(_host(), {"id": "c1"}, **kwargs)
