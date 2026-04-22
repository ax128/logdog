from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from typing import Any

import pytest

from logdog.remote.ssh_lifecycle import SSHSessionLifecycle
from logdog.remote.worker_backend import (
    ParamikoRemoteWorkerLauncher,
    RemoteWorkerBackend,
)


@dataclass(slots=True)
class _StreamItem:
    value: dict[str, Any]


class _FakeStream:
    def __init__(self, items: list[dict[str, Any]]) -> None:
        self._items = list(items)
        self._index = 0
        self.closed = False

    def __aiter__(self) -> _FakeStream:
        return self

    async def __anext__(self) -> dict[str, Any]:
        if self._index >= len(self._items):
            raise StopAsyncIteration
        item = self._items[self._index]
        self._index += 1
        return dict(item)

    async def aclose(self) -> None:
        self.closed = True


class _RecordingSession:
    def __init__(self, *, label: str) -> None:
        self.label = label
        self.requests: list[dict[str, Any]] = []
        self.stream_calls: list[dict[str, Any]] = []
        self.closed = False
        self.heartbeat_count = 0
        self.stream_payloads: dict[str, list[dict[str, Any]]] = {
            "stream_container_logs": [
                {
                    "type": "log",
                    "stream_id": "ignored-by-test",
                    "line": f"{label}:log",
                }
            ],
            "stream_docker_events": [
                {
                    "type": "event",
                    "stream_id": "ignored-by-test",
                    "action": f"{label}:event",
                }
            ],
        }

    async def request(
        self,
        *,
        action: str,
        payload: dict[str, Any] | None = None,
        request_id: str | None = None,
        timeout_seconds: float = 30.0,
    ) -> dict[str, Any]:
        call = {
            "action": action,
            "payload": dict(payload or {}),
            "request_id": request_id,
            "timeout_seconds": timeout_seconds,
        }
        self.requests.append(call)
        if action == "connect_host":
            return {
                "ok": True,
                "result": {
                    "server_version": "24.0.7",
                    "session": self.label,
                },
            }
        if action == "list_containers":
            return {
                "ok": True,
                "result": [{"id": f"{self.label}-c1", "name": "svc"}],
            }
        if action == "fetch_container_stats":
            return {"ok": True, "result": {"cpu": 1.5}}
        if action == "query_container_logs":
            return {
                "ok": True,
                "result": [{"line": f"{self.label}:history"}],
            }
        if action == "restart_container_for_host":
            return {"ok": True, "result": {"restarted": True}}
        if action == "exec_container_for_host":
            return {"ok": True, "result": {"stdout": "ok", "exit_code": 0}}
        if action == "collect_host_metrics_for_host":
            return {"ok": True, "result": {"cpu_pct": 10}}
        if action == "shutdown":
            return {"ok": True, "result": {"accepted": True}}
        return {"ok": True, "result": {"action": action}}

    async def open_stream(
        self,
        *,
        action: str,
        payload: dict[str, Any] | None = None,
        request_id: str,
        stream_id: str,
    ) -> _FakeStream:
        self.stream_calls.append(
            {
                "action": action,
                "payload": dict(payload or {}),
                "request_id": request_id,
                "stream_id": stream_id,
            }
        )
        return _FakeStream(self.stream_payloads.get(action, []))

    async def close(self) -> None:
        self.closed = True

    async def send_heartbeat(self) -> None:
        self.heartbeat_count += 1


class _FailingHeartbeatSession(_RecordingSession):
    async def send_heartbeat(self) -> None:
        self.heartbeat_count += 1
        raise RuntimeError("heartbeat failed")


class _RecordingLauncher:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def open_channel(self, host: dict[str, Any], *, interpreter: str) -> object:
        self.calls.append(
            {
                "host": dict(host),
                "interpreter": interpreter,
            }
        )
        return object()


class _BlockingLauncher:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.started = asyncio.Event()
        self.released = asyncio.Event()
        self.channel = object()

    async def prepare_session(self, host: dict[str, Any]) -> dict[str, Any]:
        self.calls.append(dict(host))
        self.started.set()
        await self.released.wait()
        return {"channel": self.channel, "interpreter": "python3"}


class _ClosableChannel:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


class _FailingConnectSession(_RecordingSession):
    async def request(
        self,
        *,
        action: str,
        payload: dict[str, Any] | None = None,
        request_id: str | None = None,
        timeout_seconds: float = 30.0,
    ) -> dict[str, Any]:
        if action == "connect_host":
            raise RuntimeError("connect failed")
        return await super().request(
            action=action,
            payload=payload,
            request_id=request_id,
            timeout_seconds=timeout_seconds,
        )


class _FailingCloseSession(_RecordingSession):
    async def close(self) -> None:
        self.closed = True
        raise RuntimeError("close failed")


class _FakeExecChannel:
    def __init__(
        self,
        *,
        stdout: bytes = b"",
        stderr: bytes = b"",
        exit_status: int = 0,
    ) -> None:
        self._stdout = bytearray(stdout)
        self._stderr = bytearray(stderr)
        self._exit_status = int(exit_status)
        self.command: str | None = None
        self.timeout: float | None = None
        self.closed = False

    def settimeout(self, value: float) -> None:
        self.timeout = float(value)

    def exec_command(self, command: str) -> None:
        self.command = str(command)

    def recv_ready(self) -> bool:
        return len(self._stdout) > 0

    def recv(self, size: int) -> bytes:
        if not self._stdout:
            return b""
        chunk = bytes(self._stdout[:size])
        del self._stdout[:size]
        return chunk

    def recv_stderr_ready(self) -> bool:
        return len(self._stderr) > 0

    def recv_stderr(self, size: int) -> bytes:
        if not self._stderr:
            return b""
        chunk = bytes(self._stderr[:size])
        del self._stderr[:size]
        return chunk

    def exit_status_ready(self) -> bool:
        return not self._stdout and not self._stderr

    def recv_exit_status(self) -> int:
        return self._exit_status

    def close(self) -> None:
        self.closed = True


class _FakeWorkerChannel(_FakeExecChannel):
    def __init__(self) -> None:
        super().__init__()
        self.sent: list[bytes] = []

    def sendall(self, data: bytes) -> None:
        self.sent.append(bytes(data))


class _FakeTransport:
    def __init__(self, channels: list[_FakeExecChannel]) -> None:
        self._channels = list(channels)
        self.opened: list[_FakeExecChannel] = []

    def open_session(self, timeout: float | None = None) -> _FakeExecChannel:
        assert self._channels, "no fake channels remaining"
        channel = self._channels.pop(0)
        self.opened.append(channel)
        if timeout is not None:
            channel.settimeout(timeout)
        return channel


class _FakeSFTP:
    def __init__(self) -> None:
        self.mkdirs: list[str] = []
        self.uploads: dict[str, bytes] = {}
        self.closed = False

    def mkdir(self, path: str) -> None:
        normalized = str(path)
        if normalized not in self.mkdirs:
            self.mkdirs.append(normalized)

    def putfo(self, fileobj: Any, remote_path: str) -> None:
        self.uploads[str(remote_path)] = bytes(fileobj.read())

    def close(self) -> None:
        self.closed = True


class _FakeSSHClient:
    def __init__(self, *, transport: _FakeTransport, sftp: _FakeSFTP) -> None:
        self.transport = transport
        self.sftp = sftp
        self.connected_with: dict[str, Any] | None = None
        self.policy_name: str | None = None
        self.system_host_keys_loaded = False
        self.closed = False

    def load_system_host_keys(self) -> None:
        self.system_host_keys_loaded = True

    def set_missing_host_key_policy(self, policy: Any) -> None:
        self.policy_name = type(policy).__name__

    def connect(self, **kwargs: Any) -> None:
        self.connected_with = dict(kwargs)

    def get_transport(self) -> _FakeTransport:
        return self.transport

    def open_sftp(self) -> _FakeSFTP:
        return self.sftp

    def close(self) -> None:
        self.closed = True


def _host(name: str = "prod") -> dict[str, Any]:
    return {
        "name": name,
        "url": "ssh://root@example-host:22",
        "remote_worker": {"enabled": True},
    }


@pytest.mark.asyncio
async def test_remote_worker_backend_reuses_one_session_per_host() -> None:
    lifecycle = SSHSessionLifecycle()
    launcher = _RecordingLauncher()
    sessions: list[_RecordingSession] = []

    def session_factory(*, channel: object) -> _RecordingSession:
        session = _RecordingSession(label=f"s{len(sessions) + 1}")
        sessions.append(session)
        return session

    backend = RemoteWorkerBackend(
        lifecycle=lifecycle,
        launcher=launcher,
        session_factory=session_factory,
    )
    host = _host("prod")

    first = await backend.connect_host(host)
    second = await backend.connect_host(host)

    assert first == second
    assert len(launcher.calls) == 1
    assert len(sessions) == 1
    assert sessions[0].requests == [
        {
            "action": "connect_host",
            "payload": {"host": host, "timeout_seconds": 10.0},
            "request_id": "req-1",
            "timeout_seconds": 30.0,
        }
    ]


@pytest.mark.asyncio
async def test_remote_worker_backend_waits_for_inflight_connect_and_reuses_session() -> None:
    lifecycle = SSHSessionLifecycle()
    launcher = _BlockingLauncher()
    sessions: list[_RecordingSession] = []

    def session_factory(*, channel: object) -> _RecordingSession:
        session = _RecordingSession(label=f"s{len(sessions) + 1}")
        sessions.append(session)
        return session

    backend = RemoteWorkerBackend(
        lifecycle=lifecycle,
        launcher=launcher,
        session_factory=session_factory,
    )
    host = _host("prod")

    first_task = asyncio.create_task(backend.connect_host(host))
    await asyncio.wait_for(launcher.started.wait(), timeout=1.0)
    second_task = asyncio.create_task(backend.connect_host(host))
    await asyncio.sleep(0)
    launcher.released.set()

    first = await asyncio.wait_for(first_task, timeout=1.0)
    second = await asyncio.wait_for(second_task, timeout=1.0)

    assert first == second
    assert len(launcher.calls) == 1
    assert len(sessions) == 1


@pytest.mark.asyncio
async def test_remote_worker_backend_separates_sessions_by_host() -> None:
    lifecycle = SSHSessionLifecycle()
    launcher = _RecordingLauncher()
    sessions: list[_RecordingSession] = []

    def session_factory(*, channel: object) -> _RecordingSession:
        session = _RecordingSession(label=f"s{len(sessions) + 1}")
        sessions.append(session)
        return session

    backend = RemoteWorkerBackend(
        lifecycle=lifecycle,
        launcher=launcher,
        session_factory=session_factory,
    )

    await backend.connect_host(_host("prod-a"))
    await backend.connect_host(_host("prod-b"))

    assert len(launcher.calls) == 2
    assert len(sessions) == 2
    assert launcher.calls[0]["host"]["name"] == "prod-a"
    assert launcher.calls[1]["host"]["name"] == "prod-b"


@pytest.mark.asyncio
async def test_remote_worker_backend_streams_pass_stream_ids_to_session() -> None:
    lifecycle = SSHSessionLifecycle()
    launcher = _RecordingLauncher()
    sessions: list[_RecordingSession] = []

    def session_factory(*, channel: object) -> _RecordingSession:
        session = _RecordingSession(label=f"s{len(sessions) + 1}")
        sessions.append(session)
        return session

    backend = RemoteWorkerBackend(
        lifecycle=lifecycle,
        launcher=launcher,
        session_factory=session_factory,
    )
    host = _host()
    container = {"id": "c1"}

    log_items = []
    async for item in backend.stream_container_logs(host, container, tail=5):
        log_items.append(item)

    event_items = []
    async for item in backend.stream_docker_events(host, filters={"type": "event"}):
        event_items.append(item)

    assert log_items == [
        {
            "type": "log",
            "stream_id": "ignored-by-test",
            "line": "s1:log",
        }
    ]
    assert event_items == [
        {
            "type": "event",
            "stream_id": "ignored-by-test",
            "action": "s1:event",
        }
    ]
    assert sessions[0].stream_calls[0]["stream_id"].startswith("stream-")
    assert sessions[0].stream_calls[0]["request_id"].startswith("req-")
    assert sessions[0].stream_calls[0]["action"] == "stream_container_logs"
    assert sessions[0].stream_calls[1]["action"] == "stream_docker_events"


@pytest.mark.asyncio
async def test_remote_worker_backend_reuses_same_session_for_request_operations() -> None:
    lifecycle = SSHSessionLifecycle()
    launcher = _RecordingLauncher()
    sessions: list[_RecordingSession] = []

    def session_factory(*, channel: object) -> _RecordingSession:
        session = _RecordingSession(label=f"s{len(sessions) + 1}")
        sessions.append(session)
        return session

    backend = RemoteWorkerBackend(
        lifecycle=lifecycle,
        launcher=launcher,
        session_factory=session_factory,
    )
    host = _host()
    container = {"id": "c1", "name": "svc"}

    logs = await backend.query_container_logs(host, container, since="cursor-1")
    exec_result = await backend.exec_container_for_host(
        host,
        container,
        command="echo ok",
    )
    metrics = await backend.collect_host_metrics_for_host(
        host,
        collect_load=False,
    )

    assert len(launcher.calls) == 1
    assert len(sessions) == 1
    assert logs == [{"line": "s1:history"}]
    assert exec_result == {"stdout": "ok", "exit_code": 0}
    assert metrics == {"cpu_pct": 10}
    assert [call["action"] for call in sessions[0].requests] == [
        "connect_host",
        "query_container_logs",
        "exec_container_for_host",
        "collect_host_metrics_for_host",
    ]


@pytest.mark.asyncio
async def test_remote_worker_backend_can_reopen_after_close_host() -> None:
    lifecycle = SSHSessionLifecycle()
    launcher = _RecordingLauncher()
    sessions: list[_RecordingSession] = []

    def session_factory(*, channel: object) -> _RecordingSession:
        session = _RecordingSession(label=f"s{len(sessions) + 1}")
        sessions.append(session)
        return session

    backend = RemoteWorkerBackend(
        lifecycle=lifecycle,
        launcher=launcher,
        session_factory=session_factory,
    )
    host = _host()

    await backend.connect_host(host)
    await backend.close_host(host)
    assert lifecycle.can_retry("prod") is True
    await backend.connect_host(host)

    assert len(launcher.calls) == 2
    assert sessions[0].closed is True
    assert sessions[1].closed is False


@pytest.mark.asyncio
async def test_remote_worker_backend_cleans_up_session_and_channel_on_connect_failure() -> None:
    lifecycle = SSHSessionLifecycle()
    launcher = _RecordingLauncher()
    channel = _ClosableChannel()
    sessions: list[_FailingConnectSession] = []

    async def prepare_session(host: dict[str, Any]) -> dict[str, Any]:
        launcher.calls.append(dict(host))
        return {
            "channel": channel,
            "interpreter": "python3",
        }

    launcher.prepare_session = prepare_session  # type: ignore[assignment]

    def session_factory(*, channel: object) -> _FailingConnectSession:
        session = _FailingConnectSession(label=f"s{len(sessions) + 1}")
        sessions.append(session)
        return session

    backend = RemoteWorkerBackend(
        lifecycle=lifecycle,
        launcher=launcher,
        session_factory=session_factory,
    )

    with pytest.raises(RuntimeError, match="connect failed"):
        await backend.connect_host(_host())

    assert len(launcher.calls) == 1
    assert len(sessions) == 1
    assert sessions[0].closed is True
    assert channel.closed is True
    assert lifecycle.can_retry("prod") is True


@pytest.mark.asyncio
async def test_remote_worker_backend_close_all_clears_cached_sessions() -> None:
    lifecycle = SSHSessionLifecycle()
    launcher = _RecordingLauncher()
    sessions: list[_RecordingSession] = []

    def session_factory(*, channel: object) -> _RecordingSession:
        session = _RecordingSession(label=f"s{len(sessions) + 1}")
        sessions.append(session)
        return session

    backend = RemoteWorkerBackend(
        lifecycle=lifecycle,
        launcher=launcher,
        session_factory=session_factory,
    )

    await backend.connect_host(_host("prod-a"))
    await backend.connect_host(_host("prod-b"))
    await backend.close_all()
    await backend.connect_host(_host("prod-a"))

    assert len(launcher.calls) == 3
    assert sessions[0].closed is True
    assert sessions[1].closed is True
    assert sessions[2].closed is False


@pytest.mark.asyncio
async def test_remote_worker_backend_probes_python3_before_python() -> None:
    lifecycle = SSHSessionLifecycle()
    launcher = _RecordingLauncher()
    sessions: list[_RecordingSession] = []
    probe_calls: list[str] = []

    def probe_interpreter(host: dict[str, Any], candidate: str) -> str | None:
        probe_calls.append(candidate)
        if candidate == "python3":
            return None
        return candidate

    def session_factory(*, channel: object) -> _RecordingSession:
        session = _RecordingSession(label=f"s{len(sessions) + 1}")
        sessions.append(session)
        return session

    backend = RemoteWorkerBackend(
        lifecycle=lifecycle,
        launcher=launcher,
        session_factory=session_factory,
        probe_interpreter=probe_interpreter,
    )

    await backend.connect_host(_host())

    assert probe_calls == ["python3", "python"]
    assert launcher.calls == [
        {
            "host": _host(),
            "interpreter": "python",
        }
    ]


@pytest.mark.asyncio
async def test_remote_worker_backend_sends_periodic_heartbeats() -> None:
    lifecycle = SSHSessionLifecycle()
    launcher = _RecordingLauncher()
    sessions: list[_RecordingSession] = []

    def session_factory(*, channel: object) -> _RecordingSession:
        session = _RecordingSession(label=f"s{len(sessions) + 1}")
        sessions.append(session)
        return session

    backend = RemoteWorkerBackend(
        lifecycle=lifecycle,
        launcher=launcher,
        session_factory=session_factory,
        heartbeat_interval_seconds=0.01,
    )
    host = _host()

    await backend.connect_host(host)
    await asyncio.sleep(0.03)
    await backend.close_host(host)

    assert sessions[0].heartbeat_count >= 1
    assert lifecycle.get_state("prod")["last_heartbeat_at"] is not None


@pytest.mark.asyncio
async def test_remote_worker_backend_marks_closed_even_if_close_host_close_fails() -> None:
    lifecycle = SSHSessionLifecycle()
    launcher = _RecordingLauncher()
    sessions: list[_RecordingSession] = []

    def session_factory(*, channel: object) -> _RecordingSession:
        session = _FailingCloseSession(label=f"s{len(sessions) + 1}")
        sessions.append(session)
        return session

    backend = RemoteWorkerBackend(
        lifecycle=lifecycle,
        launcher=launcher,
        session_factory=session_factory,
    )
    host = _host()

    await backend.connect_host(host)

    with pytest.raises(RuntimeError, match="close failed"):
        await backend.close_host(host)

    assert sessions[0].closed is True
    assert lifecycle.can_retry("prod") is True


@pytest.mark.asyncio
async def test_remote_worker_backend_marks_closed_when_heartbeat_close_fails() -> None:
    lifecycle = SSHSessionLifecycle()
    launcher = _RecordingLauncher()
    sessions: list[_RecordingSession] = []

    def session_factory(*, channel: object) -> _RecordingSession:
        if not sessions:
            session = _FailingHeartbeatSession(label="s1")
            original_close = session.close

            async def close() -> None:
                await original_close()
                raise RuntimeError("close failed")

            session.close = close  # type: ignore[assignment]
        else:
            session = _RecordingSession(label=f"s{len(sessions) + 1}")
        sessions.append(session)
        return session

    backend = RemoteWorkerBackend(
        lifecycle=lifecycle,
        launcher=launcher,
        session_factory=session_factory,
        heartbeat_interval_seconds=0.01,
    )
    host = _host()

    await backend.connect_host(host)
    await asyncio.sleep(0.03)

    assert sessions[0].heartbeat_count >= 1
    for _ in range(100):
        if lifecycle.can_retry("prod"):
            break
        await asyncio.sleep(0.01)
    assert lifecycle.can_retry("prod") is True


@pytest.mark.asyncio
async def test_remote_worker_backend_close_host_requests_remote_shutdown_first() -> None:
    lifecycle = SSHSessionLifecycle()
    launcher = _RecordingLauncher()
    sessions: list[_RecordingSession] = []

    def session_factory(*, channel: object) -> _RecordingSession:
        session = _RecordingSession(label=f"s{len(sessions) + 1}")
        sessions.append(session)
        return session

    backend = RemoteWorkerBackend(
        lifecycle=lifecycle,
        launcher=launcher,
        session_factory=session_factory,
    )
    host = _host()

    await backend.connect_host(host)
    await backend.close_host(host)

    assert sessions[0].requests[-1] == {
        "action": "shutdown",
        "payload": {"reason": "close_host requested", "host": host},
        "request_id": "req-2",
        "timeout_seconds": 5.0,
    }
    assert sessions[0].closed is True


@pytest.mark.asyncio
async def test_remote_worker_backend_heartbeat_failure_closes_session_and_allows_reconnect() -> None:
    lifecycle = SSHSessionLifecycle()
    launcher = _RecordingLauncher()
    sessions: list[_RecordingSession] = []

    def session_factory(*, channel: object) -> _RecordingSession:
        if not sessions:
            session = _FailingHeartbeatSession(label="s1")
        else:
            session = _RecordingSession(label=f"s{len(sessions) + 1}")
        sessions.append(session)
        return session

    backend = RemoteWorkerBackend(
        lifecycle=lifecycle,
        launcher=launcher,
        session_factory=session_factory,
        heartbeat_interval_seconds=0.01,
    )
    host = _host()

    await backend.connect_host(host)
    await asyncio.sleep(0.03)

    assert sessions[0].heartbeat_count >= 1
    assert sessions[0].closed is True
    assert lifecycle.can_retry("prod") is True

    reopened = await backend.connect_host(host)

    assert reopened["session"] == "s2"
    assert len(launcher.calls) == 2
    await backend.close_host(host)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("strict_host_key_value", "expected_policy"),
    [
        (False, "AutoAddPolicy"),
        ("false", "AutoAddPolicy"),
        ("0", "AutoAddPolicy"),
        (True, "RejectPolicy"),
        ("true", "RejectPolicy"),
    ],
)
async def test_paramiko_remote_worker_launcher_prepares_session_on_single_client(
    strict_host_key_value: object,
    expected_policy: str,
) -> None:
    python3_probe = _FakeExecChannel(stdout=b"", stderr=b"missing\n", exit_status=127)
    python_probe = _FakeExecChannel(stdout=b"python\n", exit_status=0)
    tempdir_probe = _FakeExecChannel(
        stdout=b"/tmp/logdog-remote-worker-abcd\n",
        exit_status=0,
    )
    worker_channel = _FakeWorkerChannel()
    transport = _FakeTransport(
        [python3_probe, python_probe, tempdir_probe, worker_channel]
    )
    sftp = _FakeSFTP()
    client = _FakeSSHClient(transport=transport, sftp=sftp)

    launcher = ParamikoRemoteWorkerLauncher(
        ssh_client_factory=lambda: client,
    )

    prepared = await launcher.prepare_session(
        {
            "name": "prod",
            "url": "ssh://deploy@example-host:2222",
            "ssh_key": "/keys/id_ed25519",
            "strict_host_key": strict_host_key_value,
            "remote_worker": {
                "enabled": True,
                "temp_root": "/tmp/logdog-test",
                "heartbeat_timeout_seconds": 120,
                "heartbeat_poll_interval_seconds": 0.25,
            },
        }
    )

    assert prepared["interpreter"] == "python"
    channel = prepared["channel"]
    assert channel is not None
    assert client.connected_with == {
        "hostname": "example-host",
        "port": 2222,
        "username": "deploy",
        "timeout": 30.0,
        "banner_timeout": 30.0,
        "auth_timeout": 30.0,
        "look_for_keys": True,
        "key_filename": "/keys/id_ed25519",
    }
    assert client.policy_name == expected_policy
    assert python3_probe.command is not None and "python3" in python3_probe.command
    assert python_probe.command is not None and "python" in python_probe.command
    assert tempdir_probe.command is not None and "/tmp/logdog-test" in tempdir_probe.command
    assert worker_channel.command is not None
    assert "logdog.remote.worker_main" in worker_channel.command
    assert "PYTHONPATH=" in worker_channel.command
    assert "LOGDOG_REMOTE_DEPLOY_ROOT=" in worker_channel.command
    assert "LOGDOG_REMOTE_TEMP_ROOT=" in worker_channel.command
    assert "LOGDOG_REMOTE_HEARTBEAT_TIMEOUT_SECONDS=120" in worker_channel.command
    assert "LOGDOG_REMOTE_HEARTBEAT_POLL_INTERVAL_SECONDS=0.25" in worker_channel.command
    assert any(
        path.endswith(os.path.join("logdog", "remote", "worker_main.py"))
        or path.endswith("logdog/remote/worker_main.py")
        for path in sftp.uploads
    )
    assert any(
        path.endswith(os.path.join("logdog", "pipeline", "filter.py"))
        or path.endswith("logdog/pipeline/filter.py")
        for path in sftp.uploads
    )
    assert sftp.closed is True

    close = getattr(channel, "close", None)
    assert callable(close)
    close()
    assert worker_channel.closed is True
    assert client.closed is True
