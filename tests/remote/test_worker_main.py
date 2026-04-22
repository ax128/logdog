from __future__ import annotations

import asyncio
import json
import queue
import subprocess
from typing import Any

import pytest

import logdog.remote.worker_main as worker_main
from logdog.remote.worker_main import RemoteWorkerProcess
from logdog.remote.worker_protocol import FrameReader, encode_frame


class _FakeChannel:
    def __init__(self) -> None:
        self._reads: queue.Queue[bytes | None] = queue.Queue()
        self.writes: list[bytes] = []
        self.closed = False

    def push_message(self, message: dict[str, Any]) -> None:
        frame = encode_frame(message)
        midpoint = max(1, len(frame) // 2)
        self._reads.put(frame[:midpoint])
        self._reads.put(frame[midpoint:])

    def finish(self) -> None:
        self._reads.put(None)

    def read(
        self,
        _size: int,
        *,
        timeout_seconds: float | None = None,
    ) -> bytes | None:
        timeout = 1.0 if timeout_seconds is None else float(timeout_seconds)
        try:
            item = self._reads.get(timeout=timeout)
        except queue.Empty:
            return None
        if item is None:
            return b""
        return item

    def write(self, data: bytes) -> int:
        self.writes.append(bytes(data))
        return len(data)

    def flush(self) -> None:
        return None

    def close(self) -> None:
        self.closed = True


class _BackendStub:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def connect_host(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.calls.append(("connect_host", dict(payload)))
        return {"server_version": "24.0.7"}

    async def list_containers(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        self.calls.append(("list_containers", dict(payload)))
        return [{"id": "c1", "name": "svc"}]

    async def stream_logs(self, payload: dict[str, Any]):
        self.calls.append(("stream_logs", dict(payload)))
        yield {
            "timestamp": "2026-04-21T10:00:00Z",
            "line": "ERROR token=abc",
        }

    async def stream_container_logs(self, host: dict[str, Any], container: dict[str, Any], **kwargs: Any):
        payload = {"host": dict(host), "container": dict(container), **dict(kwargs)}
        async for item in self.stream_logs(payload):
            yield item

    async def stream_events(self, payload: dict[str, Any]):
        self.calls.append(("stream_events", dict(payload)))
        yield {
            "time": 1713693600,
            "action": "restart",
        }

    async def stream_docker_events(self, host: dict[str, Any], **kwargs: Any):
        payload = {"host": dict(host), **dict(kwargs)}
        async for item in self.stream_events(payload):
            yield item


class _BlockingBackendStub(_BackendStub):
    def __init__(self) -> None:
        super().__init__()
        self.request_started = asyncio.Event()
        self.stream_started = asyncio.Event()
        self.release_request = asyncio.Event()
        self.release_stream = asyncio.Event()

    async def connect_host(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.calls.append(("connect_host", dict(payload)))
        self.request_started.set()
        await self.release_request.wait()
        return {"server_version": "24.0.7"}

    async def stream_logs(self, payload: dict[str, Any]):
        self.calls.append(("stream_logs", dict(payload)))
        self.stream_started.set()
        await self.release_stream.wait()
        yield {
            "timestamp": "2026-04-21T10:00:00Z",
            "line": "ERROR token=abc",
        }


class _FakeStreamProcess:
    def __init__(self, stdout_chunks: list[bytes], returncode: int = 0) -> None:
        self.stdout = _FakeStreamReader(stdout_chunks)
        self.returncode = returncode
        self.stderr = _FakeStreamReader([])
        self.terminated = False
        self.killed = False

    async def wait(self) -> int:
        return self.returncode

    def terminate(self) -> None:
        self.terminated = True

    def kill(self) -> None:
        self.killed = True


class _FakeStreamReader:
    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = list(chunks)

    async def readline(self) -> bytes:
        if not self._chunks:
            return b""
        return self._chunks.pop(0)

    async def read(self) -> bytes:
        if not self._chunks:
            return b""
        data = b"".join(self._chunks)
        self._chunks.clear()
        return data


def _decode_messages(channel: _FakeChannel) -> list[dict[str, Any]]:
    reader = FrameReader()
    messages: list[dict[str, Any]] = []
    for chunk in channel.writes:
        messages.extend(reader.feed(chunk))
    messages.extend(reader.close())
    return messages


@pytest.mark.asyncio
async def test_docker_cli_worker_backend_uses_docker_and_proc() -> None:
    calls: list[list[str]] = []

    def fake_run(
        cmd: list[str],
        *,
        check: bool,
        capture_output: bool,
        text: bool,
        timeout: float | None = None,
        env: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        calls.append(list(cmd))
        if cmd[:2] == ["docker", "version"]:
            payload = {
                "Client": {"Version": "25.0.0"},
                "Server": {"Version": "24.0.7"},
            }
            return subprocess.CompletedProcess(cmd, 0, json.dumps(payload), "")
        if cmd[:2] == ["docker", "ps"]:
            line = json.dumps(
                {
                    "ID": "abc123",
                    "Image": "svc:latest",
                    "Names": "svc",
                    "Status": "Up 5 minutes",
                    "CreatedAt": "2026-04-21 10:00:00 +0000 UTC",
                    "Ports": "80/tcp",
                }
            )
            return subprocess.CompletedProcess(cmd, 0, f"{line}\n", "")
        if cmd[:2] == ["docker", "inspect"]:
            payload = [
                {
                    "Id": "abc123",
                    "Name": "/svc",
                    "RestartCount": 4,
                    "State": {"Status": "running"},
                }
            ]
            return subprocess.CompletedProcess(cmd, 0, json.dumps(payload), "")
        if cmd[:2] == ["docker", "stats"]:
            line = json.dumps(
                {
                    "Container": "abc123",
                    "Name": "svc",
                    "CPUPerc": "1.5%",
                    "MemUsage": "10MiB / 128MiB",
                    "MemPerc": "7.8%",
                    "NetIO": "1kB / 2kB",
                    "BlockIO": "0B / 0B",
                    "PIDs": "3",
                }
            )
            return subprocess.CompletedProcess(cmd, 0, f"{line}\n", "")
        if cmd[:2] == ["docker", "logs"]:
            return subprocess.CompletedProcess(
                cmd,
                0,
                "2026-04-21T10:00:00Z hello\n2026-04-21T10:00:01Z world\n",
                "",
            )
        if cmd[:2] == ["docker", "restart"]:
            return subprocess.CompletedProcess(cmd, 0, "abc123\n", "")
        if cmd[:2] == ["docker", "exec"]:
            return subprocess.CompletedProcess(cmd, 0, "exec-ok\n", "")
        raise AssertionError(f"unexpected command: {cmd}")

    async def fake_create_subprocess_exec(*cmd: str, **kwargs: Any) -> _FakeStreamProcess:
        calls.append(list(cmd))
        if cmd[:2] == ("docker", "logs"):
            return _FakeStreamProcess(
                [b"2026-04-21T10:00:00Z stream-one\n", b"2026-04-21T10:00:01Z stream-two\n"]
            )
        if cmd[:2] == ("docker", "events"):
            return _FakeStreamProcess([b'{"status":"restart","id":"abc123"}\n'])
        raise AssertionError(f"unexpected stream command: {cmd}")

    def fake_getloadavg() -> tuple[float, float, float]:
        return (0.1, 0.2, 0.3)

    class _Usage:
        total = 1000
        used = 200
        free = 800

    proc_stat_reads = iter(
        [
            "cpu  1 2 3 4 5 6 7 8 9 10\n",
            "cpu  2 3 4 5 6 7 8 9 10 11\n",
        ]
    )

    def fake_read_text(_self: Any, path: str) -> str:
        if path == "/proc/loadavg":
            return "0.10 0.20 0.30 1/1 1\n"
        if path == "/proc/meminfo":
            return "MemTotal:       1024 kB\nMemAvailable:    512 kB\n"
        if path == "/proc/uptime":
            return "123.45 67.89\n"
        if path == "/proc/stat":
            return next(proc_stat_reads)
        if path == "/proc/net/dev":
            return "Inter-|   Receive                                                |  Transmit\n  eth0: 1 2 0 0 0 0 0 0 3 4 0 0 0 0 0 0\n"
        raise AssertionError(f"unexpected path: {path}")

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(worker_main.subprocess, "run", fake_run)
    monkeypatch.setattr(worker_main.asyncio, "create_subprocess_exec", fake_create_subprocess_exec)
    monkeypatch.setattr(worker_main.os, "getloadavg", fake_getloadavg)
    monkeypatch.setattr(worker_main.shutil, "disk_usage", lambda _path: _Usage())
    monkeypatch.setattr(worker_main.DockerCLIWorkerBackend, "_read_text", fake_read_text)
    try:
        backend = worker_main.DockerCLIWorkerBackend()
        connect = await backend.connect_host({"name": "prod"})
        containers = await backend.list_containers_for_host({"name": "prod"})
        stats = await backend.fetch_container_stats(
            {"name": "prod"}, {"id": "abc123", "name": "svc"}
        )
        logs = await backend.query_container_logs(
            {"name": "prod"},
            {"id": "abc123", "name": "svc"},
            since="2026-04-21 10:00:00",
            until="2026-04-21 11:00:00",
            max_lines=2,
        )
        restart = await backend.restart_container_for_host(
            {"name": "prod"}, {"id": "abc123", "name": "svc"}
        )
        exec_result = await backend.exec_container_for_host(
            {"name": "prod"},
            {"id": "abc123", "name": "svc"},
            command="echo ok",
        )
        metrics = await backend.collect_host_metrics_for_host({"name": "prod"})
        stream_items = [
            item
            async for item in backend.stream_container_logs(
                {"name": "prod"}, {"id": "abc123", "name": "svc"}
            )
        ]
        event_items = [
            item
            async for item in backend.stream_docker_events({"name": "prod"})
        ]
    finally:
        monkeypatch.undo()

    assert connect["server_version"] == "24.0.7"
    assert containers[0]["id"] == "abc123"
    assert containers[0]["restart_count"] == 4
    assert stats["container_id"] == "abc123"
    assert stats["cpu_percent"] == pytest.approx(1.5)
    assert stats["memory_stats"]["usage"] == 10 * 1024 * 1024
    assert stats["memory_stats"]["limit"] == 128 * 1024 * 1024
    assert logs[0]["line"] == "hello"
    assert restart["container_id"] == "abc123"
    assert restart["restarted"] is True
    assert exec_result["output"] == "exec-ok"
    assert metrics["cpu_percent"] == 80.0
    assert metrics["load_1"] == pytest.approx(0.1)
    assert metrics["load_5"] == pytest.approx(0.2)
    assert metrics["load_15"] == pytest.approx(0.3)
    assert metrics["mem_total"] == 1024 * 1024
    assert metrics["mem_available"] == 512 * 1024
    assert metrics["disk_root_total"] == 1000
    assert metrics["net_rx"] == 1
    assert metrics["source"] == "remote-worker"
    assert stream_items[0]["line"] == "stream-one"
    assert event_items[0]["action"] == "restart"
    assert event_items[0]["container_id"] == "abc123"
    history_logs_cmd = next(
        cmd for cmd in calls if cmd[:2] == ["docker", "logs"] and "--until" in cmd
    )
    assert history_logs_cmd[history_logs_cmd.index("--since") + 1] == "2026-04-21T10:00:00Z"
    assert history_logs_cmd[history_logs_cmd.index("--until") + 1] == "2026-04-21T11:00:00Z"
    assert any(cmd[:2] == ["docker", "version"] for cmd in calls)
    assert any(cmd[:2] == ["docker", "logs"] for cmd in calls)
    assert any(cmd[:2] == ["docker", "events"] for cmd in calls)


@pytest.mark.asyncio
async def test_docker_cli_worker_backend_exec_includes_stderr_output() -> None:
    def fake_run(
        cmd: list[str],
        *,
        check: bool,
        capture_output: bool,
        text: bool,
        timeout: float | None = None,
        env: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        _ = check, capture_output, text, timeout, env
        if cmd[:2] == ["docker", "exec"]:
            return subprocess.CompletedProcess(cmd, 17, "stdout-line\n", "stderr-line\n")
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(worker_main.subprocess, "run", fake_run)
    try:
        backend = worker_main.DockerCLIWorkerBackend()
        result = await backend.exec_container_for_host(
            {"name": "prod"},
            {"id": "abc123", "name": "svc"},
            command="echo ok",
        )
    finally:
        monkeypatch.undo()

    assert result["exit_code"] == 17
    assert result["command"] == "echo ok"
    assert result["output"] == "stdout-line\nstderr-line"


def test_worker_main_entrypoint_uses_stdio_transport(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    class _FakeTransport:
        def __init__(self) -> None:
            captured["transport"] = self

    class _FakeBackend:
        def __init__(self) -> None:
            captured["backend"] = self

    async def fake_run_remote_worker(**kwargs: Any) -> None:
        captured["kwargs"] = kwargs

    monkeypatch.setattr(worker_main, "StdIOTransport", _FakeTransport)
    monkeypatch.setattr(worker_main, "DockerCLIWorkerBackend", _FakeBackend)
    monkeypatch.setattr(worker_main, "run_remote_worker", fake_run_remote_worker)

    worker_main.main([])

    assert isinstance(captured["kwargs"]["transport"], _FakeTransport)
    assert isinstance(captured["kwargs"]["backend"], _FakeBackend)


def test_worker_main_entrypoint_passes_remote_cleanup_hook(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}
    cleaned: list[tuple[str, bool]] = []

    class _FakeTransport:
        def __init__(self) -> None:
            return None

    class _FakeBackend:
        def __init__(self) -> None:
            return None

    async def fake_run_remote_worker(**kwargs: Any) -> None:
        captured["kwargs"] = kwargs

    monkeypatch.setenv("LOGDOG_REMOTE_TEMP_ROOT", "/tmp/logdog-remote-worker.abcd")
    monkeypatch.setenv("LOGDOG_REMOTE_DEPLOY_ROOT", "/tmp/logdog-remote-worker.abcd")
    monkeypatch.setenv("LOGDOG_REMOTE_HEARTBEAT_TIMEOUT_SECONDS", "123")
    monkeypatch.setenv("LOGDOG_REMOTE_HEARTBEAT_POLL_INTERVAL_SECONDS", "0.5")
    monkeypatch.setattr(worker_main, "StdIOTransport", _FakeTransport)
    monkeypatch.setattr(worker_main, "DockerCLIWorkerBackend", _FakeBackend)
    monkeypatch.setattr(worker_main, "run_remote_worker", fake_run_remote_worker)
    monkeypatch.setattr(
        worker_main.shutil,
        "rmtree",
        lambda path, ignore_errors=True: cleaned.append((str(path), bool(ignore_errors))),
    )

    worker_main.main([])
    asyncio.run(captured["kwargs"]["cleanup_hook"]({"reason": "connection closed"}))

    assert captured["kwargs"]["workspace_root"] == "/tmp/logdog-remote-worker.abcd"
    assert captured["kwargs"]["idle_timeout_seconds"] == pytest.approx(123.0)
    assert captured["kwargs"]["poll_interval_seconds"] == pytest.approx(0.5)
    assert cleaned == [("/tmp/logdog-remote-worker.abcd", True)]


def test_worker_main_entrypoint_cleanup_skips_non_logdog_deploy_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}
    cleaned: list[tuple[str, bool]] = []

    class _FakeTransport:
        def __init__(self) -> None:
            return None

    class _FakeBackend:
        def __init__(self) -> None:
            return None

    async def fake_run_remote_worker(**kwargs: Any) -> None:
        captured["kwargs"] = kwargs

    monkeypatch.setenv("LOGDOG_REMOTE_DEPLOY_ROOT", "/tmp/custom-worker-root")
    monkeypatch.setenv("LOGDOG_REMOTE_TEMP_ROOT", "/tmp/custom-worker-root")
    monkeypatch.setattr(worker_main, "StdIOTransport", _FakeTransport)
    monkeypatch.setattr(worker_main, "DockerCLIWorkerBackend", _FakeBackend)
    monkeypatch.setattr(worker_main, "run_remote_worker", fake_run_remote_worker)
    monkeypatch.setattr(
        worker_main.shutil,
        "rmtree",
        lambda path, ignore_errors=True: cleaned.append((str(path), bool(ignore_errors))),
    )

    worker_main.main([])
    asyncio.run(captured["kwargs"]["cleanup_hook"]({"reason": "connection closed"}))

    assert cleaned == []


@pytest.mark.asyncio
async def test_remote_worker_process_handles_request_response_and_cleanup() -> None:
    channel = _FakeChannel()
    backend = _BackendStub()
    cleaned: list[str] = []
    process = RemoteWorkerProcess(
        channel=channel,
        docker_backend=backend,
        cleanup=lambda reason: cleaned.append(reason),
        heartbeat_timeout_seconds=5.0,
    )

    task = asyncio.create_task(process.serve_forever())
    channel.push_message(
        {
            "type": "request",
            "request_id": "req-1",
            "action": "connect_host",
            "payload": {"host": {"name": "prod"}},
        }
    )
    await asyncio.sleep(0.05)
    channel.push_message(
        {
            "type": "request",
            "request_id": "req-2",
            "action": "shutdown",
            "payload": {},
        }
    )

    await asyncio.wait_for(task, timeout=1.0)

    messages = _decode_messages(channel)
    assert messages[0] == {
        "type": "response",
        "request_id": "req-1",
        "ok": True,
        "result": {"server_version": "24.0.7"},
    }
    assert messages[1] == {
        "type": "response",
        "request_id": "req-2",
        "ok": True,
        "result": {"accepted": True, "reason": "shutdown requested"},
    }
    assert messages[2] == {"type": "shutdown_ack", "reason": "shutdown requested"}
    assert backend.calls == [("connect_host", {"name": "prod"})]
    assert cleaned == ["shutdown requested"]


@pytest.mark.asyncio
async def test_remote_worker_process_passes_timeout_seconds_to_backend_when_present() -> None:
    channel = _FakeChannel()
    received: list[float | None] = []

    class _Backend:
        async def connect_host(
            self,
            host: dict[str, Any],
            *,
            timeout_seconds: float | None = None,
        ) -> dict[str, Any]:
            received.append(timeout_seconds)
            return {"server_version": "24.0.7"}

    process = RemoteWorkerProcess(
        channel=channel,
        docker_backend=_Backend(),
        cleanup=lambda _reason: None,
        heartbeat_timeout_seconds=5.0,
    )

    task = asyncio.create_task(process.serve_forever())
    channel.push_message(
        {
            "type": "request",
            "request_id": "req-timeout-1",
            "action": "connect_host",
            "payload": {"host": {"name": "prod"}, "timeout_seconds": 12.0},
        }
    )
    await asyncio.sleep(0.05)
    channel.push_message(
        {
            "type": "request",
            "request_id": "req-timeout-2",
            "action": "shutdown",
            "payload": {},
        }
    )

    await asyncio.wait_for(task, timeout=1.0)

    messages = _decode_messages(channel)
    assert messages[0] == {
        "type": "response",
        "request_id": "req-timeout-1",
        "ok": True,
        "result": {"server_version": "24.0.7"},
    }
    assert received == [12.0]


@pytest.mark.asyncio
async def test_remote_worker_process_emits_stream_frames() -> None:
    channel = _FakeChannel()
    backend = _BackendStub()
    process = RemoteWorkerProcess(
        channel=channel,
        docker_backend=backend,
        cleanup=lambda _reason: None,
        heartbeat_timeout_seconds=5.0,
    )

    task = asyncio.create_task(process.serve_forever())
    channel.push_message(
        {
            "type": "request",
            "request_id": "req-stream-1",
            "action": "stream_logs",
            "stream_id": "stream-1",
            "payload": {
                "host": {"name": "prod"},
                "container": {"id": "c1", "name": "svc"},
                "pipeline": {
                    "min_level": "error",
                    "redact": [{"pattern": r"token=\S+", "replace": "token=***"}],
                },
            },
        }
    )
    await asyncio.sleep(0.05)
    channel.push_message(
        {
            "type": "request",
            "request_id": "req-stream-2",
            "action": "shutdown",
            "payload": {},
        }
    )

    await asyncio.wait_for(task, timeout=1.0)

    messages = _decode_messages(channel)
    assert messages[0] == {
        "type": "response",
        "request_id": "req-stream-1",
        "ok": True,
        "stream_id": "stream-1",
    }
    assert messages[1]["type"] == "log"
    assert messages[1]["stream_id"] == "stream-1"
    assert messages[1]["line"] == "ERROR token=***"
    assert "metadata" in messages[1]
    assert messages[2] == {
        "type": "response",
        "request_id": "req-stream-2",
        "ok": True,
        "result": {"accepted": True, "reason": "shutdown requested"},
    }
    assert messages[3] == {"type": "shutdown_ack", "reason": "shutdown requested"}


@pytest.mark.asyncio
async def test_remote_worker_process_heartbeat_timeout_triggers_cleanup() -> None:
    channel = _FakeChannel()
    backend = _BackendStub()
    cleaned: list[str] = []
    process = RemoteWorkerProcess(
        channel=channel,
        docker_backend=backend,
        cleanup=lambda reason: cleaned.append(reason),
        heartbeat_timeout_seconds=0.05,
        heartbeat_poll_interval_seconds=0.01,
    )

    task = asyncio.create_task(process.serve_forever())

    await asyncio.wait_for(task, timeout=1.0)

    assert cleaned == ["heartbeat timeout"]


@pytest.mark.asyncio
async def test_remote_worker_process_heartbeat_timeout_cleans_up_active_request() -> None:
    channel = _FakeChannel()
    backend = _BlockingBackendStub()
    cleaned: list[str] = []
    process = RemoteWorkerProcess(
        channel=channel,
        docker_backend=backend,
        cleanup=lambda reason: cleaned.append(reason),
        heartbeat_timeout_seconds=0.1,
        heartbeat_poll_interval_seconds=0.01,
    )

    task = asyncio.create_task(process.serve_forever())
    channel.push_message(
        {
            "type": "request",
            "request_id": "req-1",
            "action": "connect_host",
            "payload": {"host": {"name": "prod"}},
        }
    )
    await asyncio.wait_for(backend.request_started.wait(), timeout=1.0)

    await asyncio.wait_for(task, timeout=1.0)

    assert cleaned == ["heartbeat timeout"]
    assert channel.closed is True


@pytest.mark.asyncio
async def test_remote_worker_process_heartbeat_timeout_cleans_up_active_stream() -> None:
    channel = _FakeChannel()
    backend = _BlockingBackendStub()
    cleaned: list[str] = []
    process = RemoteWorkerProcess(
        channel=channel,
        docker_backend=backend,
        cleanup=lambda reason: cleaned.append(reason),
        heartbeat_timeout_seconds=0.1,
        heartbeat_poll_interval_seconds=0.01,
    )

    task = asyncio.create_task(process.serve_forever())
    channel.push_message(
        {
            "type": "request",
            "request_id": "req-stream-1",
            "action": "stream_logs",
            "stream_id": "stream-1",
            "payload": {
                "host": {"name": "prod"},
                "container": {"id": "c1", "name": "svc"},
            },
        }
    )
    await asyncio.wait_for(backend.stream_started.wait(), timeout=1.0)

    await asyncio.wait_for(task, timeout=1.0)

    assert cleaned == ["heartbeat timeout"]
    assert channel.closed is True


@pytest.mark.asyncio
async def test_remote_worker_process_shutdown_does_not_wait_for_active_request() -> None:
    channel = _FakeChannel()
    backend = _BlockingBackendStub()
    cleaned: list[str] = []
    process = RemoteWorkerProcess(
        channel=channel,
        docker_backend=backend,
        cleanup=lambda reason: cleaned.append(reason),
        heartbeat_timeout_seconds=5.0,
    )

    task = asyncio.create_task(process.serve_forever())
    channel.push_message(
        {
            "type": "request",
            "request_id": "req-1",
            "action": "connect_host",
            "payload": {"host": {"name": "prod"}},
        }
    )
    await asyncio.wait_for(backend.request_started.wait(), timeout=1.0)
    channel.push_message(
        {
            "type": "request",
            "request_id": "req-2",
            "action": "shutdown",
            "payload": {},
        }
    )

    await asyncio.wait_for(task, timeout=1.0)

    messages = _decode_messages(channel)
    assert messages == [
        {
            "type": "response",
            "request_id": "req-2",
            "ok": True,
            "result": {"accepted": True, "reason": "shutdown requested"},
        },
        {"type": "shutdown_ack", "reason": "shutdown requested"},
    ]
    assert cleaned == ["shutdown requested"]
    assert channel.closed is True


@pytest.mark.asyncio
async def test_remote_worker_process_shutdown_does_not_wait_for_active_stream() -> None:
    channel = _FakeChannel()
    backend = _BlockingBackendStub()
    cleaned: list[str] = []
    process = RemoteWorkerProcess(
        channel=channel,
        docker_backend=backend,
        cleanup=lambda reason: cleaned.append(reason),
        heartbeat_timeout_seconds=5.0,
    )

    task = asyncio.create_task(process.serve_forever())
    channel.push_message(
        {
            "type": "request",
            "request_id": "req-stream-1",
            "action": "stream_logs",
            "stream_id": "stream-1",
            "payload": {
                "host": {"name": "prod"},
                "container": {"id": "c1", "name": "svc"},
            },
        }
    )
    await asyncio.wait_for(backend.stream_started.wait(), timeout=1.0)
    channel.push_message(
        {
            "type": "request",
            "request_id": "req-stream-2",
            "action": "shutdown",
            "payload": {},
        }
    )

    await asyncio.wait_for(task, timeout=1.0)

    messages = _decode_messages(channel)
    assert messages == [
        {
            "type": "response",
            "request_id": "req-stream-1",
            "ok": True,
            "stream_id": "stream-1",
        },
        {
            "type": "response",
            "request_id": "req-stream-2",
            "ok": True,
            "result": {"accepted": True, "reason": "shutdown requested"},
        },
        {"type": "shutdown_ack", "reason": "shutdown requested"},
    ]
    assert cleaned == ["shutdown requested"]
    assert channel.closed is True


@pytest.mark.asyncio
async def test_remote_worker_process_cancel_stream_cancels_active_stream_task() -> None:
    channel = _FakeChannel()
    backend = _BlockingBackendStub()
    cleaned: list[str] = []
    process = RemoteWorkerProcess(
        channel=channel,
        docker_backend=backend,
        cleanup=lambda reason: cleaned.append(reason),
        heartbeat_timeout_seconds=5.0,
    )

    task = asyncio.create_task(process.serve_forever())
    channel.push_message(
        {
            "type": "request",
            "request_id": "req-stream-1",
            "action": "stream_logs",
            "stream_id": "stream-1",
            "payload": {
                "host": {"name": "prod"},
                "container": {"id": "c1", "name": "svc"},
            },
        }
    )
    await asyncio.wait_for(backend.stream_started.wait(), timeout=1.0)
    channel.push_message(
        {
            "type": "request",
            "request_id": "req-cancel-1",
            "action": "cancel_stream",
            "payload": {"stream_id": "stream-1"},
        }
    )
    channel.push_message(
        {
            "type": "request",
            "request_id": "req-stream-2",
            "action": "shutdown",
            "payload": {},
        }
    )

    await asyncio.wait_for(task, timeout=1.0)

    messages = _decode_messages(channel)
    assert messages[0] == {
        "type": "response",
        "request_id": "req-stream-1",
        "ok": True,
        "stream_id": "stream-1",
    }
    assert messages[1] == {
        "type": "response",
        "request_id": "req-cancel-1",
        "ok": True,
        "result": {"stream_id": "stream-1", "cancelled": True},
    }
    assert messages[2] == {
        "type": "response",
        "request_id": "req-stream-2",
        "ok": True,
        "result": {"accepted": True, "reason": "shutdown requested"},
    }
    assert messages[3] == {"type": "shutdown_ack", "reason": "shutdown requested"}
    assert all(message.get("type") != "log" for message in messages)
    assert cleaned == ["shutdown requested"]
    assert channel.closed is True


@pytest.mark.asyncio
async def test_remote_worker_process_connection_closed_cleans_up_active_stream() -> None:
    channel = _FakeChannel()
    backend = _BlockingBackendStub()
    cleaned: list[str] = []
    process = RemoteWorkerProcess(
        channel=channel,
        docker_backend=backend,
        cleanup=lambda reason: cleaned.append(reason),
        heartbeat_timeout_seconds=5.0,
    )

    task = asyncio.create_task(process.serve_forever())
    channel.push_message(
        {
            "type": "request",
            "request_id": "req-stream-1",
            "action": "stream_logs",
            "stream_id": "stream-1",
            "payload": {
                "host": {"name": "prod"},
                "container": {"id": "c1", "name": "svc"},
            },
        }
    )
    await asyncio.wait_for(backend.stream_started.wait(), timeout=1.0)
    channel.finish()

    await asyncio.wait_for(task, timeout=1.0)

    messages = _decode_messages(channel)
    assert messages == [
        {
            "type": "response",
            "request_id": "req-stream-1",
            "ok": True,
            "stream_id": "stream-1",
        }
    ]
    assert cleaned == ["connection closed"]
    assert channel.closed is True


@pytest.mark.asyncio
async def test_remote_worker_process_rejects_forbidden_pipeline_payload() -> None:
    channel = _FakeChannel()
    backend = _BackendStub()
    process = RemoteWorkerProcess(
        channel=channel,
        docker_backend=backend,
        cleanup=lambda _reason: None,
        heartbeat_timeout_seconds=5.0,
    )

    task = asyncio.create_task(process.serve_forever())
    channel.push_message(
        {
            "type": "request",
            "request_id": "req-stream-1",
            "action": "stream_logs",
            "stream_id": "stream-1",
            "payload": {
                "host": {"name": "prod"},
                "container": {"id": "c1", "name": "svc"},
                "pipeline": {
                    "min_level": "error",
                    "preprocessors": [{"name": "json_extract"}],
                },
            },
        }
    )
    await asyncio.sleep(0.05)
    channel.push_message(
        {
            "type": "request",
            "request_id": "req-stream-2",
            "action": "shutdown",
            "payload": {},
        }
    )

    await asyncio.wait_for(task, timeout=1.0)

    messages = _decode_messages(channel)
    assert messages[0]["type"] == "response"
    assert messages[0]["request_id"] == "req-stream-1"
    assert messages[0]["ok"] is False
    assert messages[0]["error"]["type"] == "ValueError"
    assert "preprocessors" in messages[0]["error"]["message"]
    assert messages[1] == {
        "type": "response",
        "request_id": "req-stream-2",
        "ok": True,
        "result": {"accepted": True, "reason": "shutdown requested"},
    }
    assert messages[2] == {"type": "shutdown_ack", "reason": "shutdown requested"}
