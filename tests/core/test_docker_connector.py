from __future__ import annotations

import asyncio
import threading
import time

import pytest

import logdog.core.docker_connector as docker_connector


class _FakeContainerObj:
    def __init__(self, container_id: str, name: str, status: str, attrs: dict, stats_payload: dict):
        self.id = container_id
        self.name = name
        self.status = status
        self.attrs = attrs
        self._stats_payload = stats_payload

    def stats(self, *, stream: bool = False):
        assert stream is False
        return self._stats_payload


class _FakeLowLevelApi:
    """Mimics ``client.api`` with low-level container methods."""

    def __init__(self, containers: list[_FakeContainerObj]):
        self._containers = containers

    def containers(self, *, all: bool = True):
        assert all is True
        return [
            {
                "Id": c.id,
                "Names": [f"/{c.name}"],
                "State": c.status,
                "Status": c.status,
            }
            for c in self._containers
        ]

    def inspect_container(self, container_id: str):
        for c in self._containers:
            if c.id == container_id:
                return c.attrs
        raise KeyError(container_id)


class _FakeContainersApi:
    def __init__(self, containers: list[_FakeContainerObj]):
        self._containers = containers

    def list(self, *, all: bool = True):
        assert all is True
        return list(self._containers)

    def get(self, container_id: str):
        for c in self._containers:
            if c.id == container_id:
                return c
        raise KeyError(container_id)


class _FakeDockerClient:
    def __init__(self, *, base_url: str, use_ssh_client: bool = False, timeout: int | None = None):
        self.base_url = base_url
        self.use_ssh_client = use_ssh_client
        _objs = [
            _FakeContainerObj(
                "c1",
                "svc-1",
                "running",
                {"RestartCount": 3},
                {"cpu_stats": {"cpu_usage": {"total_usage": 1}}},
            )
        ]
        self.containers = _FakeContainersApi(_objs)
        self.api = _FakeLowLevelApi(_objs)
        self.closed = False

    def version(self):
        return {"Version": "24.0.7", "ApiVersion": "1.43"}

    def close(self):
        self.closed = True


class _FakeDockerModule:
    DockerClient = _FakeDockerClient


class _PoolFakeDockerClient(_FakeDockerClient):
    created: list["_PoolFakeDockerClient"] = []

    def __init__(self, *, base_url: str, use_ssh_client: bool = False, timeout: int | None = None):
        super().__init__(base_url=base_url, use_ssh_client=use_ssh_client)
        type(self).created.append(self)


class _PoolFakeDockerModule:
    DockerClient = _PoolFakeDockerClient


class _HealthFakeDockerClient(_FakeDockerClient):
    created: list["_HealthFakeDockerClient"] = []

    def __init__(self, *, base_url: str, use_ssh_client: bool = False, timeout: int | None = None):
        super().__init__(base_url=base_url, use_ssh_client=use_ssh_client)
        self.fail_health = False
        self.close_calls = 0
        type(self).created.append(self)

    def version(self):
        if self.fail_health:
            raise RuntimeError("stale docker client")
        return super().version()

    def close(self):
        self.close_calls += 1
        super().close()


class _HealthFakeDockerModule:
    DockerClient = _HealthFakeDockerClient


class _FakeClock:
    def __init__(self, start: float = 0.0) -> None:
        self.value = float(start)

    def now(self) -> float:
        return self.value

    def set(self, value: float) -> None:
        self.value = float(value)


@pytest.mark.asyncio
async def test_connect_docker_host_uses_ssh_client_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(docker_connector, "_IMPORT_MODULE", lambda _name: _FakeDockerModule())
    monkeypatch.setattr(docker_connector, "_TO_THREAD", lambda fn: _run_sync(fn))

    out = await docker_connector.connect_docker_host({"name": "prod", "url": "ssh://deploy@10.0.1.10"})

    assert out["server_version"] == "24.0.7"
    assert out["api_version"] == "1.43"


@pytest.mark.asyncio
async def test_list_containers_and_fetch_stats(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(docker_connector, "_IMPORT_MODULE", lambda _name: _FakeDockerModule())
    monkeypatch.setattr(docker_connector, "_TO_THREAD", lambda fn: _run_sync(fn))

    host = {"name": "local", "url": "unix:///var/run/docker.sock"}
    containers = await docker_connector.list_containers_for_host(host)
    stats = await docker_connector.fetch_container_stats(host, {"id": "c1"})

    assert containers == [
        {
            "id": "c1",
            "name": "svc-1",
            "status": "running",
            "restart_count": 3,
        }
    ]
    assert stats["cpu_stats"]["cpu_usage"]["total_usage"] == 1


@pytest.mark.asyncio
async def test_connect_docker_host_raises_when_sdk_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    def raise_import(_name: str):
        raise ModuleNotFoundError("docker")

    monkeypatch.setattr(docker_connector, "_IMPORT_MODULE", raise_import)

    with pytest.raises(RuntimeError):
        await docker_connector.connect_docker_host({"name": "local", "url": "unix:///var/run/docker.sock"})


@pytest.mark.asyncio
async def test_docker_client_pool_reuses_client_for_same_host() -> None:
    _PoolFakeDockerClient.created = []
    pool = docker_connector.DockerClientPool(
        module_loader=lambda: _PoolFakeDockerModule(),
        to_thread=_run_sync,
    )
    host = {"name": "local", "url": "unix:///var/run/docker.sock"}

    await pool.connect_host(host)
    await pool.list_containers_for_host(host)

    assert len(_PoolFakeDockerClient.created) == 1
    assert _PoolFakeDockerClient.created[0].closed is False

    await pool.close_all()
    assert _PoolFakeDockerClient.created[0].closed is True


@pytest.mark.asyncio
async def test_docker_client_pool_recreates_client_when_host_url_changes() -> None:
    _PoolFakeDockerClient.created = []
    pool = docker_connector.DockerClientPool(
        module_loader=lambda: _PoolFakeDockerModule(),
        to_thread=_run_sync,
    )

    await pool.connect_host({"name": "h1", "url": "unix:///var/run/docker.sock"})
    await pool.connect_host({"name": "h1", "url": "ssh://deploy@10.0.1.10"})

    assert len(_PoolFakeDockerClient.created) == 2
    assert _PoolFakeDockerClient.created[0].closed is True
    assert _PoolFakeDockerClient.created[1].closed is False

    await pool.close_all()
    assert _PoolFakeDockerClient.created[1].closed is True


@pytest.mark.asyncio
async def test_docker_client_pool_recreates_unhealthy_client_before_operation() -> None:
    _HealthFakeDockerClient.created = []
    pool = docker_connector.DockerClientPool(
        module_loader=lambda: _HealthFakeDockerModule(),
        to_thread=_run_sync,
    )
    host = {"name": "h1", "url": "unix:///var/run/docker.sock"}

    first = await pool.connect_host(host)
    assert first["server_version"] == "24.0.7"
    assert len(_HealthFakeDockerClient.created) == 1

    _HealthFakeDockerClient.created[0].fail_health = True
    second = await pool.connect_host(host)

    assert second["server_version"] == "24.0.7"
    assert len(_HealthFakeDockerClient.created) == 2
    assert _HealthFakeDockerClient.created[0].closed is True
    assert _HealthFakeDockerClient.created[0].close_calls == 1
    assert _HealthFakeDockerClient.created[1].closed is False

    await pool.close_all()


@pytest.mark.asyncio
async def test_docker_client_pool_evicts_lru_when_capacity_reached() -> None:
    _PoolFakeDockerClient.created = []
    clock = _FakeClock()
    pool = docker_connector.DockerClientPool(
        module_loader=lambda: _PoolFakeDockerModule(),
        to_thread=_run_sync,
        monotonic_fn=clock.now,
        max_clients=1,
    )

    clock.set(1.0)
    await pool.connect_host({"name": "h1", "url": "unix:///var/run/docker.sock"})
    clock.set(2.0)
    await pool.connect_host({"name": "h2", "url": "unix:///var/run/docker.sock"})

    assert len(_PoolFakeDockerClient.created) == 2
    assert _PoolFakeDockerClient.created[0].closed is True
    assert _PoolFakeDockerClient.created[1].closed is False

    await pool.close_all()
    assert _PoolFakeDockerClient.created[1].closed is True


@pytest.mark.asyncio
async def test_docker_client_pool_recreates_client_after_idle_timeout() -> None:
    _PoolFakeDockerClient.created = []
    clock = _FakeClock()
    pool = docker_connector.DockerClientPool(
        module_loader=lambda: _PoolFakeDockerModule(),
        to_thread=_run_sync,
        monotonic_fn=clock.now,
        max_idle_seconds=5.0,
    )
    host = {"name": "h1", "url": "unix:///var/run/docker.sock"}

    clock.set(0.0)
    await pool.connect_host(host)
    assert len(_PoolFakeDockerClient.created) == 1

    clock.set(10.0)
    await pool.connect_host(host)

    assert len(_PoolFakeDockerClient.created) == 2
    assert _PoolFakeDockerClient.created[0].closed is True
    assert _PoolFakeDockerClient.created[1].closed is False

    await pool.close_all()


async def _run_sync(fn):
    return fn()


# ---------------------------------------------------------------------------
# ssh_key injection tests
# ---------------------------------------------------------------------------


def test_docker_client_kwargs_includes_ssh_key_for_ssh_url():
    kwargs = docker_connector._docker_client_kwargs(
        {"url": "ssh://deploy@10.0.0.1", "ssh_key": "/home/user/.ssh/id_ed25519"}
    )
    assert kwargs["base_url"] == "ssh://deploy@10.0.0.1"
    assert kwargs["use_ssh_client"] is True
    assert kwargs["_ssh_key"] == "/home/user/.ssh/id_ed25519"


def test_docker_client_kwargs_no_ssh_key_for_non_ssh_url():
    kwargs = docker_connector._docker_client_kwargs(
        {"url": "unix:///var/run/docker.sock", "ssh_key": "/some/key"}
    )
    assert "_ssh_key" not in kwargs
    assert "use_ssh_client" not in kwargs


def test_docker_client_kwargs_no_ssh_key_when_not_configured():
    kwargs = docker_connector._docker_client_kwargs({"url": "ssh://host"})
    assert kwargs["use_ssh_client"] is True
    assert "_ssh_key" not in kwargs


def test_docker_client_kwargs_uses_host_specific_docker_timeout():
    kwargs = docker_connector._docker_client_kwargs(
        {"url": "ssh://deploy@10.0.0.1", "docker_timeout_seconds": 42}
    )
    assert kwargs["timeout"] == 42.0


def test_make_docker_client_strips_private_kwargs():
    """_make_docker_client must not forward _ssh_key to DockerClient."""
    import unittest.mock as mock

    received: dict = {}

    class _TrackingClient:
        def __init__(self, **kw):
            received.update(kw)

    class _TrackingModule:
        DockerClient = _TrackingClient

    kwargs = {"base_url": "unix:///var/run/docker.sock", "_ssh_key": "/some/key"}
    with mock.patch.object(
        docker_connector, "_IMPORT_MODULE", side_effect=ImportError("no docker")
    ):
        docker_connector._make_docker_client(_TrackingModule(), kwargs)

    assert "_ssh_key" not in received
    assert received["base_url"] == "unix:///var/run/docker.sock"
    # original kwargs dict must NOT be mutated
    assert "_ssh_key" in kwargs


def test_make_docker_client_injects_key_filename_into_ssh_params():
    """_make_docker_client injects key_filename via the SSHHTTPAdapter monkey-patch."""
    import unittest.mock as mock

    captured_ssh_params: dict = {}

    class _FakeSSHHTTPAdapter:
        def _create_paramiko_client(self, base_url: str) -> None:
            self.ssh_params = {"hostname": "10.0.0.1", "port": None, "username": "deploy"}

    fake_adapter_cls = _FakeSSHHTTPAdapter

    class _FakeSshconn:
        SSHHTTPAdapter = fake_adapter_cls

    class _CapturingClient:
        def __init__(self, **kw):
            # Simulate adapter creation and call the (possibly patched) method.
            adapter = fake_adapter_cls()
            adapter._create_paramiko_client("ssh://deploy@10.0.0.1")
            captured_ssh_params.update(adapter.ssh_params)

    class _CapturingModule:
        DockerClient = _CapturingClient

    with mock.patch.object(
        docker_connector,
        "_IMPORT_MODULE",
        return_value=_FakeSshconn(),
    ):
        docker_connector._make_docker_client(
            _CapturingModule(),
            {"base_url": "ssh://deploy@10.0.0.1", "_ssh_key": "/home/user/.ssh/id_ed25519"},
        )

    assert captured_ssh_params.get("key_filename") == "/home/user/.ssh/id_ed25519"


@pytest.mark.asyncio
async def test_docker_client_pool_recreates_client_when_ssh_key_changes() -> None:
    """Pool must treat a changed ssh_key as a config change requiring a new client."""
    import unittest.mock as mock

    _PoolFakeDockerClient.created = []
    pool = docker_connector.DockerClientPool(
        module_loader=lambda: _PoolFakeDockerModule(),
        to_thread=_run_sync,
    )

    with mock.patch.object(
        docker_connector, "_IMPORT_MODULE", side_effect=ImportError("no docker")
    ):
        await pool.connect_host(
            {"name": "h1", "url": "ssh://deploy@10.0.0.1", "ssh_key": "/keys/id_rsa"}
        )
        await pool.connect_host(
            {"name": "h1", "url": "ssh://deploy@10.0.0.1", "ssh_key": "/keys/id_ed25519"}
        )

    assert len(_PoolFakeDockerClient.created) == 2, "new ssh_key should trigger new client"
    assert _PoolFakeDockerClient.created[0].closed is True
    await pool.close_all()


@pytest.mark.asyncio
async def test_docker_client_pool_uses_host_specific_timeout_for_wait_for(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorded_timeouts: list[float] = []

    async def record_wait_for(awaitable, timeout):
        recorded_timeouts.append(float(timeout))
        return await awaitable

    pool = docker_connector.DockerClientPool(
        module_loader=lambda: _PoolFakeDockerModule(),
        to_thread=_run_sync,
    )
    host = {
        "name": "prod",
        "url": "ssh://deploy@10.0.1.10",
        "docker_timeout_seconds": 0.2,
    }

    monkeypatch.setattr(docker_connector, "_DEFAULT_SSH_TIMEOUT", 0.01)
    monkeypatch.setattr(docker_connector.asyncio, "wait_for", record_wait_for)

    containers = await pool.list_containers_for_host(host)

    assert containers == [
        {
            "id": "c1",
            "name": "svc-1",
            "status": "running",
            "restart_count": 3,
        }
    ]
    assert recorded_timeouts == [0.2, 0.2]

    await pool.close_all()


@pytest.mark.asyncio
async def test_docker_client_pool_discards_client_after_operation_timeout() -> None:
    _PoolFakeDockerClient.created = []
    pool = docker_connector.DockerClientPool(
        module_loader=lambda: _PoolFakeDockerModule(),
    )
    host = {
        "name": "prod",
        "url": "unix:///var/run/docker.sock",
        "docker_timeout_seconds": 0.05,
    }

    first_client = await pool._get_client(host)
    assert len(_PoolFakeDockerClient.created) == 1

    with pytest.raises(asyncio.TimeoutError):
        await pool._run(host, lambda _client: time.sleep(0.2))

    replacement_client = await pool._get_client(host)

    assert replacement_client is not first_client
    assert first_client.closed is True
    assert len(_PoolFakeDockerClient.created) == 2

    await pool.close_all()


def test_run_in_daemon_thread_does_not_raise_when_loop_already_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    thread_errors: list[BaseException] = []

    def _capture_thread_error(args: threading.ExceptHookArgs) -> None:
        thread_errors.append(args.exc_value)

    monkeypatch.setattr(threading, "excepthook", _capture_thread_error)

    async def _exercise_timeout() -> None:
        _PoolFakeDockerClient.created = []
        pool = docker_connector.DockerClientPool(
            module_loader=lambda: _PoolFakeDockerModule(),
        )
        host = {
            "name": "prod",
            "url": "unix:///var/run/docker.sock",
            "docker_timeout_seconds": 0.01,
        }

        await pool._get_client(host)
        with pytest.raises(asyncio.TimeoutError):
            await pool._run(host, lambda _client: time.sleep(0.05))
        await pool.close_all()

    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        loop.run_until_complete(_exercise_timeout())
    finally:
        loop.close()
        asyncio.set_event_loop(None)

    time.sleep(0.1)

    assert thread_errors == []
