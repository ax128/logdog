from __future__ import annotations

import pytest

import logwatch.core.docker_connector as docker_connector


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
    def __init__(self, *, base_url: str, use_ssh_client: bool = False):
        self.base_url = base_url
        self.use_ssh_client = use_ssh_client
        self.containers = _FakeContainersApi(
            [
                _FakeContainerObj(
                    "c1",
                    "svc-1",
                    "running",
                    {"RestartCount": 3},
                    {"cpu_stats": {"cpu_usage": {"total_usage": 1}}},
                )
            ]
        )
        self.closed = False

    def version(self):
        return {"Version": "24.0.7", "ApiVersion": "1.43"}

    def close(self):
        self.closed = True


class _FakeDockerModule:
    DockerClient = _FakeDockerClient


class _PoolFakeDockerClient(_FakeDockerClient):
    created: list["_PoolFakeDockerClient"] = []

    def __init__(self, *, base_url: str, use_ssh_client: bool = False):
        super().__init__(base_url=base_url, use_ssh_client=use_ssh_client)
        type(self).created.append(self)


class _PoolFakeDockerModule:
    DockerClient = _PoolFakeDockerClient


class _HealthFakeDockerClient(_FakeDockerClient):
    created: list["_HealthFakeDockerClient"] = []

    def __init__(self, *, base_url: str, use_ssh_client: bool = False):
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
