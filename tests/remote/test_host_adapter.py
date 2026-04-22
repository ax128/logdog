from __future__ import annotations

import pytest

from logdog.remote.host_adapter import HybridHostAdapter


class _BackendStub:
    def __init__(self, name: str) -> None:
        self.name = name
        self.calls: list[tuple[str, tuple, dict]] = []

    async def connect_host(self, host: dict) -> dict:
        self.calls.append(("connect_host", (host,), {}))
        return {"backend": self.name, "op": "connect_host"}

    async def list_containers_for_host(self, host: dict) -> list[dict]:
        self.calls.append(("list_containers_for_host", (host,), {}))
        return [{"backend": self.name}]

    async def fetch_container_stats(self, host: dict, container: dict) -> dict:
        self.calls.append(("fetch_container_stats", (host, container), {}))
        return {"backend": self.name, "op": "fetch_container_stats"}

    async def query_container_logs(self, host: dict, container: dict, **kwargs) -> list[dict]:
        self.calls.append(("query_container_logs", (host, container), kwargs))
        return [{"backend": self.name, "kwargs": kwargs}]

    async def stream_container_logs(self, host: dict, container: dict, **kwargs):
        self.calls.append(("stream_container_logs", (host, container), kwargs))
        yield {"backend": self.name, "stream": "logs", "kwargs": kwargs}

    async def stream_docker_events(self, host: dict, **kwargs):
        self.calls.append(("stream_docker_events", (host,), kwargs))
        yield {"backend": self.name, "stream": "events", "kwargs": kwargs}

    async def restart_container_for_host(self, host: dict, container: dict, **kwargs) -> dict:
        self.calls.append(("restart_container_for_host", (host, container), kwargs))
        return {"backend": self.name, "kwargs": kwargs}

    async def exec_container_for_host(self, host: dict, container: dict, **kwargs) -> dict:
        self.calls.append(("exec_container_for_host", (host, container), kwargs))
        return {"backend": self.name, "kwargs": kwargs}

    async def collect_host_metrics_for_host(self, host: dict, **kwargs) -> dict:
        self.calls.append(("collect_host_metrics_for_host", (host,), kwargs))
        return {"backend": self.name, "kwargs": kwargs}

    async def close_host(self, host_name_or_host) -> None:
        self.calls.append(("close_host", (host_name_or_host,), {}))

    async def close_all(self) -> None:
        self.calls.append(("close_all", (), {}))


@pytest.mark.asyncio
async def test_hybrid_host_adapter_routes_ssh_worker_hosts_to_worker_backend() -> None:
    direct = _BackendStub("direct")
    worker = _BackendStub("worker")
    adapter = HybridHostAdapter(direct_backend=direct, worker_backend=worker)
    host = {
        "name": "prod",
        "url": "ssh://root@example-host:22",
        "remote_worker": {"enabled": True},
    }

    result = await adapter.list_containers_for_host(host)

    assert result == [{"backend": "worker"}]
    assert worker.calls == [("list_containers_for_host", (host,), {})]
    assert direct.calls == []


@pytest.mark.asyncio
async def test_hybrid_host_adapter_defaults_ssh_hosts_to_worker_backend() -> None:
    direct = _BackendStub("direct")
    worker = _BackendStub("worker")
    adapter = HybridHostAdapter(direct_backend=direct, worker_backend=worker)
    host = {"name": "prod", "url": "ssh://root@example-host:22"}

    result = await adapter.list_containers_for_host(host)

    assert result == [{"backend": "worker"}]
    assert worker.calls == [("list_containers_for_host", (host,), {})]
    assert direct.calls == []


@pytest.mark.asyncio
async def test_hybrid_host_adapter_allows_disabling_remote_worker_for_ssh_host() -> (
    None
):
    direct = _BackendStub("direct")
    worker = _BackendStub("worker")
    adapter = HybridHostAdapter(direct_backend=direct, worker_backend=worker)
    host = {
        "name": "prod",
        "url": "ssh://root@example-host:22",
        "remote_worker": {"enabled": False},
    }

    result = await adapter.list_containers_for_host(host)

    assert result == [{"backend": "direct"}]
    assert direct.calls == [("list_containers_for_host", (host,), {})]
    assert worker.calls == []


@pytest.mark.asyncio
async def test_hybrid_host_adapter_routes_non_worker_hosts_to_direct_backend() -> None:
    direct = _BackendStub("direct")
    worker = _BackendStub("worker")
    adapter = HybridHostAdapter(direct_backend=direct, worker_backend=worker)
    host = {"name": "local", "url": "unix:///var/run/docker.sock"}

    result = await adapter.connect_host(host)

    assert result == {"backend": "direct", "op": "connect_host"}
    assert direct.calls == [("connect_host", (host,), {})]
    assert worker.calls == []


@pytest.mark.asyncio
async def test_hybrid_host_adapter_routes_host_metrics_to_selected_backend() -> None:
    direct = _BackendStub("direct")
    worker = _BackendStub("worker")
    adapter = HybridHostAdapter(direct_backend=direct, worker_backend=worker)
    worker_host = {
        "name": "prod",
        "url": "ssh://root@example-host:22",
        "remote_worker": {"enabled": True},
    }
    direct_host = {"name": "local", "url": "unix:///var/run/docker.sock"}

    worker_result = await adapter.collect_host_metrics_for_host(
        worker_host, timeout_seconds=5
    )
    direct_result = await adapter.collect_host_metrics_for_host(
        direct_host, timeout_seconds=7
    )

    assert worker_result["backend"] == "worker"
    assert direct_result["backend"] == "direct"
    assert worker.calls[-1] == (
        "collect_host_metrics_for_host",
        (worker_host,),
        {"timeout_seconds": 5},
    )
    assert direct.calls[-1] == (
        "collect_host_metrics_for_host",
        (direct_host,),
        {"timeout_seconds": 7},
    )


@pytest.mark.asyncio
async def test_hybrid_host_adapter_closes_matching_backend_for_host() -> None:
    direct = _BackendStub("direct")
    worker = _BackendStub("worker")
    adapter = HybridHostAdapter(direct_backend=direct, worker_backend=worker)
    worker_host = {
        "name": "prod",
        "url": "ssh://root@example-host:22",
        "remote_worker": {"enabled": True},
    }

    await adapter.close_host(worker_host)
    await adapter.close_host("local")

    assert worker.calls == [("close_host", (worker_host,), {})]
    assert direct.calls == [("close_host", ("local",), {})]


@pytest.mark.asyncio
async def test_hybrid_host_adapter_routes_known_worker_host_name_to_worker_backend() -> None:
    direct = _BackendStub("direct")
    worker = _BackendStub("worker")
    adapter = HybridHostAdapter(direct_backend=direct, worker_backend=worker)
    worker_host = {
        "name": "prod",
        "url": "ssh://root@example-host:22",
        "remote_worker": {"enabled": True},
    }

    await adapter.connect_host(worker_host)
    await adapter.close_host("prod")

    assert worker.calls[-1] == ("close_host", ("prod",), {})
    assert direct.calls == []


@pytest.mark.asyncio
async def test_hybrid_host_adapter_close_all_closes_both_backends() -> None:
    direct = _BackendStub("direct")
    worker = _BackendStub("worker")
    adapter = HybridHostAdapter(direct_backend=direct, worker_backend=worker)

    await adapter.close_all()

    assert direct.calls == [("close_all", (), {})]
    assert worker.calls == [("close_all", (), {})]


@pytest.mark.asyncio
async def test_hybrid_host_adapter_routes_streams_to_selected_backend() -> None:
    direct = _BackendStub("direct")
    worker = _BackendStub("worker")
    adapter = HybridHostAdapter(direct_backend=direct, worker_backend=worker)
    worker_host = {
        "name": "prod",
        "url": "ssh://root@example-host:22",
        "remote_worker": {"enabled": True},
    }
    direct_host = {"name": "local", "url": "unix:///var/run/docker.sock"}
    container = {"id": "c1"}

    worker_logs = [
        item
        async for item in adapter.stream_container_logs(
            worker_host,
            container,
            since="cursor-1",
        )
    ]
    direct_events = [
        item
        async for item in adapter.stream_docker_events(
            direct_host,
            filters={"type": ["container"]},
        )
    ]

    assert worker_logs == [
        {"backend": "worker", "stream": "logs", "kwargs": {"since": "cursor-1"}}
    ]
    assert direct_events == [
        {
            "backend": "direct",
            "stream": "events",
            "kwargs": {"filters": {"type": ["container"]}},
        }
    ]


def test_hybrid_host_adapter_treats_remote_worker_as_ssh_only() -> None:
    direct = _BackendStub("direct")
    worker = _BackendStub("worker")
    adapter = HybridHostAdapter(direct_backend=direct, worker_backend=worker)

    assert (
        adapter.uses_worker_backend(
            {
                "name": "local",
                "url": "unix:///var/run/docker.sock",
                "remote_worker": {"enabled": True},
            }
        )
        is False
    )
    assert (
        adapter.uses_worker_backend(
            {
                "name": "prod",
                "url": "ssh://root@example-host:22",
                "remote_worker": {"enabled": True},
            }
        )
        is True
    )
    assert (
        adapter.uses_worker_backend({"name": "prod", "url": "ssh://root@example-host:22"})
        is True
    )
    assert (
        adapter.uses_worker_backend(
            {
                "name": "prod",
                "url": "ssh://root@example-host:22",
                "remote_worker": {"enabled": False},
            }
        )
        is False
    )
