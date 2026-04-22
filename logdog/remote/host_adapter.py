from __future__ import annotations

from typing import Any


class HybridHostAdapter:
    def __init__(
        self,
        *,
        direct_backend: Any,
        worker_backend: Any,
    ) -> None:
        self._direct_backend = direct_backend
        self._worker_backend = worker_backend
        self._known_hosts: dict[str, dict[str, Any]] = {}

    def uses_worker_backend(self, host: dict[str, Any] | None) -> bool:
        if not isinstance(host, dict):
            return False
        url = str(host.get("url") or "").strip().lower()
        if not url.startswith("ssh://"):
            return False
        remote_worker = host.get("remote_worker")
        # Default-on for ssh:// hosts. Opt out via `remote_worker.enabled: false`.
        if remote_worker is None:
            return True
        if isinstance(remote_worker, bool):
            return remote_worker
        if not isinstance(remote_worker, dict):
            return True

        enabled = remote_worker.get("enabled")
        if enabled is None:
            return True
        if isinstance(enabled, bool):
            return enabled
        if isinstance(enabled, (int, float)):
            return bool(enabled)
        if isinstance(enabled, str):
            normalized = enabled.strip().lower()
            if normalized in {"", "0", "false", "no", "off"}:
                return False
            if normalized in {"1", "true", "yes", "on"}:
                return True
        return bool(enabled)

    def _backend_for_host(self, host: dict[str, Any]) -> Any:
        if self.uses_worker_backend(host):
            return self._worker_backend
        return self._direct_backend

    def _remember_host(self, host: dict[str, Any]) -> None:
        host_name = str(host.get("name") or "").strip()
        if host_name == "":
            return
        self._known_hosts[host_name] = dict(host)

    async def connect_host(self, host: dict[str, Any]) -> dict[str, Any]:
        self._remember_host(host)
        backend = self._backend_for_host(host)
        return dict(await backend.connect_host(host))

    async def list_containers_for_host(self, host: dict[str, Any]) -> list[dict[str, Any]]:
        self._remember_host(host)
        backend = self._backend_for_host(host)
        return list(await backend.list_containers_for_host(host))

    async def fetch_container_stats(
        self, host: dict[str, Any], container: dict[str, Any]
    ) -> dict[str, Any]:
        self._remember_host(host)
        backend = self._backend_for_host(host)
        return dict(await backend.fetch_container_stats(host, container))

    async def query_container_logs(
        self,
        host: dict[str, Any],
        container: dict[str, Any],
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        self._remember_host(host)
        backend = self._backend_for_host(host)
        return list(await backend.query_container_logs(host, container, **kwargs))

    async def stream_container_logs(
        self,
        host: dict[str, Any],
        container: dict[str, Any],
        **kwargs: Any,
    ):
        self._remember_host(host)
        backend = self._backend_for_host(host)
        async for item in backend.stream_container_logs(host, container, **kwargs):
            yield dict(item)

    async def stream_docker_events(
        self,
        host: dict[str, Any],
        **kwargs: Any,
    ):
        self._remember_host(host)
        backend = self._backend_for_host(host)
        async for item in backend.stream_docker_events(host, **kwargs):
            yield dict(item)

    async def restart_container_for_host(
        self,
        host: dict[str, Any],
        container: dict[str, Any],
        **kwargs: Any,
    ) -> dict[str, Any]:
        self._remember_host(host)
        backend = self._backend_for_host(host)
        return dict(await backend.restart_container_for_host(host, container, **kwargs))

    async def exec_container_for_host(
        self,
        host: dict[str, Any],
        container: dict[str, Any],
        **kwargs: Any,
    ) -> dict[str, Any]:
        self._remember_host(host)
        backend = self._backend_for_host(host)
        return dict(await backend.exec_container_for_host(host, container, **kwargs))

    async def collect_host_metrics_for_host(
        self,
        host: dict[str, Any],
        **kwargs: Any,
    ) -> dict[str, Any]:
        self._remember_host(host)
        backend = self._backend_for_host(host)
        collect = getattr(backend, "collect_host_metrics_for_host", None)
        if not callable(collect):
            raise AttributeError("backend does not implement collect_host_metrics_for_host")
        return dict(await collect(host, **kwargs))

    async def close_host(self, host_name_or_host: Any) -> None:
        if isinstance(host_name_or_host, dict):
            self._remember_host(host_name_or_host)
            backend = self._backend_for_host(host_name_or_host)
        else:
            cached_host = self._known_hosts.get(str(host_name_or_host or "").strip())
            if cached_host is not None:
                backend = self._backend_for_host(cached_host)
            else:
                backend = self._direct_backend
        close = getattr(backend, "close_host", None)
        if callable(close):
            await close(host_name_or_host)

    async def close_all(self) -> None:
        direct_close_all = getattr(self._direct_backend, "close_all", None)
        if callable(direct_close_all):
            await direct_close_all()
        worker_close_all = getattr(self._worker_backend, "close_all", None)
        if callable(worker_close_all):
            await worker_close_all()
