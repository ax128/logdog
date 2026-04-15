import pytest

from logwatch.llm.tools import (
    RateLimiter,
    create_tool_registry,
    validate_query_logs_args,
)


def test_query_logs_range_limit_raises_when_hours_exceed_max() -> None:
    with pytest.raises(ValueError):
        validate_query_logs_args(hours=72, max_hours=24)


def test_query_logs_range_limit_allows_within_max() -> None:
    normalized = validate_query_logs_args(hours=6, max_hours=24)
    assert normalized == 6


def test_query_logs_range_limit_rejects_non_positive_hours() -> None:
    with pytest.raises(ValueError):
        validate_query_logs_args(hours=0, max_hours=24)


def test_rate_limit_per_user_within_window() -> None:
    now = [0.0]

    def now_fn() -> float:
        return now[0]

    rl = RateLimiter(limit=2, window_seconds=60, time_fn=now_fn)
    assert rl.allow("u1") is True
    assert rl.allow("u1") is True
    assert rl.allow("u1") is False


def test_rate_limit_expire_window_allows_again() -> None:
    now = [0.0]

    def now_fn() -> float:
        return now[0]

    rl = RateLimiter(limit=2, window_seconds=60, time_fn=now_fn)
    assert rl.allow("u1") is True
    assert rl.allow("u1") is True
    assert rl.allow("u1") is False

    now[0] = 61.0
    assert rl.allow("u1") is True


def test_rate_limit_isolated_per_user() -> None:
    now = [0.0]

    def now_fn() -> float:
        return now[0]

    rl = RateLimiter(limit=1, window_seconds=60, time_fn=now_fn)
    assert rl.allow("u1") is True
    assert rl.allow("u1") is False
    assert rl.allow("u2") is True


def test_rate_limit_eviction_by_ttl_and_capacity() -> None:
    now = [0.0]

    def now_fn() -> float:
        return now[0]

    rl = RateLimiter(
        limit=1,
        window_seconds=60,
        time_fn=now_fn,
        bucket_ttl_seconds=2,
        max_buckets=2,
    )
    assert rl.allow("u1") is True
    assert rl.allow("u2") is True

    now[0] = 3.0
    assert rl.allow("u3") is True

    assert len(rl._buckets) <= 2
    assert "u1" not in rl._buckets


class _HostManagerStub:
    def __init__(self) -> None:
        self._host = {"name": "prod-a", "url": "unix:///var/run/docker.sock"}

    def list_host_statuses(self) -> list[dict[str, str]]:
        return [
            {
                "name": "prod-a",
                "url": "unix:///var/run/docker.sock",
                "status": "connected",
            }
        ]

    def get_host_config(self, name: str) -> dict[str, str] | None:
        if name != "prod-a":
            return None
        return dict(self._host)


class _MetricsWriterStub:
    async def write_audit(
        self, payload, *, redact_patterns=(), max_chars=10_000
    ) -> None:
        return None


@pytest.mark.asyncio
async def test_query_logs_tool_uses_configured_max_hours() -> None:
    async def list_containers_fn(_host: dict[str, str]) -> list[dict[str, str]]:
        return [{"id": "c1", "name": "api"}]

    async def query_logs_fn(
        _host: dict[str, str],
        _container: dict[str, str],
        *,
        since: str,
        until: str | None,
        max_lines: int,
    ) -> list[dict[str, str]]:
        return [{"timestamp": since, "line": str(max_lines), "until": str(until)}]

    registry = create_tool_registry(
        host_manager=_HostManagerStub(),
        metrics_writer_factory=lambda: _MetricsWriterStub(),
        app_config={"llm": {"tools": {"query_logs": {"max_hours": 2}}}},
        list_containers_fn=list_containers_fn,
        query_logs_fn=query_logs_fn,
    )

    with pytest.raises(ValueError, match="hours must be <= 2"):
        await registry["query_logs"].invoke(
            user_id="alice",
            arguments={"host": "prod-a", "container_id": "c1", "hours": 3},
        )


@pytest.mark.asyncio
async def test_query_logs_tool_rejects_max_lines_above_config_cap() -> None:
    async def list_containers_fn(_host: dict[str, str]) -> list[dict[str, str]]:
        return [{"id": "c1", "name": "api"}]

    async def query_logs_fn(
        _host: dict[str, str],
        _container: dict[str, str],
        *,
        since: str,
        until: str | None,
        max_lines: int,
    ) -> list[dict[str, str]]:
        return [{"timestamp": since, "line": str(max_lines), "until": str(until)}]

    registry = create_tool_registry(
        host_manager=_HostManagerStub(),
        metrics_writer_factory=lambda: _MetricsWriterStub(),
        app_config={"llm": {"tools": {"query_logs": {"max_lines": 200}}}},
        list_containers_fn=list_containers_fn,
        query_logs_fn=query_logs_fn,
    )

    with pytest.raises(ValueError, match="max_lines must be <= 200"):
        await registry["query_logs"].invoke(
            user_id="alice",
            arguments={
                "host": "prod-a",
                "container_id": "c1",
                "hours": 1,
                "max_lines": 201,
            },
        )


def test_create_tool_registry_rejects_excessive_query_hours_cap() -> None:
    with pytest.raises(ValueError, match="query_logs.max_hours must be <= 168"):
        create_tool_registry(
            host_manager=_HostManagerStub(),
            metrics_writer_factory=lambda: _MetricsWriterStub(),
            app_config={"llm": {"tools": {"query_logs": {"max_hours": 999}}}},
        )
