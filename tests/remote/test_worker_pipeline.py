from __future__ import annotations

import pytest

from logdog.pipeline.preprocessor.base import LogLine
from logdog.remote.worker_pipeline import RemoteWorkerPipeline, validate_remote_pipeline_config


def _line(
    content: str,
    *,
    level: str | None = None,
    container_id: str = "c1",
    container_name: str = "api",
    host_name: str = "prod-a",
) -> LogLine:
    return LogLine(
        host_name=host_name,
        container_id=container_id,
        container_name=container_name,
        timestamp="2026-04-20T10:00:00Z",
        content=content,
        level=level,
    )


def test_remote_worker_pipeline_normal_path_filters_and_buffers() -> None:
    pipeline = RemoteWorkerPipeline(
        config={
            "include": [r"service=api"],
            "exclude": [r"skip-me"],
            "min_level": "warn",
            "redact": [{"pattern": r"secret=\S+", "replace": "secret=***"}],
            "custom_alerts": [{"pattern": r"secret=\*\*\*", "category": "BUSINESS"}],
            "head": 5,
            "tail": 5,
        },
        buffer_capacity=8,
        dedup_window=4,
    )

    result = pipeline.process(
        [
            _line("service=api secret=alpha", level="error"),
            _line("service=api skip-me secret=beta", level="error"),
            _line("service=api secret=gamma", level="info"),
        ]
    )

    assert [item.content for item in result] == ["service=api secret=***"]
    assert result[0].metadata is not None
    assert result[0].metadata.get("triggered") is True
    assert result[0].metadata.get("matched_category") == "BUSINESS"

    buffered = pipeline.buffer.snapshot("c1")
    assert [item.content for item in buffered] == ["service=api secret=***"]
    assert pipeline.buffer.dropped("c1") == 0


def test_remote_worker_pipeline_buffer_evicts_oldest_per_container() -> None:
    pipeline = RemoteWorkerPipeline(
        config={"min_level": "debug", "head": 10, "tail": 10},
        buffer_capacity=2,
        dedup_window=1,
    )

    pipeline.process(
        [
            _line("one", level="info"),
            _line("two", level="info"),
            _line("three", level="info"),
        ]
    )

    buffered = pipeline.buffer.snapshot("c1")
    assert [item.content for item in buffered] == ["two", "three"]
    assert pipeline.buffer.dropped("c1") == 1
    assert pipeline.buffer.total_dropped() == 1


def test_remote_worker_pipeline_dedups_with_fixed_window() -> None:
    pipeline = RemoteWorkerPipeline(
        config={"min_level": "debug", "head": 10, "tail": 10},
        buffer_capacity=8,
        dedup_window=2,
    )

    result = pipeline.process(
        [
            _line("A", level="info"),
            _line("B", level="info"),
            _line("A", level="info"),
            _line("C", level="info"),
            _line("A", level="info"),
        ]
    )

    assert [item.content for item in result] == ["A", "B", "C", "A"]
    assert pipeline.last_stats.deduped == 1


def test_remote_worker_pipeline_filter_order_keeps_raw_matches_before_redaction() -> None:
    pipeline = RemoteWorkerPipeline(
        config={
            "include": [r"needle"],
            "min_level": "error",
            "redact": [
                {"pattern": r"needle", "replace": "x"},
                {"pattern": r"error", "replace": "info"},
            ],
            "head": 10,
            "tail": 10,
        },
        buffer_capacity=8,
        dedup_window=2,
    )

    result = pipeline.process(
        [
            _line("needle error token=abc"),
            _line("missing error token=abc"),
        ]
    )

    assert len(result) == 1
    assert result[0].content == "x info token=abc"
    assert result[0].metadata is not None
    assert result[0].metadata.get("triggered") is False


def test_remote_worker_pipeline_redact_stabilizes_dedup_and_alert_classification() -> None:
    pipeline = RemoteWorkerPipeline(
        config={
            "min_level": "debug",
            "redact": [{"pattern": r"token=\S+", "replace": "token=***"}],
            "custom_alerts": [{"pattern": r"token=\*\*\*", "category": "BUSINESS"}],
            "head": 10,
            "tail": 10,
        },
        buffer_capacity=8,
        dedup_window=2,
    )

    first = pipeline.classify(_line("login token=alpha", level="info"))
    second = pipeline.classify(_line("login token=beta", level="info"))

    assert first.triggered is True
    assert first.redacted_line == "login token=***"
    assert first.matched_category == "BUSINESS"
    assert second == first

    result = pipeline.process(
        [
            _line("login token=alpha", level="info"),
            _line("login token=beta", level="info"),
        ]
    )

    assert [item.content for item in result] == ["login token=***"]
    assert result[0].metadata is not None
    assert result[0].metadata.get("matched_category") == "BUSINESS"
    assert pipeline.last_stats.deduped == 1


def test_validate_remote_pipeline_config_allows_deterministic_subset() -> None:
    config = validate_remote_pipeline_config(
        {
            "include": [r"service=api"],
            "exclude": [r"debug"],
            "min_level": "error",
            "redact": [{"pattern": r"token=\S+", "replace": "token=***"}],
            "custom_alerts": [{"pattern": r"timeout", "category": "ops"}],
            "alert_keywords": ["panic"],
            "head": 10,
            "tail": 5,
            "dedup_window": 4,
        }
    )

    assert config == {
        "include": [r"service=api"],
        "exclude": [r"debug"],
        "min_level": "error",
        "redact": [{"pattern": r"token=\S+", "replace": "token=***"}],
        "custom_alerts": [{"pattern": r"timeout", "category": "ops"}],
        "alert_keywords": ["panic"],
        "head": 10,
        "tail": 5,
        "dedup_window": 4,
    }


def test_validate_remote_pipeline_config_rejects_forbidden_keys() -> None:
    for key in ("preprocessors", "preprocessor", "module", "path"):
        try:
            validate_remote_pipeline_config({key: "blocked"})
        except ValueError as exc:
            assert key in str(exc)
        else:
            raise AssertionError(f"expected forbidden key rejection for {key}")


def test_validate_remote_pipeline_config_rejects_unknown_keys() -> None:
    try:
        validate_remote_pipeline_config({"buffer_capacity": 99})
    except ValueError as exc:
        assert "buffer_capacity" in str(exc)
    else:
        raise AssertionError("expected unknown key rejection")


def test_validate_remote_pipeline_config_rejects_nested_forbidden_keys() -> None:
    with pytest.raises(ValueError) as excinfo:
        validate_remote_pipeline_config(
            {
                "redact": [
                    {
                        "pattern": r"token=\S+",
                        "replace": "token=***",
                        "module": "blocked",
                    }
                ]
            }
        )
    assert "module" in str(excinfo.value)


def test_validate_remote_pipeline_config_rejects_invalid_value_types() -> None:
    with pytest.raises(TypeError):
        validate_remote_pipeline_config({"include": "not-a-list"})
    with pytest.raises(TypeError):
        validate_remote_pipeline_config({"alert_keywords": "ERROR"})
    with pytest.raises(TypeError):
        validate_remote_pipeline_config({"custom_alerts": ["not-a-dict"]})


def test_validate_remote_pipeline_config_rejects_overlong_patterns() -> None:
    overlong = "x" * 1025
    with pytest.raises(ValueError, match="too long"):
        validate_remote_pipeline_config({"include": [overlong]})
    with pytest.raises(ValueError, match="too long"):
        validate_remote_pipeline_config({"redact": [{"pattern": overlong, "replace": "***"}]})
    with pytest.raises(ValueError, match="too long"):
        validate_remote_pipeline_config(
            {"custom_alerts": [{"pattern": overlong, "category": "ops"}]}
        )
