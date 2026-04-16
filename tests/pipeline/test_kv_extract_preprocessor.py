from __future__ import annotations

from logdog.pipeline.preprocessor.base import LogLine
from logdog.pipeline.preprocessor.kv_extract import KvExtractPreprocessor


def _line(content: str, level: str | None = None) -> LogLine:
    return LogLine(
        host_name="h",
        container_id="c",
        container_name="app",
        timestamp="2026-04-15T10:00:00Z",
        content=content,
        level=level,
    )


def test_kv_extract_name() -> None:
    assert KvExtractPreprocessor.name == "kv_extract"


def test_kv_extract_non_kv_passes_through() -> None:
    line = _line("plain text log")
    assert KvExtractPreprocessor().process([line])[0].content == "plain text log"


def test_kv_extract_parses_common_fields() -> None:
    line = _line('level=error msg="db timeout" trace_id=abc status=504')
    out = KvExtractPreprocessor().process([line])[0]
    assert "level=error" in out.content
    assert "msg=db timeout" in out.content
    assert "trace_id=abc" in out.content
    assert "status=504" in out.content


def test_kv_extract_backfills_level_from_severity() -> None:
    line = _line("severity=warning message=latency_spike")
    out = KvExtractPreprocessor().process([line])[0]
    assert out.level == "warning"


def test_kv_extract_does_not_override_existing_level() -> None:
    line = _line("level=info msg=ok", level="error")
    out = KvExtractPreprocessor().process([line])[0]
    assert out.level == "error"


def test_kv_extract_include_extra_fields() -> None:
    line = _line("level=error foo=bar msg=boom")
    out = KvExtractPreprocessor(config={"include_extra_fields": True}).process(
        [line]
    )[0]
    assert "foo=bar" in out.content


def test_kv_extract_marks_metadata() -> None:
    line = _line("level=error msg=boom")
    out = KvExtractPreprocessor().process([line])[0]
    assert out.metadata is not None
    assert out.metadata.get("kv_parsed") is True
    assert out.metadata.get("kv_keys") == 2
