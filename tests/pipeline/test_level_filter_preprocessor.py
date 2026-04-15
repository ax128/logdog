from __future__ import annotations

from logdog.pipeline.preprocessor.base import LogLine
from logdog.pipeline.preprocessor.level_filter import LevelFilterPreprocessor


def _line(content: str, level: str | None = None) -> LogLine:
    return LogLine(
        host_name="h", container_id="c", container_name="app",
        timestamp="2026-04-15T10:00:00Z", content=content, level=level,
    )


def test_level_filter_empty():
    assert LevelFilterPreprocessor().process([]) == []


def test_level_filter_name():
    assert LevelFilterPreprocessor.name == "level_filter"


def test_level_filter_default_min_warn_drops_debug_info():
    lines = [
        _line("d", level="debug"),
        _line("i", level="info"),
        _line("w", level="warn"),
        _line("e", level="error"),
    ]
    result = LevelFilterPreprocessor().process(lines)
    assert [l.level for l in result] == ["warn", "error"]


def test_level_filter_min_error_drops_warn():
    lines = [_line("w", level="warn"), _line("e", level="error"), _line("f", level="fatal")]
    result = LevelFilterPreprocessor(config={"min_level": "error"}).process(lines)
    assert [l.level for l in result] == ["error", "fatal"]


def test_level_filter_min_debug_keeps_all():
    lines = [_line("x", level=lv) for lv in ["debug", "info", "warn", "error"]]
    result = LevelFilterPreprocessor(config={"min_level": "debug"}).process(lines)
    assert [l.level for l in result] == ["debug", "info", "warn", "error"]


def test_level_filter_unknown_level_kept_fail_open():
    line = _line("something happened")   # no level, no keywords
    assert LevelFilterPreprocessor(config={"min_level": "error"}).process([line]) == [line]


def test_level_filter_detects_level_from_content():
    lines = [_line("[DEBUG] boot"), _line("[WARN] disk high"), _line("[ERROR] conn refused")]
    result = LevelFilterPreprocessor().process(lines)   # default min=warn
    assert result[0].content == "[WARN] disk high"
    assert result[1].content == "[ERROR] conn refused"


def test_level_filter_warning_alias():
    line = _line("msg", level="warning")
    assert LevelFilterPreprocessor(config={"min_level": "warn"}).process([line]) == [line]


def test_level_filter_panic_treated_as_fatal():
    line = _line("panic: nil ptr", level="panic")
    assert LevelFilterPreprocessor(config={"min_level": "error"}).process([line]) == [line]


def test_level_filter_unknown_min_level_falls_back_to_warn(caplog):
    import logging
    with caplog.at_level(logging.WARNING, logger="logdog.pipeline.preprocessor.level_filter"):
        p = LevelFilterPreprocessor(config={"min_level": "verbose"})
    assert any("verbose" in r.message for r in caplog.records)
    # falls back to warn: debug line dropped, warn line kept
    lines = [_line("d", level="debug"), _line("w", level="warn")]
    assert [l.level for l in p.process(lines)] == ["warn"]
