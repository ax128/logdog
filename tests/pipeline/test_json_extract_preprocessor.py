from __future__ import annotations

import json

from logdog.pipeline.preprocessor.base import LogLine
from logdog.pipeline.preprocessor.json_extract import JsonExtractPreprocessor


def _line(content: str, level: str | None = None) -> LogLine:
    return LogLine(
        host_name="h", container_id="c", container_name="app",
        timestamp="2026-04-15T10:00:00Z", content=content, level=level,
    )


def _j(data: dict) -> str:
    return json.dumps(data)


def test_json_extract_empty():
    assert JsonExtractPreprocessor().process([]) == []


def test_json_extract_name():
    assert JsonExtractPreprocessor.name == "json_extract"


def test_json_extract_non_json_passes_through():
    line = _line("plain text log")
    assert JsonExtractPreprocessor().process([line])[0].content == "plain text log"


def test_json_extract_parses_message_field():
    line = _line(_j({"level": "error", "message": "connection refused"}))
    result = JsonExtractPreprocessor().process([line])
    assert "message=connection refused" in result[0].content


def test_json_extract_parses_msg_alias():
    line = _line(_j({"level": "warn", "msg": "disk full"}))
    assert "msg=disk full" in JsonExtractPreprocessor().process([line])[0].content


def test_json_extract_backfills_level():
    line = _line(_j({"level": "error", "message": "oops"}))
    assert JsonExtractPreprocessor().process([line])[0].level == "error"


def test_json_extract_does_not_override_existing_level():
    line = _line(_j({"level": "debug", "message": "hi"}), level="error")
    assert JsonExtractPreprocessor().process([line])[0].level == "error"


def test_json_extract_skips_absent_fields_silently():
    line = _line(_j({"message": "hello"}))
    content = JsonExtractPreprocessor().process([line])[0].content
    assert "message=hello" in content
    assert "error=" not in content


def test_json_extract_custom_fields():
    line = _line(_j({"req_id": "abc", "status": 500, "path": "/api"}))
    result = JsonExtractPreprocessor(
        config={"fields": ["req_id", "status", "path"]}
    ).process([line])
    content = result[0].content
    assert "req_id=abc" in content
    assert "status=500" in content


def test_json_extract_marks_metadata():
    line = _line(_j({"message": "ok"}))
    assert JsonExtractPreprocessor().process([line])[0].metadata.get("json_parsed") is True


def test_json_extract_mixed_batch():
    lines = [_line("plain"), _line(_j({"level": "error", "message": "boom"})), _line("also plain")]
    result = JsonExtractPreprocessor().process(lines)
    assert result[0].content == "plain"
    assert "message=boom" in result[1].content
    assert result[2].content == "also plain"
