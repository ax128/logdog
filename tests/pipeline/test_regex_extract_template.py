from __future__ import annotations

from pathlib import Path

from logdog.pipeline.preprocessor.base import LogLine
from logdog.pipeline.preprocessor.loader import load_builtin_preprocessors


def _line(content: str, level: str | None = None) -> LogLine:
    return LogLine(
        host_name="h",
        container_id="c",
        container_name="app",
        timestamp="2026-04-15T10:00:00Z",
        content=content,
        level=level,
    )


def test_regex_extract_skips_invalid_patterns_and_keeps_valid_one(monkeypatch) -> None:
    monkeypatch.setenv("LOGDOG_ENABLE_TEMPLATE_PREPROCESSORS", "1")
    templates_dir = Path(__file__).resolve().parents[2] / "templates" / "preprocessors"
    configs = [
        {
            "name": "regex_extract",
            "patterns": [
                r"(?P<broken",  # invalid regex
                r"level=(?P<level>\w+)\s+msg=(?P<message>.+)",
            ],
            "fields": ["level", "message"],
        }
    ]

    preprocessors = load_builtin_preprocessors(configs, templates_dir=templates_dir)

    assert len(preprocessors) == 1
    result = preprocessors[0].process([_line("level=error msg=database timeout")])[0]
    assert "level=error" in result.content
    assert "message=database timeout" in result.content
    assert result.level == "error"
