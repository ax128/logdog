from __future__ import annotations

from logdog.pipeline.preprocessor.base import LogLine
from logdog.pipeline.preprocessor.dedup import DedupPreprocessor


def _line(content: str, **kw) -> LogLine:
    return LogLine(
        host_name=kw.get("host_name", "h"),
        container_id=kw.get("container_id", "c1"),
        container_name=kw.get("container_name", "app"),
        timestamp=kw.get("timestamp", "2026-04-15T10:00:00Z"),
        content=content,
        level=kw.get("level"),
    )


def test_dedup_empty_input():
    assert DedupPreprocessor().process([]) == []


def test_dedup_no_repeats_passes_through():
    lines = [_line("a"), _line("b"), _line("c")]
    assert [l.content for l in DedupPreprocessor().process(lines)] == ["a", "b", "c"]


def test_dedup_run_within_threshold_kept_intact():
    lines = [_line("x")] * 3
    assert len(DedupPreprocessor().process(lines)) == 3


def test_dedup_run_longer_than_threshold_collapsed():
    lines = [_line("x")] * 10
    result = DedupPreprocessor().process(lines)
    assert len(result) == 2
    assert result[0].content == "x"
    assert result[1].content == "[上条日志重复 ×9 次，已折叠]"
    assert result[1].metadata is not None
    assert result[1].metadata.get("deduped") is True
    assert result[1].metadata.get("original_count") == 10


def test_dedup_custom_max_consecutive():
    lines = [_line("y")] * 5
    result = DedupPreprocessor(config={"max_consecutive": 2}).process(lines)
    assert len(result) == 2
    assert result[1].content == "[上条日志重复 ×4 次，已折叠]"


def test_dedup_invalid_config_clamped_to_one():
    # max_consecutive=0 should behave as 1 (clamp)
    lines = [_line("z")] * 3
    result = DedupPreprocessor(config={"max_consecutive": 0}).process(lines)
    assert len(result) == 2
    assert result[1].content == "[上条日志重复 ×2 次，已折叠]"


def test_dedup_non_consecutive_repeats_not_collapsed():
    lines = [_line("a"), _line("b"), _line("a"), _line("b")]
    assert [l.content for l in DedupPreprocessor().process(lines)] == ["a", "b", "a", "b"]


def test_dedup_multiple_runs_in_one_batch():
    lines = [_line("err")] * 5 + [_line("ok")] + [_line("err")] * 5
    result = DedupPreprocessor().process(lines)
    assert len(result) == 5
    assert result[0].content == "err"
    assert result[2].content == "ok"
    assert result[3].content == "err"


def test_dedup_summary_inherits_host_and_container():
    lines = [_line("x", host_name="prod", container_name="api")] * 5
    summary = DedupPreprocessor().process(lines)[1]
    assert summary.host_name == "prod"
    assert summary.container_name == "api"


def test_dedup_name():
    assert DedupPreprocessor.name == "dedup"
