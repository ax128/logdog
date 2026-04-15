from __future__ import annotations

from logdog.pipeline.preprocessor.base import LogLine
from logdog.pipeline.preprocessor.head_tail import HeadTailPreprocessor


def _lines(n: int) -> list[LogLine]:
    return [
        LogLine(host_name="h", container_id="c", container_name="app",
                timestamp="2026-04-15T10:00:00Z", content=f"line {i}")
        for i in range(n)
    ]


def test_head_tail_empty():
    assert HeadTailPreprocessor().process([]) == []


def test_head_tail_name():
    assert HeadTailPreprocessor.name == "head_tail"


def test_head_tail_short_batch_passes_through():
    lines = _lines(30)   # default head+tail=40
    assert HeadTailPreprocessor().process(lines) == lines


def test_head_tail_exactly_threshold_passes_through():
    assert HeadTailPreprocessor().process(_lines(40)) == _lines(40)


def test_head_tail_long_batch_inserts_marker():
    lines = _lines(100)
    result = HeadTailPreprocessor().process(lines)
    assert len(result) == 41              # 20 + marker + 20
    assert result[0].content == "line 0"
    assert result[19].content == "line 19"
    assert "省略" in result[20].content
    assert result[21].content == "line 80"
    assert result[-1].content == "line 99"


def test_head_tail_marker_metadata():
    result = HeadTailPreprocessor().process(_lines(100))
    marker = result[20]
    assert "60" in marker.content         # 100 − 20 − 20 = 60 dropped
    assert marker.metadata.get("head_tail_marker") is True
    assert marker.metadata.get("dropped") == 60


def test_head_tail_custom_head_tail():
    lines = _lines(50)
    result = HeadTailPreprocessor(config={"head": 5, "tail": 5}).process(lines)
    assert len(result) == 11
    assert result[0].content == "line 0"
    assert result[4].content == "line 4"
    assert result[6].content == "line 45"
    assert result[-1].content == "line 49"


def test_head_tail_marker_inherits_container_info():
    result = HeadTailPreprocessor(config={"head": 5, "tail": 5}).process(_lines(50))
    marker = result[5]
    assert marker.host_name == "h"
    assert marker.container_name == "app"
