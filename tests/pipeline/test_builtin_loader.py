from __future__ import annotations

from pathlib import Path
from textwrap import dedent

from logdog.pipeline.preprocessor.base import LogLine
from logdog.pipeline.preprocessor.loader import load_builtin_preprocessors, load_preprocessors


def _line(content: str) -> LogLine:
    return LogLine(host_name="h", container_id="c", container_name="app",
                   timestamp="t", content=content)


# --- load_builtin_preprocessors: built-in names ---

def test_load_builtin_empty_list():
    assert load_builtin_preprocessors([], templates_dir=None) == []


def test_load_builtin_dedup():
    result = load_builtin_preprocessors([{"name": "dedup"}], templates_dir=None)
    assert result[0].name == "dedup"


def test_load_builtin_level_filter():
    result = load_builtin_preprocessors([{"name": "level_filter"}], templates_dir=None)
    assert result[0].name == "level_filter"


def test_load_builtin_json_extract():
    result = load_builtin_preprocessors([{"name": "json_extract"}], templates_dir=None)
    assert result[0].name == "json_extract"


def test_load_builtin_head_tail():
    result = load_builtin_preprocessors([{"name": "head_tail"}], templates_dir=None)
    assert result[0].name == "head_tail"


def test_load_builtin_unknown_name_skipped_with_warning(caplog, tmp_path):
    with caplog.at_level("WARNING"):
        result = load_builtin_preprocessors([{"name": "nonexistent"}], templates_dir=tmp_path)
    assert result == []
    assert any("nonexistent" in r.message for r in caplog.records)


def test_load_builtin_multiple_in_order():
    result = load_builtin_preprocessors(
        [{"name": "dedup"}, {"name": "level_filter"}],
        templates_dir=None,
    )
    assert [p.name for p in result] == ["dedup", "level_filter"]


# --- load_builtin_preprocessors: file fallback ---

def _write_script(path: Path, name: str) -> None:
    path.write_text(dedent(f"""
        from logdog.pipeline.preprocessor.base import BasePreprocessor

        class _{name.capitalize()}Preprocessor(BasePreprocessor):
            name = "{name}"
            def process(self, lines):
                return [l for l in lines if "drop" not in l.content]

        PREPROCESSOR = _{name.capitalize()}Preprocessor
    """).strip() + "\n")


def test_load_builtin_file_fallback(tmp_path):
    _write_script(tmp_path / "custom_filter.py", "custom_filter")
    result = load_builtin_preprocessors(
        [{"name": "custom_filter"}], templates_dir=tmp_path
    )
    assert len(result) == 1
    assert result[0].name == "custom_filter"


def test_load_builtin_file_fallback_applies_logic(tmp_path):
    _write_script(tmp_path / "custom_filter.py", "custom_filter")
    result = load_builtin_preprocessors(
        [{"name": "custom_filter"}], templates_dir=tmp_path
    )
    lines = [_line("keep this"), _line("drop this")]
    out = result[0].process(lines)
    assert len(out) == 1
    assert out[0].content == "keep this"


def test_load_builtin_builtin_takes_priority_over_file(tmp_path):
    # Even if a file named "dedup.py" exists, the built-in class wins
    _write_script(tmp_path / "dedup.py", "dedup")
    result = load_builtin_preprocessors([{"name": "dedup"}], templates_dir=tmp_path)
    # Should be the real DedupPreprocessor, not the file
    from logdog.pipeline.preprocessor.dedup import DedupPreprocessor
    assert isinstance(result[0], DedupPreprocessor)


# --- load_preprocessors integration ---

def test_load_preprocessors_backward_compat(tmp_path):
    # No builtin_configs → only default
    result = load_preprocessors(tmp_path / "empty")
    assert [p.name for p in result] == ["default"]


def test_load_preprocessors_with_builtin_configs(tmp_path):
    result = load_preprocessors(tmp_path / "empty", builtin_configs=[{"name": "dedup"}])
    assert [p.name for p in result] == ["default", "dedup"]


def test_load_preprocessors_with_file_based_custom(tmp_path):
    _write_script(tmp_path / "my_filter.py", "my_filter")
    result = load_preprocessors(
        tmp_path / "empty",
        builtin_configs=[{"name": "my_filter"}],
        templates_dir=tmp_path,
    )
    assert [p.name for p in result] == ["default", "my_filter"]
