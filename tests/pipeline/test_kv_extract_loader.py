from __future__ import annotations

from logdog.pipeline.preprocessor.loader import load_builtin_preprocessors


def test_load_builtin_kv_extract() -> None:
    result = load_builtin_preprocessors([{"name": "kv_extract"}], templates_dir=None)
    assert len(result) == 1
    assert result[0].name == "kv_extract"
