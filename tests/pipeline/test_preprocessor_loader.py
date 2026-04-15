from __future__ import annotations

import importlib
import importlib.util
from pathlib import Path
from textwrap import dedent

import pytest


def _import_preprocessor_loader():
    spec = importlib.util.find_spec("logdog.pipeline.preprocessor.loader")
    assert spec is not None
    return importlib.import_module("logdog.pipeline.preprocessor.loader")


def _import_preprocessor_base():
    spec = importlib.util.find_spec("logdog.pipeline.preprocessor.base")
    assert spec is not None
    return importlib.import_module("logdog.pipeline.preprocessor.base")


def _write_module(path: Path, content: str) -> None:
    path.write_text(dedent(content).strip() + "\n", encoding="utf-8")


def test_load_preprocessors_returns_default_when_directory_empty(
    tmp_path: Path,
) -> None:
    base = _import_preprocessor_base()
    loader = _import_preprocessor_loader()

    preprocessors = loader.load_preprocessors(tmp_path / "config" / "preprocessors")
    lines = [
        base.LogLine(
            host_name="prod-a",
            container_id="abcdef123456",
            container_name="api",
            timestamp="2026-04-11T10:00:00Z",
            content="raw log line",
        )
    ]

    assert [preprocessor.name for preprocessor in preprocessors] == ["default"]
    assert preprocessors[0].process(lines) == lines


def test_load_preprocessors_skips_user_modules_by_default(tmp_path: Path) -> None:
    loader = _import_preprocessor_loader()
    config_dir = tmp_path / "config" / "preprocessors"
    config_dir.mkdir(parents=True)
    _write_module(
        config_dir / "10_custom.py",
        """
        from logdog.pipeline.preprocessor.base import BasePreprocessor


        class CustomPreprocessor(BasePreprocessor):
            name = "custom"

            def process(self, lines):
                return lines


        PREPROCESSOR = CustomPreprocessor()
        """,
    )

    preprocessors = loader.load_preprocessors(config_dir)

    assert [preprocessor.name for preprocessor in preprocessors] == ["default"]


def test_load_preprocessors_supports_explicit_enable_switch(tmp_path: Path) -> None:
    loader = _import_preprocessor_loader()
    config_dir = tmp_path / "config" / "preprocessors"
    config_dir.mkdir(parents=True)
    _write_module(
        config_dir / "10_custom.py",
        """
        from logdog.pipeline.preprocessor.base import BasePreprocessor


        class CustomPreprocessor(BasePreprocessor):
            name = "custom"

            def process(self, lines):
                return lines


        PREPROCESSOR = CustomPreprocessor()
        """,
    )

    preprocessors = loader.load_preprocessors(
        config_dir,
        enable_user_preprocessors=True,
    )

    assert [preprocessor.name for preprocessor in preprocessors] == ["default", "custom"]


def test_load_preprocessors_supports_class_or_instance_exports_calls_on_load_and_skips_invalid_modules(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    base = _import_preprocessor_base()
    loader = _import_preprocessor_loader()
    config_dir = tmp_path / "config" / "preprocessors"
    config_dir.mkdir(parents=True)

    _write_module(
        config_dir / "20_second.py",
        """
        from logdog.pipeline.preprocessor.base import BasePreprocessor


        class SecondPreprocessor(BasePreprocessor):
            name = "second"

            def process(self, lines):
                for line in lines:
                    line.content = line.content + "|second"
                return lines


        PREPROCESSOR = SecondPreprocessor()
        """,
    )
    _write_module(
        config_dir / "10_first.py",
        """
        from logdog.pipeline.preprocessor.base import BasePreprocessor


        class FirstPreprocessor(BasePreprocessor):
            name = "first"
            loaded = False

            def on_load(self) -> None:
                self.loaded = True

            def process(self, lines):
                for line in lines:
                    line.content = line.content + "|first"
                return lines


        PREPROCESSOR = FirstPreprocessor
        """,
    )
    _write_module(
        config_dir / "30_invalid.py",
        """
        PREPROCESSOR = object()
        """,
    )

    with caplog.at_level("WARNING"):
        preprocessors = loader.load_preprocessors(
            config_dir,
            enable_user_preprocessors=True,
        )

    lines = [
        base.LogLine(
            host_name="prod-a",
            container_id="abcdef123456",
            container_name="api",
            timestamp="2026-04-11T10:00:00Z",
            content="raw",
        )
    ]
    for preprocessor in preprocessors:
        lines = preprocessor.process(lines)

    assert [preprocessor.name for preprocessor in preprocessors] == [
        "default",
        "first",
        "second",
    ]
    assert getattr(preprocessors[1], "loaded", False) is True
    assert lines[0].content == "raw|first|second"
    assert any("30_invalid.py" in record.message for record in caplog.records)
