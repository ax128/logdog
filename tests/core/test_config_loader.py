from __future__ import annotations

from pathlib import Path
from textwrap import dedent

import pytest

from logdog.core.config import load_app_config, resolve_config_path


def _write_yaml(path: Path, content: str) -> Path:
    path.write_text(dedent(content).strip() + "\n", encoding="utf-8")
    return path


def test_load_app_config_reads_yaml_mapping(tmp_path: Path) -> None:
    config_path = _write_yaml(
        tmp_path / "logdog.yaml",
        """
        metrics:
          sample_interval_seconds: 45
          host_system:
            enabled: true
            sample_interval_seconds: 60
        hosts:
          - name: edge
            url: unix:///var/run/docker.sock
        """,
    )

    loaded = load_app_config(config_path)

    assert loaded["metrics"]["sample_interval_seconds"] == 45
    assert loaded["metrics"]["host_system"]["enabled"] is True
    assert loaded["metrics"]["host_system"]["sample_interval_seconds"] == 60
    assert loaded["hosts"] == [{"name": "edge", "url": "unix:///var/run/docker.sock"}]


def test_load_app_config_requires_mapping_root(tmp_path: Path) -> None:
    config_path = _write_yaml(
        tmp_path / "logdog.yaml",
        """
        - name: edge
          url: unix:///var/run/docker.sock
        """,
    )

    with pytest.raises(TypeError, match="mapping"):
        load_app_config(config_path)


def test_resolve_config_path_prefers_explicit_argument(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LOGDOG_CONFIG", "/tmp/from-env.yaml")

    assert resolve_config_path("/tmp/from-arg.yaml") == "/tmp/from-arg.yaml"


def test_resolve_config_path_uses_env_when_argument_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LOGDOG_CONFIG", "/tmp/from-env.yaml")

    assert resolve_config_path(None) == "/tmp/from-env.yaml"


def test_resolve_config_path_falls_back_to_default_relative_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("LOGDOG_CONFIG", raising=False)

    assert resolve_config_path(None) == "config/logdog.yaml"
