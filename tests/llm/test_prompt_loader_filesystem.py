from __future__ import annotations

from pathlib import Path
from textwrap import dedent

import pytest

import logdog.llm.prompts.loader as prompt_loader
from logdog.llm.prompts.loader import load_template


def test_load_template_reads_non_alert_prompt_from_filesystem() -> None:
    tmpl = load_template("default_interval")

    rendered = tmpl.render(
        {
            "scene": "interval",
            "host_name": "prod-a",
            "container_name": "api",
            "timestamp": "2026-04-11T10:05:00Z",
            "logs": ["line1", "line2"],
            "metrics": "cpu=92% mem=512MiB",
        }
    )

    assert "prod-a" in rendered
    assert "api" in rendered
    assert "2026-04-11T10:05:00Z" in rendered
    assert "line1\nline2" in rendered
    assert "cpu=92% mem=512MiB" in rendered


def test_load_template_reads_custom_prompt_name_from_filesystem(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    prompts_dir = tmp_path / "templates" / "prompts"
    prompts_dir.mkdir(parents=True)
    (prompts_dir / "quant_alert.md").write_text(
        dedent(
            """
            Quant alert
            Host: {host_name}
            Container: {container_name}
            Time: {timestamp}
            Logs:
            {logs}
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(prompt_loader, "PROMPTS_DIR", prompts_dir, raising=False)

    tmpl = load_template("quant_alert")

    rendered = tmpl.render(
        {
            "scene": "alert",
            "host_name": "prod-quant",
            "container_name": "matcher",
            "timestamp": "2026-04-11T10:00:00Z",
            "logs": ["fill failed"],
        }
    )

    assert "Quant alert" in rendered
    assert "prod-quant" in rendered
    assert "matcher" in rendered
    assert "fill failed" in rendered


def test_load_template_falls_back_to_builtin_default_alert_when_file_missing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        prompt_loader,
        "PROMPTS_DIR",
        tmp_path / "templates" / "prompts",
        raising=False,
    )

    tmpl = load_template("default_alert")

    rendered = tmpl.render(
        {
            "scene": "alert",
            "host_name": "prod-a",
            "container_name": "api",
            "timestamp": "2026-04-11T10:00:00Z",
            "logs": ["line1"],
        }
    )

    assert "prod-a" in rendered
    assert "api" in rendered
    assert "2026-04-11T10:00:00Z" in rendered
    assert rendered.endswith("line1")


@pytest.mark.parametrize(
    ("template_name", "context", "expected_snippets"),
    [
        (
            "default_interval",
            {
                "scene": "interval",
                "host_name": "prod-a",
                "container_name": "api",
                "timestamp": "2026-04-11T10:00:00Z",
                "logs": ["line1"],
                "metrics": "cpu=95%",
            },
            ["prod-a", "api", "2026-04-11T10:00:00Z", "cpu=95%"],
        ),
        (
            "default_heartbeat",
            {
                "scene": "heartbeat",
                "host_name": "prod-a",
                "timestamp": "2026-04-11T10:00:00Z",
                "container_status": "api=ok",
                "total_containers": 2,
            },
            ["prod-a", "2026-04-11T10:00:00Z", "api=ok", "2"],
        ),
    ],
)
def test_load_template_falls_back_to_builtin_non_alert_defaults_when_file_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    template_name: str,
    context: dict[str, object],
    expected_snippets: list[str],
) -> None:
    monkeypatch.setattr(
        prompt_loader,
        "PROMPTS_DIR",
        tmp_path / "templates" / "prompts",
        raising=False,
    )

    tmpl = load_template(template_name)

    rendered = tmpl.render(context)

    for snippet in expected_snippets:
        assert snippet in rendered


def test_load_template_rejects_invalid_template_name() -> None:
    with pytest.raises(ValueError, match="Invalid prompt template name"):
        load_template("../not_a_real_template")


def test_load_template_raises_for_missing_valid_custom_template_name(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    prompts_dir = tmp_path / "templates" / "prompts"
    prompts_dir.mkdir(parents=True)
    monkeypatch.setattr(prompt_loader, "PROMPTS_DIR", prompts_dir, raising=False)

    with pytest.raises(FileNotFoundError, match="quant_missing"):
        load_template("quant_missing")
