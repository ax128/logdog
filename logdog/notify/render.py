from __future__ import annotations

from pathlib import Path
from typing import Any

from logdog.llm.prompts.base import coerce_template_value


OUTPUT_TEMPLATES_DIR = Path(__file__).resolve().parents[2] / "templates" / "output"
_OUTPUT_TEMPLATE_NAMES = {
    "standard",
    "brief",
    "detailed",
    "ops_friendly",
    "business",
    "heartbeat",
}


def render_output(template_name: str, context: dict[str, Any] | None) -> str:
    canonical_name = _normalize_template_name(template_name)
    template_path = OUTPUT_TEMPLATES_DIR / f"{canonical_name}.md"
    if not template_path.is_file():
        raise FileNotFoundError(f"Output template file not found: {template_path}")

    template_text = template_path.read_text(encoding="utf-8")
    return template_text.format_map(_stringify_context(context or {})).strip()


def _normalize_template_name(template_name: str) -> str:
    normalized = (template_name or "").strip().lower().replace("-", "_")
    if normalized not in _OUTPUT_TEMPLATE_NAMES:
        raise ValueError(f"Unknown output template: {template_name!r}")
    return normalized


def _stringify_context(context: dict[str, Any]) -> dict[str, str]:
    return {key: coerce_template_value(value) for key, value in context.items()}
