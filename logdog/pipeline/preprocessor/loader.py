from __future__ import annotations

import importlib.util
import logging
import os
from pathlib import Path
from typing import Any, cast

from logdog.pipeline.preprocessor.base import BasePreprocessor
from logdog.pipeline.preprocessor.default import DefaultPreprocessor


logger = logging.getLogger(__name__)

# Directory for user-defined custom preprocessor scripts (alongside prompt templates).
PREPROCESSORS_TEMPLATE_DIR = (
    Path(__file__).resolve().parents[3] / "templates" / "preprocessors"
)

# Legacy directory kept for backward-compatibility (auto-load when env var set).
DEFAULT_PREPROCESSORS_DIR = (
    Path(__file__).resolve().parents[3] / "config" / "preprocessors"
)

_ENABLE_ENV_VAR = "LOGDOG_ENABLE_USER_PREPROCESSORS"
_ENV_TRUTHY_VALUES = frozenset({"1", "true", "yes", "on"})


def _builtin_registry() -> dict[str, type[BasePreprocessor]]:
    """Lazy import to avoid circular dependencies at module load time."""
    from logdog.pipeline.preprocessor.dedup import DedupPreprocessor
    from logdog.pipeline.preprocessor.head_tail import HeadTailPreprocessor
    from logdog.pipeline.preprocessor.json_extract import JsonExtractPreprocessor
    from logdog.pipeline.preprocessor.level_filter import LevelFilterPreprocessor

    return {
        "dedup": DedupPreprocessor,
        "head_tail": HeadTailPreprocessor,
        "json_extract": JsonExtractPreprocessor,
        "level_filter": LevelFilterPreprocessor,
    }


def load_builtin_preprocessors(
    configs: list[dict[str, Any]],
    *,
    templates_dir: Path | str | None = None,
) -> list[BasePreprocessor]:
    """Resolve and instantiate preprocessors from a list of config dicts.

    Name resolution order for each {"name": "..."} entry:
    1. Built-in registry (dedup, level_filter, json_extract, head_tail).
    2. templates/preprocessors/<name>.py (or templates_dir override).
    Unknown names are skipped with a warning.
    """
    registry = _builtin_registry()
    tdir = (
        Path(templates_dir) if templates_dir is not None else PREPROCESSORS_TEMPLATE_DIR
    )
    result: list[BasePreprocessor] = []
    for cfg in configs:
        name = str(cfg.get("name") or "").strip()
        if not name:
            continue
        preprocessor = _resolve_one(name, cfg, registry, tdir)
        if preprocessor is None:
            logger.warning("unknown preprocessor %r — not in built-ins or %s", name, tdir)
            continue
        result.append(preprocessor)
    return result


def _resolve_one(
    name: str,
    cfg: dict[str, Any],
    registry: dict[str, type[BasePreprocessor]],
    templates_dir: Path,
) -> BasePreprocessor | None:
    # 1. Built-in registry takes priority.
    cls = registry.get(name)
    if cls is not None:
        try:
            return _initialize_preprocessor(cls(config=cfg))
        except Exception as exc:  # noqa: BLE001
            logger.warning("failed to init built-in preprocessor %r: %s", name, exc)
            return None

    # 2. File fallback: templates/preprocessors/<name>.py
    script_path = templates_dir / f"{name}.py"
    if script_path.is_file():
        return _load_preprocessor_from_file(script_path, config=cfg)

    return None


def load_preprocessors(
    directory: str | Path = DEFAULT_PREPROCESSORS_DIR,
    *,
    enable_user_preprocessors: bool | None = None,
    builtin_configs: list[dict[str, Any]] | None = None,
    templates_dir: Path | str | None = None,
) -> list[BasePreprocessor]:
    """Return the full preprocessor chain for a host/watcher.

    Chain order:
    1. DefaultPreprocessor (always first, identity transform).
    2. Named built-ins and custom file preprocessors from builtin_configs.
    3. Legacy auto-loaded user modules from directory (opt-in via env var).
    """
    preprocessors: list[BasePreprocessor] = [
        _initialize_preprocessor(DefaultPreprocessor())
    ]
    if builtin_configs:
        preprocessors.extend(
            load_builtin_preprocessors(builtin_configs, templates_dir=templates_dir)
        )

    preprocessor_dir = Path(directory)
    if not preprocessor_dir.exists() or not preprocessor_dir.is_dir():
        return preprocessors
    if not _resolve_user_preprocessors_enabled(enable_user_preprocessors):
        return preprocessors

    for module_path in sorted(preprocessor_dir.glob("*.py")):
        if module_path.name == "__init__.py":
            continue
        if module_path.is_symlink():
            logger.warning("Skipping symlink preprocessor module: %s", module_path.name)
            continue
        loaded = _load_preprocessor_from_file(module_path)
        if loaded is not None:
            preprocessors.append(loaded)
    return preprocessors


def _load_preprocessor_from_file(
    module_path: Path,
    *,
    config: dict[str, Any] | None = None,
) -> BasePreprocessor | None:
    spec = importlib.util.spec_from_file_location(
        f"logdog_user_preprocessor_{module_path.stem}", module_path
    )
    if spec is None or spec.loader is None:
        logger.warning("Skipping invalid preprocessor module: %s", module_path.name)
        return None
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Skipping invalid preprocessor module: %s (%s)", module_path.name, exc
        )
        return None
    candidate = getattr(module, "PREPROCESSOR", None)
    try:
        preprocessor = _coerce_preprocessor(candidate, config=config)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Skipping invalid preprocessor module: %s (%s)", module_path.name, exc
        )
        return None
    if preprocessor is None:
        logger.warning("Skipping invalid preprocessor module: %s", module_path.name)
        return None
    try:
        return _initialize_preprocessor(preprocessor)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Skipping invalid preprocessor module: %s (%s)", module_path.name, exc
        )
        return None


def _coerce_preprocessor(
    candidate: object,
    *,
    config: dict[str, Any] | None = None,
) -> BasePreprocessor | None:
    if isinstance(candidate, BasePreprocessor):
        return candidate
    if isinstance(candidate, type) and issubclass(candidate, BasePreprocessor):
        try:
            return cast(BasePreprocessor, candidate(config=config))
        except TypeError:
            return cast(BasePreprocessor, candidate())
    return None


def _initialize_preprocessor(preprocessor: BasePreprocessor) -> BasePreprocessor:
    preprocessor.on_load()
    return preprocessor


def _resolve_user_preprocessors_enabled(enabled: bool | None) -> bool:
    if enabled is not None:
        return bool(enabled)
    raw = os.getenv(_ENABLE_ENV_VAR, "").strip().lower()
    return raw in _ENV_TRUTHY_VALUES
