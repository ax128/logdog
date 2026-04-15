from __future__ import annotations

import importlib.util
import logging
import os
from pathlib import Path
from typing import cast

from logwatch.pipeline.preprocessor.base import BasePreprocessor
from logwatch.pipeline.preprocessor.default import DefaultPreprocessor


logger = logging.getLogger(__name__)
DEFAULT_PREPROCESSORS_DIR = (
    Path(__file__).resolve().parents[3] / "config" / "preprocessors"
)
_ENABLE_ENV_VAR = "LOGWATCH_ENABLE_USER_PREPROCESSORS"
_ENV_TRUTHY_VALUES = frozenset({"1", "true", "yes", "on"})


def load_preprocessors(
    directory: str | Path = DEFAULT_PREPROCESSORS_DIR,
    *,
    enable_user_preprocessors: bool | None = None,
) -> list[BasePreprocessor]:
    preprocessors: list[BasePreprocessor] = [
        _initialize_preprocessor(DefaultPreprocessor())
    ]
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


def _load_preprocessor_from_file(module_path: Path) -> BasePreprocessor | None:
    spec = importlib.util.spec_from_file_location(
        f"logwatch_user_preprocessor_{module_path.stem}",
        module_path,
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
    preprocessor = _coerce_preprocessor(candidate)
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


def _coerce_preprocessor(candidate: object) -> BasePreprocessor | None:
    if isinstance(candidate, BasePreprocessor):
        return candidate
    if isinstance(candidate, type) and issubclass(candidate, BasePreprocessor):
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
