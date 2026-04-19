from __future__ import annotations

import re
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Iterable


@dataclass(frozen=True, slots=True)
class RuleResult:
    triggered: bool
    redacted_line: str
    matched_category: str | None


_DEFAULT_KEYWORDS: tuple[str, ...] = (
    "error",
    "fatal",
    "panic",
    "timeout",
    "failed",
    "oom",
)
_MAX_PATTERN_LENGTH = 1024


def _validate_pattern(pattern: str) -> str:
    if len(pattern) > _MAX_PATTERN_LENGTH:
        raise ValueError(
            f"regex pattern too long: {len(pattern)} > {_MAX_PATTERN_LENGTH}"
        )
    return pattern


@lru_cache(maxsize=512)
def _compile_pattern(pattern: str) -> re.Pattern[str]:
    return re.compile(_validate_pattern(pattern))


def _search_pattern(pattern: str, text: str) -> bool:
    return _compile_pattern(pattern).search(text) is not None


def _sub_pattern(pattern: str, replace: str, text: str) -> str:
    return _compile_pattern(pattern).sub(replace, text)


def _as_pattern(item: Any) -> str:
    if isinstance(item, str):
        return item
    if isinstance(item, dict):
        p = item.get("pattern")
        if isinstance(p, str):
            return p
    raise TypeError(
        f"invalid rule item: expected str|dict with 'pattern', got {type(item)!r}"
    )


def _iter_list(x: Any) -> list[Any]:
    if x is None:
        return []
    if isinstance(x, list):
        return x
    # Be forgiving for configs that use tuples.
    if isinstance(x, tuple):
        return list(x)
    raise TypeError(f"invalid config list: got {type(x)!r}")


def apply_rules(line: str, config: dict) -> RuleResult:
    """
    Rule order (MUST):
    1) ignore hit -> return not triggered (no redact, no alert)
    2) redact replacements
    3) custom_alerts check
    4) system keyword fallback
    """
    rules = config.get("rules") or {}

    ignore_rules = _iter_list(rules.get("ignore"))
    for r in ignore_rules:
        if _search_pattern(_as_pattern(r), line):
            return RuleResult(
                triggered=False, redacted_line=line, matched_category=None
            )

    redacted = line
    redact_rules = _iter_list(rules.get("redact"))
    for r in redact_rules:
        if isinstance(r, dict):
            pattern = r.get("pattern")
            replace = r.get("replace", "")
        elif isinstance(r, (list, tuple)) and len(r) == 2:
            pattern, replace = r
        else:
            raise TypeError(
                "invalid redact rule: expected dict with pattern/replace or (pattern, replace) tuple"
            )
        if not isinstance(pattern, str) or not isinstance(replace, str):
            raise TypeError("invalid redact rule: pattern/replace must be str")
        redacted = _sub_pattern(pattern, replace, redacted)

    custom_alerts = _iter_list(rules.get("custom_alerts"))
    for a in custom_alerts:
        if not isinstance(a, dict):
            raise TypeError("invalid custom_alerts rule: expected dict")
        pattern = a.get("pattern")
        if not isinstance(pattern, str):
            raise TypeError("invalid custom_alerts rule: missing str 'pattern'")
        if _search_pattern(pattern, redacted):
            category = a.get("category")
            if category is None:
                category = "BUSINESS"
            elif not isinstance(category, str):
                raise TypeError(
                    "invalid custom_alerts rule: 'category' must be str|None"
                )
            elif category.strip() == "":
                category = "BUSINESS"
            return RuleResult(
                triggered=True, redacted_line=redacted, matched_category=category
            )

    ak = rules.get("alert_keywords")
    if ak is None:
        ak = config.get("alert_keywords")
    if ak is None:
        keywords: Iterable[str] = _DEFAULT_KEYWORDS
    elif isinstance(ak, str):
        raise TypeError("alert_keywords must be a list/tuple of strings, not str")
    elif isinstance(ak, (list, tuple)):
        keywords = ak
    else:
        raise TypeError("alert_keywords must be a list/tuple of strings")

    # Validate items eagerly so configs with invalid elements fail fast
    # (even if an earlier keyword would have matched).
    for kw in keywords:
        if not isinstance(kw, str):
            raise TypeError("alert_keywords items must be str")

    lower = redacted.lower()
    for kw in keywords:
        if kw == "":
            continue
        if kw.lower() in lower:
            # Keep categories minimal but useful for per-category cooldown.
            if kw.lower() == "oom":
                category = "OOM"
            else:
                category = "ERROR"
            return RuleResult(
                triggered=True, redacted_line=redacted, matched_category=category
            )

    return RuleResult(triggered=False, redacted_line=redacted, matched_category=None)
