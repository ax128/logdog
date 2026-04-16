from __future__ import annotations

import re

from logdog.pipeline.preprocessor.base import BasePreprocessor, LogLine


_DEFAULT_FIELDS = [
    "level",
    "severity",
    "message",
    "msg",
    "error",
    "trace_id",
    "request_id",
    "method",
    "path",
    "status",
    "latency_ms",
]
_DEFAULT_LEVEL_KEYS = ["level", "severity", "lvl"]
_KNOWN_LEVELS = frozenset(
    {
        "debug",
        "info",
        "warn",
        "warning",
        "error",
        "fatal",
        "panic",
        "critical",
    }
)
_PAIR_RE = re.compile(
    r"(?P<key>[A-Za-z_][A-Za-z0-9_.-]*)=(?P<value>\"(?:[^\"\\]|\\.)*\"|'(?:[^'\\]|\\.)*'|\S+)"
)


def _coerce_list(value: object, fallback: list[str]) -> list[str]:
    if isinstance(value, (list, tuple)):
        out = [str(item).strip() for item in value if str(item).strip() != ""]
        if out:
            return out
    return list(fallback)


def _unquote_value(raw: str) -> str:
    text = str(raw)
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {"\"", "'"}:
        body = text[1:-1]
        if text[0] == '"':
            body = body.replace(r'\"', '"')
        if text[0] == "'":
            body = body.replace(r"\'", "'")
        body = body.replace(r"\\", "\\")
        return body
    return text


def _parse_kv_pairs(content: str) -> tuple[dict[str, str], list[str]]:
    pairs: dict[str, str] = {}
    key_order: list[str] = []
    for match in _PAIR_RE.finditer(content):
        key = str(match.group("key")).strip()
        value = _unquote_value(str(match.group("value")))
        if key == "":
            continue
        if key not in pairs:
            key_order.append(key)
        pairs[key] = value
    return pairs, key_order


class KvExtractPreprocessor(BasePreprocessor):
    """Parse plain-text key=value logs into normalized structured text.

    Config keys:
        fields (list[str], default common observability fields):
            preferred key order in output.
        include_extra_fields (bool, default False):
            append keys not listed in fields after preferred keys.
        level_keys (list[str], default ["level","severity","lvl"]):
            keys used for LogLine.level backfill when level is missing.

    Example input:
        level=error msg="db timeout" trace_id=abc status=504 path=/pay

    Output:
        level=error msg=db timeout trace_id=abc status=504 path=/pay
    """

    name = "kv_extract"

    def __init__(self, config: dict | None = None) -> None:
        cfg = config or {}
        self._fields = _coerce_list(cfg.get("fields"), _DEFAULT_FIELDS)
        self._include_extra_fields = bool(cfg.get("include_extra_fields", False))
        self._level_keys = _coerce_list(cfg.get("level_keys"), _DEFAULT_LEVEL_KEYS)

    def process(self, lines: list[LogLine]) -> list[LogLine]:
        result: list[LogLine] = []
        for line in lines:
            pairs, key_order = _parse_kv_pairs(line.content)
            if not pairs:
                result.append(line)
                continue

            selected_keys = [key for key in self._fields if key in pairs]
            if self._include_extra_fields:
                selected_keys.extend(
                    [key for key in key_order if key not in set(selected_keys)]
                )

            parts = [
                f"{key}={pairs[key]}"
                for key in selected_keys
                if pairs.get(key) not in {None, ""}
            ]
            text = " ".join(parts).strip() or line.content

            extracted_level = None
            for key in self._level_keys:
                raw = str(pairs.get(key) or "").strip().lower()
                if raw in _KNOWN_LEVELS:
                    extracted_level = raw
                    break

            result.append(
                LogLine(
                    host_name=line.host_name,
                    container_id=line.container_id,
                    container_name=line.container_name,
                    timestamp=line.timestamp,
                    content=text,
                    level=line.level if line.level is not None else extracted_level,
                    metadata={
                        **(line.metadata or {}),
                        "kv_parsed": True,
                        "kv_keys": len(pairs),
                    },
                )
            )
        return result
