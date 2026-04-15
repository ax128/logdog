from __future__ import annotations

from dataclasses import dataclass
from fnmatch import fnmatch
from typing import Any


@dataclass(frozen=True, slots=True)
class NotifyRoutingPolicy:
    default_channels: tuple[str, ...]
    rules: tuple[dict[str, Any], ...]

    def resolve(
        self,
        *,
        host: str,
        category: str,
        context: dict[str, Any] | None,
    ) -> dict[str, Any]:
        normalized_host = str(host or "").strip()
        normalized_category = str(category or "").strip().upper()
        normalized_context = dict(context or {})
        container_name = str(normalized_context.get("container_name") or "").strip()
        container_id = str(normalized_context.get("container_id") or "").strip()

        for rule in self.rules:
            match_cfg = rule.get("match") or {}
            if _matches_rule(
                match_cfg=match_cfg,
                host=normalized_host,
                category=normalized_category,
                container_name=container_name,
                container_id=container_id,
            ):
                deliver_cfg = dict(rule.get("deliver") or {})
                channels = _normalize_channels(deliver_cfg.get("channels"))
                if channels:
                    return {
                        "channels": channels,
                        "message_mode": _normalize_optional_message_mode(
                            deliver_cfg.get("message_mode")
                        ),
                        "rule": str(rule.get("name") or ""),
                    }

        return {
            "channels": list(self.default_channels),
            "message_mode": None,
            "rule": "default",
        }


def build_notify_routing_policy(config: dict[str, Any] | None) -> NotifyRoutingPolicy:
    cfg = dict(config or {})
    default_channels = tuple(_normalize_channels(cfg.get("default_channels")))
    raw_rules = cfg.get("rules")
    if raw_rules is None:
        rules_list: list[dict[str, Any]] = []
    elif isinstance(raw_rules, list):
        rules_list = [dict(item) for item in raw_rules if isinstance(item, dict)]
    else:
        raise TypeError("notify.routing.rules must be a list")

    sorted_rules = sorted(
        rules_list,
        key=lambda item: int(item.get("priority") or 0),
        reverse=True,
    )
    return NotifyRoutingPolicy(
        default_channels=default_channels, rules=tuple(sorted_rules)
    )


def _matches_rule(
    *,
    match_cfg: dict[str, Any],
    host: str,
    category: str,
    container_name: str,
    container_id: str,
) -> bool:
    host_patterns = _normalize_patterns(match_cfg.get("hosts"))
    if host_patterns and not _matches_any_pattern(host, host_patterns):
        return False

    category_patterns = [
        item.upper() for item in _normalize_patterns(match_cfg.get("categories"))
    ]
    if category_patterns and not _matches_any_pattern(category, category_patterns):
        return False

    container_patterns = _normalize_patterns(match_cfg.get("containers"))
    if container_patterns:
        if not (
            _matches_any_pattern(container_name, container_patterns)
            or _matches_any_pattern(container_id, container_patterns)
        ):
            return False

    return True


def _normalize_patterns(value: Any) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, (list, tuple)):
        raise TypeError("notify.routing.match fields must be list")
    return [str(item).strip() for item in value if str(item).strip() != ""]


def _matches_any_pattern(value: str, patterns: list[str]) -> bool:
    if value.strip() == "":
        return False
    return any(fnmatch(value, pattern) for pattern in patterns)


def _normalize_channels(value: Any) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, (list, tuple)):
        raise TypeError("notify.routing channels must be a list")
    return [str(item).strip().lower() for item in value if str(item).strip() != ""]


def _normalize_optional_message_mode(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    if text == "":
        return None
    return text
