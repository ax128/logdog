from __future__ import annotations

from dataclasses import dataclass
from typing import Any


DEFAULT_DANGEROUS_TOOLS = frozenset({"restart_container"})
DEFAULT_CONFIRMATION_FIELDS = ("confirmed", "confirmation")
TRUTHY_CONFIRMATION_VALUES = frozenset({"1", "true", "yes", "confirm", "confirmed"})


@dataclass(frozen=True, slots=True)
class ToolPermissionPolicy:
    dangerous_tools: frozenset[str] = DEFAULT_DANGEROUS_TOOLS
    dangerous_host_allowlist: frozenset[str] = frozenset()
    confirmation_fields: tuple[str, ...] = DEFAULT_CONFIRMATION_FIELDS


def load_permission_policy(app_config: dict[str, Any] | None) -> ToolPermissionPolicy:
    root_cfg = app_config if isinstance(app_config, dict) else {}
    llm_value = root_cfg.get("llm")
    llm_cfg = llm_value if isinstance(llm_value, dict) else {}
    permissions_value = llm_cfg.get("permissions")
    permissions_cfg = permissions_value if isinstance(permissions_value, dict) else {}

    dangerous_tools_raw = permissions_cfg.get(
        "dangerous_tools",
        permissions_cfg.get("dangerous_actions", tuple(DEFAULT_DANGEROUS_TOOLS)),
    )
    allowlist_raw = permissions_cfg.get("dangerous_host_allowlist", ())
    confirmation_fields_raw = permissions_cfg.get(
        "confirmation_fields", DEFAULT_CONFIRMATION_FIELDS
    )

    return ToolPermissionPolicy(
        dangerous_tools=_normalize_str_collection(
            dangerous_tools_raw,
            default=tuple(DEFAULT_DANGEROUS_TOOLS),
        ),
        dangerous_host_allowlist=_normalize_str_collection(allowlist_raw),
        confirmation_fields=tuple(
            _normalize_str_collection(
                confirmation_fields_raw,
                default=DEFAULT_CONFIRMATION_FIELDS,
            )
        )
        or DEFAULT_CONFIRMATION_FIELDS,
    )


def ensure_tool_allowed(
    tool_name: str,
    arguments: dict[str, Any] | None,
    *,
    policy: ToolPermissionPolicy,
) -> None:
    normalized_tool_name = str(tool_name or "").strip()
    if normalized_tool_name == "":
        raise ValueError("tool_name must not be empty")

    normalized_args = arguments if isinstance(arguments, dict) else {}
    if normalized_tool_name not in policy.dangerous_tools:
        return

    host = str(normalized_args.get("host") or "").strip()
    if host == "":
        raise PermissionError("dangerous tool requires host")
    if host not in policy.dangerous_host_allowlist:
        raise PermissionError(f"host {host!r} is not in dangerous action allowlist")
    if not has_explicit_confirmation(normalized_args, policy=policy):
        raise PermissionError("dangerous tool requires explicit confirmation")


def has_explicit_confirmation(
    arguments: dict[str, Any] | None,
    *,
    policy: ToolPermissionPolicy,
) -> bool:
    normalized_args = arguments if isinstance(arguments, dict) else {}
    for field in policy.confirmation_fields:
        value = normalized_args.get(field)
        if value is True:
            return True
        if (
            isinstance(value, str)
            and value.strip().lower() in TRUTHY_CONFIRMATION_VALUES
        ):
            return True
    return False


def _normalize_str_collection(
    raw_value: Any,
    *,
    default: tuple[str, ...] = (),
) -> frozenset[str]:
    if raw_value is None:
        items: tuple[Any, ...] = default
    elif isinstance(raw_value, (list, tuple, set, frozenset)):
        items = tuple(raw_value)
    else:
        items = (raw_value,)

    normalized = {str(item).strip() for item in items if str(item).strip() != ""}
    return frozenset(normalized)
