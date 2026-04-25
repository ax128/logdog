from __future__ import annotations

import hashlib
import hmac
import json
import threading
import time
from dataclasses import dataclass, field
from typing import Any


DEFAULT_DANGEROUS_TOOLS = frozenset({"restart_container", "exec_container"})
DEFAULT_CONFIRMATION_FIELDS = ("confirmed", "confirmation")
DEFAULT_APPROVAL_TOKEN_TTL_SECONDS = 300
APPROVAL_TOKEN_FIELD = "approval_token"
APPROVAL_TOKEN_VERSION = "v1"


class ConsumedTokenStore:
    """Thread-safe store that tracks consumed approval-token signatures."""

    __slots__ = ("_consumed", "_lock")

    def __init__(self) -> None:
        self._consumed: dict[str, float] = {}
        self._lock = threading.Lock()

    def consume(self, signature: str, expires_at: float) -> bool:
        with self._lock:
            self._prune(expires_at)
            if signature in self._consumed:
                return False
            self._consumed[signature] = expires_at
            return True

    def prune(self, now: float) -> None:
        with self._lock:
            self._prune(now)

    def _prune(self, now: float) -> None:
        expired = [sig for sig, exp in self._consumed.items() if exp < now]
        for sig in expired:
            del self._consumed[sig]


@dataclass(frozen=True)
class ToolPermissionPolicy:
    dangerous_tools: frozenset[str] = DEFAULT_DANGEROUS_TOOLS
    dangerous_host_allowlist: frozenset[str] = frozenset()
    confirmation_fields: tuple[str, ...] = DEFAULT_CONFIRMATION_FIELDS
    approval_secret: str = ""
    approval_token_ttl_seconds: int = DEFAULT_APPROVAL_TOKEN_TTL_SECONDS
    _consumed_tokens: ConsumedTokenStore = field(
        default_factory=ConsumedTokenStore, compare=False, hash=False, repr=False,
    )


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
    approval_secret = str(permissions_cfg.get("approval_secret") or "").strip()
    approval_token_ttl_seconds = int(
        permissions_cfg.get(
            "approval_token_ttl_seconds",
            DEFAULT_APPROVAL_TOKEN_TTL_SECONDS,
        )
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
        approval_secret=approval_secret,
        approval_token_ttl_seconds=max(1, approval_token_ttl_seconds),
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
    if not has_valid_approval_token(
        normalized_tool_name,
        normalized_args,
        policy=policy,
    ):
        raise PermissionError("dangerous tool requires valid approval token")


def issue_approval_token(
    tool_name: str,
    arguments: dict[str, Any] | None,
    *,
    secret: str,
    issued_at: int | float | None = None,
    ttl_seconds: int = DEFAULT_APPROVAL_TOKEN_TTL_SECONDS,
) -> str:
    normalized_secret = str(secret or "").strip()
    if normalized_secret == "":
        raise ValueError("approval secret must not be empty")
    ttl = int(ttl_seconds)
    if ttl <= 0:
        raise ValueError("approval token ttl must be > 0")
    expires_at = int(issued_at if issued_at is not None else time.time()) + ttl
    payload = _approval_signature_payload(
        tool_name,
        arguments,
        expires_at=expires_at,
    )
    signature = hmac.new(
        normalized_secret.encode("utf-8"),
        payload,
        hashlib.sha256,
    ).hexdigest()
    return f"{APPROVAL_TOKEN_VERSION}:{expires_at}:{signature}"


def issue_approval_token_for_policy(
    tool_name: str,
    arguments: dict[str, Any] | None,
    *,
    policy: ToolPermissionPolicy,
    issued_at: int | float | None = None,
) -> str:
    return issue_approval_token(
        tool_name,
        arguments,
        secret=policy.approval_secret,
        issued_at=issued_at,
        ttl_seconds=policy.approval_token_ttl_seconds,
    )


def has_valid_approval_token(
    tool_name: str,
    arguments: dict[str, Any] | None,
    *,
    policy: ToolPermissionPolicy,
    now: int | float | None = None,
) -> bool:
    normalized_secret = str(policy.approval_secret or "").strip()
    if normalized_secret == "":
        return False

    normalized_args = arguments if isinstance(arguments, dict) else {}
    raw_token = str(normalized_args.get(APPROVAL_TOKEN_FIELD) or "").strip()
    if raw_token == "":
        return False

    try:
        version, expires_raw, provided_signature = raw_token.split(":", 2)
    except ValueError:
        return False
    if version != APPROVAL_TOKEN_VERSION:
        return False
    try:
        expires_at = int(expires_raw)
    except ValueError:
        return False
    current_time = int(now if now is not None else time.time())
    if expires_at < current_time:
        policy._consumed_tokens.prune(current_time)
        return False

    payload = _approval_signature_payload(
        tool_name,
        normalized_args,
        expires_at=expires_at,
    )
    expected_signature = hmac.new(
        normalized_secret.encode("utf-8"),
        payload,
        hashlib.sha256,
    ).hexdigest()
    if not hmac.compare_digest(provided_signature, expected_signature):
        return False

    return policy._consumed_tokens.consume(provided_signature, float(expires_at))


def _approval_signature_payload(
    tool_name: str,
    arguments: dict[str, Any] | None,
    *,
    expires_at: int,
) -> bytes:
    normalized_tool_name = str(tool_name or "").strip()
    if normalized_tool_name == "":
        raise ValueError("tool_name must not be empty")
    normalized_args = arguments if isinstance(arguments, dict) else {}
    filtered_args = {
        str(key): normalized_args[key]
        for key in sorted(normalized_args)
        if key not in {*DEFAULT_CONFIRMATION_FIELDS, APPROVAL_TOKEN_FIELD}
    }
    serialized_args = json.dumps(
        filtered_args,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    return (
        f"{APPROVAL_TOKEN_VERSION}:{normalized_tool_name}:{expires_at}:{serialized_args}"
    ).encode("utf-8")


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
