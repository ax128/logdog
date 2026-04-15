from __future__ import annotations

import keyword
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field, create_model


TOOL_METAS: dict[str, dict[str, Any]] = {
    "list_hosts": {
        "name": "list_hosts",
        "description": "List configured hosts and statuses.",
        "read_only": True,
        "parameters": {},
    },
    "list_containers": {
        "name": "list_containers",
        "description": "List containers for a host.",
        "read_only": True,
        "parameters": {
            "host": {
                "type": "string",
                "description": "Host name, e.g. 'local' or 'prod'",
                "required": True,
            },
        },
    },
    "query_logs": {
        "name": "query_logs",
        "description": "Query recent container logs.",
        "read_only": True,
        "parameters": {
            "host": {"type": "string", "description": "Host name, e.g. 'local' or 'prod'", "required": True},
            "container_id": {"type": "string", "description": "Container name or id", "required": True},
            "hours": {"type": "integer", "description": "Hours of logs to query", "required": False, "default": 1},
            "max_lines": {"type": "integer", "description": "Max lines to return", "required": False, "default": None},
        },
    },
    "get_metrics": {
        "name": "get_metrics",
        "description": "Query container metrics from storage.",
        "read_only": True,
        "parameters": {
            "host": {"type": "string", "description": "Host name, e.g. 'local' or 'prod'", "required": True},
            "container_id": {"type": "string", "description": "Container name or id", "required": True},
            "hours": {"type": "integer", "description": "Hours of metrics", "required": False, "default": 1},
            "limit": {"type": "integer", "description": "Max data points to return", "required": False, "default": None},
        },
    },
    "get_alerts": {
        "name": "get_alerts",
        "description": "List recent stored alerts.",
        "read_only": True,
        "parameters": {
            "limit": {"type": "integer", "description": "Max alerts to return", "required": False, "default": None},
        },
    },
    "mute_alert": {
        "name": "mute_alert",
        "description": "Create an alert mute entry.",
        "read_only": False,
        "parameters": {
            "host": {"type": "string", "description": "Host name, e.g. 'local' or 'prod'", "required": True},
            "container_id": {"type": "string", "description": "Container name or id", "required": True},
            "category": {"type": "string", "description": "Alert category to mute", "required": True},
            "hours": {"type": "integer", "description": "Mute duration in hours", "required": False, "default": 1},
            "reason": {"type": "string", "description": "Reason for muting", "required": False, "default": ""},
        },
    },
    "unmute_alert": {
        "name": "unmute_alert",
        "description": "Remove an alert mute entry.",
        "read_only": False,
        "parameters": {
            "host": {"type": "string", "description": "Host name, e.g. 'local' or 'prod'", "required": True},
            "container_id": {"type": "string", "description": "Container name or id", "required": True},
            "category": {"type": "string", "description": "Alert category to unmute", "required": True},
        },
    },
    "restart_container": {
        "name": "restart_container",
        "description": "Restart a container on an allowlisted host. Requires confirmed=true.",
        "read_only": False,
        "parameters": {
            "host": {"type": "string", "description": "Host name, e.g. 'local' or 'prod'", "required": True},
            "container_id": {"type": "string", "description": "Container name or id", "required": True},
            "timeout": {"type": "integer", "description": "Restart timeout seconds", "required": False, "default": 10},
            "confirmed": {
                "type": "boolean",
                "description": "Must be true to confirm restart execution",
                "required": True,
            },
        },
    },
}


def _resolve_py_type(pinfo: dict[str, Any]) -> Any:
    """Map TOOL_META type string to Python type."""
    type_map: dict[str, Any] = {
        "string": str,
        "integer": int,
        "number": float,
        "boolean": bool,
        "array": list[Any],
        "object": dict[str, Any],
    }
    return type_map.get(str(pinfo.get("type", "string")), str)


def build_args_schema(
    tool_name: str, parameters: dict[str, dict[str, Any]]
) -> type[BaseModel] | None:
    """Dynamically build a Pydantic model from TOOL_META parameters dict."""
    if not parameters:
        return None

    used_names: set[str] = set()
    field_defs: dict[str, Any] = {}

    for pname, pinfo in parameters.items():
        internal_name = _safe_field_name(pname, used_names)
        py_type = _resolve_py_type(pinfo)
        desc = str(pinfo.get("description", ""))
        required = bool(pinfo.get("required", False))

        field_kwargs: dict[str, Any] = {"description": desc}
        if internal_name != pname:
            field_kwargs["alias"] = pname

        if required:
            field_defs[internal_name] = (py_type, Field(**field_kwargs))
        else:
            field_defs[internal_name] = (
                Optional[py_type],
                Field(default=pinfo.get("default"), **field_kwargs),
            )

    return create_model(
        f"{tool_name}_args",
        __config__=ConfigDict(populate_by_name=True),
        **field_defs,
    )


def _safe_field_name(name: str, used: set[str]) -> str:
    candidate = name
    if (
        not candidate.isidentifier()
        or keyword.iskeyword(candidate)
        or hasattr(BaseModel, candidate)
    ):
        candidate = f"{candidate}_field"
    while candidate in used:
        candidate += "_"
    used.add(candidate)
    return candidate
