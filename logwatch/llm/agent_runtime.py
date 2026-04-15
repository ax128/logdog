from __future__ import annotations

import contextvars
import hashlib
import importlib
import logging
import time
from typing import Any, Callable, Mapping

from langchain_core.messages import HumanMessage


logger = logging.getLogger(__name__)

DEFAULT_CHAT_FALLBACK_MESSAGE = "Agent unavailable right now. Please try again shortly."
_CURRENT_RUNTIME_USER_ID: contextvars.ContextVar[str] = contextvars.ContextVar(
    "logwatch_agent_runtime_user_id",
    default="anonymous",
)


class AgentRuntime:
    def __init__(
        self,
        agent: Any | None,
        *,
        default_fallback: str = DEFAULT_CHAT_FALLBACK_MESSAGE,
    ) -> None:
        self._agent = agent
        self._default_fallback = default_fallback

    def invoke_text(
        self,
        prompt: str,
        *,
        user_id: str | None = None,
        session_key: str | None = None,
        fallback: str | None = None,
    ) -> str:
        resolved_fallback = str(fallback or self._default_fallback)
        if self._agent is None:
            return resolved_fallback

        config = _build_runtime_config(user_id=user_id, session_key=session_key)
        runtime_user_id = str(user_id or "anonymous").strip() or "anonymous"
        reset_token = _CURRENT_RUNTIME_USER_ID.set(runtime_user_id)

        try:
            result = self._agent.invoke(
                {"messages": [HumanMessage(content=prompt)]}, config,
            )
        except Exception:  # noqa: BLE001 - keep websocket/analyzer fallback path stable
            logger.warning("agent runtime invoke failed", exc_info=True)
            return resolved_fallback
        finally:
            _CURRENT_RUNTIME_USER_ID.reset(reset_token)

        try:
            rendered = render_agent_text(result)
        except Exception:  # noqa: BLE001 - keep websocket/analyzer fallback path stable
            logger.warning("agent runtime render failed", exc_info=True)
            return resolved_fallback

        if rendered.strip() == "":
            return resolved_fallback
        return rendered

    async def ainvoke_text(
        self,
        prompt: str,
        *,
        user_id: str | None = None,
        session_key: str | None = None,
        fallback: str | None = None,
    ) -> str:
        resolved_fallback = str(fallback or self._default_fallback)
        if self._agent is None:
            return resolved_fallback

        config = _build_runtime_config(user_id=user_id, session_key=session_key)
        runtime_user_id = str(user_id or "anonymous").strip() or "anonymous"
        reset_token = _CURRENT_RUNTIME_USER_ID.set(runtime_user_id)

        try:
            result = await self._agent.ainvoke(
                {"messages": [HumanMessage(content=prompt)]}, config,
            )
        except Exception:  # noqa: BLE001 - keep websocket/analyzer fallback path stable
            logger.warning("agent runtime ainvoke failed", exc_info=True)
            return resolved_fallback
        finally:
            _CURRENT_RUNTIME_USER_ID.reset(reset_token)

        try:
            rendered = render_agent_text(result)
        except Exception:  # noqa: BLE001 - keep websocket/analyzer fallback path stable
            logger.warning("agent runtime render failed", exc_info=True)
            return resolved_fallback

        if rendered.strip() == "":
            return resolved_fallback
        return rendered


def build_thread_id(*, user_id: str, session_key: str) -> str:
    material = f"{str(user_id or '').strip()}::{str(session_key or '').strip()}"
    digest = hashlib.sha256(material.encode("utf-8")).hexdigest()[:24]
    return f"chat:{digest}"


def build_chat_runtime(
    *,
    tool_registry: Mapping[str, Any] | None,
    model: str | None = None,
    api_base: str | None = None,
    api_key: str | None = None,
    runtime_factory: Callable[..., Any] | None = None,
    checkpointer_factory: Callable[[], Any] | None = None,
) -> AgentRuntime:
    resolved_registry = dict(tool_registry or {})
    if runtime_factory is None and not resolved_registry:
        return AgentRuntime(None)

    factory = runtime_factory or _load_deep_agent_factory()
    if factory is None:
        return AgentRuntime(None)

    checkpointer = _build_checkpointer(checkpointer_factory)

    factory_kwargs: dict[str, Any] = {
        "tools": _normalize_runtime_tools(list(resolved_registry.values())),
        "checkpointer": checkpointer,
    }
    if model:
        factory_kwargs["model"] = model
    if api_base:
        factory_kwargs["api_base"] = api_base
    if api_key:
        factory_kwargs["api_key"] = api_key

    try:
        agent = factory(**factory_kwargs)
    except Exception:  # noqa: BLE001 - runtime creation must not crash chat handler
        logger.warning("chat runtime creation failed", exc_info=True)
        return AgentRuntime(None)

    return AgentRuntime(agent)


def build_analyzer_runtime(
    *,
    tool_registry: Mapping[str, Any] | None = None,
    model: str | None = None,
    api_base: str | None = None,
    api_key: str | None = None,
    runtime_factory: Callable[..., Any] | None = None,
) -> AgentRuntime | None:
    factory = runtime_factory or _load_deep_agent_factory()
    if factory is None:
        return None

    factory_kwargs: dict[str, Any] = {
        "tools": _normalize_runtime_tools(list((tool_registry or {}).values())),
    }
    if model:
        factory_kwargs["model"] = model
    if api_base:
        factory_kwargs["api_base"] = api_base
    if api_key:
        factory_kwargs["api_key"] = api_key

    try:
        agent = factory(**factory_kwargs)
    except Exception:  # noqa: BLE001 - analyzer must fall back to plaintext path
        logger.warning("analyzer runtime creation failed", exc_info=True)
        return None

    return AgentRuntime(agent, default_fallback="")


def render_agent_text(result: Any) -> str:
    if result is None:
        raise ValueError("agent result is empty")

    if isinstance(result, str):
        return result.strip()

    if isinstance(result, dict):
        for key in ("output", "content", "text"):
            value = result.get(key)
            if isinstance(value, str) and value.strip() != "":
                return value.strip()
        if "messages" in result:
            return _render_messages(result.get("messages"))

    for attr in ("output", "content", "text"):
        value = getattr(result, attr, None)
        if isinstance(value, str) and value.strip() != "":
            return value.strip()

    messages = getattr(result, "messages", None)
    if messages is not None:
        return _render_messages(messages)

    raise ValueError("unable to render agent result")


def _render_messages(messages: Any) -> str:
    if not isinstance(messages, list) or not messages:
        raise ValueError("agent messages are empty")

    message = messages[-1]
    if isinstance(message, str):
        return message.strip()
    if isinstance(message, dict):
        content = message.get("content")
        if isinstance(content, str):
            return content.strip()
    content = getattr(message, "content", None)
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        text = _extract_text_from_content_blocks(content)
        if text:
            return text
    raise ValueError("agent message content is empty")


def _extract_text_from_content_blocks(blocks: list[Any]) -> str:
    """Extract text from content block lists (OpenAI Responses API format)."""
    parts: list[str] = []
    for block in blocks:
        if isinstance(block, str):
            parts.append(block)
        elif isinstance(block, dict):
            text = block.get("text") or block.get("content") or ""
            if isinstance(text, str) and text.strip():
                parts.append(text.strip())
    return "\n".join(parts)


def _build_runtime_config(
    *,
    user_id: str | None,
    session_key: str | None,
) -> dict[str, dict[str, str]]:
    configurable: dict[str, str] = {}
    if user_id is not None and str(user_id).strip() != "":
        configurable["user_id"] = str(user_id).strip()
    if session_key is not None and str(session_key).strip() != "":
        normalized_user = configurable.get("user_id", "anonymous")
        normalized_session = str(session_key).strip()
        configurable["thread_id"] = build_thread_id(
            user_id=normalized_user,
            session_key=normalized_session,
        )
        configurable["session_key"] = normalized_session
    return {"configurable": configurable}


def _normalize_runtime_tools(tools: list[Any]) -> list[Any]:
    normalized: list[Any] = []
    for tool in tools:
        if _is_registered_agent_tool(tool):
            normalized.append(_wrap_registered_tool(tool))
            continue
        normalized.append(tool)
    return normalized


def _is_registered_agent_tool(tool: Any) -> bool:
    return all(
        hasattr(tool, attr) for attr in ("name", "description", "invoke")
    ) and callable(getattr(tool, "invoke", None))


def _wrap_registered_tool(tool: Any) -> Any:
    import asyncio
    import concurrent.futures

    from logwatch.llm.tool_defs import TOOL_METAS, build_args_schema

    name = str(getattr(tool, "name", "logwatch_tool"))
    description = str(getattr(tool, "description", name))

    # Build args_schema from TOOL_META if available
    meta = TOOL_METAS.get(name)
    args_schema = None
    if meta is not None:
        args_schema = build_args_schema(name, meta.get("parameters", {}))

    async def _atool(**kwargs: Any) -> str:
        result = await tool.invoke(
            user_id=_CURRENT_RUNTIME_USER_ID.get(),
            arguments=kwargs,
        )
        if hasattr(result, "is_error") and result.is_error:
            return f"[ERROR] {result.content}"
        if hasattr(result, "content"):
            return result.content
        return str(result)

    def _tool(**kwargs: Any) -> Any:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if loop is not None and loop.is_running():
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                return pool.submit(asyncio.run, _atool(**kwargs)).result()
        return asyncio.run(_atool(**kwargs))

    _tool.__name__ = name
    _tool.__doc__ = description

    try:
        from langchain_core.tools import tool as lc_tool

        wrapped = (
            lc_tool(args_schema=args_schema)(_tool)
            if args_schema is not None
            else lc_tool(_tool)
        )
        wrapped.handle_tool_error = True
        return wrapped
    except ImportError:
        from langchain_core.tools import StructuredTool

        st_kwargs: dict[str, Any] = {
            "func": _tool,
            "coroutine": _atool,
            "name": name,
            "description": description,
        }
        if args_schema is not None:
            st_kwargs["args_schema"] = args_schema
        return StructuredTool.from_function(**st_kwargs)


def _import_base_checkpointer_saver() -> type | None:
    """Import BaseCheckpointSaver if langgraph is available, else return None."""
    try:
        from langgraph.checkpoint.base import BaseCheckpointSaver  # type: ignore[import-untyped]
        return BaseCheckpointSaver
    except Exception:  # noqa: BLE001
        return None


def _make_bounded_checkpointer_class() -> type:
    """Build _BoundedCheckpointer inheriting from BaseCheckpointSaver when available.

    Deferring class construction to runtime lets the module load without langgraph
    installed, while still satisfying LangGraph's isinstance check at agent-build time.
    """
    base = _import_base_checkpointer_saver() or object

    class _BoundedCheckpointer(base):  # type: ignore[valid-type,misc]
        """Wraps a checkpointer with per-thread TTL and a hard cap on total sessions.

        Eviction runs on every write (``put`` / ``aput``) so the memory footprint
        stays bounded without requiring a background task:

        - Sessions not written to within *ttl_seconds* are evicted.
        - When the live session count exceeds *max_sessions*, the oldest sessions
          (by last-write time) are evicted first.

        Thread deletion is delegated to ``inner.delete_thread`` so all storage
        structures (``storage``, ``writes``, ``blobs``) are cleaned up correctly.
        All other attribute access is transparently forwarded to *inner*.
        """

        _EVICT_EVERY_N = 10  # run full TTL scan only every N writes

        def __init__(
            self,
            inner: Any,
            *,
            max_sessions: int = 100,
            ttl_seconds: float = 3600.0,
        ) -> None:
            # Do NOT call super().__init__() — BaseCheckpointSaver sets self.serde
            # which would conflict with our delegation via __getattr__.
            self._inner = inner
            self._max_sessions = max_sessions
            self._ttl_seconds = ttl_seconds
            self._access_times: dict[str, float] = {}
            self._write_count = 0

        # ------------------------------------------------------------------
        # Eviction helpers
        # ------------------------------------------------------------------

        def _record_access(self, thread_id: str) -> None:
            """Update the access timestamp for *thread_id* and trigger eviction."""
            self._access_times[thread_id] = time.monotonic()
            self._write_count += 1
            # Full TTL scan is relatively cheap but not needed every write.
            if self._write_count % self._EVICT_EVERY_N == 0:
                self._evict_expired()
            self._evict_over_limit()

        def _delete_thread(self, thread_id: str) -> None:
            self._access_times.pop(thread_id, None)
            delete_fn = getattr(self._inner, "delete_thread", None)
            if callable(delete_fn):
                try:
                    delete_fn(thread_id)
                except Exception:  # noqa: BLE001
                    logger.warning(
                        "bounded checkpointer: delete_thread failed for %s",
                        thread_id,
                        exc_info=True,
                    )

        def _evict_expired(self) -> None:
            now = time.monotonic()
            expired = [
                k for k, t in self._access_times.items()
                if (now - t) > self._ttl_seconds
            ]
            for thread_id in expired:
                logger.debug("bounded checkpointer: evicting expired session %s", thread_id)
                self._delete_thread(thread_id)

        def _evict_over_limit(self) -> None:
            over = len(self._access_times) - self._max_sessions
            if over <= 0:
                return
            # Evict the *over* oldest sessions.
            sorted_keys = sorted(self._access_times, key=lambda k: self._access_times[k])
            for thread_id in sorted_keys[:over]:
                logger.debug("bounded checkpointer: evicting over-limit session %s", thread_id)
                self._delete_thread(thread_id)

        # ------------------------------------------------------------------
        # Thread-id extraction
        # ------------------------------------------------------------------

        @staticmethod
        def _thread_id_from_config(config: Any) -> str:
            if isinstance(config, dict):
                configurable = config.get("configurable", {})
                if isinstance(configurable, dict):
                    return str(configurable.get("thread_id", ""))
            return ""

        # ------------------------------------------------------------------
        # Delegated BaseCheckpointSaver methods (read path)
        # ------------------------------------------------------------------

        def get_tuple(self, config: Any) -> Any:
            return self._inner.get_tuple(config)

        def list(self, config: Any, **kwargs: Any) -> Any:
            return self._inner.list(config, **kwargs)

        async def aget_tuple(self, config: Any) -> Any:
            return await self._inner.aget_tuple(config)

        async def alist(self, config: Any, **kwargs: Any) -> Any:
            return self._inner.alist(config, **kwargs)

        def get_next_version(self, current: Any, channel: Any) -> Any:
            return self._inner.get_next_version(current, channel)

        # ------------------------------------------------------------------
        # Intercepted write methods (track + evict on every write)
        # ------------------------------------------------------------------

        def put(self, config: Any, *args: Any, **kwargs: Any) -> Any:
            result = self._inner.put(config, *args, **kwargs)
            thread_id = self._thread_id_from_config(config)
            if thread_id:
                self._record_access(thread_id)
            return result

        async def aput(self, config: Any, *args: Any, **kwargs: Any) -> Any:
            result = await self._inner.aput(config, *args, **kwargs)
            thread_id = self._thread_id_from_config(config)
            if thread_id:
                self._record_access(thread_id)
            return result

        def put_writes(self, config: Any, *args: Any, **kwargs: Any) -> Any:
            result = self._inner.put_writes(config, *args, **kwargs)
            thread_id = self._thread_id_from_config(config)
            if thread_id:
                self._record_access(thread_id)
            return result

        async def aput_writes(self, config: Any, *args: Any, **kwargs: Any) -> Any:
            result = await self._inner.aput_writes(config, *args, **kwargs)
            thread_id = self._thread_id_from_config(config)
            if thread_id:
                self._record_access(thread_id)
            return result

        # ------------------------------------------------------------------
        # Transparent delegation for any remaining attributes
        # ------------------------------------------------------------------

        def __getattr__(self, name: str) -> Any:  # pragma: no cover
            return getattr(self._inner, name)

    return _BoundedCheckpointer


# Module-level class built once at import time.
_BoundedCheckpointer = _make_bounded_checkpointer_class()


def _build_checkpointer(
    checkpointer_factory: Callable[[], Any] | None,
) -> Any | None:
    factory = checkpointer_factory or _load_checkpointer_factory()
    if factory is None:
        return None
    try:
        inner = factory()
        return _BoundedCheckpointer(inner)
    except Exception:  # noqa: BLE001 - runtime can still operate without memory backend
        logger.warning("chat runtime checkpointer creation failed", exc_info=True)
        return None


def _load_deep_agent_factory() -> Callable[..., Any] | None:
    try:
        module = importlib.import_module("deepagents")
    except Exception:  # noqa: BLE001 - deepagents is optional at runtime
        return None

    factory = getattr(module, "create_deep_agent", None)
    if not callable(factory):
        return None
    return factory


def _load_checkpointer_factory() -> Callable[[], Any] | None:
    try:
        module = importlib.import_module("langgraph.checkpoint.memory")
    except Exception:  # noqa: BLE001 - langgraph is optional at runtime
        return None

    factory = getattr(module, "InMemorySaver", None)
    if not callable(factory):
        return None
    return factory
