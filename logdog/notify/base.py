from __future__ import annotations

from abc import ABC, abstractmethod


DEFAULT_MESSAGE_MODE = "text"
_MESSAGE_MODE_ALIASES = {
    "text": "text",
    "txt": "txt",
    "md": "md",
    "markdown": "md",
    "doc": "doc",
    "markdown_doc": "doc",
    "markdown.doc": "doc",
}


def normalize_message_mode(
    mode: str | None,
    *,
    default: str = DEFAULT_MESSAGE_MODE,
) -> str:
    raw_mode = str(mode or "").strip().lower().replace("-", "_")
    if raw_mode == "":
        raw_mode = str(default or DEFAULT_MESSAGE_MODE).strip().lower().replace(
            "-", "_"
        )
    normalized = _MESSAGE_MODE_ALIASES.get(raw_mode)
    if normalized is None:
        supported = ", ".join(sorted(_MESSAGE_MODE_ALIASES))
        raise ValueError(f"unsupported message mode: {mode!r}, supported: {supported}")
    return normalized


def format_message_for_mode(
    message: str,
    *,
    mode: str,
    host: str | None = None,
    category: str | None = None,
) -> str:
    normalized = normalize_message_mode(mode)
    text = str(message or "")
    if normalized != "doc":
        return text

    head = ["# LogDog Notification"]
    if host:
        head.append(f"- Host: {host}")
    if category:
        head.append(f"- Category: {category}")
    if text.strip() == "":
        return "\n".join(head)
    return "\n".join(head + ["", text])


def split_message(message: str, max_chars: int) -> list[str]:
    if max_chars <= 0:
        raise ValueError("max_chars must be > 0")

    text = str(message or "")
    if text == "":
        return []

    chunks: list[str] = []
    cursor = 0
    while cursor < len(text):
        remaining = text[cursor:]
        if len(remaining) <= max_chars:
            chunks.append(remaining)
            break

        window = text[cursor : cursor + max_chars]
        split_at = _resolve_split_index(window, max_chars=max_chars)
        raw_chunk = text[cursor : cursor + split_at]
        chunk = raw_chunk.rstrip("\n")
        if chunk == "":
            chunk = raw_chunk

        chunks.append(chunk)
        cursor += max(1, split_at)
        while cursor < len(text) and text[cursor] == "\n":
            cursor += 1
    return chunks


def _resolve_split_index(window: str, *, max_chars: int) -> int:
    min_split = max(1, int(max_chars * 0.5))
    for sep in ("\n\n", "\n", "。", "！", "？", ";", "；", "，", ",", " "):
        idx = window.rfind(sep, min_split)
        if idx >= 0:
            return idx + len(sep)
    return max_chars


class BaseNotifier(ABC):
    def __init__(self, *, name: str, max_message_chars: int = 4096) -> None:
        if not name:
            raise ValueError("name must not be empty")
        if max_message_chars <= 0:
            raise ValueError("max_message_chars must be > 0")

        self.name = name
        self.max_message_chars = max_message_chars

    @abstractmethod
    async def send(self, host: str, message: str, category: str) -> None:
        """Send message to a notifier target."""
        raise NotImplementedError
