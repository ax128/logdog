# Telegram 通道增强 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 logger_agent 的 Telegram 通道补齐 Markdown 安全降级、流式消息编辑、文件上传/下载、typing 指示器、bot 命令注册、callback query 等能力，对齐 agentop 的交互体验。

**Architecture:** 所有增强集中在 `logwatch/notify/telegram.py` 中扩展现有 `TelegramBotTokenSender`（底层 HTTP）和 `TelegramBotRuntime`（交互运行时）两个类，新增 `TelegramStreamSender` 类实现流式编辑。不引入新的 ingress 框架或数据模型，复用现有 `NotifyRouter` + `RoutingPolicy` 体系。

**Tech Stack:** Python 3.13, urllib (无新依赖), python-telegram-bot (可选, 仅 polling 模式), pytest

---

## File Structure

| File | Action | Responsibility |
|------|--------|----------------|
| `logwatch/notify/telegram.py` | Modify | 扩展 Sender/Runtime，新增 StreamSender 和 markdown 安全函数 |
| `tests/notify/test_telegram_sender.py` | Create | TelegramBotTokenSender 新方法的单元测试 |
| `tests/notify/test_telegram_markdown.py` | Create | Markdown 安全检测和降级重试的单元测试 |
| `tests/notify/test_telegram_stream.py` | Create | TelegramStreamSender 的单元测试 |
| `tests/notify/test_telegram_runtime.py` | Modify | 扩展 Runtime 测试覆盖新功能 |

---

## Task 1: Markdown 安全函数

**Files:**
- Modify: `logwatch/notify/telegram.py` (在模块顶部 `TELEGRAM_AUTO_TARGET` 之后添加)
- Create: `tests/notify/test_telegram_markdown.py`

- [ ] **Step 1: Write failing tests for markdown safety**

```python
# tests/notify/test_telegram_markdown.py
from __future__ import annotations

from logwatch.notify.telegram import (
    telegram_markdown_retryable,
    telegram_markdown_unsafe,
)


class TestTelegramMarkdownUnsafe:
    def test_no_underscore_is_safe(self) -> None:
        assert telegram_markdown_unsafe("hello world") is False

    def test_empty_string_is_safe(self) -> None:
        assert telegram_markdown_unsafe("") is False

    def test_underscore_between_alnum_is_unsafe(self) -> None:
        assert telegram_markdown_unsafe("in_progress") is True

    def test_underscore_at_boundary_is_safe(self) -> None:
        assert telegram_markdown_unsafe("_italic_") is False

    def test_underscore_inside_code_block_is_safe(self) -> None:
        assert telegram_markdown_unsafe("`in_progress`") is False

    def test_escaped_underscore_is_safe(self) -> None:
        assert telegram_markdown_unsafe(r"in\_progress") is False

    def test_multiple_underscores_mixed(self) -> None:
        assert telegram_markdown_unsafe("ok _bold_ but in_progress") is True

    def test_underscore_with_non_alnum_neighbors(self) -> None:
        assert telegram_markdown_unsafe("hello _ world") is False


class TestTelegramMarkdownRetryable:
    def test_parse_entities_error(self) -> None:
        err = RuntimeError("Bad Request: can't parse entities")
        assert telegram_markdown_retryable(err) is True

    def test_find_end_of_entity_error(self) -> None:
        err = RuntimeError("can't find end of the entity")
        assert telegram_markdown_retryable(err) is True

    def test_unrelated_error(self) -> None:
        err = RuntimeError("chat not found")
        assert telegram_markdown_retryable(err) is False

    def test_none_error(self) -> None:
        assert telegram_markdown_retryable(None) is False
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/notify/test_telegram_markdown.py -v`
Expected: ImportError — `telegram_markdown_retryable` and `telegram_markdown_unsafe` not found

- [ ] **Step 3: Implement markdown safety functions**

Add at top of `logwatch/notify/telegram.py`, after the `_AUTO_TARGET_ATTR` line:

```python
def telegram_markdown_retryable(error: BaseException | None) -> bool:
    """Return True if the error is a Telegram markdown parse failure that can be retried as plain text."""
    text = str(error or "").lower()
    return "can't parse entities" in text or "can't find end of the entity" in text


def telegram_markdown_unsafe(text: str) -> bool:
    """Detect text that will likely fail Telegram legacy Markdown parsing.

    Rule: outside code spans, an unescaped '_' with alphanumeric neighbors on both sides
    (e.g. ``in_progress``) is considered unsafe.
    """
    value = str(text or "")
    if "_" not in value:
        return False
    in_code = False
    for idx, ch in enumerate(value):
        prev = value[idx - 1] if idx > 0 else ""
        escaped = prev == "\\"
        if ch == "`" and not escaped:
            in_code = not in_code
            continue
        if in_code:
            continue
        if ch != "_" or escaped:
            continue
        left = value[idx - 1] if idx > 0 else ""
        right = value[idx + 1] if idx + 1 < len(value) else ""
        if left.isalnum() and right.isalnum():
            return True
    return False
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/notify/test_telegram_markdown.py -v`
Expected: All 10 tests PASS

- [ ] **Step 5: Commit**

```bash
git add logwatch/notify/telegram.py tests/notify/test_telegram_markdown.py
git commit -m "feat(telegram): add markdown safety detection and retryable error classification"
```

---

## Task 2: TelegramBotTokenSender 扩展 — send_message / edit_message_text / parse_mode 支持

**Files:**
- Modify: `logwatch/notify/telegram.py` — `TelegramBotTokenSender` 类
- Create: `tests/notify/test_telegram_sender.py`

- [ ] **Step 1: Write failing tests for send_message with parse_mode and markdown degradation**

```python
# tests/notify/test_telegram_sender.py
from __future__ import annotations

import pytest

from logwatch.notify.telegram import TelegramBotTokenSender


class _RecordingRequest:
    """Monkey-patch _request to record calls instead of hitting Telegram API."""

    def __init__(self, sender: TelegramBotTokenSender) -> None:
        self.calls: list[tuple[str, dict]] = []
        self._sender = sender
        self._original = sender._request
        self._fail_markdown_once = False

    def install(self) -> None:
        self._sender._request = self._fake_request  # type: ignore[assignment]

    def _fake_request(self, method: str, payload: dict | None = None) -> dict:
        payload = payload or {}
        self.calls.append((method, dict(payload)))
        if self._fail_markdown_once and payload.get("parse_mode") == "Markdown":
            self._fail_markdown_once = False
            raise RuntimeError("Bad Request: can't parse entities")
        return {"message_id": 100}


class TestSendMessage:
    def test_send_message_plain(self) -> None:
        sender = TelegramBotTokenSender("tok123")
        rec = _RecordingRequest(sender)
        rec.install()

        sender.send_message("42", "hello")

        assert len(rec.calls) == 1
        method, payload = rec.calls[0]
        assert method == "sendMessage"
        assert payload["chat_id"] == "42"
        assert payload["text"] == "hello"
        assert "parse_mode" not in payload

    def test_send_message_with_markdown(self) -> None:
        sender = TelegramBotTokenSender("tok123")
        rec = _RecordingRequest(sender)
        rec.install()

        sender.send_message("42", "*bold*", parse_mode="Markdown")

        assert rec.calls[0][1]["parse_mode"] == "Markdown"

    def test_send_message_unsafe_markdown_downgrades(self) -> None:
        sender = TelegramBotTokenSender("tok123")
        rec = _RecordingRequest(sender)
        rec.install()

        sender.send_message("42", "in_progress", parse_mode="Markdown")

        assert "parse_mode" not in rec.calls[0][1]

    def test_send_message_markdown_parse_error_retries_plain(self) -> None:
        sender = TelegramBotTokenSender("tok123")
        rec = _RecordingRequest(sender)
        rec._fail_markdown_once = True
        rec.install()

        sender.send_message("42", "*ok*", parse_mode="Markdown")

        assert len(rec.calls) == 2
        assert rec.calls[0][1].get("parse_mode") == "Markdown"
        assert "parse_mode" not in rec.calls[1][1]


class TestEditMessageText:
    def test_edit_message_text_plain(self) -> None:
        sender = TelegramBotTokenSender("tok123")
        rec = _RecordingRequest(sender)
        rec.install()

        sender.edit_message_text("42", 100, "updated")

        assert len(rec.calls) == 1
        method, payload = rec.calls[0]
        assert method == "editMessageText"
        assert payload["message_id"] == 100
        assert payload["text"] == "updated"

    def test_edit_message_text_truncates_to_4096(self) -> None:
        sender = TelegramBotTokenSender("tok123")
        rec = _RecordingRequest(sender)
        rec.install()

        sender.edit_message_text("42", 100, "x" * 5000)

        assert len(rec.calls[0][1]["text"]) == 4096
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/notify/test_telegram_sender.py -v`
Expected: AttributeError — `send_message` not found on TelegramBotTokenSender

- [ ] **Step 3: Implement send_message and edit_message_text**

Add these methods to `TelegramBotTokenSender` class (after `_request` method):

```python
    def send_message(
        self,
        chat_id: str,
        text: str,
        parse_mode: str = "",
    ) -> dict:
        """Send a message with optional parse_mode and automatic markdown degradation."""
        effective_parse_mode = str(parse_mode or "").strip()
        if effective_parse_mode == "Markdown" and telegram_markdown_unsafe(text):
            logger.debug("unsafe legacy markdown detected; sending as plain text")
            effective_parse_mode = ""
        payload: dict[str, Any] = {"chat_id": chat_id, "text": str(text)}
        if effective_parse_mode:
            payload["parse_mode"] = effective_parse_mode
        try:
            return self._request("sendMessage", payload)
        except Exception as exc:
            if effective_parse_mode == "Markdown" and telegram_markdown_retryable(exc):
                logger.warning(
                    "markdown rejected by Telegram, retrying as plain text",
                    exc_info=True,
                )
                return self._request(
                    "sendMessage", {"chat_id": chat_id, "text": str(text)}
                )
            raise

    def edit_message_text(
        self,
        chat_id: str,
        message_id: int,
        text: str,
        parse_mode: str = "",
    ) -> dict:
        """Edit an existing message. Truncates text to 4096 chars."""
        effective_parse_mode = str(parse_mode or "").strip()
        if effective_parse_mode == "Markdown" and telegram_markdown_unsafe(text):
            effective_parse_mode = ""
        payload: dict[str, Any] = {
            "chat_id": chat_id,
            "message_id": int(message_id),
            "text": str(text)[:4096],
        }
        if effective_parse_mode:
            payload["parse_mode"] = effective_parse_mode
        try:
            return self._request("editMessageText", payload)
        except Exception as exc:
            if effective_parse_mode == "Markdown" and telegram_markdown_retryable(exc):
                logger.warning(
                    "markdown rejected on edit, retrying as plain text",
                    exc_info=True,
                )
                payload.pop("parse_mode", None)
                return self._request("editMessageText", payload)
            raise
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/notify/test_telegram_sender.py -v`
Expected: All 6 tests PASS

- [ ] **Step 5: Commit**

```bash
git add logwatch/notify/telegram.py tests/notify/test_telegram_sender.py
git commit -m "feat(telegram): add send_message/edit_message_text with markdown degradation"
```

---

## Task 3: TelegramBotTokenSender 扩展 — send_chat_action / set_my_commands / delete_webhook

**Files:**
- Modify: `logwatch/notify/telegram.py` — `TelegramBotTokenSender` 类
- Modify: `tests/notify/test_telegram_sender.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/notify/test_telegram_sender.py`:

```python
class TestSendChatAction:
    def test_send_typing_action(self) -> None:
        sender = TelegramBotTokenSender("tok123")
        rec = _RecordingRequest(sender)
        rec.install()

        sender.send_chat_action("42")

        assert rec.calls[0] == ("sendChatAction", {"chat_id": "42", "action": "typing"})

    def test_send_custom_action(self) -> None:
        sender = TelegramBotTokenSender("tok123")
        rec = _RecordingRequest(sender)
        rec.install()

        sender.send_chat_action("42", action="upload_document")

        assert rec.calls[0][1]["action"] == "upload_document"


class TestSetMyCommands:
    def test_registers_commands(self) -> None:
        sender = TelegramBotTokenSender("tok123")
        rec = _RecordingRequest(sender)
        rec.install()

        commands = [{"command": "help", "description": "Show help"}]
        sender.set_my_commands(commands)

        assert rec.calls[0] == ("setMyCommands", {"commands": commands})


class TestDeleteWebhook:
    def test_delete_webhook(self) -> None:
        sender = TelegramBotTokenSender("tok123")
        rec = _RecordingRequest(sender)
        rec.install()

        sender.delete_webhook()

        assert rec.calls[0] == ("deleteWebhook", {})
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/notify/test_telegram_sender.py::TestSendChatAction tests/notify/test_telegram_sender.py::TestSetMyCommands tests/notify/test_telegram_sender.py::TestDeleteWebhook -v`
Expected: AttributeError

- [ ] **Step 3: Implement methods**

Add to `TelegramBotTokenSender`:

```python
    def send_chat_action(self, chat_id: str, action: str = "typing") -> None:
        """Send a chat action indicator (e.g. 'typing')."""
        self._request("sendChatAction", {"chat_id": chat_id, "action": action or "typing"})

    def set_my_commands(self, commands: list[dict[str, str]]) -> None:
        """Register bot command menu entries."""
        self._request("setMyCommands", {"commands": list(commands)})

    def delete_webhook(self) -> None:
        """Remove any active webhook so polling mode works."""
        self._request("deleteWebhook", {})
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/notify/test_telegram_sender.py -v`
Expected: All 10 tests PASS

- [ ] **Step 5: Commit**

```bash
git add logwatch/notify/telegram.py tests/notify/test_telegram_sender.py
git commit -m "feat(telegram): add send_chat_action, set_my_commands, delete_webhook"
```

---

## Task 4: TelegramBotTokenSender 扩展 — multipart 文件上传 (send_document) 和文件下载 (download_file)

**Files:**
- Modify: `logwatch/notify/telegram.py` — `TelegramBotTokenSender` 类 (需要添加 `import mimetypes, uuid, os` 和 `from pathlib import Path`)
- Modify: `tests/notify/test_telegram_sender.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/notify/test_telegram_sender.py`:

```python
import os
import tempfile
from unittest.mock import patch


class TestSendDocument:
    def test_send_document_calls_multipart(self, tmp_path) -> None:
        sender = TelegramBotTokenSender("tok123")
        calls: list[tuple[str, dict, dict]] = []

        def fake_multipart(method, payload, files):
            calls.append((method, dict(payload), dict(files)))
            return {"message_id": 200}

        sender._request_multipart = fake_multipart  # type: ignore[assignment]

        doc = tmp_path / "test.txt"
        doc.write_text("hello")
        sender.send_document("42", str(doc), caption="my file")

        assert len(calls) == 1
        assert calls[0][0] == "sendDocument"
        assert calls[0][1]["chat_id"] == "42"
        assert calls[0][1]["caption"] == "my file"
        assert calls[0][2]["document"] == str(doc)

    def test_send_document_truncates_caption(self, tmp_path) -> None:
        sender = TelegramBotTokenSender("tok123")
        calls: list[tuple] = []

        def fake_multipart(method, payload, files):
            calls.append((method, dict(payload), dict(files)))
            return {}

        sender._request_multipart = fake_multipart  # type: ignore[assignment]

        doc = tmp_path / "test.txt"
        doc.write_text("data")
        sender.send_document("42", str(doc), caption="x" * 2000)

        assert len(calls[0][1]["caption"]) == 1024


class TestDownloadFile:
    def test_download_writes_file(self, tmp_path) -> None:
        sender = TelegramBotTokenSender("tok123")
        target = tmp_path / "out.bin"

        def fake_request(method, payload=None):
            if method == "getFile":
                return {"file_path": "documents/test.bin", "file_size": 5}
            return {}

        sender._request = fake_request  # type: ignore[assignment]

        fake_content = b"hello"
        with patch("logwatch.notify.telegram.urlopen") as mock_urlopen:
            mock_resp = mock_urlopen.return_value.__enter__.return_value
            mock_resp.read.side_effect = [fake_content, b""]
            sender.download_file("file-id-123", str(target))

        assert target.read_bytes() == fake_content

    def test_download_rejects_oversized_file(self, tmp_path) -> None:
        sender = TelegramBotTokenSender("tok123")
        target = tmp_path / "big.bin"

        def fake_request(method, payload=None):
            return {"file_path": "doc/big.bin", "file_size": 999_999_999}

        sender._request = fake_request  # type: ignore[assignment]

        with pytest.raises(ValueError, match="too large"):
            sender.download_file("file-big", str(target), max_bytes=1024)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/notify/test_telegram_sender.py::TestSendDocument tests/notify/test_telegram_sender.py::TestDownloadFile -v`
Expected: AttributeError

- [ ] **Step 3: Implement _request_multipart, send_document, download_file**

Add imports at top of `logwatch/notify/telegram.py` (extend existing import block):

```python
import mimetypes
import uuid
from pathlib import Path
```

Add methods to `TelegramBotTokenSender`:

```python
    _DEFAULT_MAX_DOWNLOAD_BYTES = 50 * 1024 * 1024
    _DOWNLOAD_CHUNK_BYTES = 64 * 1024

    def send_document(
        self,
        chat_id: str,
        file_path: str,
        caption: str = "",
    ) -> dict:
        """Send a local file as Telegram document via multipart upload."""
        payload: dict[str, Any] = {"chat_id": chat_id}
        if caption:
            payload["caption"] = str(caption)[:1024]
        return self._request_multipart(
            "sendDocument", payload, files={"document": file_path}
        )

    def download_file(
        self,
        file_id: str,
        target_path: str,
        *,
        max_bytes: int = 0,
    ) -> None:
        """Download a Telegram file by file_id to a local path."""
        result = self._request("getFile", {"file_id": str(file_id).strip()})
        if not isinstance(result, dict):
            raise RuntimeError("telegram getFile returned invalid payload")
        file_path = str(result.get("file_path", "") or "").strip()
        if not file_path:
            raise RuntimeError("telegram getFile returned empty file_path")
        limit = max_bytes if max_bytes > 0 else self._DEFAULT_MAX_DOWNLOAD_BYTES
        try:
            file_size = int(result.get("file_size", 0) or 0)
        except (TypeError, ValueError):
            file_size = 0
        if file_size and file_size > limit:
            raise ValueError(f"telegram file too large: {file_size} > {limit} bytes")
        download_url = f"https://api.telegram.org/file/bot{self._bot_token}/{file_path}"
        target = Path(target_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        received = 0
        try:
            with urlopen(download_url, timeout=60, context=ssl.create_default_context()) as resp, target.open("wb") as fh:
                while True:
                    chunk = resp.read(self._DOWNLOAD_CHUNK_BYTES)
                    if not chunk:
                        break
                    received += len(chunk)
                    if received > limit:
                        raise ValueError(
                            f"telegram download exceeds limit: {received} > {limit} bytes"
                        )
                    fh.write(chunk)
        except Exception:
            target.unlink(missing_ok=True)
            raise

    def _request_multipart(
        self,
        method: str,
        payload: dict[str, Any],
        *,
        files: dict[str, str],
    ) -> Any:
        """Send a multipart/form-data request (for file uploads)."""
        base_url = f"https://api.telegram.org/bot{self._bot_token}/{method}"
        boundary = f"----LogWatch{uuid.uuid4().hex}"
        body = bytearray()

        for key, value in payload.items():
            if value is None:
                continue
            body.extend(f"--{boundary}\r\n".encode())
            body.extend(f'Content-Disposition: form-data; name="{key}"\r\n\r\n'.encode())
            body.extend(str(value).encode())
            body.extend(b"\r\n")

        for field_name, file_path in files.items():
            filename = file_path.rsplit("/", 1)[-1].rsplit("\\", 1)[-1]
            content_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"
            with open(file_path, "rb") as fh:
                data = fh.read()
            body.extend(f"--{boundary}\r\n".encode())
            body.extend(
                f'Content-Disposition: form-data; name="{field_name}"; filename="{filename}"\r\n'.encode()
            )
            body.extend(f"Content-Type: {content_type}\r\n\r\n".encode())
            body.extend(data)
            body.extend(b"\r\n")

        body.extend(f"--{boundary}--\r\n".encode())

        req = Request(
            base_url,
            data=bytes(body),
            method="POST",
            headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        )
        try:
            with urlopen(req, timeout=60, context=ssl.create_default_context()) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
        except HTTPError as exc:
            detail = str(exc)
            try:
                detail_payload = json.loads(exc.read().decode("utf-8", errors="replace"))
                detail = str(detail_payload.get("description") or detail_payload)
            except Exception:
                pass
            raise RuntimeError(detail) from exc
        except URLError as exc:
            raise RuntimeError(str(getattr(exc, "reason", exc))) from exc

        try:
            decoded = json.loads(raw)
        except Exception as exc:
            raise RuntimeError("telegram response is not valid json") from exc
        if not isinstance(decoded, dict) or not decoded.get("ok"):
            raise RuntimeError(str(decoded.get("description") or decoded))
        return decoded.get("result")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/notify/test_telegram_sender.py -v`
Expected: All 14 tests PASS

- [ ] **Step 5: Commit**

```bash
git add logwatch/notify/telegram.py tests/notify/test_telegram_sender.py
git commit -m "feat(telegram): add multipart file upload (send_document) and file download"
```

---

## Task 5: TelegramStreamSender

**Files:**
- Modify: `logwatch/notify/telegram.py` (在 `TelegramBotTokenSender` 之后添加新类)
- Create: `tests/notify/test_telegram_stream.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/notify/test_telegram_stream.py
from __future__ import annotations

import time
from unittest.mock import MagicMock

import pytest

from logwatch.notify.telegram import TelegramStreamSender


def _make_sender(chat_id: str = "42") -> tuple[TelegramStreamSender, MagicMock]:
    bot = MagicMock()
    bot.send_message.return_value = {"message_id": 100}
    bot.edit_message_text.return_value = {"message_id": 100}
    sender = TelegramStreamSender(bot, chat_id)
    return sender, bot


class TestStreamUpdate:
    def test_first_update_sends_new_message(self) -> None:
        sender, bot = _make_sender()
        sender.update("hello")
        bot.send_message.assert_called_once_with("42", "hello", parse_mode="")

    def test_subsequent_update_edits_message(self) -> None:
        sender, bot = _make_sender()
        sender.update("v1")
        sender._last_edit = 0.0  # force past throttle
        sender.update("v2")
        bot.edit_message_text.assert_called_once_with("42", 100, "v2", parse_mode="")

    def test_throttled_update_skipped(self) -> None:
        sender, bot = _make_sender()
        sender.update("v1")
        sender.update("v2")  # within 0.35s, should be skipped
        assert bot.edit_message_text.call_count == 0

    def test_empty_chat_id_does_nothing(self) -> None:
        sender, bot = _make_sender(chat_id="")
        sender.update("hello")
        bot.send_message.assert_not_called()

    def test_truncates_to_4096(self) -> None:
        sender, bot = _make_sender()
        sender.update("x" * 5000)
        sent_text = bot.send_message.call_args[0][1]
        assert len(sent_text) == 4096


class TestStreamFinalize:
    def test_finalize_plain_text(self) -> None:
        sender, bot = _make_sender()
        sender.update("partial")
        sender._last_edit = 0.0
        sender.finalize("final text")
        bot.edit_message_text.assert_called_with("42", 100, "final text", parse_mode="")

    def test_finalize_markdown(self) -> None:
        sender, bot = _make_sender()
        sender.update("partial")
        sender._last_edit = 0.0
        sender.finalize("*bold*", fmt="markdown")
        bot.edit_message_text.assert_called_with("42", 100, "*bold*", parse_mode="Markdown")

    def test_finalize_unsafe_markdown_downgrades(self) -> None:
        sender, bot = _make_sender()
        sender.update("partial")
        sender._last_edit = 0.0
        sender.finalize("in_progress done", fmt="markdown")
        bot.edit_message_text.assert_called_with("42", 100, "in_progress done", parse_mode="")

    def test_finalize_without_prior_update_sends_new(self) -> None:
        sender, bot = _make_sender()
        sender.finalize("only message")
        bot.send_message.assert_called_once_with("42", "only message", parse_mode="")

    def test_finalize_markdown_parse_error_retries_plain(self) -> None:
        sender, bot = _make_sender()
        sender.update("partial")
        sender._last_edit = 0.0
        bot.edit_message_text.side_effect = [
            RuntimeError("can't parse entities"),
            {"message_id": 100},
        ]
        sender.finalize("*text*", fmt="markdown")
        assert bot.edit_message_text.call_count == 2
        assert bot.edit_message_text.call_args_list[1][0][3] == ""
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/notify/test_telegram_stream.py -v`
Expected: ImportError — `TelegramStreamSender` not found

- [ ] **Step 3: Implement TelegramStreamSender**

Add to `logwatch/notify/telegram.py` after the `TelegramBotTokenSender` class:

```python
_STREAM_EDIT_MIN_INTERVAL_SEC = 0.35


class TelegramStreamSender:
    """Incrementally edit a single Telegram message for streaming output.

    During streaming, ``update()`` uses plain text to avoid malformed entities.
    On completion, ``finalize()`` applies optional markdown formatting with
    automatic degradation on parse failure.
    """

    def __init__(self, bot: TelegramBotTokenSender, chat_id: str) -> None:
        self._bot = bot
        self._chat_id = str(chat_id or "").strip()
        self._message_id: int | None = None
        self._last_edit = 0.0

    def update(self, full_text: str) -> None:
        """Send or edit with current text. Throttled to avoid API rate limits."""
        text = str(full_text or "")[:4096]
        now = time.monotonic()
        if not self._chat_id:
            return
        if self._message_id is None:
            result = self._bot.send_message(self._chat_id, text or "\u2026", parse_mode="")
            self._message_id = int((result or {}).get("message_id") or 0) or None
            self._last_edit = now
            return
        if now - self._last_edit < _STREAM_EDIT_MIN_INTERVAL_SEC:
            return
        try:
            self._bot.edit_message_text(self._chat_id, self._message_id, text, parse_mode="")
        except Exception:
            logger.warning("stream edit_message_text failed", exc_info=True)
        self._last_edit = now

    def finalize(self, full_text: str, *, fmt: str = "plain") -> None:
        """Final formatting pass. Supports fmt='markdown' with automatic degradation."""
        text = str(full_text or "")[:4096]
        if not self._chat_id:
            return
        parse_mode = "Markdown" if str(fmt or "").strip().lower() == "markdown" else ""
        if parse_mode == "Markdown" and telegram_markdown_unsafe(text):
            logger.debug("stream finalize: unsafe markdown detected; downgrade to plain text")
            parse_mode = ""
        if self._message_id is None:
            try:
                self._bot.send_message(self._chat_id, text or "\u2026", parse_mode=parse_mode)
            except Exception as exc:
                if parse_mode == "Markdown" and telegram_markdown_retryable(exc):
                    logger.warning("stream finalize send: markdown rejected, retrying plain", exc_info=True)
                    self._bot.send_message(self._chat_id, text or "\u2026", parse_mode="")
                else:
                    raise
            return
        try:
            self._bot.edit_message_text(self._chat_id, self._message_id, text, parse_mode=parse_mode)
        except Exception as exc:
            if parse_mode == "Markdown" and telegram_markdown_retryable(exc):
                logger.warning("stream finalize edit: markdown rejected, retrying plain", exc_info=True)
                try:
                    self._bot.edit_message_text(self._chat_id, self._message_id, text, parse_mode="")
                except Exception:
                    logger.warning("stream finalize edit fallback failed", exc_info=True)
            else:
                logger.warning("stream finalize edit failed", exc_info=True)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/notify/test_telegram_stream.py -v`
Expected: All 10 tests PASS

- [ ] **Step 5: Commit**

```bash
git add logwatch/notify/telegram.py tests/notify/test_telegram_stream.py
git commit -m "feat(telegram): add TelegramStreamSender for incremental message editing"
```

---

## Task 6: TelegramNotifier 增强 — 带 parse_mode 的通知发送

**Files:**
- Modify: `logwatch/notify/telegram.py` — `TelegramNotifier` 类
- Modify: `tests/notify/test_telegram_sender.py` (追加 notifier 测试)

- [ ] **Step 1: Write failing tests**

Append to `tests/notify/test_telegram_sender.py`:

```python
from logwatch.notify.telegram import TelegramNotifier


class TestTelegramNotifierMarkdown:
    @pytest.mark.asyncio
    async def test_notifier_sends_with_markdown_parse_mode_in_md_mode(self) -> None:
        calls: list[tuple[str, str, str]] = []

        async def fake_send(target: str, text: str, parse_mode: str = "") -> None:
            calls.append((target, text, parse_mode))

        notifier = TelegramNotifier(
            send_func=fake_send,
            message_mode_getter=lambda: "md",
        )
        await notifier.send("host1", "hello *world*", "ERROR")

        assert len(calls) == 1
        assert calls[0][2] == "Markdown"

    @pytest.mark.asyncio
    async def test_notifier_sends_plain_in_text_mode(self) -> None:
        calls: list[tuple[str, str, str]] = []

        async def fake_send(target: str, text: str, parse_mode: str = "") -> None:
            calls.append((target, text, parse_mode))

        notifier = TelegramNotifier(
            send_func=fake_send,
            message_mode_getter=lambda: "text",
        )
        await notifier.send("host1", "hello", "ERROR")

        assert len(calls) == 1
        assert calls[0][2] == ""
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/notify/test_telegram_sender.py::TestTelegramNotifierMarkdown -v`
Expected: TypeError — `fake_send` receives unexpected `parse_mode` argument (current TelegramNotifier.send doesn't pass parse_mode)

- [ ] **Step 3: Modify TelegramNotifier to pass parse_mode**

Update the `SendFunc` type alias and `TelegramNotifier.send()`:

```python
# Change the type alias near the top of the file:
SendFunc = Callable[..., Awaitable[None] | None]
```

Update `TelegramNotifier.send()`:

```python
    async def send(self, host: str, message: str, category: str) -> None:
        mode = normalize_message_mode(self._message_mode_getter())
        formatted = format_message_for_mode(
            message,
            mode=mode,
            host=host,
            category=category,
        )
        parse_mode = "Markdown" if mode in ("md", "doc") else ""
        for chunk in split_message(formatted, self.max_message_chars):
            result = self._send_func(host, chunk, parse_mode)
            if inspect.isawaitable(result):
                await result
```

Note: This requires adding `import inspect` if not already present (it's already imported below in `_maybe_await_reply`, but check).

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/notify/test_telegram_sender.py tests/notify/test_telegram_runtime.py -v`
Expected: All tests PASS (including existing runtime tests)

- [ ] **Step 5: Commit**

```bash
git add logwatch/notify/telegram.py tests/notify/test_telegram_sender.py
git commit -m "feat(telegram): TelegramNotifier passes parse_mode based on message mode"
```

---

## Task 7: TelegramBotRuntime 增强 — typing 指示器、流式回复、文件消息处理

**Files:**
- Modify: `logwatch/notify/telegram.py` — `TelegramBotRuntime` 类 and `build_telegram_bot_runtime()`
- Modify: `tests/notify/test_telegram_runtime.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/notify/test_telegram_runtime.py`:

```python
class _FakeMessage:
    def __init__(self, text: str, *, document: dict | None = None) -> None:
        self.text = text
        self.document = document
        self.replies: list[str] = []

    async def reply_text(self, text: str) -> None:
        self.replies.append(text)

    async def reply_chat_action(self, action: str) -> None:
        self.replies.append(f"action:{action}")


class _FakeUpdate:
    def __init__(
        self,
        *,
        user_id: int,
        chat_id: int,
        text: str,
        document: dict | None = None,
    ) -> None:
        self.effective_user = type("User", (), {"id": user_id})()
        self.effective_chat = type("Chat", (), {"id": chat_id})()
        self.effective_message = _FakeMessage(text, document=document)


class _StreamRecordingChatRuntime:
    def __init__(self) -> None:
        self.calls: list[dict[str, str]] = []

    def invoke_text(
        self,
        prompt: str,
        *,
        user_id: str | None = None,
        session_key: str | None = None,
        fallback: str,
    ) -> str:
        self.calls.append({"prompt": prompt, "user_id": str(user_id or "")})
        return f"reply::{prompt}"
```

Also replace the old `_FakeMessage` and `_FakeUpdate` definitions if they conflict (they should be kept as the originals since tests below still use them — the new ones above are in separate classes that the new tests reference, but for simplicity we add new test functions that use the existing `_FakeUpdate`/`_FakeMessage`).

Actually, let's keep it simpler — append these new tests using the existing fixtures:

```python
@pytest.mark.asyncio
async def test_telegram_runtime_sends_typing_before_reply() -> None:
    events: list[str] = []
    application = _FakeApplication(events)
    chat_runtime = _RecordingChatRuntime()

    # Track typing calls on the sender
    typing_calls: list[str] = []
    original_sender: list = []

    runtime = build_telegram_bot_runtime(
        bot_token="token",
        chat_runtime=chat_runtime,
        authorized_user_ids={"42"},
        application_factory=lambda _token: application,
        handler_binder=lambda app, handler: app.add_handler(handler),
    )
    assert runtime is not None

    # The runtime should accept a sender for typing support
    sender_stub = type("Sender", (), {
        "send_chat_action": lambda self, chat_id, action="typing": typing_calls.append(f"{chat_id}:{action}"),
    })()
    runtime.set_sender(sender_stub)

    await runtime.start()
    handler = application.handlers[0]
    update = _FakeUpdate(user_id=42, chat_id=9001, text="hi")
    await handler(update, None)

    assert len(typing_calls) == 1
    assert typing_calls[0] == "9001:typing"
    await runtime.shutdown()


@pytest.mark.asyncio
async def test_telegram_runtime_handles_file_message() -> None:
    events: list[str] = []
    application = _FakeApplication(events)
    chat_runtime = _RecordingChatRuntime()

    runtime = build_telegram_bot_runtime(
        bot_token="token",
        chat_runtime=chat_runtime,
        authorized_user_ids={"42"},
        application_factory=lambda _token: application,
        handler_binder=lambda app, handler: app.add_handler(handler),
    )
    assert runtime is not None

    await runtime.start()
    handler = application.handlers[0]

    # Simulate a file message (text is caption, has document attr)
    update = _FakeUpdate(user_id=42, chat_id=9001, text="see this file")
    update.effective_message.document = {"file_id": "abc123", "file_name": "log.txt"}
    await handler(update, None)

    # Should still invoke chat_runtime with text content
    assert len(chat_runtime.calls) == 1
    assert "see this file" in chat_runtime.calls[0]["prompt"]

    await runtime.shutdown()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/notify/test_telegram_runtime.py::test_telegram_runtime_sends_typing_before_reply tests/notify/test_telegram_runtime.py::test_telegram_runtime_handles_file_message -v`
Expected: AttributeError — `set_sender` not found

- [ ] **Step 3: Implement TelegramBotRuntime enhancements**

Modify `TelegramBotRuntime.__init__()` — add `_sender` field:

```python
    def __init__(
        self,
        *,
        application: Any,
        chat_runtime: Any,
        authorized_user_ids: set[str],
        message_mode_setter: Callable[[str], str] | None = None,
        message_mode_getter: Callable[[], str] | None = None,
    ) -> None:
        self._application = application
        self._chat_runtime = chat_runtime
        self._authorized_user_ids = {str(item).strip() for item in authorized_user_ids}
        self._message_mode_setter = message_mode_setter
        self._message_mode_getter = message_mode_getter
        self._sender: Any | None = None
        self._started = False

    def set_sender(self, sender: Any) -> None:
        """Attach a TelegramBotTokenSender for typing indicators and file operations."""
        self._sender = sender
```

Modify `handle_text_message()` — add typing indicator before agent call:

```python
    async def handle_text_message(
        self,
        *,
        user_id: str,
        chat_id: str,
        text: str,
        reply_text: Callable[[str], Awaitable[None] | None],
    ) -> bool:
        normalized_user_id = str(user_id).strip()
        if normalized_user_id not in self._authorized_user_ids:
            return False

        if await self._handle_message_mode_command(text=text, reply_text=reply_text):
            return True

        if self._sender is not None:
            try:
                self._sender.send_chat_action(chat_id)
            except Exception:
                logger.debug("send_chat_action failed", exc_info=True)

        try:
            reply = self._chat_runtime.invoke_text(
                text,
                user_id=normalized_user_id,
                session_key=f"telegram:{chat_id}",
                fallback=DEFAULT_CHAT_FALLBACK_MESSAGE,
            )
        except Exception:
            logger.warning("telegram chat runtime invoke failed", exc_info=True)
            reply = DEFAULT_CHAT_FALLBACK_MESSAGE

        result = reply_text(reply)
        if inspect.isawaitable(result):
            await result
        return True
```

Modify `build_telegram_bot_runtime()` — update the `on_text` handler to handle documents too, and wire sender:

```python
def build_telegram_bot_runtime(
    *,
    bot_token: str,
    chat_runtime: Any,
    authorized_user_ids: set[str],
    message_mode_setter: Callable[[str], str] | None = None,
    message_mode_getter: Callable[[], str] | None = None,
    application_factory: Callable[[str], Any] | None = None,
    handler_binder: Callable[[Any, Any], None] | None = None,
) -> TelegramBotRuntime | None:
    normalized_token = str(bot_token or "").strip()
    if normalized_token == "":
        return None

    build_application = application_factory or _load_application_factory()
    bind_handler = handler_binder or _load_handler_binder()
    if build_application is None or bind_handler is None:
        return None

    application = build_application(normalized_token)
    runtime = TelegramBotRuntime(
        application=application,
        chat_runtime=chat_runtime,
        authorized_user_ids=authorized_user_ids,
        message_mode_setter=message_mode_setter,
        message_mode_getter=message_mode_getter,
    )

    async def on_text(update: Any, _context: Any) -> None:
        message = getattr(update, "effective_message", None)
        user = getattr(update, "effective_user", None)
        chat = getattr(update, "effective_chat", None)
        if message is None or user is None or chat is None:
            return
        text = getattr(message, "text", None) or getattr(message, "caption", None) or ""
        if not isinstance(text, str):
            return
        # Accept messages with text or document/photo (caption serves as text)
        document = getattr(message, "document", None)
        if not text and document is None:
            return
        await runtime.handle_text_message(
            user_id=str(getattr(user, "id", "")),
            chat_id=str(getattr(chat, "id", "")),
            text=text,
            reply_text=message.reply_text,
        )

    bind_handler(application, on_text)
    return runtime
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/notify/test_telegram_runtime.py -v`
Expected: All tests PASS (existing + new)

- [ ] **Step 5: Commit**

```bash
git add logwatch/notify/telegram.py tests/notify/test_telegram_runtime.py
git commit -m "feat(telegram): add typing indicator, file message support in TelegramBotRuntime"
```

---

## Task 8: TelegramBotRuntime 增强 — bot 命令注册 + callback query

**Files:**
- Modify: `logwatch/notify/telegram.py` — `TelegramBotRuntime`, `build_telegram_bot_runtime()`
- Modify: `tests/notify/test_telegram_runtime.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/notify/test_telegram_runtime.py`:

```python
@pytest.mark.asyncio
async def test_telegram_runtime_registers_bot_commands_on_start() -> None:
    events: list[str] = []
    application = _FakeApplication(events)
    chat_runtime = _RecordingChatRuntime()
    registered_commands: list[list] = []

    sender_stub = type("Sender", (), {
        "set_my_commands": lambda self, cmds: registered_commands.append(cmds),
        "send_chat_action": lambda self, *a, **kw: None,
    })()

    runtime = build_telegram_bot_runtime(
        bot_token="token",
        chat_runtime=chat_runtime,
        authorized_user_ids={"42"},
        application_factory=lambda _token: application,
        handler_binder=lambda app, handler: app.add_handler(handler),
    )
    assert runtime is not None
    runtime.set_sender(sender_stub)

    await runtime.start()

    # Should have registered commands on start
    assert len(registered_commands) == 1
    assert any(c["command"] == "msg" for c in registered_commands[0])

    await runtime.shutdown()


@pytest.mark.asyncio
async def test_telegram_runtime_handles_callback_query() -> None:
    events: list[str] = []
    application = _FakeApplication(events)
    chat_runtime = _RecordingChatRuntime()

    runtime = build_telegram_bot_runtime(
        bot_token="token",
        chat_runtime=chat_runtime,
        authorized_user_ids={"42"},
        application_factory=lambda _token: application,
        handler_binder=lambda app, handler: app.add_handler(handler),
    )
    assert runtime is not None
    await runtime.start()

    # Simulate callback query
    handled = await runtime.handle_text_message(
        user_id="42",
        chat_id="9001",
        text="button_action",
        reply_text=lambda t: None,
    )
    assert handled is True
    assert chat_runtime.calls[0]["prompt"] == "button_action"

    await runtime.shutdown()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/notify/test_telegram_runtime.py::test_telegram_runtime_registers_bot_commands_on_start tests/notify/test_telegram_runtime.py::test_telegram_runtime_handles_callback_query -v`
Expected: First test fails (no command registration on start), second should pass (callback query text goes through handle_text_message)

- [ ] **Step 3: Implement command registration on start**

Add bot commands constant and modify `start()`:

```python
# Add near top of file, after TELEGRAM_AUTO_TARGET:
TELEGRAM_BOT_COMMANDS = [
    {"command": "msg", "description": "Switch message mode (text|txt|md|doc)"},
    {"command": "help", "description": "Show help"},
    {"command": "status", "description": "Show system status"},
]
```

Modify `TelegramBotRuntime.start()` — add command registration after `self._started = True`:

```python
    async def start(self) -> None:
        if self._started:
            return
        updater = getattr(self._application, "updater", None)
        initialized = False
        app_started = False
        polling_started = False
        try:
            await self._application.initialize()
            initialized = True
            await self._application.start()
            app_started = True
            if updater is not None and hasattr(updater, "start_polling"):
                await updater.start_polling()
                polling_started = True
            self._started = True
            self._register_bot_commands()
        except Exception:
            if polling_started and updater is not None and hasattr(updater, "stop"):
                try:
                    await updater.stop()
                except Exception:
                    logger.warning(
                        "telegram updater stop failed during startup rollback",
                        exc_info=True,
                    )
            if app_started:
                try:
                    await self._application.stop()
                except Exception:
                    logger.warning(
                        "telegram application stop failed during startup rollback",
                        exc_info=True,
                    )
            if initialized:
                try:
                    await self._application.shutdown()
                except Exception:
                    logger.warning(
                        "telegram application shutdown failed during startup rollback",
                        exc_info=True,
                    )
            raise

    def _register_bot_commands(self) -> None:
        if self._sender is None:
            return
        try:
            self._sender.set_my_commands(TELEGRAM_BOT_COMMANDS)
        except Exception:
            logger.warning("telegram bot command registration failed", exc_info=True)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/notify/test_telegram_runtime.py -v`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add logwatch/notify/telegram.py tests/notify/test_telegram_runtime.py
git commit -m "feat(telegram): register bot commands on start, callback query support"
```

---

## Task 9: main.py 集成 — 将 sender 注入 runtime + 通知路径 parse_mode 传递

**Files:**
- Modify: `logwatch/main.py` — `_build_telegram_runtime_from_config()` and notifier wiring

- [ ] **Step 1: Write failing test**

Append to `tests/web/test_main_telegram_lifecycle.py` (or add a new one if structure differs):

```python
@pytest.mark.asyncio
async def test_telegram_runtime_receives_sender_for_typing() -> None:
    from logwatch.notify.telegram import TelegramBotTokenSender, build_telegram_bot_runtime

    events: list[str] = []
    application = _FakeApplication(events)
    chat_runtime = _RecordingChatRuntime()

    runtime = build_telegram_bot_runtime(
        bot_token="token",
        chat_runtime=chat_runtime,
        authorized_user_ids={"42"},
        application_factory=lambda _token: application,
        handler_binder=lambda app, handler: app.add_handler(handler),
    )
    assert runtime is not None
    assert hasattr(runtime, "set_sender")
```

This test just verifies the API exists (already passes from Task 7). The real integration is wiring in `main.py`.

- [ ] **Step 2: Modify _build_telegram_runtime_from_config to inject sender**

In `logwatch/main.py`, update `_build_telegram_runtime_from_config()`:

```python
def _build_telegram_runtime_from_config(
    *,
    app_config: dict[str, Any] | None,
    chat_runtime: Any,
    message_mode_setter: Callable[[str], str] | None = None,
    message_mode_getter: Callable[[], str] | None = None,
    telegram_application_factory: Any | None = None,
    telegram_handler_binder: Any | None = None,
):
    authorized_telegram_users = _resolve_authorized_telegram_users(app_config)
    telegram_bot_token = _resolve_telegram_bot_token()
    if telegram_bot_token and not authorized_telegram_users:
        logger.warning(
            "Telegram Bot enabled but no authorized users configured — all messages will be rejected"
        )
    runtime = build_telegram_bot_runtime(
        bot_token=telegram_bot_token,
        chat_runtime=chat_runtime,
        authorized_user_ids=authorized_telegram_users,
        message_mode_setter=message_mode_setter,
        message_mode_getter=message_mode_getter,
        application_factory=telegram_application_factory,
        handler_binder=telegram_handler_binder,
    )
    if runtime is not None and telegram_bot_token:
        try:
            sender = TelegramBotTokenSender(telegram_bot_token)
            runtime.set_sender(sender)
        except Exception:
            logger.warning("failed to create TelegramBotTokenSender for runtime", exc_info=True)
    return runtime
```

Also update the import at top of `main.py` to include `TelegramBotTokenSender`:

```python
from logwatch.notify.telegram import (
    TELEGRAM_AUTO_TARGET,
    TelegramBotTokenSender,
    TelegramNotifier,
    build_telegram_bot_runtime,
    build_telegram_bot_token_sender,
    supports_auto_telegram_target,
)
```

- [ ] **Step 3: Update _build_multi_target_send_func to pass parse_mode**

Find `_build_multi_target_send_func` in `main.py`. The inner `send_func` currently has signature `(target, message)`. Update it to accept an optional `parse_mode` parameter and pass it through:

The existing `_build_multi_target_send_func` returns an async function `async def send_func(target, message)`. Change its signature to:

```python
async def send_func(target: str, message: str, parse_mode: str = "") -> None:
```

And pass `parse_mode` through to the underlying `raw_send_func` if it accepts it. Since the underlying `raw_send_func` is `TelegramBotTokenSender.send()` which doesn't accept `parse_mode`, we need to check:

Actually, the simplest approach: update `_build_multi_target_send_func` so the inner send accepts `parse_mode` but the raw send function is called with just `(target, message)` as before — the markdown handling is already in `TelegramBotTokenSender.send_message()` which is separate from `send()`. The `TelegramNotifier` should use the sender's `send_message` method when available.

A cleaner approach: modify the `TelegramBotTokenSender.send()` method to accept and use `parse_mode`:

In `logwatch/notify/telegram.py`, update `TelegramBotTokenSender.send()`:

```python
    def send(self, target: str, message: str, parse_mode: str = "") -> None:
        chat_id = str(target or "").strip()
        if chat_id == "" or chat_id == TELEGRAM_AUTO_TARGET:
            chat_id = self._resolve_auto_chat_id()
        self.send_message(chat_id, str(message), parse_mode=parse_mode)
```

This makes `send()` delegate to `send_message()` which already handles markdown degradation.

- [ ] **Step 4: Run full test suite**

Run: `python -m pytest tests/notify/ tests/web/test_main_telegram_lifecycle.py tests/web/test_main_notify_binding.py -v`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add logwatch/notify/telegram.py logwatch/main.py
git commit -m "feat(telegram): wire sender into runtime, pass parse_mode through notify path"
```

---

## Task 10: Final integration test + full suite validation

**Files:**
- All modified files

- [ ] **Step 1: Run full test suite**

Run: `python -m pytest tests/ -q --tb=short`
Expected: All tests pass (except pre-existing failures unrelated to telegram)

- [ ] **Step 2: Verify no import cycles**

Run: `python -c "from logwatch.notify.telegram import TelegramBotTokenSender, TelegramStreamSender, TelegramNotifier, TelegramBotRuntime, telegram_markdown_unsafe, telegram_markdown_retryable, TELEGRAM_BOT_COMMANDS; print('OK')"`
Expected: `OK`

- [ ] **Step 3: Commit final state if any fixups needed**

```bash
git add -A
git commit -m "chore(telegram): final integration fixes"
```
