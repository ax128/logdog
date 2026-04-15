from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pytest

from logwatch.notify.telegram import TelegramBotTokenSender


def _make_sender() -> TelegramBotTokenSender:
    return TelegramBotTokenSender("fake-token")


class TestSendMessage:
    def test_send_message_plain(self) -> None:
        sender = _make_sender()
        calls: list[tuple[str, dict[str, Any]]] = []

        def fake_request(method: str, payload: dict[str, Any] | None = None) -> Any:
            calls.append((method, payload or {}))
            return {"message_id": 1}

        sender._request = fake_request  # type: ignore[assignment]
        sender.send_message("123", "hello")
        assert calls == [("sendMessage", {"chat_id": "123", "text": "hello"})]

    def test_send_message_with_parse_mode(self) -> None:
        sender = _make_sender()
        calls: list[tuple[str, dict[str, Any]]] = []

        def fake_request(method: str, payload: dict[str, Any] | None = None) -> Any:
            calls.append((method, payload or {}))
            return {"message_id": 1}

        sender._request = fake_request  # type: ignore[assignment]
        sender.send_message("123", "hello", parse_mode="HTML")
        assert calls == [
            ("sendMessage", {"chat_id": "123", "text": "hello", "parse_mode": "HTML"})
        ]

    def test_send_message_downgrades_unsafe_markdown(self) -> None:
        sender = _make_sender()
        calls: list[tuple[str, dict[str, Any]]] = []

        def fake_request(method: str, payload: dict[str, Any] | None = None) -> Any:
            calls.append((method, payload or {}))
            return {"message_id": 1}

        sender._request = fake_request  # type: ignore[assignment]
        sender.send_message("123", "in_progress", parse_mode="Markdown")
        assert len(calls) == 1
        assert "parse_mode" not in calls[0][1]

    def test_send_message_retries_on_markdown_parse_error(self) -> None:
        sender = _make_sender()
        calls: list[tuple[str, dict[str, Any]]] = []
        call_count = 0

        def fake_request(method: str, payload: dict[str, Any] | None = None) -> Any:
            nonlocal call_count
            call_count += 1
            calls.append((method, dict(payload or {})))
            if call_count == 1 and payload and "parse_mode" in payload:
                raise RuntimeError("Can't parse entities: bad markdown")
            return {"message_id": 1}

        sender._request = fake_request  # type: ignore[assignment]
        sender.send_message("123", "_ok_", parse_mode="Markdown")
        assert len(calls) == 2
        assert "parse_mode" in calls[0][1]
        assert "parse_mode" not in calls[1][1]


class TestEditMessageText:
    def test_edit_message_text_plain(self) -> None:
        sender = _make_sender()
        calls: list[tuple[str, dict[str, Any]]] = []

        def fake_request(method: str, payload: dict[str, Any] | None = None) -> Any:
            calls.append((method, payload or {}))
            return {}

        sender._request = fake_request  # type: ignore[assignment]
        sender.edit_message_text("123", 42, "updated")
        assert calls == [
            (
                "editMessageText",
                {"chat_id": "123", "message_id": 42, "text": "updated"},
            )
        ]

    def test_edit_message_text_truncates(self) -> None:
        sender = _make_sender()
        calls: list[tuple[str, dict[str, Any]]] = []

        def fake_request(method: str, payload: dict[str, Any] | None = None) -> Any:
            calls.append((method, payload or {}))
            return {}

        sender._request = fake_request  # type: ignore[assignment]
        long_text = "x" * 5000
        sender.edit_message_text("123", 42, long_text)
        assert len(calls[0][1]["text"]) == 4096

    def test_edit_message_text_downgrades_unsafe_markdown(self) -> None:
        sender = _make_sender()
        calls: list[tuple[str, dict[str, Any]]] = []

        def fake_request(method: str, payload: dict[str, Any] | None = None) -> Any:
            calls.append((method, payload or {}))
            return {}

        sender._request = fake_request  # type: ignore[assignment]
        sender.edit_message_text("123", 42, "in_progress", parse_mode="Markdown")
        assert "parse_mode" not in calls[0][1]


class TestSendChatAction:
    def test_send_chat_action(self) -> None:
        sender = _make_sender()
        calls: list[tuple[str, dict[str, Any]]] = []

        def fake_request(method: str, payload: dict[str, Any] | None = None) -> Any:
            calls.append((method, payload or {}))
            return True

        sender._request = fake_request  # type: ignore[assignment]
        sender.send_chat_action("123")
        assert calls == [
            ("sendChatAction", {"chat_id": "123", "action": "typing"})
        ]

    def test_send_chat_action_custom(self) -> None:
        sender = _make_sender()
        calls: list[tuple[str, dict[str, Any]]] = []

        def fake_request(method: str, payload: dict[str, Any] | None = None) -> Any:
            calls.append((method, payload or {}))
            return True

        sender._request = fake_request  # type: ignore[assignment]
        sender.send_chat_action("123", action="upload_document")
        assert calls[0][1]["action"] == "upload_document"


class TestSetMyCommands:
    def test_set_my_commands(self) -> None:
        sender = _make_sender()
        calls: list[tuple[str, dict[str, Any]]] = []

        def fake_request(method: str, payload: dict[str, Any] | None = None) -> Any:
            calls.append((method, payload or {}))
            return True

        sender._request = fake_request  # type: ignore[assignment]
        cmds = [{"command": "start", "description": "Start"}]
        sender.set_my_commands(cmds)
        assert calls == [("setMyCommands", {"commands": cmds})]


class TestDeleteWebhook:
    def test_delete_webhook(self) -> None:
        sender = _make_sender()
        calls: list[tuple[str, dict[str, Any]]] = []

        def fake_request(method: str, payload: dict[str, Any] | None = None) -> Any:
            calls.append((method, payload or {}))
            return True

        sender._request = fake_request  # type: ignore[assignment]
        sender.delete_webhook()
        assert calls == [("deleteWebhook", {})]


class TestSendDocument:
    def test_send_document(self, tmp_path: Any) -> None:
        sender = _make_sender()
        calls: list[tuple[str, dict[str, str], dict[str, str]]] = []

        def fake_multipart(
            method: str, payload: dict[str, str], *, files: dict[str, str]
        ) -> Any:
            calls.append((method, payload, files))
            return {"message_id": 99}

        sender._request_multipart = fake_multipart  # type: ignore[assignment]
        test_file = tmp_path / "doc.txt"
        test_file.write_text("content")
        sender.send_document("123", str(test_file), caption="my doc")
        assert len(calls) == 1
        assert calls[0][0] == "sendDocument"
        assert calls[0][1]["chat_id"] == "123"
        assert calls[0][1]["caption"] == "my doc"
        assert calls[0][2]["document"] == str(test_file)

    def test_send_document_truncates_caption(self, tmp_path: Any) -> None:
        sender = _make_sender()
        calls: list[tuple[str, dict[str, str], dict[str, str]]] = []

        def fake_multipart(
            method: str, payload: dict[str, str], *, files: dict[str, str]
        ) -> Any:
            calls.append((method, payload, files))
            return {"message_id": 99}

        sender._request_multipart = fake_multipart  # type: ignore[assignment]
        test_file = tmp_path / "doc.txt"
        test_file.write_text("content")
        sender.send_document("123", str(test_file), caption="x" * 2000)
        assert len(calls[0][1]["caption"]) == 1024


class TestDownloadFile:
    def test_download_file(self, tmp_path: Any) -> None:
        sender = _make_sender()
        request_calls: list[tuple[str, dict[str, Any]]] = []

        def fake_request(method: str, payload: dict[str, Any] | None = None) -> Any:
            request_calls.append((method, payload or {}))
            return {"file_path": "documents/file.txt"}

        sender._request = fake_request  # type: ignore[assignment]
        dest = tmp_path / "downloaded.txt"

        import io
        fake_response = io.BytesIO(b"file content here")
        fake_response.read_orig = fake_response.read

        with patch("logwatch.notify.telegram.urlopen") as mock_urlopen:
            ctx = mock_urlopen.return_value.__enter__.return_value
            ctx.read = fake_response.read
            result = sender.download_file("file123", str(dest))

        assert request_calls[0] == ("getFile", {"file_id": "file123"})
        assert result == dest


class TestSendDelegatesToSendMessage:
    def test_send_calls_send_message(self) -> None:
        sender = _make_sender()
        sm_calls: list[tuple[str, str, str]] = []

        def fake_send_message(
            chat_id: str, text: str, parse_mode: str = ""
        ) -> Any:
            sm_calls.append((chat_id, text, parse_mode))
            return {"message_id": 1}

        sender.send_message = fake_send_message  # type: ignore[assignment]
        sender.send("456", "hi there")
        assert sm_calls == [("456", "hi there", "")]
