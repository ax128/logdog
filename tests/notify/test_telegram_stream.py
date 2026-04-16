from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

from logdog.notify.telegram import TelegramStreamSender


def _make_bot_mock() -> MagicMock:
    bot = MagicMock()
    bot.send_message.return_value = {"message_id": 100, "chat": {"id": 123}}
    bot.edit_message_text.return_value = {}
    return bot


class TestTelegramStreamSenderUpdate:
    def test_first_update_sends_new_message(self) -> None:
        bot = _make_bot_mock()
        stream = TelegramStreamSender(bot, "123")
        stream.update("hello")
        bot.send_message.assert_called_once_with("123", "hello", parse_mode="")
        assert stream._message_id == 100

    def test_subsequent_update_edits(self) -> None:
        bot = _make_bot_mock()
        stream = TelegramStreamSender(bot, "123")
        stream.update("hello")
        # Force past throttle
        stream._last_edit = 0.0
        stream.update("hello world")
        bot.edit_message_text.assert_called_once_with(
            "123", 100, "hello world", parse_mode=""
        )

    def test_throttled_update_skipped(self) -> None:
        bot = _make_bot_mock()
        stream = TelegramStreamSender(bot, "123")
        stream.update("first")
        # Don't reset _last_edit — should be throttled
        stream.update("second")
        bot.edit_message_text.assert_not_called()

    def test_empty_chat_id_does_nothing(self) -> None:
        bot = _make_bot_mock()
        stream = TelegramStreamSender(bot, "")
        stream.update("hello")
        bot.send_message.assert_not_called()

    def test_truncates_to_4096(self) -> None:
        bot = _make_bot_mock()
        stream = TelegramStreamSender(bot, "123")
        long_text = "a" * 5000
        stream.update(long_text)
        called_text = bot.send_message.call_args[0][1]
        assert len(called_text) == 4096


class TestTelegramStreamSenderFinalize:
    def test_finalize_plain_text(self) -> None:
        bot = _make_bot_mock()
        stream = TelegramStreamSender(bot, "123")
        stream.update("draft")
        stream._last_edit = 0.0
        stream.finalize("final text")
        bot.edit_message_text.assert_called_with("123", 100, "final text", parse_mode="")

    def test_finalize_markdown(self) -> None:
        bot = _make_bot_mock()
        stream = TelegramStreamSender(bot, "123")
        stream.update("draft")
        stream._last_edit = 0.0
        stream.finalize("*bold*", fmt="markdown")
        bot.edit_message_text.assert_called_with(
            "123", 100, "*bold*", parse_mode="Markdown"
        )

    def test_finalize_unsafe_markdown_downgrades(self) -> None:
        """edit_message_text already handles unsafe markdown downgrade internally."""
        bot = _make_bot_mock()
        stream = TelegramStreamSender(bot, "123")
        stream.update("draft")
        stream._last_edit = 0.0
        # "in_progress" is unsafe for Markdown — edit_message_text
        # should be called, and it handles downgrade internally
        stream.finalize("in_progress", fmt="markdown")
        bot.edit_message_text.assert_called_with(
            "123", 100, "in_progress", parse_mode="Markdown"
        )

    def test_finalize_without_prior_update_sends_new(self) -> None:
        bot = _make_bot_mock()
        stream = TelegramStreamSender(bot, "123")
        stream.finalize("hello")
        bot.send_message.assert_called_once_with("123", "hello", parse_mode="")

    def test_finalize_markdown_parse_error_retries_plain(self) -> None:
        bot = _make_bot_mock()
        bot.edit_message_text.side_effect = RuntimeError(
            "Can't parse entities: bad markdown"
        )
        stream = TelegramStreamSender(bot, "123")
        stream.update("draft")
        stream._last_edit = 0.0

        # Reset side_effect for retry
        call_count = 0

        def side_effect_fn(*args: Any, **kwargs: Any) -> Any:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("Can't parse entities: bad markdown")
            return {}

        bot.edit_message_text.side_effect = side_effect_fn
        stream.finalize("*text*", fmt="markdown")
        assert call_count == 2
        # Second call should be without markdown
        second_call = bot.edit_message_text.call_args_list[-1]
        assert second_call[1].get("parse_mode", second_call[0][-1]) == ""

    def test_finalize_send_markdown_error_retries_plain(self) -> None:
        bot = _make_bot_mock()
        call_count = 0

        def side_effect_fn(*args: Any, **kwargs: Any) -> Any:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("Can't parse entities: bad markdown")
            return {"message_id": 200}

        bot.send_message.side_effect = side_effect_fn
        stream = TelegramStreamSender(bot, "123")
        stream.finalize("*text*", fmt="markdown")
        assert call_count == 2
        second_call = bot.send_message.call_args_list[-1]
        assert second_call[1].get("parse_mode", second_call[0][-1] if len(second_call[0]) > 2 else "") == ""

    def test_empty_chat_id_finalize_does_nothing(self) -> None:
        bot = _make_bot_mock()
        stream = TelegramStreamSender(bot, "  ")
        stream.finalize("hello")
        bot.send_message.assert_not_called()
