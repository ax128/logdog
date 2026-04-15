from __future__ import annotations

import pytest

from logwatch.notify.telegram import telegram_markdown_retryable, telegram_markdown_unsafe


class TestTelegramMarkdownUnsafe:
    def test_no_underscore_is_safe(self) -> None:
        assert telegram_markdown_unsafe("hello world") is False

    def test_empty_string_is_safe(self) -> None:
        assert telegram_markdown_unsafe("") is False

    def test_in_progress_is_unsafe(self) -> None:
        assert telegram_markdown_unsafe("in_progress") is True

    def test_italic_boundary_is_safe(self) -> None:
        assert telegram_markdown_unsafe("_italic_") is False

    def test_inside_code_span_is_safe(self) -> None:
        assert telegram_markdown_unsafe("`in_progress`") is False

    def test_escaped_underscore_is_safe(self) -> None:
        assert telegram_markdown_unsafe("in\\_progress") is False

    def test_mixed_with_unsafe_part(self) -> None:
        assert telegram_markdown_unsafe("ok _bold_ but in_progress") is True

    def test_space_around_underscore_is_safe(self) -> None:
        assert telegram_markdown_unsafe("hello _ world") is False


class TestTelegramMarkdownRetryable:
    def test_cant_parse_entities(self) -> None:
        exc = RuntimeError("Can't parse entities: something went wrong")
        assert telegram_markdown_retryable(exc) is True

    def test_cant_find_end_of_entity(self) -> None:
        exc = RuntimeError("Can't find end of the entity starting at byte offset 5")
        assert telegram_markdown_retryable(exc) is True

    def test_other_error_not_retryable(self) -> None:
        exc = RuntimeError("connection refused")
        assert telegram_markdown_retryable(exc) is False

    def test_none_error_not_retryable(self) -> None:
        assert telegram_markdown_retryable(None) is False
