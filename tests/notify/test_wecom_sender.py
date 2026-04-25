from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from logdog.notify.wecom import WecomWebhookSender


def test_wecom_send_succeeds_on_first_try() -> None:
    sender = WecomWebhookSender(timeout_seconds=5, max_retries=3)
    with patch("logdog.notify.wecom.urlopen") as mock_urlopen:
        mock_resp = MagicMock()
        mock_resp.read.return_value = b'{"errcode": 0}'
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp
        sender.send("https://example.com/webhook", "test message")
    mock_urlopen.assert_called_once()


def test_wecom_send_retries_without_blocking_sleep() -> None:
    """Verify time.sleep is NOT called during retries."""
    sender = WecomWebhookSender(timeout_seconds=5, max_retries=2)
    from urllib.error import URLError

    with patch("logdog.notify.wecom.urlopen", side_effect=URLError("fail")):
        with patch("logdog.notify.wecom.time") as mock_time:
            mock_time.sleep = MagicMock()
            with pytest.raises(RuntimeError, match="wecom webhook send failed"):
                sender.send("https://example.com/webhook", "test")
            mock_time.sleep.assert_not_called()
