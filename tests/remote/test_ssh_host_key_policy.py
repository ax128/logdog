from __future__ import annotations

from unittest.mock import MagicMock, patch


def _make_mock_client():
    client = MagicMock()
    client.load_system_host_keys = MagicMock()
    client.set_missing_host_key_policy = MagicMock()
    return client


def test_strict_host_key_uses_reject_policy() -> None:
    from logdog.collector.host_metrics_probe import _set_host_key_policy

    client = _make_mock_client()
    with patch("logdog.collector.host_metrics_probe.paramiko") as mock_paramiko:
        mock_paramiko.RejectPolicy = MagicMock(return_value="reject_sentinel")
        mock_paramiko.WarningPolicy = MagicMock(return_value="warning_sentinel")
        _set_host_key_policy(client, strict_host_key=True)
    client.set_missing_host_key_policy.assert_called_once_with("reject_sentinel")


def test_non_strict_host_key_uses_warning_policy() -> None:
    from logdog.collector.host_metrics_probe import _set_host_key_policy

    client = _make_mock_client()
    with patch("logdog.collector.host_metrics_probe.paramiko") as mock_paramiko:
        mock_paramiko.RejectPolicy = MagicMock(return_value="reject_sentinel")
        mock_paramiko.WarningPolicy = MagicMock(return_value="warning_sentinel")
        _set_host_key_policy(client, strict_host_key=False)
    client.set_missing_host_key_policy.assert_called_once_with("warning_sentinel")


def test_non_strict_logs_warning() -> None:
    from logdog.collector.host_metrics_probe import _set_host_key_policy

    client = _make_mock_client()
    with patch("logdog.collector.host_metrics_probe.paramiko") as mock_paramiko:
        mock_paramiko.WarningPolicy = MagicMock(return_value="warning_sentinel")
        with patch("logdog.collector.host_metrics_probe.logger") as mock_logger:
            _set_host_key_policy(client, strict_host_key=False)
            mock_logger.warning.assert_called_once()
            assert "WarningPolicy" in str(mock_logger.warning.call_args)
