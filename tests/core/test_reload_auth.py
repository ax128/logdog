import pytest

from logwatch.web.api import verify_admin_token


def test_admin_token_missing_header_permission_error() -> None:
    with pytest.raises(PermissionError):
        verify_admin_token(None, "admin")


def test_admin_token_non_bearer_permission_error() -> None:
    with pytest.raises(PermissionError):
        verify_admin_token("Token admin", "admin")


def test_admin_token_wrong_token_permission_error() -> None:
    with pytest.raises(PermissionError):
        verify_admin_token("Bearer wrong", "admin")
