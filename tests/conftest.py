from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _set_default_web_tokens(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WEB_AUTH_TOKEN", "web-token")
    monkeypatch.setenv("WEB_ADMIN_TOKEN", "admin-token")
