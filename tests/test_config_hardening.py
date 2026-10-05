"""Config/API/Telegram hardening: proxy validation, allowlist warning."""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from racing_sync.telegram_bot import allowlist_open_warning


def test_allowlist_warning():
    assert allowlist_open_warning(SimpleNamespace(admin_user_ids=[])) is not None
    assert "admin_user_ids" in allowlist_open_warning(
        SimpleNamespace(admin_user_ids=[]))
    assert allowlist_open_warning(SimpleNamespace(admin_user_ids=[123])) is None
    # Missing/unconfigured counts as open (the default under test).
    assert allowlist_open_warning(SimpleNamespace()) is not None
    assert allowlist_open_warning(None) is None


def _api_cfg(**kw):
    from racing_sync.config import APIConfig

    base = dict(enabled=True, trust_nginx_header=True,
                api_token="sekret-token-value")
    base.update(kw)
    return APIConfig(**base)


def test_trusted_proxies_accept_sane():
    cfg = _api_cfg(trusted_proxies=["127.0.0.1", "::1", "localhost",
                                    "10.0.0.0/8", "fd00::/8"])
    assert cfg.trusted_proxies[0] == "127.0.0.1"


def test_trusted_proxies_reject_world():
    from pydantic import ValidationError

    for bad in (["0.0.0.0/0"], ["::/0"], ["proxy.lan"], [""], ["10.0.0.0/8", "ok" * 0 + "??"]):
        with pytest.raises(ValidationError):
            _api_cfg(trusted_proxies=bad)


def test_trusted_proxies_unchecked_when_header_untrusted():
    """Validation only gates the dangerous combination (header trust on)."""
    from racing_sync.config import APIConfig

    cfg = APIConfig(enabled=True, trust_nginx_header=False,
                    api_token="sekret-token-value",
                    trusted_proxies=["0.0.0.0/0"])
    assert cfg.trusted_proxies == ["0.0.0.0/0"]
