"""Config/API/Telegram hardening: proxy validation, allowlist warning."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from racing_sync.telegram_bot import allowlist_open_warning


def test_allowlist_warning():
    assert allowlist_open_warning(SimpleNamespace(admin_user_ids=[])) is not None
    assert "admin_user_ids" in allowlist_open_warning(SimpleNamespace(admin_user_ids=[]))
    assert allowlist_open_warning(SimpleNamespace(admin_user_ids=[123])) is None
    # Missing/unconfigured counts as open (the default under test).
    assert allowlist_open_warning(SimpleNamespace()) is not None
    assert allowlist_open_warning(None) is None


def _api_cfg(**kw):
    from racing_sync.config import APIConfig

    base = dict(enabled=True, trust_nginx_header=True, api_token="sekret-token-value")
    base.update(kw)
    return APIConfig(**base)


def test_trusted_proxies_accept_sane():
    cfg = _api_cfg(trusted_proxies=["127.0.0.1", "::1", "localhost", "10.0.0.0/8", "fd00::/8"])
    assert cfg.trusted_proxies[0] == "127.0.0.1"


def test_trusted_proxies_reject_world():
    from pydantic import ValidationError

    for bad in (["0.0.0.0/0"], ["::/0"], ["proxy.lan"], [""], ["10.0.0.0/8", "ok" * 0 + "??"]):
        with pytest.raises(ValidationError):
            _api_cfg(trusted_proxies=bad)


def test_trusted_proxies_unchecked_when_header_untrusted():
    """Validation only gates the dangerous combination (header trust on)."""
    from racing_sync.config import APIConfig

    cfg = APIConfig(
        enabled=True,
        trust_nginx_header=False,
        api_token="sekret-token-value",
        trusted_proxies=["0.0.0.0/0"],
    )
    assert cfg.trusted_proxies == ["0.0.0.0/0"]


def _tg_cfg(**kw):
    from racing_sync.config import TelegramConfig

    base = dict(enabled=False)
    base.update(kw)
    return TelegramConfig(**base)


def test_admin_ids_reject_non_positive():
    """Zero/negative allowlist entries fail at load, not at first tap."""
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        _tg_cfg(admin_user_ids=[123, 0])
    with pytest.raises(ValidationError):
        _tg_cfg(admin_user_ids=[-5])


def test_admin_ids_deduped():
    """Duplicates normalize away; order of first appearance wins."""
    cfg = _tg_cfg(admin_user_ids=[123, 456, 123])
    assert cfg.admin_user_ids == [123, 456]
    assert _tg_cfg().admin_user_ids == []


def test_deluge_sftp_disabled_needs_no_state_dir():
    """A leftover disabled stub must load; "" means unset, not Path(".")."""
    import pydantic

    from racing_sync.config import DelugeSFTPConfig

    assert DelugeSFTPConfig(enabled=False).state_dir is None
    assert DelugeSFTPConfig(enabled=False, state_dir="").state_dir is None

    with pytest.raises(pydantic.ValidationError, match="state_dir"):
        DelugeSFTPConfig(enabled=True, ssh_password="pwd", state_dir="")


def test_dest_config_rejects_unusable_origin_and_relative_path():
    """Bad origins and a relative save_path must fail at load, not at RPC."""
    import pydantic

    from racing_sync.config import DestConfig

    good = DestConfig(host="http://localhost:8080", save_path="/downloads")
    assert str(good.save_path).replace("\\", "/") == "/downloads"

    for bad_host in (
        "localhost:8080",
        "ftp://x.example",
        "http://user:pw@x.example",
        "http://x.example?q=1",
    ):
        with pytest.raises(pydantic.ValidationError):
            DestConfig(host=bad_host, save_path="/downloads")

    with pytest.raises(pydantic.ValidationError, match="absolute"):
        DestConfig(host="http://localhost:8080", save_path="downloads")


def test_allowlist_warning_survives_a_broken_config():
    """The function returns a message or None — never a boolean."""

    class _Bad:
        @property
        def admin_user_ids(self):
            raise RuntimeError("boom")

    out = allowlist_open_warning(_Bad())
    assert isinstance(out, str) and out


def test_source_category_rejects_a_comma_list():
    """The value is sent verbatim; a comma list would match nothing."""
    import pydantic

    from racing_sync.config import SourceConfig

    with pytest.raises(pydantic.ValidationError, match="ONE category"):
        SourceConfig(type="qbittorrent", host="http://localhost:8080", category="auto,manual")

    assert (
        SourceConfig(type="qbittorrent", host="http://localhost:8080", category="auto*").category
        == "auto*"
    )
