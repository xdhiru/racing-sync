"""Client hardening: URL gates, fail-closed daemon check, progress clamp."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from racing_sync.prowlarr import _is_fetchable_http_url


def test_fetchable_blocks_numeric_loopbacks():
    # Numeric literals get the SAME verdict as their dotted form:
    # loopback is allowed (Prowlarr lives local), metadata is refused.
    assert _is_fetchable_http_url("http://2130706433/x") is True
    assert _is_fetchable_http_url("http://0x7f000001/x") is True
    assert _is_fetchable_http_url("http://127.0.0.1/x") is True  # prowlarr local
    assert _is_fetchable_http_url("http://169.254.169.254/x") is False
    assert _is_fetchable_http_url("http://2852039166/x") is False
    assert _is_fetchable_http_url("https://indexer.example/x") is True
    assert _is_fetchable_http_url("ftp://indexer.example/x") is False
    assert _is_fetchable_http_url("http:///x") is False


def test_qb_progress_clamped():
    from racing_sync.clients.qbittorrent import _torrent_from_qb

    base = {"hash": "a" * 40, "name": "T", "save_path": "/s",
            "size": 100, "state": "downloading", "ratio": 2.5}
    assert _torrent_from_qb({**base, "progress": 1.5}).progress == 1.0
    assert _torrent_from_qb({**base, "progress": -0.5}).progress == 0.0
    assert _torrent_from_qb({**base, "progress": 0.5}).progress == 0.5
    assert _torrent_from_qb({**base, "progress": 1.5}).ratio == 2.5


@pytest.mark.anyio
async def test_qb_add_rejects_bad_urls():
    from racing_sync.clients.qbittorrent import QBittorrentClient

    cfg = MagicMock()
    cfg.username = "u"
    client = QBittorrentClient.__new__(QBittorrentClient)
    from racing_sync.config import HTTPClientConfig

    http_cfg = MagicMock()  # plain: spec'd mocks hide real attrs
    from racing_sync.clients.http_base import HTTPClientBase
    HTTPClientBase.__init__(client, http_cfg, label="t")
    client._add_lock = __import__("asyncio").Lock()
    client.request = AsyncMock()
    for bad in ("file:///etc/passwd",
                "http:///no-host/x",
                "gopher://x/y",
                "not a url at all"):
        with pytest.raises(ValueError):
            await client.add_torrent(urls=[bad], save_path="/s")
    assert client.request.await_count == 0
    for good in ("magnet:?xt=urn:btih:" + "a" * 40,
                 "https://tracker.example/x"):
        client.request = AsyncMock(return_value=_Resp("Ok."))
        res = await client.add_torrent(urls=[good], save_path="/s")
        assert res.accepted is True


class _Resp:
    def __init__(self, text):
        self._text = text

    async def text(self):
        return self._text

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


@pytest.mark.anyio
async def test_deluge_health_check_failure_is_auth_error():
    """A health check that cannot complete must not return 'authed'."""
    from racing_sync.clients.deluge import DelugeClient
    from racing_sync.clients.http_base import AuthError
    from racing_sync.config import SourceConfig

    src = SourceConfig(
        type="deluge", host="http://127.0.0.1:8112", password="secret",
        deluge_sftp={"enabled": True, "ssh_host": "127.0.0.1",
                     "ssh_password": "pwd",
                     "state_dir": "/var/lib/deluged/state"},
    )
    client = DelugeClient.__new__(DelugeClient)
    from racing_sync.config import HTTPClientConfig
    from racing_sync.clients.http_base import HTTPClientBase
    HTTPClientBase.__init__(
        client, HTTPClientConfig.from_source(src), label="t")

    login = MagicMock()
    login.status = 200
    login.json = AsyncMock(
        return_value={"result": True, "error": None, "id": 1})

    def _post(url, **kw):
        m = kw.get("json") or {}
        if m.get("method") == "auth.login":
            return _Ctx(login)
        raise OSError("daemon probe blew up")

    class _Ctx:
        def __init__(self, r):
            self._r = r

        async def __aenter__(self):
            return self._r

        async def __aexit__(self, *a):
            return False

    client._session = MagicMock()
    client._session.post = _post
    with pytest.raises(AuthError, match="health check failed"):
        await client._do_client_auth()


def test_deluge_scan_lock_eager():
    from racing_sync.clients.deluge import DelugeClient
    from racing_sync.config import SourceConfig
    import asyncio

    client = DelugeClient(SourceConfig(
        type="deluge", host="http://127.0.0.1:8112", password="secret",
        deluge_sftp={"enabled": True, "ssh_host": "127.0.0.1",
                     "ssh_password": "pwd",
                     "state_dir": "/var/lib/deluged/state"},
    ))
    assert isinstance(client._scan_lock, asyncio.Lock)
