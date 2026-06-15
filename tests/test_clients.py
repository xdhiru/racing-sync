from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock

from racing_sync.config import DestConfig, SourceConfig
from racing_sync.clients.qbittorrent import QBittorrentClient
from racing_sync.clients.deluge import DelugeClient
from racing_sync.clients.abstract import TorrentFile


def test_instantiate_qbittorrent_client():
    cfg = DestConfig(type="qbittorrent", host="http://localhost:8080", save_path="/downloads")
    client = QBittorrentClient(cfg, label="dest-qb")
    assert client._add_lock is not None


def test_instantiate_deluge_client():
    cfg = SourceConfig(
        type="deluge",
        host="http://localhost:8112",
        password="secret",
        deluge_sftp={
            "enabled": True,
            "ssh_host": "127.0.0.1",
            "ssh_password": "pwd",
            "state_dir": "/var/lib/deluged/state",
        },
    )
    client = DelugeClient(cfg)
    assert client is not None
    assert cfg.deluge_sftp.ssh_host == "127.0.0.1"


@pytest.mark.anyio
async def test_qbittorrent_export_torrent():
    cfg = DestConfig(type="qbittorrent", host="http://localhost:8080", save_path="/downloads")
    client = QBittorrentClient(cfg, label="dest-qb")

    class DummyResponseContext:
        async def __aenter__(self):
            resp = MagicMock()
            resp.read = AsyncMock(return_value=b"d8:announce...")
            return resp

        async def __aexit__(self, *args):
            pass

    async def mock_request(method, endpoint, params=None, **kwargs):
        assert method == "GET"
        assert endpoint == "/api/v2/torrents/export"
        assert params == {"hash": "abc12345"}
        return DummyResponseContext()

    client.request = mock_request
    data = await client.export_torrent("abc12345")
    assert data == b"d8:announce..."


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
async def test_http_client_base_nginx_modes():
    from racing_sync.clients.http_base import HTTPClientBase, AuthError
    from racing_sync.config import HTTPClientConfig

    # mode="off": no Authorization header
    cfg_off = HTTPClientConfig(
        host="http://127.0.0.1:8080",
        username="user",
        password="pass",
        nginx_mode="off",
    )
    client_off = HTTPClientBase(cfg_off)
    await client_off.start()
    try:
        assert "Authorization" not in client_off.session.headers
    finally:
        await client_off.close()

    # mode="basic": Authorization header present
    cfg_basic = HTTPClientConfig(
        host="http://127.0.0.1:8080",
        username="user",
        password="pass",
        nginx_mode="basic",
    )
    client_basic = HTTPClientBase(cfg_basic)
    await client_basic.start()
    try:
        assert "Authorization" in client_basic.session.headers
        assert client_basic.session.headers["Authorization"].startswith("Basic ")
    finally:
        await client_basic.close()

    # mode="form_post" without nginx_url raises AuthError
    cfg_form = HTTPClientConfig(
        host="http://127.0.0.1:8080",
        username="user",
        password="pass",
        nginx_mode="form_post",
        nginx_url="",
    )
    client_form = HTTPClientBase(cfg_form)
    await client_form.start()
    try:
        with pytest.raises(AuthError, match="no nginx_url"):
            await client_form._auth()
    finally:
        await client_form.close()


@pytest.mark.anyio
async def test_qbittorrent_set_file_priorities_batched():
    cfg = DestConfig(type="qbittorrent", host="http://localhost:8080", save_path="/downloads")
    client = QBittorrentClient(cfg, label="dest-qb")

    files = [
        TorrentFile(name="f0.mkv", size_bytes=100, priority=1, progress=0.0),
        TorrentFile(name="f1.mkv", size_bytes=100, priority=1, progress=0.0),
        TorrentFile(name="f2.mkv", size_bytes=100, priority=1, progress=0.0),
        TorrentFile(name="f3.mkv", size_bytes=100, priority=1, progress=0.0),
    ]
    client.get_torrent_files = AsyncMock(return_value=files)

    posted_requests = []

    class DummyResponseContext:
        def __init__(self, data):
            self.data = data

        async def __aenter__(self):
            resp = MagicMock()
            resp.read = AsyncMock(return_value=b"")
            return resp

        async def __aexit__(self, *args):
            pass

    async def mock_request(method, endpoint, data=None, **kwargs):
        # inspect FormData fields
        fields = {params["name"]: value for params, headers, value in data._fields}
        posted_requests.append((endpoint, fields))
        return DummyResponseContext(data)

    client.request = mock_request

    # Priorities: f0, f1 -> 0, f2, f3 -> 1
    await client.set_file_priorities("hash1", {"f0.mkv": 0, "f1.mkv": 0, "f2.mkv": 1, "f3.mkv": 1})

    assert len(posted_requests) == 2
    assert posted_requests[0] == ("/api/v2/torrents/filePrio", {"hash": "hash1", "id": "0|1", "priority": "0"})
    assert posted_requests[1] == ("/api/v2/torrents/filePrio", {"hash": "hash1", "id": "2|3", "priority": "1"})

