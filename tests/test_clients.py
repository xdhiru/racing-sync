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


@pytest.mark.anyio
async def test_qbittorrent_set_save_path_uses_hashes_field():
    cfg = DestConfig(type="qbittorrent", host="http://localhost:8080", save_path="/downloads")
    client = QBittorrentClient(cfg, label="dest-qb")

    class DummyResponseContext:
        async def __aenter__(self):
            resp = MagicMock()
            resp.read = AsyncMock(return_value=b"")
            return resp

        async def __aexit__(self, *args):
            pass

    async def mock_request(method, endpoint, data=None, **kwargs):
        assert method == "POST"
        assert endpoint == "/api/v2/torrents/setLocation"
        fields = {field[0]["name"]: field[2] for field in data._fields}
        assert "hashes" in fields
        assert "hash" not in fields
        assert fields["hashes"] == "abc12345"
        assert fields["location"] == "/new/path"
        return DummyResponseContext()

    client.request = mock_request
    await client.set_save_path("abc12345", "/new/path")



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


@pytest.mark.anyio
async def test_http_client_auth_retry_releases_initial_response():
    from racing_sync.clients.http_base import HTTPClientBase
    from racing_sync.config import HTTPClientConfig

    cfg = HTTPClientConfig(
        host="http://127.0.0.1:8080",
        username="user",
        password="pass",
        nginx_mode="off",
    )
    client = HTTPClientBase(cfg, label="test-auth-retry")
    client._authed = True
    client._session = MagicMock()
    client._auth = AsyncMock()

    resp_401 = MagicMock()
    resp_401.status = 401
    resp_401.read = AsyncMock(return_value=b"Unauthorized")
    resp_401.close = MagicMock()

    resp_200 = MagicMock()
    resp_200.status = 200
    resp_200.read = AsyncMock(return_value=b"OK")
    resp_200.close = MagicMock()

    client._session.request = AsyncMock(side_effect=[resp_401, resp_200])

    res = await client.request("GET", "/test", retry_auth=True)
    assert res == resp_200
    resp_401.read.assert_awaited_once()
    resp_401.close.assert_called_once()
    client._auth.assert_awaited_once()


@pytest.mark.anyio
async def test_http_client_start_idempotent():
    from racing_sync.clients.http_base import HTTPClientBase
    from racing_sync.config import HTTPClientConfig

    cfg = HTTPClientConfig(host="http://127.0.0.1:8080", nginx_mode="off")
    client = HTTPClientBase(cfg)
    await client.start()
    s1 = client.session
    await client.start()
    s2 = client.session
    assert s1 is s2
    await client.close()


@pytest.mark.anyio
async def test_http_client_form_post_verifies_login_body():
    from racing_sync.clients.http_base import HTTPClientBase, AuthError
    from racing_sync.config import HTTPClientConfig

    cfg = HTTPClientConfig(
        host="http://127.0.0.1:8080",
        nginx_mode="form_post",
        nginx_url="http://127.0.0.1:8080/login",
        username="u",
        password="p",
    )
    client = HTTPClientBase(cfg)
    client._session = MagicMock()
    client._do_client_auth = AsyncMock()

    class MockResp:
        def __init__(self, text):
            self.status = 200
            self._text = text
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            pass
        async def text(self):
            return self._text

    client._session.post = MagicMock(return_value=MockResp('<html><input type="password"/>Invalid password</html>'))
    with pytest.raises(AuthError, match="rejected credentials"):
        await client._auth(force=True)


@pytest.mark.anyio
async def test_http_client_request_releases_connection_on_4xx():
    import aiohttp
    from racing_sync.clients.http_base import HTTPClientBase
    from racing_sync.config import HTTPClientConfig

    cfg = HTTPClientConfig(host="http://127.0.0.1:8080", nginx_mode="off")
    client = HTTPClientBase(cfg)
    client._authed = True
    client._session = MagicMock()

    resp_500 = MagicMock()
    resp_500.status = 500
    resp_500.text = AsyncMock(return_value="Server Error")
    resp_500.close = MagicMock()
    resp_500.request_info = MagicMock()
    resp_500.history = ()

    client._session.request = AsyncMock(return_value=resp_500)

    with pytest.raises(aiohttp.ClientResponseError):
        await client.request("GET", "/fail", retry_auth=False)
    resp_500.close.assert_called_once()


@pytest.mark.anyio
async def test_http_client_files_param_handling():
    import aiohttp
    from racing_sync.clients.http_base import HTTPClientBase
    from racing_sync.config import HTTPClientConfig

    cfg = HTTPClientConfig(host="http://127.0.0.1:8080", nginx_mode="off")
    client = HTTPClientBase(cfg)
    client._authed = True
    client._session = MagicMock()

    resp_200 = MagicMock()
    resp_200.status = 200
    resp_200.close = MagicMock()

    recorded_kwargs = {}

    async def mock_req(method, url, **kwargs):
        recorded_kwargs.update(kwargs)
        return resp_200

    client._session.request = mock_req

    res = await client.request("POST", "/upload", files={"torrent": b"d8:announce...e"})
    assert res == resp_200
    assert isinstance(recorded_kwargs.get("data"), aiohttp.FormData)



@pytest.mark.anyio
async def test_deluge_list_torrents_progress_and_hash_filtering():
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

    mock_torrents = {
        "hash_1": {
            "name": "Torrent 1",
            "progress": 1.5,  # 1.5% in Deluge
            "state": "Downloading",
            "total_size": 1000,
            "label": "",
            "save_path": "/downloads",
            "ratio": 0.0,
            "trackers": [],
            "time_added": 123456,
        },
        "hash_2": {
            "name": "Torrent 2",
            "progress": 100.0,  # 100% in Deluge
            "state": "Seeding",
            "total_size": 2000,
            "label": "",
            "save_path": "/downloads",
            "ratio": 1.0,
            "trackers": [],
            "time_added": 123457,
        },
    }

    client._rpc = AsyncMock(return_value=mock_torrents)

    # 1. Full list: progress should be scaled 0.0-1.0
    torrents = await client.list_torrents()
    assert len(torrents) == 2
    t1 = next(t for t in torrents if t.hash == "hash_1")
    t2 = next(t for t in torrents if t.hash == "hash_2")
    assert pytest.approx(t1.progress, 0.001) == 0.015
    assert not t1.is_complete()
    assert pytest.approx(t2.progress, 0.001) == 1.0
    assert t2.is_complete()

    # 2. Filtered by hash
    filtered = await client.list_torrents(hashes=["HASH_2"])
    assert len(filtered) == 1
    assert filtered[0].hash == "hash_2"

    # 3. get_torrent: selects the specific requested torrent even when RPC returns all
    client.get_torrent_files = AsyncMock(return_value=[])
    got_t2 = await client.get_torrent("hash_2")
    assert got_t2 is not None
    assert got_t2.hash == "hash_2"

    # 4. Generator in hashes: does not get exhausted before client-side filtering
    gen = (h for h in ["HASH_1"])
    from_gen = await client.list_torrents(hashes=gen)
    assert len(from_gen) == 1
    assert from_gen[0].hash == "hash_1"



@pytest.mark.anyio
async def test_deluge_set_file_priorities_indexed():
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

    async def mock_rpc(method, params=None):
        if method == "core.get_torrent_status":
            return {
                "files": [
                    {"path": "file0.mkv", "index": 0},
                    {"path": "file1.mkv", "index": 1},
                ],
                "file_priorities": [1, 1],
            }
        if method == "core.set_torrent_file_priorities":
            return True
        return None

    rpc_mock = AsyncMock(side_effect=mock_rpc)
    client._rpc = rpc_mock

    await client.set_file_priorities("hash_abc", {"file1.mkv": 0})
    rpc_mock.assert_any_call("core.set_torrent_file_priorities", ["hash_abc", [1, 0]])


