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


@pytest.mark.anyio
async def test_deluge_add_torrent_rejects_none_result_and_uses_uuid_filename():
    from unittest.mock import AsyncMock
    from racing_sync.clients.deluge import DelugeClient
    from racing_sync.config import SourceConfig

    cfg = SourceConfig(
        type="deluge",
        host="http://localhost:8112",
        password="secret",
        deluge_sftp={"enabled": True, "ssh_host": "127.0.0.1", "ssh_password": "pwd", "state_dir": "/var/lib/deluged/state"},
    )
    client = DelugeClient(cfg)

    # 1. Deluge returns [None] on rejection -> accepted should be False
    client._rpc = AsyncMock(return_value=None)
    res_rejected = await client.add_torrent(urls=["http://example.com/rejected.torrent"], save_path="/downloads")
    assert res_rejected.accepted is False
    assert res_rejected.hash is None

    # 2. Deluge returns unique uuid filenames for multiple .torrent files
    blob1 = b"d8:announce11:http://test1e"
    blob2 = b"d8:announce11:http://test2e"
    filenames_used: list[str] = []

    async def mock_rpc(method, params=None):
        if method == "core.add_torrent_file":
            filenames_used.append(params[0])
            return "hash_123"
        return None

    client._rpc = AsyncMock(side_effect=mock_rpc)
    res_success = await client.add_torrent(torrent_files=[blob1, blob2], save_path="/downloads")
    assert res_success.accepted is True
    assert res_success.hash == "hash_123"
    assert len(filenames_used) == 2
    # Filenames must be unique and not match identical blob[:6].hex() prefix
    assert filenames_used[0] != filenames_used[1]
    assert not filenames_used[0].startswith("64383a616e6e")


@pytest.mark.anyio
async def test_deluge_set_file_priorities_propagates_auth_error():
    from unittest.mock import AsyncMock
    from racing_sync.clients.deluge import DelugeClient
    from racing_sync.clients.http_base import AuthError
    from racing_sync.config import SourceConfig

    cfg = SourceConfig(
        type="deluge",
        host="http://localhost:8112",
        password="secret",
        deluge_sftp={"enabled": True, "ssh_host": "127.0.0.1", "ssh_password": "pwd", "state_dir": "/var/lib/deluged/state"},
    )
    client = DelugeClient(cfg)
    client._rpc = AsyncMock(side_effect=AuthError("session expired"))

    with pytest.raises(AuthError, match="session expired"):
        await client.set_file_priorities("hash_123", {"file1.mkv": 1})


@pytest.mark.anyio
async def test_deluge_get_torrent_files_scales_progress_and_propagates_auth():
    from unittest.mock import AsyncMock
    from racing_sync.clients.deluge import DelugeClient
    from racing_sync.clients.http_base import AuthError
    from racing_sync.config import SourceConfig

    cfg = SourceConfig(
        type="deluge",
        host="http://localhost:8112",
        password="secret",
        deluge_sftp={"enabled": True, "ssh_host": "127.0.0.1", "ssh_password": "pwd", "state_dir": "/var/lib/deluged/state"},
    )
    client = DelugeClient(cfg)

    # 1. Progress scaled from 0-100 down to 0-1.0
    client._rpc = AsyncMock(return_value={
        "files": [
            {"path": "ep1.mkv", "size": 1000, "index": 0},
            {"path": "ep2.mkv", "size": 2000, "index": 1},
        ],
        "file_priorities": [1, 1],
        "file_progress": [50.0, 100.0],  # 50% and 100% in Deluge
    })
    files = await client.get_torrent_files("hash_123")
    assert len(files) == 2
    assert pytest.approx(files[0].progress, 0.001) == 0.5
    assert pytest.approx(files[1].progress, 0.001) == 1.0

    # 2. AuthError is not swallowed as fallback
    client._rpc = AsyncMock(side_effect=AuthError("unauthorized"))
    with pytest.raises(AuthError, match="unauthorized"):
        await client.get_torrent_files("hash_123")


@pytest.mark.anyio
async def test_deluge_files_from_torrent_file_catches_sftp_error():
    from unittest.mock import patch
    from racing_sync.clients.deluge import DelugeClient
    from racing_sync.config import SourceConfig

    cfg = SourceConfig(
        type="deluge",
        host="http://localhost:8112",
        password="secret",
        deluge_sftp={"enabled": True, "ssh_host": "127.0.0.1", "ssh_password": "pwd", "state_dir": "/var/lib/deluged/state"},
    )
    client = DelugeClient(cfg)

    with patch("racing_sync.sftp_source.SFTPExporter.fetch_torrent", side_effect=ConnectionRefusedError("SSH server down")):
        files = await client._files_from_torrent_file("hash_123")
        assert files == []


@pytest.mark.anyio
async def test_deluge_files_prefers_shared_sftp_exporter():
    """Shared exporter must be reused; no fresh handshake per call."""
    from unittest.mock import AsyncMock, MagicMock, patch
    from racing_sync.clients.deluge import DelugeClient
    from racing_sync.config import SourceConfig

    cfg = SourceConfig(
        type="deluge",
        host="http://localhost:8112",
        password="secret",
        deluge_sftp={"enabled": True, "ssh_host": "127.0.0.1", "ssh_password": "pwd", "state_dir": "/var/lib/deluged/state"},
    )
    client = DelugeClient(cfg)
    shared = MagicMock()
    shared.fetch_torrent.return_value = None
    client.set_sftp_exporter(shared)

    with patch("racing_sync.sftp_source.SFTPExporter") as mock_cls:
        files = await client._files_from_torrent_file("a" * 40)
        assert files == []
        mock_cls.assert_not_called()
    shared.fetch_torrent.assert_called_once()


@pytest.mark.anyio
async def test_http_client_authed_reset_on_close_and_origin_rfc6454():
    from racing_sync.clients.http_base import HTTPClientBase
    from racing_sync.config import HTTPClientConfig

    cfg = HTTPClientConfig(host="http://seedbox.lan:8080/qbittorrent/path", nginx_mode="off")
    client = HTTPClientBase(cfg)
    await client.start()
    client._authed = True

    # Origin header must not contain sub-path
    assert client.session.headers["Origin"] == "http://seedbox.lan:8080"
    assert client.session.headers["Referer"] == "http://seedbox.lan:8080/qbittorrent/path/"

    await client.close()
    assert client._authed is False
    assert client._session is None

    # Restarting must also keep _authed False until auth completes
    await client.start()
    assert client._authed is False
    await client.close()


@pytest.mark.anyio
async def test_http_client_form_data_rebuilt_per_attempt_and_tuple_specs():
    import aiohttp
    from racing_sync.clients.http_base import HTTPClientBase
    from racing_sync.config import HTTPClientConfig

    cfg = HTTPClientConfig(host="http://127.0.0.1:8080", nginx_mode="off")
    client = HTTPClientBase(cfg)
    client._authed = True
    client._session = MagicMock()

    resp_fail = MagicMock()
    resp_fail.status = 500
    resp_fail.close = MagicMock()

    resp_ok = MagicMock()
    resp_ok.status = 200
    resp_ok.close = MagicMock()

    attempts_data: list[aiohttp.FormData] = []

    async def mock_req(method, url, data=None, **kwargs):
        attempts_data.append(data)
        if len(attempts_data) == 1:
            raise aiohttp.ClientOSError("network glitch")
        return resp_ok

    client._session.request = mock_req

    files = [
        ("t1", ("file1.torrent", b"content1", "application/x-bittorrent")),
        ("t2", "file2.torrent", b"content2", "application/x-bittorrent", {"X-Custom": "val"}),
    ]
    res = await client.request("POST", "/upload", data={"key": "val"}, files=files)
    assert res == resp_ok
    assert len(attempts_data) == 2
    # Ensure distinct FormData objects were passed on each attempt
    assert attempts_data[0] is not attempts_data[1]

    # Verify field extraction on the successful FormData
    fd = attempts_data[1]
    field_names = [f[0]["name"] for f in fd._fields]
    assert "key" in field_names
    assert "t1" in field_names
    assert "t2" in field_names

    t1_field = next(f for f in fd._fields if f[0]["name"] == "t1")
    assert t1_field[0]["filename"] == "file1.torrent"
    assert t1_field[1].get("Content-Type") == "application/x-bittorrent"

    t2_field = next(f for f in fd._fields if f[0]["name"] == "t2")
    assert t2_field[0]["filename"] == "file2.torrent"
    assert t2_field[1].get("Content-Type") == "application/x-bittorrent"


@pytest.mark.anyio
async def test_http_client_retries_transient_status_codes():
    from racing_sync.clients.http_base import HTTPClientBase
    from racing_sync.config import HTTPClientConfig

    cfg = HTTPClientConfig(host="http://127.0.0.1:8080", nginx_mode="off")
    client = HTTPClientBase(cfg)
    client._authed = True
    client._session = MagicMock()

    resp_502 = MagicMock()
    resp_502.status = 502
    resp_502.headers = {}
    resp_502.read = AsyncMock(return_value=b"Bad Gateway")
    resp_502.close = MagicMock()

    resp_429 = MagicMock()
    resp_429.status = 429
    resp_429.headers = {"Retry-After": "0.01"}
    resp_429.read = AsyncMock(return_value=b"Too Many Requests")
    resp_429.close = MagicMock()

    resp_200 = MagicMock()
    resp_200.status = 200
    resp_200.close = MagicMock()

    client._session.request = AsyncMock(side_effect=[resp_502, resp_429, resp_200])

    res = await client.request("GET", "/transient")
    assert res == resp_200
    assert resp_502.read.await_count == 1
    assert resp_429.read.await_count == 1


@pytest.mark.anyio
async def test_http_client_does_not_conflate_403_with_auth_error():
    import aiohttp
    from racing_sync.clients.http_base import HTTPClientBase, AuthError
    from racing_sync.config import HTTPClientConfig

    cfg = HTTPClientConfig(host="http://127.0.0.1:8080", nginx_mode="off")
    client = HTTPClientBase(cfg)
    client._authed = True
    client._session = MagicMock()
    client._auth = AsyncMock()  # Login succeeds!

    resp_403_initial = MagicMock()
    resp_403_initial.status = 403
    resp_403_initial.read = AsyncMock(return_value=b"Forbidden")
    resp_403_initial.close = MagicMock()

    resp_403_after_login = MagicMock()
    resp_403_after_login.status = 403
    resp_403_after_login.text = AsyncMock(return_value="Permission denied")
    resp_403_after_login.close = MagicMock()
    resp_403_after_login.request_info = MagicMock()
    resp_403_after_login.history = ()

    client._session.request = AsyncMock(side_effect=[resp_403_initial, resp_403_after_login])

    # Must raise aiohttp.ClientResponseError (status 403), NOT AuthError
    with pytest.raises(aiohttp.ClientResponseError) as exc_info:
        await client.request("POST", "/admin_action", retry_auth=True)
    assert exc_info.value.status == 403


def test_torrent_from_qb_sanitizes_single_file_content_path():
    from racing_sync.clients.qbittorrent import _torrent_from_qb

    # 1. content_path is a single file when save_path is empty
    d1 = {
        "hash": "abc12345",
        "name": "Movie.mkv",
        "save_path": "",
        "content_path": "/mnt/nvme/downloads/Movie.mkv",
    }
    t1 = _torrent_from_qb(d1)
    assert t1.save_path.replace("\\", "/") == "/mnt/nvme/downloads"

    # 2. save_path itself accidentally has a file extension
    d2 = {
        "hash": "abc12345",
        "name": "Movie.mkv",
        "save_path": "/mnt/nvme/downloads/Movie.mkv",
        "content_path": "/mnt/nvme/downloads/Movie.mkv",
    }
    t2 = _torrent_from_qb(d2)
    assert t2.save_path.replace("\\", "/") == "/mnt/nvme/downloads"

    # 3. normal directory save_path
    d3 = {
        "hash": "abc12345",
        "name": "Show.S01",
        "save_path": "/mnt/nvme/downloads",
        "content_path": "/mnt/nvme/downloads/Show.S01",
    }
    t3 = _torrent_from_qb(d3)
    assert t3.save_path.replace("\\", "/") == "/mnt/nvme/downloads"


@pytest.mark.anyio
async def test_qbittorrent_get_torrent_parallelizes_rtts():
    cfg = DestConfig(type="qbittorrent", host="http://localhost:8080", save_path="/downloads")
    client = QBittorrentClient(cfg, label="dest-qb")

    mock_t = MagicMock()
    mock_t.hash = "abc"
    client.list_torrents = AsyncMock(return_value=[mock_t])
    client.get_torrent_files = AsyncMock(return_value=[])
    client.get_trackers = AsyncMock(return_value=["http://tracker"])

    t = await client.get_torrent("abc")
    assert t is mock_t
    assert t.trackers == ["http://tracker"]
    client.list_torrents.assert_awaited_once_with(hashes=["abc"])
    client.get_torrent_files.assert_awaited_once_with("abc")
    client.get_trackers.assert_awaited_once_with("abc")


@pytest.mark.anyio
async def test_qbittorrent_add_torrent_handles_duplicates_and_hex_validation():
    from racing_sync.watchdir import _bencode
    cfg = DestConfig(type="qbittorrent", host="http://localhost:8080", save_path="/downloads")
    client = QBittorrentClient(cfg, label="dest-qb")

    torrent_blob = _bencode({
        b"announce": b"http://tracker/announce",
        b"info": {
            b"name": b"Duplicate.Movie",
            b"length": 1000,
            b"piece length": 16384,
            b"pieces": b"12345678901234567890",
        },
    })
    from racing_sync.watchdir import _bencoded_info_hash
    expected_hash, _, _, _ = _bencoded_info_hash(torrent_blob)

    class DummyResponseContext:
        def __init__(self, text):
            self.text = text

        async def __aenter__(self):
            resp = MagicMock()
            resp.text = AsyncMock(return_value=self.text)
            return resp

        async def __aexit__(self, *args):
            pass

    # 1. qB returns "Fails.", but torrent already exists on qB -> accepted=True, detail="already added"
    client.request = AsyncMock(return_value=DummyResponseContext("Fails."))
    existing_t = MagicMock()
    client.get_torrent = AsyncMock(return_value=existing_t)

    res_dup = await client.add_torrent(torrent_files=[torrent_blob], save_path="/downloads")
    assert res_dup.accepted is True
    assert res_dup.hash == expected_hash.lower()
    assert res_dup.detail == "already added"

    # 2. qB returns "Fails." and torrent does not exist -> accepted=False
    client.get_torrent = AsyncMock(return_value=None)
    res_fail = await client.add_torrent(torrent_files=[torrent_blob], save_path="/downloads")
    assert res_fail.accepted is False
    assert res_fail.detail == "Fails."

    # 3. 40-character non-hex response (e.g. error message) -> hash must be None
    non_hex_40 = "This is an error message of 40 chars!!!!"
    assert len(non_hex_40) == 40
    client.request = AsyncMock(return_value=DummyResponseContext(non_hex_40))
    res_non_hex = await client.add_torrent(urls=["http://example.com/test.torrent"], save_path="/downloads")
    assert res_non_hex.hash is None

    # 4. 40-character hex response -> hash is parsed
    hex_40 = "a" * 40
    client.request = AsyncMock(return_value=DummyResponseContext(hex_40))
    res_hex = await client.add_torrent(urls=["http://example.com/test.torrent"], save_path="/downloads")
    assert res_hex.hash == hex_40
    assert res_hex.accepted is True



