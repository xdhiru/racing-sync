import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi.testclient import TestClient

from racing_sync.api import build_app
from racing_sync.config import APIConfig, AppConfig
from racing_sync.recovery import RecoveryReport


@pytest.fixture
def anyio_backend():
    return "asyncio"


def test_api_recover_returns_dictionary():
    cfg = MagicMock(spec=AppConfig)
    cfg.api = APIConfig(enabled=True, api_token="secret", trust_nginx_header=False)

    coord = MagicMock()
    coord.cfg = cfg
    coord.dest_client = MagicMock()
    coord.store = MagicMock()

    rpt = RecoveryReport()
    rpt.kept.append("hash1")
    rpt.resumed.append("hash2")

    with patch("racing_sync.api.reconcile", new_callable=AsyncMock) as mock_reconcile:
        mock_reconcile.return_value = rpt
        app = build_app(coord)
        client = TestClient(app)

        resp = client.post("/api/recover", headers={"x-api-token": "secret"})
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, dict)
        assert data["summary"] == rpt.summary()
        assert data["kept"] == ["hash1"]
        assert data["resumed"] == ["hash2"]
        assert data["re_added"] == []
        assert data["orphans"] == []
        assert data["unknowns"] == []


def test_auth_rejects_nginx_header_from_untrusted_remote_ip():
    cfg = MagicMock(spec=AppConfig)
    cfg.api = APIConfig(enabled=True, api_token="", trust_nginx_header=True)

    coord = MagicMock()
    coord.cfg = cfg
    coord.store = MagicMock()
    coord.store.all.return_value = []

    app = build_app(coord)
    # Remote IP 198.51.100.1 is not in trusted_proxies
    client = TestClient(app, client=("198.51.100.1", 50000))

    resp = client.get("/api/state", headers={"X-Authenticated-User": "admin"})
    assert resp.status_code == 403
    assert resp.json()["detail"] == "untrusted proxy for nginx auth header"


def test_auth_accepts_nginx_header_from_trusted_client():
    cfg = MagicMock(spec=AppConfig)
    cfg.api = APIConfig(enabled=True, api_token="", trust_nginx_header=True)

    coord = MagicMock()
    coord.cfg = cfg
    coord.store = MagicMock()
    coord.store.all.return_value = []

    app = build_app(coord)
    # 127.0.0.1 is in default trusted_proxies
    client = TestClient(app, client=("127.0.0.1", 50000))

    resp = client.get("/api/state", headers={"X-Authenticated-User": "admin"})
    assert resp.status_code == 200
    assert resp.json() == []


def test_auth_rejects_loopback_when_not_in_custom_trusted_proxies():
    cfg = MagicMock(spec=AppConfig)
    cfg.api = APIConfig(
        enabled=True,
        api_token="",
        trust_nginx_header=True,
        trusted_proxies=["10.0.0.1"],
    )

    coord = MagicMock()
    coord.cfg = cfg
    coord.store = MagicMock()
    coord.store.all.return_value = []

    app = build_app(coord)
    # Connecting from 127.0.0.1 is rejected when trusted_proxies only contains 10.0.0.1
    client = TestClient(app, client=("127.0.0.1", 50000))
    resp = client.get("/api/state", headers={"X-Authenticated-User": "admin"})
    assert resp.status_code == 403
    assert resp.json()["detail"] == "untrusted proxy for nginx auth header"


def test_auth_token_validation():
    cfg = MagicMock(spec=AppConfig)
    cfg.api = APIConfig(enabled=True, api_token="topsecret", trust_nginx_header=False)

    coord = MagicMock()
    coord.cfg = cfg
    coord.store = MagicMock()
    coord.store.all.return_value = []

    app = build_app(coord)
    client = TestClient(app)

    # Missing token
    assert client.get("/api/state").status_code == 401
    # Wrong token
    assert client.get("/api/state", headers={"X-Api-Token": "wrong"}).status_code == 401
    # Correct token
    assert client.get("/api/state", headers={"X-Api-Token": "topsecret"}).status_code == 200


def test_logs_query_limit_bounds():
    cfg = MagicMock(spec=AppConfig)
    cfg.api = APIConfig(enabled=True, api_token="secret", trust_nginx_header=False)

    coord = MagicMock()
    coord.cfg = cfg
    coord.store = MagicMock()
    coord.store.iter_logs.return_value = []

    app = build_app(coord)
    client = TestClient(app)
    headers = {"X-Api-Token": "secret"}

    # Valid limits
    resp = client.get("/api/logs?limit=50", headers=headers)
    assert resp.status_code == 200
    coord.store.iter_logs.assert_called_with(limit=50)

    # Limit below minimum (0) -> 422
    resp_under = client.get("/api/logs?limit=0", headers=headers)
    assert resp_under.status_code == 422

    # Limit above maximum (1001) -> 422
    resp_over = client.get("/api/logs?limit=1001", headers=headers)
    assert resp_over.status_code == 422


def test_scan_watch_endpoint():
    cfg = MagicMock(spec=AppConfig)
    cfg.api = APIConfig(enabled=True, api_token="secret", trust_nginx_header=False)

    coord = MagicMock()
    coord.cfg = cfg
    coord.watch = MagicMock()
    coord.scan_watch = AsyncMock(return_value=[MagicMock(), MagicMock()])

    app = build_app(coord)
    client = TestClient(app)
    headers = {"X-Api-Token": "secret"}

    resp = client.post("/api/scan-watch", headers=headers)
    assert resp.status_code == 200
    assert resp.json() == {"items": 2}
    coord.scan_watch.assert_awaited_once()


@pytest.mark.anyio
async def test_coordinator_scan_watch_ingests_and_returns_items(anyio_backend):
    from unittest.mock import AsyncMock
    from racing_sync.coordinator import Coordinator
    from racing_sync.watchdir import WatchItem

    coord = object.__new__(Coordinator)
    coord.cfg = MagicMock(spec=AppConfig)
    coord.cfg.watch_dir = MagicMock(delete_after_pickup=True)
    coord.store = MagicMock()
    coord.store.get.return_value = None

    mock_watch = MagicMock()
    item = WatchItem(
        infohash="a" * 40,
        name="Test.Torrent",
        size_bytes=1000,
        announce_url="http://tracker/announce",
        torrent_bytes=b"torrentdata",
        torrent_path=MagicMock(),
    )
    mock_watch.scan_once = AsyncMock(return_value=[item])
    mock_watch.delete_picked_up = AsyncMock()
    coord.watch = mock_watch

    items = await coord.scan_watch()
    assert items == [item]
    coord.store.upsert.assert_called_once()
    mock_watch.delete_picked_up.assert_awaited_once_with(item)


def test_api_retry_and_ssd_endpoints():
    from racing_sync.state import State, TorrentState

    cfg = MagicMock(spec=AppConfig)
    cfg.api = APIConfig(enabled=True, api_token="secret", trust_nginx_header=False)
    cfg.ssd = MagicMock(path="/downloads")

    coord = MagicMock()
    coord.cfg = cfg
    coord.store = MagicMock()

    app = build_app(coord)
    client = TestClient(app)
    headers = {"X-Api-Token": "secret"}

    # 1. /api/ssd
    with patch("racing_sync.api.ssd_free_bytes", return_value=500_000_000):
        resp_ssd = client.get("/api/ssd", headers=headers)
        assert resp_ssd.status_code == 200
        assert resp_ssd.json() == {"free_bytes": 500_000_000, "path": "/downloads"}

    # 2. /api/retry/{hash} 404 (unknown hash)
    coord.store.get.return_value = None
    resp_404 = client.post("/api/retry/" + "a" * 40, headers=headers)
    assert resp_404.status_code == 404
    assert resp_404.json()["detail"] == "unknown hash"

    # 2b. /api/retry/{hash} 422 (not a 40-char hex infohash)
    resp_422 = client.post("/api/retry/not-a-hash", headers=headers)
    assert resp_422.status_code == 422

    # 3. /api/retry/{hash} 409 (not FAILED) — uppercase accepted via normalization
    ts_downloading = TorrentState(source_infohash="b" * 40, state=State.DOWNLOADING)
    coord.store.get.return_value = ts_downloading
    resp_409 = client.post("/api/retry/" + "B" * 40, headers=headers)
    assert resp_409.status_code == 409
    assert resp_409.json()["detail"] == "state is downloading"

    # 4. /api/retry/{hash} 200 (in FAILED state)
    ts_failed = TorrentState(source_infohash="c" * 40, state=State.FAILED)
    coord.store.get.return_value = ts_failed
    resp_200 = client.post("/api/retry/" + "c" * 40, headers=headers)
    assert resp_200.status_code == 200
    coord.store.transition.assert_called_once_with(ts_failed, State.QUEUED, error="")


def test_build_app_without_fastapi_raises_error():
    coord = MagicMock()
    with patch("racing_sync.api.HAS_FASTAPI", False):
        with pytest.raises(RuntimeError, match="FastAPI is required"):
            build_app(coord)


