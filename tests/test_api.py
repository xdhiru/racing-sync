from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch
from fastapi.testclient import TestClient

from racing_sync.api import build_app
from racing_sync.config import APIConfig, AppConfig
from racing_sync.recovery import RecoveryReport


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
    # Default TestClient has client.host == "testclient" which is in trusted set
    client = TestClient(app)

    resp = client.get("/api/state", headers={"X-Authenticated-User": "admin"})
    assert resp.status_code == 200
    assert resp.json() == []


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
