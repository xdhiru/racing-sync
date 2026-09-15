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
