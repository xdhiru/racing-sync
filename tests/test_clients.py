from __future__ import annotations

from racing_sync.config import DestConfig, SourceConfig
from racing_sync.clients.qbittorrent import QBittorrentClient
from racing_sync.clients.deluge import DelugeClient


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
            "host": "127.0.0.1",
            "ssh_password": "pwd",
            "state_dir": "/var/lib/deluged/state",
        },
    )
    client = DelugeClient(cfg)
    assert client is not None


import pytest
from unittest.mock import AsyncMock, MagicMock
from racing_sync.clients.abstract import TorrentFile


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

