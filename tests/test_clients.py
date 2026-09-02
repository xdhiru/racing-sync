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
