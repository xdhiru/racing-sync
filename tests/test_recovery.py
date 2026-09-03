from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock
from pathlib import Path

from racing_sync.config import AppConfig
from racing_sync.recovery import reconcile
from racing_sync.state import State, StateStore
from racing_sync.clients.abstract import Torrent


@pytest.mark.anyio
async def test_reconcile_adopts_fuse_and_completed_torrents(tmp_path: Path):
    db_path = tmp_path / "state.db"
    store = StateStore(db_path)

    cfg = MagicMock()
    cfg.rclone.fuse.mount = Path("/mnt/fuse/torrents")
    cfg.rclone.fuse.mount_unsorted = Path("/mnt/fuse/unsorted")

    # Mock dest client returning torrents already on VPS2
    dest = AsyncMock()
    dest.list_torrents.return_value = [
        # Torrent 1: on fuse mount, complete
        Torrent(
            hash="1111111111111111111111111111111111111111",
            name="Seeding.On.Fuse.1080p.mkv",
            size_bytes=1000,
            save_path="/mnt/fuse/torrents",
            category="racing",
            progress=1.0,
            state="seeding",
        ),
        # Torrent 2: on unsorted fuse mount
        Torrent(
            hash="2222222222222222222222222222222222222222",
            name="Episode.S01E01.1080p.mkv",
            size_bytes=2000,
            save_path="/mnt/fuse/unsorted",
            category="racing",
            progress=1.0,
            state="uploading",
        ),
        # Torrent 3: incomplete on SSD (unknown)
        Torrent(
            hash="3333333333333333333333333333333333333333",
            name="Incomplete.Download.mkv",
            size_bytes=3000,
            save_path="/home/kevin/torrents/qbittorrent",
            category="racing",
            progress=0.3,
            state="downloading",
        ),
    ]

    report = await reconcile(cfg, dest=dest, store=store)

    assert len(report.kept) == 2
    assert len(report.unknowns) == 1

    # Check store has adopted the 2 fuse/completed torrents as DONE
    t1 = store.get("1111111111111111111111111111111111111111")
    assert t1 is not None
    assert t1.state == State.DONE

    t2 = store.get("2222222222222222222222222222222222222222")
    assert t2 is not None
    assert t2.state == State.DONE

    t3 = store.get("3333333333333333333333333333333333333333")
    assert t3 is None


@pytest.mark.anyio
async def test_fix_orphan_already_downloading(tmp_path: Path):
    from racing_sync.recovery import fix_orphan
    from racing_sync.state import TorrentState

    db_path = tmp_path / "state.db"
    store = StateStore(db_path)

    ts = TorrentState("hash1", state=State.DOWNLOADING)
    store.upsert(ts)

    dest = AsyncMock()
    cfg = MagicMock()
    cfg.dest.save_path = Path("/srv/qbittorrent/data")

    # fix_orphan should NOT raise ValueError when already in DOWNLOADING
    res = await fix_orphan(ts, cfg, dest=dest, store=store, sftp_bytes=b"dummy-bytes")
    assert res == State.DOWNLOADING.value
    recovered = store.get("hash1")
    assert recovered is not None
    assert recovered.state == State.DOWNLOADING

