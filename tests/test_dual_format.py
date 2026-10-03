"""Dual-format single episodes: every variant must move, not just the largest.

Classifier Case 1 used to keep only the largest variant, and the MOVING
single-file branch moved only `single_file` — the loser was wiped with
the folder while the fuse gate still expected it (stall to 24h FAILED
plus destroyed bytes). Now all variants classify together and verified
siblings move alongside the single file.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from conftest import make_coordinator

from racing_sync.clients.abstract import TorrentFile
from racing_sync.state import State, StateStore, TorrentState


def _files():
    return [
        TorrentFile(name="Show.S01E01.mkv", size_bytes=1000, progress=1.0,
                    priority=1),
        TorrentFile(name="Show.S01E01.mp4", size_bytes=100, progress=1.0,
                    priority=1),
        TorrentFile(name="Show.S01E01.srt", size_bytes=10, progress=1.0,
                    priority=1),
    ]


def test_classifier_keeps_all_variants():
    from racing_sync.classifier import classify

    cfg = MagicMock()
    cfg.ssd.skip_movie_larger_than_bytes = 100_000_000_000
    cls = classify(_files(), cfg)
    assert cls.kind == "episode"
    assert cls.single_file == "Show.S01E01.mkv"
    assert sorted(e.file_name for e in cls.episodes) == [
        "Show.S01E01.mkv", "Show.S01E01.mp4",
    ]


def _moving_episode(tmp_path, files):
    ssd = tmp_path / "ssd"
    ssd.mkdir(exist_ok=True)
    for f in files:
        p = ssd / f.name
        if f.size_bytes and f.progress >= 0.999:
            p.write_bytes(b"x" * f.size_bytes)
        elif f.size_bytes:
            p.write_bytes(b"x" * (f.size_bytes // 2))

    coord = make_coordinator()
    coord._stop = False
    coord.cfg = MagicMock()
    coord.cfg.dest.save_path = ssd
    coord.cfg.ssd.path = ssd
    coord.cfg.ssd.skip_movie_larger_than_bytes = 100_000_000_000
    coord.cfg.rclone.remote.default = "remote:media"
    coord.cfg.rclone.remote.unsorted = "remote:unsorted"
    coord.cfg.rclone.fuse.mount = ssd / "fuse"
    coord.cfg.rclone.fuse.mount_unsorted = ssd / "fuse-unsorted"
    coord.cfg.rclone.batch_move_extra_flags = []
    coord.store = StateStore(tmp_path / "state.db")
    coord.dest_client = AsyncMock()
    coord.dest_client.get_torrent_files = AsyncMock(return_value=list(files))

    async def _move(local, remote, ts, files_from=None, **kw):
        if files_from is not None:
            for n in files_from:
                p = Path(local, *str(n).split("/"))
                if p.is_file():
                    p.unlink()
        else:
            if isinstance(local, Path) and local.is_file():
                local.unlink()

    coord._rclone_move = AsyncMock(side_effect=_move)
    ts = TorrentState(
        source_infohash="d" * 40, source_name="Show",
        dest_infohash="d" * 40, save_path=str(ssd),
        classification_kind="episode",
        batches_total=1, batch_index=0, state=State.MOVING,
    )
    coord.store.upsert(ts)
    return coord, coord.store


@pytest.mark.anyio
async def test_siblings_move_with_single(tmp_path):
    """mkv (bare) + mp4/srt (files_from) all move; nothing wiped early."""
    coord, store = _moving_episode(tmp_path, _files())
    try:
        with patch("racing_sync.coordinator.wipe_local_tree",
                   new_callable=AsyncMock) as mock_wipe:
            await coord._do_moving(store.get("d" * 40))
        assert store.get("d" * 40).state == State.RE_ADDING
        # One bare single move + one sibling files_from move.
        assert coord._rclone_move.await_count == 2
        first = coord._rclone_move.await_args_list[0]
        assert first.kwargs.get("files_from", None) is None
        assert str(first.args[0]).endswith("Show.S01E01.mkv")
        second = coord._rclone_move.await_args_list[1]
        assert sorted(second.kwargs["files_from"]) == [
            "Show.S01E01.mp4", "Show.S01E01.srt",
        ]
        mock_wipe.assert_not_awaited()  # flat layout: no folder to wipe
        assert not (tmp_path / "ssd" / "Show.S01E01.mkv").exists()
        assert not (tmp_path / "ssd" / "Show.S01E01.mp4").exists()
    finally:
        store.close()


@pytest.mark.anyio
async def test_short_sibling_not_moved(tmp_path):
    """A genuinely partial variant is cleaned, never shipped."""
    files = _files()
    files[1] = TorrentFile(name="Show.S01E01.mp4", size_bytes=100,
                            progress=0.2, priority=1)
    coord, store = _moving_episode(tmp_path, files)
    try:
        with patch("racing_sync.coordinator.wipe_local_tree",
                   new_callable=AsyncMock):
            await coord._do_moving(store.get("d" * 40))
        # Only the bare single move ran (mp4 partial cleaned, srt moved).
        assert coord._rclone_move.await_count == 2
        second = coord._rclone_move.await_args_list[1]
        assert second.kwargs["files_from"] == ["Show.S01E01.srt"]
        assert not (tmp_path / "ssd" / "Show.S01E01.mp4").exists()
    finally:
        store.close()
