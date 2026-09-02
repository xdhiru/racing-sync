from __future__ import annotations

import tempfile
from pathlib import Path

from racing_sync.clients.abstract import Torrent
from racing_sync.coordinator import _looks_public
from racing_sync.state import State, StateStore, TorrentState


def test_looks_public():
    assert _looks_public(["http://nyaa.tracker.wf:7777/announce"]) is True
    assert _looks_public(["udp://tracker.opentrackr.org:1337/announce"]) is True
    assert _looks_public(["udp://open.stealth.si:80/announce"]) is True
    assert _looks_public(["http://tracker.openbittorrent.com:80/announce"]) is True
    assert _looks_public(["https://aither.cc/announce/passkey"]) is False
    assert _looks_public(["https://beyond-hd.me/announce/passkey"]) is False


def test_state_store_find_by_name():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        store = StateStore(db_path)
        try:
            ts1 = TorrentState(
                source_infohash="1" * 40,
                source_name="Show.S01E01.1080p.mkv",
                state=State.NEW,
            )
            store.upsert(ts1)

            results = store.find_by_name("Show.S01E01.1080p.mkv")
            assert len(results) == 1
            assert results[0].source_infohash == "1" * 40

            empty = store.find_by_name("Nonexistent")
            assert len(empty) == 0
        finally:
            store.close()


def test_primary_election_prefers_public():
    t_private1 = Torrent(
        hash="1" * 40,
        name="Show.S01E01.1080p.mkv",
        category="racing",
        save_path="",
        size_bytes=1000,
        state="",
        progress=1.0,
        trackers=["https://aither.cc/announce/abc"],
    )
    t_public = Torrent(
        hash="2" * 40,
        name="Show.S01E01.1080p.mkv",
        category="racing",
        save_path="",
        size_bytes=1000,
        state="",
        progress=1.0,
        trackers=["http://nyaa.tracker.wf:7777/announce"],
    )
    t_private2 = Torrent(
        hash="3" * 40,
        name="Show.S01E01.1080p.mkv",
        category="racing",
        save_path="",
        size_bytes=1000,
        state="",
        progress=1.0,
        trackers=["https://beyond-hd.me/announce/def"],
    )

    group = [t_private1, t_public, t_private2]
    primary = next((t for t in group if _looks_public(t.trackers)), group[0])
    assert primary.hash == t_public.hash
