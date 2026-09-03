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
    assert _looks_public(["https://alpha.cc/announce/passkey"]) is False
    assert _looks_public(["https://beta.me/announce/passkey"]) is False


def test_fold_path_case_folds_only_on_windows():
    import os
    from racing_sync.coordinator_content import fold_path_case

    if os.name == "nt":
        assert fold_path_case("G:/SSD/Data") == "g:/ssd/data"
    else:
        assert fold_path_case("G:/SSD/Data") == "G:/SSD/Data"
        assert fold_path_case("/srv/data") == "/srv/data"
    assert fold_path_case("") == ""
    assert fold_path_case(None) == ""


def test_announce_domain_never_returns_credential():
    from racing_sync.coordinator_content import announce_domain

    assert announce_domain("https://dl-indexer.example.net/announce/e4a7c2f19b83d05a6c7e1f349a8bd6e55") == "dl-indexer.example.net"
    assert announce_domain("https://alpha.cc/announce/xyz") == "alpha.cc"
    assert announce_domain("udp://tracker.opentrackr.org:1337/announce") == "tracker.opentrackr.org"
    assert announce_domain("https://www.example.com/announce/k") == "example.com"
    assert announce_domain("not a url with spaces") == ""
    assert announce_domain("") == ""
    assert announce_domain(None) == ""


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
        trackers=["https://alpha.cc/announce/abc"],
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
        trackers=["https://beta.me/announce/def"],
    )

    group = [t_private1, t_public, t_private2]
    primary = next((t for t in group if _looks_public(t.trackers)), group[0])
    assert primary.hash == t_public.hash


def test_normalize_content_name():
    from racing_sync.coordinator import normalize_content_name

    assert normalize_content_name("Movie.2024.1080p-GROUP") == "movie.2024.1080p-group"
    assert normalize_content_name("Movie.2024.1080p-GROUP [A1B2C3D4]") == "movie.2024.1080p-group"
    assert normalize_content_name("Movie.2024.1080p-GROUP [FL].torrent") == "movie.2024.1080p-group"
    assert normalize_content_name("Movie.2024.1080p-GROUP.mkv") == "movie.2024.1080p-group"
    assert normalize_content_name("  Movie.2024.1080p-GROUP [A1B2C3D4]  ") == "movie.2024.1080p-group"


def test_find_by_name_with_bracketed_tags():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "test.db"
        store = StateStore(db_path)
        try:
            ts1 = TorrentState(
                source_infohash="1" * 40,
                source_name="Movie.2024.1080p-GROUP.mkv",
                state=State.NEW,
            )
            store.upsert(ts1)

            # Query with [A1B2C3D4] variant matches the stored entry
            results = store.find_by_name("Movie.2024.1080p-GROUP [A1B2C3D4]")
            assert len(results) == 1
            assert results[0].source_infohash == "1" * 40
        finally:
            store.close()

