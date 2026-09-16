from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock
from pathlib import Path

from racing_sync.config import ProwlarrConfig, AppConfig
from racing_sync.prowlarr import ProwlarrClient, Indexer, TorrentHit
from racing_sync.coordinator import pick_ssd_source_for_racing, Coordinator
from racing_sync.state import StateStore, TorrentState, State
from racing_sync.clients.abstract import Torrent
from racing_sync.watchdir import _bencode, _bencoded_info_hash


def test_prowlarr_config_should_skip_title():
    cfg = ProwlarrConfig(
        enabled=True,
        base_url="http://localhost:9696",
        api_key="secret",
        download_indexer="Seedpool (API)",
        skip_query_substrings=["subsplease", "erasergroups"],
    )
    assert cfg.should_skip_title("[SubsPlease] One Piece - 1100 (1080p)") is True
    assert cfg.should_skip_title("Some.Show.S01E01.EraserGroups.720p") is True
    assert cfg.should_skip_title("SUBSPLEASE.movie") is True
    assert cfg.should_skip_title("The.Matrix.1999.1080p") is False
    assert cfg.should_skip_title("") is False


@pytest.mark.anyio
async def test_prowlarr_client_skips_search():
    cfg = ProwlarrConfig(
        enabled=True,
        base_url="http://localhost:9696",
        api_key="secret",
        download_indexer="Seedpool (API)",
        skip_query_substrings=["subsplease"],
    )
    client = ProwlarrClient(cfg)
    idx = Indexer(1, "Seedpool (API)", "torrent", True, [])

    # search_indexer
    hits = await client.search_indexer(idx, "[SubsPlease] Naruto - 01")
    assert hits == []

    # best_match
    match = await client.best_match("[SubsPlease] Bleach - 01")
    assert match is None

    # search_indexers_parallel
    results = await client.search_indexers_parallel([idx], "[SubsPlease] Gintama - 01")
    assert results == {}


@pytest.mark.anyio
async def test_pick_ssd_source_for_racing_skips_prowlarr():
    cfg = MagicMock()
    cfg.prowlarr.enabled = True
    cfg.prowlarr.should_skip_title = lambda title: "subsplease" in title.lower()
    cfg.cross_seed.allow_prowlarr_cross_seed = True
    cfg.cross_seed.refetch_public_via_prowlarr = True
    cfg.cross_seed.allow_ssh_export = True

    prowlarr = AsyncMock()
    source_client = AsyncMock()
    sftp = MagicMock()
    sftp.fetch_torrent = MagicMock(return_value=b"sftp-torrent-bytes")

    torrent = Torrent(
        hash="hash123",
        name="[SubsPlease] Frieren - 28 (1080p)",
        category="racing",
        save_path="",
        size_bytes=1000,
        state="",
        progress=1.0,
        trackers=["http://privatetracker.org/announce"],
    )

    decision = await pick_ssd_source_for_racing(
        cfg=cfg,
        source_torrent=torrent,
        other_source_torrents=[],
        prowlarr=prowlarr,
        sftp=sftp,
        source_client=source_client,
        attempt_prowlarr=True,
    )

    # Prowlarr should NOT be called at all
    prowlarr.best_match.assert_not_called()
    assert decision is None


@pytest.mark.anyio
async def test_do_new_watch_dir_skips_prowlarr(tmp_path: Path):
    raw_data = _bencode({
        b"announce": b"http://privatetracker.org/announce",
        b"info": {
            b"name": b"[SubsPlease] Attack on Titan - 01",
            b"length": 5000,
            b"piece length": 16384,
            b"pieces": b"12345678901234567890",
        },
    })
    infohash, name, total, announce = _bencoded_info_hash(raw_data)

    db_path = tmp_path / "state.db"
    store = StateStore(db_path)

    coord = object.__new__(Coordinator)
    coord.cfg = MagicMock()
    coord.cfg.dest.save_path = tmp_path / "downloads"
    coord.cfg.ssd.path = tmp_path
    coord.cfg.ssd.max_inflight_bytes = 100000000
    coord.cfg.general.state_db = db_path
    coord.cfg.general.disk_safety_margin_bytes = 1000
    coord.cfg.prowlarr.enabled = True
    coord.cfg.prowlarr.should_skip_title = lambda title: "subsplease" in title.lower()
    coord.cfg.prowlarr.is_download_indexer = lambda url: False
    coord.prowlarr = AsyncMock()
    coord.store = store
    coord.transition = lambda t, s: setattr(t, "state", s)

    ts = TorrentState(
        source_infohash=infohash,
        source_name=name,
        total_bytes=total,
        source_announce_url=announce,
        cross_seed_blob=raw_data,
        cross_seed_source="watch-dir",
        state=State.NEW,
    )

    await coord._do_new_watch_dir(ts)

    # Prowlarr search should NOT be attempted
    coord.prowlarr.search_indexers_parallel.assert_not_called()
    assert ts.cross_seed_source == "watch-dir"
    assert ts.state == State.QUEUED


def test_parse_newznab_blocks_dtd_entity_expansion():
    from racing_sync.prowlarr import _parse_newznab, ProwlarrError, Indexer

    idx = Indexer(1, "Indexer", "torrent", True, [])
    malicious_xml = """<?xml version="1.0"?>
    <!DOCTYPE lolz [
     <!ENTITY lol "lol">
     <!ELEMENT lolz (#PCDATA)>
     <!ENTITY lol1 "&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;&lol;">
    ]>
    <rss><channel><item><title>Test</title></item></channel></rss>"""

    with pytest.raises(ProwlarrError, match="untrusted XML contains DTD or entity declaration"):
        _parse_newznab(malicious_xml, idx)


def test_parse_newznab_sanitizes_unsafe_download_url():
    from racing_sync.prowlarr import _parse_newznab, Indexer

    idx = Indexer(1, "Indexer", "torrent", True, [])
    xml = """<?xml version="1.0"?>
    <rss><channel><item>
        <title>Safe.Show.S01E01</title>
        <enclosure url="file:///etc/passwd" length="1234" />
    </item></channel></rss>"""

    hits = _parse_newznab(xml, idx)
    assert len(hits) == 1
    assert hits[0].download_url == ""


def test_parse_newznab_extracts_namespaced_magnet_url():
    from racing_sync.prowlarr import _parse_newznab, Indexer

    idx = Indexer(1, "Indexer", "torrent", True, [])
    xml = """<?xml version="1.0"?>
    <rss xmlns:torznab="http://torznab.com/schemas/2015/feed">
      <channel>
        <item>
          <title>Test.Show.S01E01.1080p</title>
          <guid>abcdef123456</guid>
          <enclosure url="https://indexer.example/download/123.torrent" length="1048576" />
          <torznab:attr name="magneturl" value="magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567" />
        </item>
      </channel>
    </rss>"""

    hits = _parse_newznab(xml, idx)
    assert len(hits) == 1
    assert hits[0].magnet_url == "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567"
    assert hits[0].download_url == "https://indexer.example/download/123.torrent"


@pytest.mark.anyio
async def test_download_torrent_validates_scheme():
    from racing_sync.prowlarr import ProwlarrClient, ProwlarrError, TorrentHit

    cfg = ProwlarrConfig(enabled=True, base_url="http://localhost:9696", api_key="secret", download_indexer="idx")
    client = ProwlarrClient(cfg)
    client._session = MagicMock()

    hit = TorrentHit(
        title="Test",
        guid="1",
        indexer="idx",
        indexer_id=1,
        size_bytes=100,
        download_url="ftp://malicious.host/file.torrent",
        magnet_url="",
        info_url="",
        publish_date="",
    )

    with pytest.raises(ProwlarrError, match="invalid or unsafe download_url scheme"):
        await client.download_torrent(hit)


def test_cross_seed_tolerance_rejects_large_relative_difference_for_small_files():
    # 10 MB torrent: 2% is 200 KB.
    # A candidate differing by 5 MB was accepted under max(50MB, 2%) but must be rejected under min(50MB, 2%).
    total_bytes = 10 * 1024 * 1024
    candidate_size = total_bytes + 5 * 1024 * 1024
    tolerance = min(1024 * 1024 * 50, int(total_bytes * 0.02))
    assert abs(candidate_size - total_bytes) > tolerance


@pytest.mark.anyio
async def test_prowlarr_best_match_ranking():
    cfg = ProwlarrConfig(
        enabled=True,
        base_url="http://127.0.0.1:9696",
        api_key="secret",
        download_indexer="Seedpool (API)",
    )
    client = ProwlarrClient(cfg)
    idx = Indexer(1, "Seedpool (API)", "torrent", True, [])
    client.get_download_indexer = MagicMock(return_value=idx)

    h_partial_large = TorrentHit(
        title="Movie.2024.Extended.1080p",
        guid="1",
        indexer="Seedpool",
        indexer_id=1,
        size_bytes=10_000_000_000,
        download_url="http://prowlarr/1",
        magnet_url="",
        info_url="",
        publish_date="",
    )
    h_exact_small = TorrentHit(
        title="Movie.2024.1080p",
        guid="2",
        indexer="Seedpool",
        indexer_id=1,
        size_bytes=5_000_000_000,
        download_url="http://prowlarr/2",
        magnet_url="",
        info_url="",
        publish_date="",
    )
    h_exact_large = TorrentHit(
        title="Movie.2024.1080p",
        guid="3",
        indexer="Seedpool",
        indexer_id=1,
        size_bytes=8_000_000_000,
        download_url="http://prowlarr/3",
        magnet_url="",
        info_url="",
        publish_date="",
    )

    client.search_indexer = AsyncMock(return_value=[h_partial_large, h_exact_small, h_exact_large])

    best = await client.best_match("Movie.2024.1080p")
    assert best is not None
    # Exact match wins over partial match even if partial is larger,
    # and largest exact match wins between the two exact matches.
    assert best.guid == "3"
    assert best.size_bytes == 8_000_000_000


@pytest.mark.anyio
async def test_prowlarr_headers_not_leaked_to_external_hosts():
    from unittest.mock import AsyncMock, MagicMock
    from racing_sync.prowlarr import ProwlarrClient, TorrentHit, Indexer

    cfg = ProwlarrConfig(
        enabled=True,
        base_url="http://prowlarr.local:9696",
        api_key="super_secret_prowlarr_key",
        download_indexer="indexer1",
    )
    client = ProwlarrClient(cfg)
    client._session = MagicMock()

    # When calling Prowlarr API (search_indexer), headers should include X-Api-Key
    mock_resp = AsyncMock()
    mock_resp.text = AsyncMock(return_value="<xml></xml>")
    mock_resp.raise_for_status = MagicMock()
    client._session.get.return_value.__aenter__.return_value = mock_resp

    idx = Indexer(1, "indexer1", "torrent", True, [])
    await client.search_indexer(idx, "query")
    call_args = client._session.get.call_args
    assert call_args.kwargs.get("headers") == {"X-Api-Key": "super_secret_prowlarr_key"}

    # When downloading from external URL, headers must NOT be passed
    hit = TorrentHit(
        title="Test",
        guid="1",
        indexer="indexer1",
        indexer_id=1,
        size_bytes=100,
        download_url="https://external.tracker.org/download.php?id=123&passkey=abc",
        magnet_url="",
        info_url="",
        publish_date="",
    )

    class _MockAsyncChunks:
        def __aiter__(self):
            return self
        async def __anext__(self):
            if not hasattr(self, "_done"):
                self._done = True
                return b"d8:announcee"
            raise StopAsyncIteration

    mock_dl_resp = AsyncMock()
    mock_dl_resp.raise_for_status = MagicMock()
    mock_dl_resp.headers = {"Content-Length": "12"}
    mock_dl_resp.content.iter_chunked = MagicMock(return_value=_MockAsyncChunks())
    client._session.get.return_value.__aenter__.return_value = mock_dl_resp

    data = await client.download_torrent(hit)
    assert data == b"d8:announcee"
    dl_call_args = client._session.get.call_args
    # Verify no headers were passed to external GET
    assert "headers" not in dl_call_args.kwargs or dl_call_args.kwargs["headers"] is None


@pytest.mark.anyio
async def test_prowlarr_download_url_scrubbed_in_exceptions():
    from unittest.mock import AsyncMock, MagicMock
    from racing_sync.prowlarr import ProwlarrClient, ProwlarrError, TorrentHit

    cfg = ProwlarrConfig(
        enabled=True,
        base_url="http://prowlarr.local:9696",
        api_key="prowlarr_key",
        download_indexer="indexer1",
    )
    client = ProwlarrClient(cfg)
    client._session = MagicMock()

    hit = TorrentHit(
        title="Test",
        guid="1",
        indexer="indexer1",
        indexer_id=1,
        size_bytes=100,
        download_url="https://tracker.org/download?torrent_id=42&apikey=LEAKED_KEY_12345&passkey=SECRET",
        magnet_url="",
        info_url="",
        publish_date="",
    )

    class _MockErrorChunks:
        def __aiter__(self):
            return self
        async def __anext__(self):
            if not hasattr(self, "_done"):
                self._done = True
                return b"<html>error</html>"
            raise StopAsyncIteration

    # 1. Invalid bencoded data
    mock_resp = AsyncMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.headers = {"Content-Length": "18"}
    mock_resp.content.iter_chunked = MagicMock(return_value=_MockErrorChunks())
    client._session.get.return_value.__aenter__.return_value = mock_resp

    with pytest.raises(ProwlarrError) as exc_info:
        await client.download_torrent(hit)
    err_msg = str(exc_info.value)
    assert "LEAKED_KEY_12345" not in err_msg
    assert "SECRET" not in err_msg
    assert "https://tracker.org/download" in err_msg

    # 2. Oversize torrent
    mock_resp.headers = {"Content-Length": str(50 * 1024 * 1024)}
    with pytest.raises(ProwlarrError) as exc_info:
        await client.download_torrent(hit)
    err_msg = str(exc_info.value)
    assert "LEAKED_KEY_12345" not in err_msg
    assert "SECRET" not in err_msg
    assert "https://tracker.org/download" in err_msg

    # 3. Invalid scheme
    hit_bad_scheme = TorrentHit(
        title="Test",
        guid="2",
        indexer="indexer1",
        indexer_id=1,
        size_bytes=100,
        download_url="ftp://tracker.org/dl?key=SECRET_KEY",
        magnet_url="",
        info_url="",
        publish_date="",
    )
    with pytest.raises(ProwlarrError) as exc_info:
        await client.download_torrent(hit_bad_scheme)
    err_msg = str(exc_info.value)
    assert "SECRET_KEY" not in err_msg
    assert "ftp://tracker.org/dl" in err_msg



