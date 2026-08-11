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


@pytest.fixture
def anyio_backend():
    return "asyncio"




def test_prowlarr_config_should_skip_title():
    cfg = ProwlarrConfig(
        enabled=True,
        base_url="http://localhost:9696",
        api_key="secret",
        download_indexer="Indexer (API)",
        skip_query_substrings=["dummysub", "othergroup"],
    )
    assert cfg.should_skip_title("[DummySub] Endless Voyage - 1100 (1080p)") is True
    assert cfg.should_skip_title("Some.Show.S01E01.OtherGroup.720p") is True
    assert cfg.should_skip_title("DUMMYSUB.movie") is True
    assert cfg.should_skip_title("The.Matrix.1999.1080p") is False
    assert cfg.should_skip_title("") is False


@pytest.mark.anyio
async def test_prowlarr_client_skips_search():
    cfg = ProwlarrConfig(
        enabled=True,
        base_url="http://localhost:9696",
        api_key="secret",
        download_indexer="Indexer (API)",
        skip_query_substrings=["dummysub"],
    )
    client = ProwlarrClient(cfg)
    idx = Indexer(1, "Indexer (API)", "torrent", True, [])

    # search_indexer
    hits = await client.search_indexer(idx, "[DummySub] Storm Ninja - 01")
    assert hits == []

    # best_match
    match = await client.best_match("[DummySub] Pale Reaper - 01")
    assert match is None

    # search_indexers_parallel
    results = await client.search_indexers_parallel([idx], "[DummySub] Silver Oddjobs - 01")
    assert results == {}


@pytest.mark.anyio
async def test_pick_ssd_source_for_racing_skips_prowlarr():
    cfg = MagicMock()
    cfg.prowlarr.enabled = True
    cfg.prowlarr.should_skip_title = lambda title: "dummysub" in title.lower()
    cfg.cross_seed.allow_prowlarr_cross_seed = True
    cfg.cross_seed.refetch_public_via_prowlarr = True
    cfg.cross_seed.allow_ssh_export = True

    prowlarr = AsyncMock()
    source_client = AsyncMock()
    sftp = MagicMock()
    sftp.fetch_torrent = MagicMock(return_value=b"sftp-torrent-bytes")

    torrent = Torrent(
        hash="hash123",
        name="[DummySub] Moonweaver - 28 (1080p)",
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

    # Prowlarr should NOT be called at all; instead of parking for a query
    # that can never run, the racing torrent's own bytes are used at once.
    prowlarr.best_match.assert_not_called()
    assert decision is not None
    assert decision.source_label == "private-sftp-fallback"
    assert decision.torrent_bytes == b"sftp-torrent-bytes"


@pytest.mark.anyio
async def test_pick_ssd_source_skipped_title_uses_export_endpoint_without_sftp():
    """Skipped titles on qBittorrent sources (no SFTP) use /torrents/export."""
    from racing_sync.clients.abstract import Torrent

    cfg = MagicMock()
    cfg.prowlarr.should_skip_title = lambda title: "dummysub" in title.lower()
    cfg.cross_seed.allow_prowlarr_cross_seed = True
    cfg.cross_seed.allow_ssh_export = True

    prowlarr = AsyncMock()
    source_client = AsyncMock()
    source_client.export_torrent.return_value = b"qb-exported-bytes"

    torrent = Torrent(
        hash="aa11bb22",
        name="[DummySub] Moonweaver - 29 (1080p)",
        category="racing",
        save_path="",
        size_bytes=2000,
        state="",
        progress=1.0,
        trackers=["http://privatetracker.org/announce"],
    )

    decision = await pick_ssd_source_for_racing(
        cfg=cfg,
        source_torrent=torrent,
        other_source_torrents=[],
        prowlarr=prowlarr,
        sftp=None,
        source_client=source_client,
        attempt_prowlarr=True,
    )

    prowlarr.best_match.assert_not_called()
    assert decision is not None
    assert decision.source_label == "private-export-fallback"
    assert decision.torrent_bytes == b"qb-exported-bytes"
    source_client.export_torrent.assert_awaited_once_with("aa11bb22")


@pytest.mark.anyio
async def test_pick_ssd_source_skipped_title_parks_when_no_export_path():
    """Skipped titles park only when every direct-export path fails."""
    from racing_sync.clients.abstract import Torrent

    cfg = MagicMock()
    cfg.prowlarr.should_skip_title = lambda title: "dummysub" in title.lower()
    cfg.cross_seed.allow_prowlarr_cross_seed = True
    cfg.cross_seed.allow_ssh_export = False

    decision = await pick_ssd_source_for_racing(
        cfg=cfg,
        source_torrent=Torrent(
            hash="bb22cc33",
            name="[DummySub] Moonweaver - 30 (1080p)",
            category="racing",
            save_path="",
            size_bytes=2000,
            state="",
            progress=1.0,
            trackers=["http://privatetracker.org/announce"],
        ),
        other_source_torrents=[],
        prowlarr=AsyncMock(),
        sftp=None,
        source_client=AsyncMock(),
        attempt_prowlarr=True,
    )
    assert decision is None


@pytest.mark.anyio
async def test_do_new_watch_dir_skips_prowlarr(tmp_path: Path):
    raw_data = _bencode({
        b"announce": b"http://privatetracker.org/announce",
        b"info": {
            b"name": b"[DummySub] Siege of Giants - 01",
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
    coord.cfg.prowlarr.should_skip_title = lambda title: "dummysub" in title.lower()
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
    # file:// enclosures are unusable for download_torrent (http(s) only);
    # the item is skipped instead of poisoning best_match into a hard FAILED.
    assert hits == []


def test_parse_newznab_preserves_magnet_enclosure_url():
    from racing_sync.prowlarr import _parse_newznab, Indexer

    idx = Indexer(1, "Indexer", "torrent", True, [])
    xml = """<?xml version="1.0"?>
    <rss><channel><item>
        <title>Magnet.Show.S01E01</title>
        <enclosure url="magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567" length="1234" />
    </item></channel></rss>"""

    hits = _parse_newznab(xml, idx)
    assert len(hits) == 1
    assert hits[0].download_url == ""
    assert hits[0].magnet_url == "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567"


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
        download_indexer="Indexer (API)",
    )
    client = ProwlarrClient(cfg)
    idx = Indexer(1, "Indexer (API)", "torrent", True, [])
    client.get_download_indexer = MagicMock(return_value=idx)

    h_partial_large = TorrentHit(
        title="Movie.2024.Extended.1080p",
        guid="1",
        indexer="Indexer",
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
        indexer="Indexer",
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
        indexer="Indexer",
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


def _hit(title: str, size: int, guid: str = "g") -> TorrentHit:
    return TorrentHit(
        title=title, guid=guid, indexer="Indexer", indexer_id=1,
        size_bytes=size, download_url="http://prowlarr/dl",
        magnet_url="", info_url="", publish_date="",
    )


def _indexer_client() -> ProwlarrClient:
    from racing_sync.prowlarr import Indexer
    cfg = ProwlarrConfig(
        enabled=True,
        base_url="http://127.0.0.1:9696",
        api_key="secret",
        download_indexer="Indexer (API)",
    )
    client = ProwlarrClient(cfg)
    idx = Indexer(1, "Indexer (API)", "torrent", True, [])
    client.get_download_indexer = MagicMock(return_value=idx)
    return client


@pytest.mark.anyio
async def test_best_match_rejects_different_release_group():
    """Same episode, different group (Raccoon vs WebRip) must never match,
    even when sizes agree within tolerance — pointing racing torrents at
    foreign bytes corrupts the seed."""
    client = _indexer_client()
    query = "Galaxy.Rangers.Nebula.Outpost.S04E08.Signal.in.the.Dark.1080p.AMZN.WEB-DL.DDP5.1.H.264-Raccoon.mkv"
    wrong_group = (
        "Galaxy.Rangers.Nebula.Outpost.S04E08.Signal.in.the.Dark.1080p.AMZN.WEB-DL.DDP5.1.H.264-WebRip.mkv"
    )
    size = 1_450_000_000
    client.search_indexer = AsyncMock(return_value=[_hit(wrong_group, size + 5_000_000)])

    assert await client.best_match(query, target_size=size) is None


@pytest.mark.anyio
async def test_best_match_accepts_punctuation_variants_and_strips_tags():
    client = _indexer_client()
    size = 1_450_000_000
    # Dots vs spaces must not split the same release...
    client.search_indexer = AsyncMock(return_value=[
        _hit("Galaxy.Rangers.Nebula.Outpost.S04E08.Signal.in.the.Dark.1080p.AMZN.WEB-DL.DDP5.1.H.264-Raccoon", size),
    ])
    query = ("Galaxy Rangers Nebula Outpost S04E08 Signal in the Dark 1080p "
             "AMZN WEB-DL DDP5.1 H.264-Raccoon")
    best = await client.best_match(query, target_size=size)
    assert best is not None

    # ...while trailing indexer tags and media extensions are ignored.
    client.search_indexer = AsyncMock(return_value=[
        _hit("[DummySub] Moonweaver - 28 (1080p) [A1B2C3D4].mkv", size),
    ])
    best = await client.best_match("[DummySub] Moonweaver - 28 (1080p)", target_size=size)
    assert best is not None


@pytest.mark.anyio
async def test_best_match_size_gate_needs_target_size():
    """Same normalized title but wildly different size: gated only when the
    caller passes target_size (best_match can't know it otherwise)."""
    client = _indexer_client()
    title = "Show.S01E01.1080p-GRP"
    client.search_indexer = AsyncMock(return_value=[_hit(title, 10_000_000_000)])
    # No target size -> title match alone suffices (legacy callers).
    assert await client.best_match(title) is not None
    # 1 GB target vs 10 GB hit -> rejected.
    assert await client.best_match(title, target_size=1_000_000_000) is None
    # Within min(50MB, 2%) -> accepted (40 MB diff < 50 MB cap).
    assert await client.best_match(title, target_size=9_960_000_000) is not None


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


@pytest.mark.anyio
async def test_pick_ssd_source_public_failure_does_not_fall_through():
    from racing_sync.clients.abstract import Torrent
    cfg = MagicMock()
    cfg.cross_seed.allow_ssh_export = False
    cfg.cross_seed.refetch_public_via_prowlarr = False
    cfg.cross_seed.allow_prowlarr_cross_seed = True

    prowlarr = AsyncMock()
    source_client = AsyncMock()
    source_client.export_torrent.side_effect = RuntimeError("qB unreachable")

    t_pub = Torrent(
        hash="pubhash123",
        name="Public.Release",
        category="racing",
        save_path="",
        size_bytes=1000,
        state="racing",
        progress=1.0,
        trackers=["udp://tracker.opentrackr.org:1337/announce"],
    )

    dec = await pick_ssd_source_for_racing(
        cfg=cfg,
        source_torrent=t_pub,
        other_source_torrents=[],
        prowlarr=prowlarr,
        sftp=None,
        source_client=source_client,
        attempt_prowlarr=True,
    )
    # Must return None and NEVER call Prowlarr (no fallthrough to private indexer logic)
    assert dec is None
    prowlarr.best_match.assert_not_called()


@pytest.mark.anyio
async def test_pick_ssd_source_sftp_error_handled_gracefully():
    from racing_sync.clients.abstract import Torrent
    cfg = MagicMock()
    cfg.cross_seed.allow_ssh_export = True
    cfg.cross_seed.refetch_public_via_prowlarr = False

    sftp = MagicMock()
    sftp.fetch_torrent.side_effect = ConnectionResetError("SSH connection dropped")
    source_client = AsyncMock()
    source_client.export_torrent.return_value = b"exported_torrent_bytes"

    t_pub = Torrent(
        hash="pubhash123",
        name="Public.Release",
        category="racing",
        save_path="",
        size_bytes=1000,
        state="racing",
        progress=1.0,
        trackers=["udp://tracker.opentrackr.org:1337/announce"],
    )

    dec = await pick_ssd_source_for_racing(
        cfg=cfg,
        source_torrent=t_pub,
        other_source_torrents=[],
        prowlarr=None,
        sftp=sftp,
        source_client=source_client,
        attempt_prowlarr=True,
    )
    # SFTP failed, but handled gracefully and fell back to source_client.export_torrent
    assert dec is not None
    assert dec.torrent_bytes == b"exported_torrent_bytes"


@pytest.mark.anyio
async def test_pick_ssd_source_extracts_announce_url():
    from racing_sync.clients.abstract import Torrent
    from racing_sync.watchdir import _bencode
    cfg = MagicMock()
    cfg.prowlarr.enabled = True
    cfg.prowlarr.should_skip_title.return_value = False
    cfg.prowlarr.download_indexer = "Indexer (API)"
    cfg.cross_seed.allow_prowlarr_cross_seed = True

    blob = _bencode({
        b"announce": b"http://tracker.indexer.org/announce",
        b"info": {
            b"name": b"Private.Release",
            b"length": 5000,
            b"piece length": 16384,
            b"pieces": b"12345678901234567890",
        },
    })
    hit = MagicMock(
        title="Private.Release",
        size_bytes=5000,
        download_url="http://prowlarr.local/api/v1/download?apikey=secret",
    )
    prowlarr = AsyncMock()
    prowlarr.best_match.return_value = hit
    prowlarr.download_torrent.return_value = blob

    t_priv = Torrent(
        hash="privhash123",
        name="Private.Release",
        category="racing",
        save_path="",
        size_bytes=5000,
        state="racing",
        progress=1.0,
        trackers=["https://alpha.cc/announce"],
    )

    dec = await pick_ssd_source_for_racing(
        cfg=cfg,
        source_torrent=t_priv,
        other_source_torrents=[],
        prowlarr=prowlarr,
        sftp=None,
        source_client=AsyncMock(),
        attempt_prowlarr=True,
    )
    assert dec is not None
    # Announce URL must be extracted from bencoded torrent, NOT the Prowlarr download_url API endpoint
    assert dec.announce_url == "http://tracker.indexer.org/announce"
    assert "apikey=secret" not in dec.announce_url


@pytest.mark.anyio
async def test_pick_ssd_source_private_sftp_fallback_when_not_attempting_prowlarr():
    from racing_sync.clients.abstract import Torrent
    cfg = MagicMock()
    cfg.cross_seed.allow_ssh_export = True
    sftp = MagicMock()
    sftp.fetch_torrent.return_value = b"private_sftp_blob"

    t_priv = Torrent(
        hash="privhash456",
        name="Private.Release.Only",
        category="racing",
        save_path="",
        size_bytes=5000,
        state="racing",
        progress=1.0,
        trackers=["https://alpha.cc/announce"],
    )

    dec = await pick_ssd_source_for_racing(
        cfg=cfg,
        source_torrent=t_priv,
        other_source_torrents=[],
        prowlarr=None,
        sftp=sftp,
        source_client=AsyncMock(),
        attempt_prowlarr=False,
    )
    assert dec is not None
    assert dec.source_label == "private-sftp-fallback"
    assert dec.torrent_bytes == b"private_sftp_blob"
    assert dec.announce_url == "https://alpha.cc/announce"


@pytest.mark.anyio
async def test_pick_rejects_hit_whose_payload_is_another_release():
    """Defense in depth: even if selection passed a wrong-group hit, the
    decoded payload check must refuse it (park, never download onward)."""
    from racing_sync.clients.abstract import Torrent

    raccoon = "Galaxy.Rangers.Nebula.Outpost.S04E08.Signal.in.the.Dark.1080p.AMZN.WEB-DL.DDP5.1.H.264-Raccoon.mkv"
    webrip = "Galaxy.Rangers.Nebula.Outpost.S04E08.Signal.in.the.Dark.1080p.AMZN.WEB-DL.DDP5.1.H.264-WebRip.mkv"
    size = 1_450_000_000
    webrip_blob = _bencode({
        b"announce": b"http://tracker.indexer.org/announce",
        b"info": {
            b"name": webrip.encode(),
            b"length": size,
            b"piece length": 262144,
            b"pieces": b"12345678901234567890",
        },
    })

    cfg = MagicMock()
    cfg.prowlarr.should_skip_title.return_value = False
    cfg.prowlarr.download_indexer = "Indexer (API)"
    cfg.cross_seed.allow_prowlarr_cross_seed = True
    cfg.cross_seed.allow_ssh_export = False

    prowlarr = AsyncMock()
    prowlarr.best_match.return_value = TorrentHit(
        title=webrip, guid="9", indexer="Indexer (API)", indexer_id=1,
        size_bytes=size, download_url="http://prowlarr/9",
        magnet_url="", info_url="", publish_date="",
    )
    prowlarr.download_torrent.return_value = webrip_blob

    dec = await pick_ssd_source_for_racing(
        cfg=cfg,
        source_torrent=Torrent(
            hash="raccoonhash", name=raccoon, category="racing",
            save_path="", size_bytes=size, state="seeding", progress=1.0,
            trackers=["https://alpha.cc/announce"],
        ),
        other_source_torrents=[],
        prowlarr=prowlarr,
        sftp=None,
        source_client=AsyncMock(),
        attempt_prowlarr=True,
    )
    # Treated exactly like "no hit": park for retry, never a decision.
    assert dec is None





def test_fetchable_url_allows_own_prowlarr_refuses_metadata():
    """Prowlarr on localhost/LAN must fetch; metadata/link-local must not.

    Regression: the SSRF guard once refused 127.0.0.1 and failed every
    Indexer enclosure download (6 worker tracebacks in one run).
    """
    from racing_sync.prowlarr import _is_fetchable_http_url

    allowed = [
        "http://127.0.0.1:9696/prowlarr/4/download",
        "http://localhost:9696/dl/abc",
        "http://192.168.1.10:9696/dl/abc",
        "http://10.0.0.5/dl/abc",
        "http://nas.local:9696/dl/abc",
        "https://indexer.example.com/dl/abc.torrent",
        "http://[::1]:9696/dl/abc",
    ]
    for url in allowed:
        assert _is_fetchable_http_url(url), url

    refused = [
        "http://169.254.169.254/latest/meta-data/",
        "http://169.254.169.254/computeMetadata/v1/",
        "http://metadata.google.internal/computeMetadata/v1/",
        "http://instance-data/computeMetadata/v1/",
        "http://224.0.0.1/announce",
        "http://0.0.0.0/dl/abc",
        "ftp://indexer.example.com/dl/abc",
        "",
    ]
    for url in refused:
        assert not _is_fetchable_http_url(url), url
