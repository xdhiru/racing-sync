"""The "push from VPS1" job: start it, suppress the competition, hand over.

Fictional names only ("Example.Show.S01E01…-ExampleGroup.mkv", "seedremote"),
as everywhere in this repo.

Pinned here:

* every refusal of `start_source_push` (switch off, VPS1 gone, nothing
  complete, no per-file progress, dedupe, cap, a failed preflight/launch that
  must leave no live checkpoint behind);
* the completeness rule — a partial torrent pushes ONLY its complete files,
  because both clients preallocate and a size check would lie;
* the layout identity: push and move must reach the same `target_remote` for
  the same names (asserted against `decide_seed_layout` directly, so the two
  callers cannot drift) — and the kind they route on is the one the pushed
  names classify to, not the schema default "unknown" a pre-admission row
  carries, pinned on the row so the stages after the push agree with where
  the bytes went;
* the four suppression guards while a push is live (no timeout leech, no SSD
  reservation, no max-age FAILED, a same-content sibling defers);
* the handover: the existing all-remote evidence path admits the row with no
  SSD reservation, and a passed deadline returns the row to today's behaviour.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from conftest import make_coordinator

from racing_sync import coordinator as coordinator_module, coordinator_push
from racing_sync.clients.abstract import Torrent, TorrentFile
from racing_sync.config import AppConfig, DelugeSFTPConfig, RclonePushConfig
from racing_sync.coordinator_push import PUSH_PARK_REASON
from racing_sync.seed_layout import decide_seed_layout
from racing_sync.state import State, StateStore, TorrentState
from racing_sync.state_schema import PushState, SourcePush
from racing_sync.vps1_push import PushCheck, PushFile, PushRequest
from racing_sync.watchdir import _bencode, _bencoded_info_hash

ROOT = Path(__file__).parent.parent

EPISODE_NAME = "Example.Show.S01E01.1080p.WEB-DL.AAC2.0.H.264-ExampleGroup.mkv"
EPISODE_TOP = "Example.Show.S01E01.1080p.WEB-DL.AAC2.0.H.264-ExampleGroup"
NEXT_EPISODE = "Example.Show.S01E02.1080p.WEB-DL.AAC2.0.H.264-ExampleGroup.mkv"
THIRD_EPISODE = "Example.Show.S01E03.1080p.WEB-DL.AAC2.0.H.264-ExampleGroup.mkv"
MOVIE_NAME = "Example.Film.2026.1080p.WEB-DL.x264-ExampleGroup.mkv"
MOVIE_TOP = "Example.Film.2026.1080p.WEB-DL.x264-ExampleGroup"
SEASON_TOP = "Example.Show.S01.1080p.WEB-DL.AAC2.0.H.264-ExampleGroup"
# Extra videos with no episode tag: two episodes plus these classify `mixed`.
BONUS_ONE = "Example.Bonus.One.mkv"
BONUS_TWO = "Example.Bonus.Two.mkv"
BONUS_THREE = "Example.Bonus.Three.mkv"
ANNOUNCE = "https://tracker.example.invalid/announce/abcdef"
VPS1_DIR = "/data/racing"

SORTED_REMOTE = "seedremote:Media"
UNSORTED_REMOTE = "seedremote:Unsorted"


def _single_blob(
    name: str, size: int, *, announce: str = ANNOUNCE, piece_length: int = 16384
) -> bytes:
    return bytes(
        _bencode(
            {
                b"announce": announce.encode(),
                b"info": {
                    b"name": name.encode(),
                    b"length": size,
                    b"piece length": piece_length,
                    b"pieces": b"0" * 20,
                },
            }
        )
    )


def _wrapped_blob(top: str, files: list[tuple[str, int]]) -> bytes:
    return bytes(
        _bencode(
            {
                b"announce": ANNOUNCE.encode(),
                b"info": {
                    b"name": top.encode(),
                    b"files": [{b"length": s, b"path": [n.encode()]} for n, s in files],
                    b"piece length": 16384,
                    b"pieces": b"0" * 20,
                },
            }
        )
    )


def _hash_of(blob: bytes) -> str:
    """The infohash the blob's own bytes carry (never a hand-made hash)."""
    return _bencoded_info_hash(blob)[0].lower()


def _push_cfg(
    *,
    enabled: bool = True,
    auto: bool = False,
    cap: int = 1,
    check_s: int = 120,
    deadline: int = 600,
    leech: bool = False,
) -> AppConfig:
    """The shipped example config plus an opt-in `[source.rclone_push]`.

    Everything else is exactly what an operator ships, so the defaults under
    test are the documented ones.
    """
    cfg = AppConfig.from_toml(ROOT / "config.example.toml")
    cfg.source.deluge_sftp = DelugeSFTPConfig(
        enabled=True,
        ssh_host="127.0.0.1",
        ssh_key_path=Path("/home/deluge/.ssh/id_ed25519"),
        state_dir=Path("/home/deluge/.config/deluge/state"),
    )
    cfg.source.rclone_push = RclonePushConfig(
        enabled=enabled,
        auto_on_prowlarr_timeout=auto and enabled,
        timeout_seconds=deadline,
        max_concurrent_pushes=cap,
        poll_seconds=check_s,
    )
    cfg.rclone.remote.default = SORTED_REMOTE
    cfg.rclone.remote.unsorted = UNSORTED_REMOTE
    cfg.cross_seed.fallback_to_racing_torrent_on_prowlarr_timeout = leech
    return cfg


class _Dial:
    """Enough of the SFTP exporter for the transport's duck-typing.

    Only the DEDICATED half is ever called by the push lane (the pooled
    `run_shell` / `write_remote_file` are here so a test can prove they are
    NOT: `tests/test_push_liveness.py` counts them).
    """

    def __init__(self) -> None:
        self.commands: list[str] = []
        self.pooled_calls: list[str] = []

    def run_shell(self, command: str, timeout: float = 30.0):
        self.pooled_calls.append(command)
        return (0, "", "")

    def write_remote_file(self, path: str, data: bytes) -> bool:
        self.pooled_calls.append(path)
        return True

    def run_shell_dedicated(self, command: str, timeout: float = 30.0):
        self.commands.append(command)
        return (0, "", "")

    def write_remote_file_dedicated(self, path: str, data: bytes) -> bool:
        self.commands.append(path)
        return True

    def launch_detached(self, command: str) -> bool:
        self.commands.append("launch")
        return True


def _vps1_torrent(blob: bytes, *, name: str, size: int, progress: float = 1.0) -> Torrent:
    return Torrent(
        hash=_hash_of(blob),
        name=name,
        category="racing",
        save_path=VPS1_DIR,
        size_bytes=size,
        state="seeding" if progress >= 0.999 else "downloading",
        progress=progress,
        trackers=[ANNOUNCE],
    )


def _row(
    store: StateStore, blob: bytes, *, name: str, size: int, kind: str, state: State, **kw
) -> TorrentState:
    ts = TorrentState(
        source_infohash=_hash_of(blob),
        source_name=name,
        total_bytes=size,
        classification_kind=kind,
        state=state,
        **kw,
    )
    ts._blob = blob
    store.upsert(ts)
    # The racing .torrent bytes are cached at intake in the real pipeline;
    # the push reads its file names from that table.
    store.set_racing_blob(ts.source_infohash, blob)
    return store.get(ts.source_infohash, include_blob=False)


def _live_push(store: StateStore, infohash: str, *, expires_in: dt.timedelta, **kw) -> None:
    store.claim_push(
        infohash,
        src_dir=kw.pop("src_dir", VPS1_DIR),
        target_remote=kw.pop("target_remote", UNSORTED_REMOTE),
        deadline_at=dt.datetime.now(dt.UTC) + expires_in,
        files_total=kw.pop("files_total", 1),
        **kw,
    )


def _push_coordinator(store: StateStore, cfg: AppConfig, *, live: list, files) -> MagicMock:
    """Coordinator with a real config/store; the transport calls are mocked."""
    coord = make_coordinator(store)
    coord.cfg = cfg
    coord.store = store
    coord.sftp = _Dial()
    coord.prowlarr = None
    coord.watch = None
    coord.dest_client = AsyncMock()
    coord.dest_client.list_torrents = AsyncMock(return_value=[])
    coord.source_client = AsyncMock()
    coord.source_client.get_torrent_files = AsyncMock(return_value=files)
    coord._list_source_torrents = AsyncMock(return_value=list(live))
    coord._load_racing_torrent_bytes = AsyncMock(return_value=None)
    coord._ssd_try_reserve = AsyncMock(return_value=True)
    coord._ssd_reserved = {}
    coord._blob_fully_remote = AsyncMock(return_value=None)
    return coord


@pytest.fixture
def transport(monkeypatch):
    """Preflight / launch / progress as mocks with mutable canned results.

    The launch echoes the request back (files_total = what was actually
    listed), so a test that changes the wanted subset cannot pass by
    accident.
    """

    def _ok_launch(request, **_kwargs):
        return PushCheck(
            True,
            "",
            list_path="/tmp/x.lst",
            log_path="/tmp/x.log",
            files_total=len(request.files),
        )

    def _ok_preflight(request, **_kwargs):
        return PushCheck(True, "", files_total=len(request.files))

    holder = {
        "preflight": AsyncMock(side_effect=_ok_preflight),
        "start_copy": AsyncMock(side_effect=_ok_launch),
        "remote_progress": AsyncMock(return_value=(0, 1)),
    }
    monkeypatch.setattr(coordinator_push, "preflight", holder["preflight"])
    monkeypatch.setattr(coordinator_push, "start_copy", holder["start_copy"])
    monkeypatch.setattr(coordinator_push, "remote_progress", holder["remote_progress"])
    return holder


def _request_of(mock: AsyncMock) -> PushRequest:
    return mock.await_args.args[0]


# --------------------------------------------------------------------------- #
# refusals
# --------------------------------------------------------------------------- #


@pytest.mark.anyio
async def test_push_refused_when_the_switch_is_off(tmp_path, transport):
    store = StateStore(tmp_path / "state.db")
    try:
        blob = _single_blob(EPISODE_NAME, 2000)
        coord = _push_coordinator(
            store,
            _push_cfg(enabled=False),
            live=[_vps1_torrent(blob, name=EPISODE_TOP, size=2000)],
            files=[],
        )
        ts = _row(
            store, blob, name=EPISODE_TOP, size=2000, kind="episode", state=State.WAITING_INDEXER
        )

        started, why = await coord.start_source_push(ts)

        assert started is False
        assert "enabled = false" in why
        transport["preflight"].assert_not_awaited()
        # Not even a checkpoint: the feature must leave the table untouched.
        assert store.list_pushes() == []
        assert store.get(_hash_of(blob)).state == State.WAITING_INDEXER
    finally:
        store.close()


@pytest.mark.anyio
async def test_push_refused_when_vps1_no_longer_has_it(tmp_path, transport):
    store = StateStore(tmp_path / "state.db")
    try:
        blob = _single_blob(EPISODE_NAME, 2000)
        coord = _push_coordinator(store, _push_cfg(), live=[], files=[])
        ts = _row(
            store, blob, name=EPISODE_TOP, size=2000, kind="episode", state=State.WAITING_INDEXER
        )

        started, why = await coord.start_source_push(ts)

        assert started is False
        assert "VPS1 no longer has" in why
        assert store.push_is_live(_hash_of(blob)) is False
    finally:
        store.close()


@pytest.mark.anyio
async def test_push_refused_when_nothing_is_complete(tmp_path, transport):
    """Preallocated files make size meaningless: progress is the only proof."""
    store = StateStore(tmp_path / "state.db")
    try:
        blob = _single_blob(EPISODE_NAME, 2000)
        coord = _push_coordinator(
            store,
            _push_cfg(),
            live=[_vps1_torrent(blob, name=EPISODE_TOP, size=2000, progress=0.4)],
            files=[TorrentFile(EPISODE_NAME, 2000, progress=0.4)],
        )
        ts = _row(
            store, blob, name=EPISODE_TOP, size=2000, kind="episode", state=State.WAITING_INDEXER
        )

        started, why = await coord.start_source_push(ts)

        assert started is False
        assert "no file of it is complete" in why
        assert store.push_is_live(_hash_of(blob)) is False
    finally:
        store.close()


@pytest.mark.anyio
async def test_subset_matches_when_the_client_omits_the_torrent_top(tmp_path, transport):
    """The client list is only evidence; the names sent are always the blob's.

    Deluge and qBittorrent do not spell a wrapped torrent's file paths
    identically (with/without the top folder), so the lookup tolerates the
    suffix — while `--files-from-raw` still receives the blob-relative names
    the remote must end up holding.
    """
    store = StateStore(tmp_path / "state.db")
    try:
        blob = _wrapped_blob(EPISODE_TOP, [(EPISODE_NAME, 2000), (NEXT_EPISODE, 3000)])
        coord = _push_coordinator(
            store,
            _push_cfg(),
            live=[_vps1_torrent(blob, name=EPISODE_TOP, size=5000, progress=0.4)],
            files=[
                TorrentFile(EPISODE_NAME, 2000, progress=1.0),
                TorrentFile(NEXT_EPISODE, 3000, progress=0.3),
            ],
        )
        ts = _row(
            store, blob, name=EPISODE_TOP, size=5000, kind="episode", state=State.WAITING_INDEXER
        )

        started, why = await coord.start_source_push(ts)

        assert started is True, why
        request = _request_of(transport["start_copy"])
        assert [f.name for f in request.files] == [f"{EPISODE_TOP}/{EPISODE_NAME}"]
    finally:
        store.close()


@pytest.mark.anyio
async def test_partial_torrent_without_per_file_progress_is_refused(tmp_path, transport):
    """A client that cannot report file progress cannot prove a subset."""
    store = StateStore(tmp_path / "state.db")
    try:
        blob = _single_blob(EPISODE_NAME, 2000)
        coord = _push_coordinator(
            store,
            _push_cfg(),
            live=[_vps1_torrent(blob, name=EPISODE_TOP, size=2000, progress=0.6)],
            files=[],
        )
        coord.source_client.get_torrent_files = AsyncMock(side_effect=RuntimeError("no such API"))
        ts = _row(
            store, blob, name=EPISODE_TOP, size=2000, kind="episode", state=State.WAITING_INDEXER
        )

        started, why = await coord.start_source_push(ts)

        assert started is False
        assert "per-file progress" in why
    finally:
        store.close()


@pytest.mark.anyio
async def test_partial_torrent_pushes_only_the_complete_files(tmp_path, transport):
    store = StateStore(tmp_path / "state.db")
    try:
        blob = _wrapped_blob(EPISODE_TOP, [(EPISODE_NAME, 2000), (NEXT_EPISODE, 3000)])
        h = _hash_of(blob)
        coord = _push_coordinator(
            store,
            _push_cfg(),
            live=[_vps1_torrent(blob, name=EPISODE_TOP, size=5000, progress=0.5)],
            files=[
                TorrentFile(f"{EPISODE_TOP}/{EPISODE_NAME}", 2000, progress=1.0),
                # Preallocated: the full size is already reported at 12 %.
                TorrentFile(f"{EPISODE_TOP}/{NEXT_EPISODE}", 3000, progress=0.12),
            ],
        )
        ts = _row(
            store, blob, name=EPISODE_TOP, size=5000, kind="episode", state=State.WAITING_INDEXER
        )

        started, why = await coord.start_source_push(ts, reason="manual")

        assert started is True, why
        request = _request_of(transport["start_copy"])
        assert isinstance(request, PushRequest)
        assert [f.name for f in request.files] == [f"{EPISODE_TOP}/{EPISODE_NAME}"]
        assert request.infohash == h
        assert request.src_dir == VPS1_DIR
        # A wrapped episode lands on the unsorted remote (its own top exists).
        assert request.target_remote == UNSORTED_REMOTE
        checkpoint = store.get_push(h)
        assert checkpoint.state == PushState.RUNNING
        assert checkpoint.files_total == 1
        assert checkpoint.list_path == "/tmp/x.lst"
        assert checkpoint.log_path == "/tmp/x.log"
        assert store.push_is_live(h) is True
        fresh = store.get(h)
        assert fresh.state == State.WAITING_INDEXER
        assert fresh.indexer_next_retry_at is not None, "the row waits on the push cadence"
        assert fresh.force_direct == 0, "a push is not the racing-torrent leech"
    finally:
        store.close()


@pytest.mark.anyio
async def test_second_push_for_the_same_hash_is_refused(tmp_path, transport):
    """claim_push is the dedupe: a live record means one copy, ever."""
    store = StateStore(tmp_path / "state.db")
    try:
        blob = _single_blob(EPISODE_NAME, 2000)
        h = _hash_of(blob)
        coord = _push_coordinator(
            store, _push_cfg(), live=[_vps1_torrent(blob, name=EPISODE_TOP, size=2000)], files=[]
        )
        ts = _row(
            store, blob, name=EPISODE_TOP, size=2000, kind="episode", state=State.WAITING_INDEXER
        )

        first, _why = await coord.start_source_push(ts)
        assert first is True
        transport["start_copy"].assert_awaited_once()

        again, why = await coord.start_source_push(store.get(h))

        assert again is False
        assert "already running" in why
        transport["start_copy"].assert_awaited_once()
    finally:
        store.close()


@pytest.mark.anyio
async def test_expired_checkpoint_is_reclaimable(tmp_path, transport):
    """An expired record is not live: a fresh tap takes the hash again."""
    store = StateStore(tmp_path / "state.db")
    try:
        blob = _single_blob(EPISODE_NAME, 2000)
        h = _hash_of(blob)
        _live_push(store, h, expires_in=-dt.timedelta(seconds=1))
        assert store.push_is_live(h) is False
        coord = _push_coordinator(
            store, _push_cfg(), live=[_vps1_torrent(blob, name=EPISODE_TOP, size=2000)], files=[]
        )
        ts = _row(
            store, blob, name=EPISODE_TOP, size=2000, kind="episode", state=State.WAITING_INDEXER
        )

        started, why = await coord.start_source_push(ts)

        assert started is True, why
        assert store.get_push(h).state == PushState.RUNNING
    finally:
        store.close()


@pytest.mark.anyio
async def test_concurrency_cap_refuses_a_second_push(tmp_path, transport):
    store = StateStore(tmp_path / "state.db")
    try:
        blob = _single_blob(EPISODE_NAME, 2000)
        other = _single_blob(MOVIE_NAME, 4000)
        _live_push(store, _hash_of(other), expires_in=dt.timedelta(minutes=5))
        coord = _push_coordinator(
            store,
            _push_cfg(cap=1),
            live=[_vps1_torrent(blob, name=EPISODE_TOP, size=2000)],
            files=[],
        )
        ts = _row(
            store, blob, name=EPISODE_TOP, size=2000, kind="episode", state=State.WAITING_INDEXER
        )

        started, why = await coord.start_source_push(ts)

        assert started is False
        assert "1/1 push(es) already running" in why
        assert store.get_push(_hash_of(blob)) is None
    finally:
        store.close()


@pytest.mark.anyio
async def test_failed_preflight_leaves_no_live_checkpoint(tmp_path, transport):
    store = StateStore(tmp_path / "state.db")
    try:
        blob = _single_blob(EPISODE_NAME, 2000)
        h = _hash_of(blob)
        transport["preflight"].side_effect = lambda *a, **k: PushCheck(
            False, "push refused: no remote on VPS1"
        )
        coord = _push_coordinator(
            store, _push_cfg(), live=[_vps1_torrent(blob, name=EPISODE_TOP, size=2000)], files=[]
        )
        ts = _row(
            store, blob, name=EPISODE_TOP, size=2000, kind="episode", state=State.WAITING_INDEXER
        )

        started, why = await coord.start_source_push(ts)

        assert started is False
        assert why == "push refused: no remote on VPS1"
        assert store.push_is_live(h) is False
        assert store.get_push(h).state == PushState.ABANDONED
        transport["start_copy"].assert_not_awaited()
        # The row was never touched: no park, no state change.
        assert store.get(h).state == State.WAITING_INDEXER
    finally:
        store.close()


@pytest.mark.anyio
async def test_failed_launch_leaves_no_live_checkpoint(tmp_path, transport):
    store = StateStore(tmp_path / "state.db")
    try:
        blob = _single_blob(EPISODE_NAME, 2000)
        h = _hash_of(blob)
        transport["start_copy"].side_effect = lambda *a, **k: PushCheck(
            False, "push refused: VPS1 refused the copy"
        )
        coord = _push_coordinator(
            store, _push_cfg(), live=[_vps1_torrent(blob, name=EPISODE_TOP, size=2000)], files=[]
        )
        ts = _row(
            store, blob, name=EPISODE_TOP, size=2000, kind="episode", state=State.WAITING_INDEXER
        )

        started, why = await coord.start_source_push(ts)

        assert started is False
        assert "refused the copy" in why
        assert store.push_is_live(h) is False
        assert store.get_push(h).state == PushState.FAILED
        assert store.get(h).state == State.WAITING_INDEXER
    finally:
        store.close()


@pytest.mark.anyio
async def test_push_refused_without_the_file_names_of_this_torrent(tmp_path, transport):
    """Blob bytes belonging to another torrent must never name the push."""
    store = StateStore(tmp_path / "state.db")
    try:
        blob = _single_blob(EPISODE_NAME, 2000)
        wrong = _single_blob(MOVIE_NAME, 2000)
        h = _hash_of(blob)
        coord = _push_coordinator(
            store, _push_cfg(), live=[_vps1_torrent(blob, name=EPISODE_TOP, size=2000)], files=[]
        )
        _row(store, blob, name=EPISODE_TOP, size=2000, kind="episode", state=State.WAITING_INDEXER)
        # Every source of bytes now carries the WRONG torrent.
        store.set_racing_blob(h, wrong)
        row = store.get(h)
        row.cross_seed_blob = wrong
        store.upsert(row)

        started, why = await coord.start_source_push(store.get(h))

        assert started is False
        assert "no .torrent bytes" in why
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# layout identity with the move stage
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "name, top, row_kind, route_kind, expect_remote",
    [
        (EPISODE_NAME, EPISODE_TOP, "episode", "episode", UNSORTED_REMOTE),
        (NEXT_EPISODE, "Example.Show.S01E02.ExampleGroup", "season", "season", SORTED_REMOTE),
        (MOVIE_NAME, "Example.Film.2026.ExampleGroup", "movie", "movie", SORTED_REMOTE),
        # The proven bug: a pushed row is parked BEFORE admission, so its
        # stored kind is the schema default "unknown". The kind the bytes
        # imply decides — written here literally (never read back from the
        # code under test), so the rule stays one shared `decide_seed_layout`
        # call and the two callers still cannot drift.
        (EPISODE_NAME, EPISODE_TOP, "unknown", "episode", UNSORTED_REMOTE),
        (MOVIE_NAME, "Example.Film.2026.ExampleGroup", "unknown", "movie", SORTED_REMOTE),
    ],
)
@pytest.mark.anyio
async def test_push_layout_equals_the_move_layout_for_the_same_names(
    tmp_path, transport, name, top, row_kind, route_kind, expect_remote
):
    """Push and move call one rule: same names -> same remote + pinned top."""
    store = StateStore(tmp_path / "state.db")
    try:
        blob = _wrapped_blob(top, [(name, 2000)])
        coord = _push_coordinator(
            store,
            _push_cfg(),
            live=[_vps1_torrent(blob, name=top, size=2000, progress=0.2)],
            files=[TorrentFile(f"{top}/{name}", 2000, progress=1.0)],
        )
        ts = _row(store, blob, name=top, size=2000, kind=row_kind, state=State.WAITING_INDEXER)

        started, why = await coord.start_source_push(ts)
        assert started is True, why

        request = _request_of(transport["start_copy"])
        moved = decide_seed_layout(
            kind=route_kind,
            target_names=[f.name for f in request.files],
            # The push has no SSD batches; that is the whole point of the
            # shared helper — the decision depends on the names only, never
            # on where the bytes came from.
            batches_total=0,
            pinned_seed_subdir="",
            racing_wrapper="",
            remote_default=SORTED_REMOTE,
            remote_unsorted=UNSORTED_REMOTE,
        )
        assert request.target_remote == moved.target_remote == expect_remote
        assert store.get(_hash_of(blob)).seed_subdir == moved.seed_subdir == top
    finally:
        store.close()


@pytest.mark.anyio
async def test_flat_push_wraps_like_the_move_stage(tmp_path, transport):
    """A flat file with a folder-shaped racing sibling wraps into that folder."""
    store = StateStore(tmp_path / "state.db")
    try:
        blob = _single_blob(EPISODE_NAME, 2000)
        sibling = Torrent(
            hash="b" * 40,
            name=EPISODE_TOP,
            category="racing",
            save_path=VPS1_DIR,
            size_bytes=2000,
            state="seeding",
            progress=1.0,
            trackers=[ANNOUNCE],
        )
        coord = _push_coordinator(
            store,
            _push_cfg(),
            live=[_vps1_torrent(blob, name=EPISODE_TOP, size=2000), sibling],
            files=[],
        )
        ts = _row(
            store, blob, name=EPISODE_TOP, size=2000, kind="episode", state=State.WAITING_INDEXER
        )

        started, why = await coord.start_source_push(ts)
        assert started is True, why

        request = _request_of(transport["start_copy"])
        assert request.files == (PushFile(EPISODE_NAME, 2000),)
        assert request.src_dir == VPS1_DIR
        assert request.target_remote == f"{UNSORTED_REMOTE}/{EPISODE_TOP}"
        expected = decide_seed_layout(
            kind="episode",
            target_names=[EPISODE_NAME],
            batches_total=0,
            pinned_seed_subdir="",
            racing_wrapper=EPISODE_TOP,
            remote_default=SORTED_REMOTE,
            remote_unsorted=UNSORTED_REMOTE,
        )
        assert expected.target_remote == request.target_remote
        assert store.get(_hash_of(blob)).seed_subdir == EPISODE_TOP
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# the kind a push routes on (a pushed row is pre-admission, so it is "unknown")
# --------------------------------------------------------------------------- #


def _blob_of(files: list[tuple[str, int]]) -> bytes:
    """A .torrent whose own bytes carry exactly these names."""
    tops = {name.rsplit("/", 1)[0] for name, _s in files if "/" in name}
    if not tops:
        name, size = files[0]
        return _single_blob(name, size)
    return _wrapped_blob(next(iter(tops)), [(n.rsplit("/", 1)[-1], s) for n, s in files])


def _narrow_writes(store: StateStore, monkeypatch) -> MagicMock:
    """Spy the store's narrow writes so a test can see what was persisted."""
    spy = MagicMock(wraps=store.update_columns)
    monkeypatch.setattr(store, "update_columns", spy)
    return spy


def _kind_writes(spy: MagicMock) -> list[dict]:
    """Among those writes, the mappings that touched the classification column."""
    return [
        call.args[1]
        for call in spy.call_args_list
        if len(call.args) > 1
        and isinstance(call.args[1], dict)
        and "classification_kind" in call.args[1]
    ]


def _classify_spy(monkeypatch) -> list[int]:
    """Count the classifier calls the push makes, still returning the real answer."""
    from racing_sync.classifier import classify as real_classify

    seen: list[int] = []

    def _spy(files, cfg):
        seen.append(len(list(files)))
        return real_classify(files, cfg)

    monkeypatch.setattr(coordinator_module, "classify", _spy)
    return seen


@pytest.mark.parametrize(
    "files, expect_kind, expect_remote",
    [
        # The regression the production log proved: a single-file movie — here
        # a BluRay one — went to the unsorted remote, because the row's stored
        # kind is the schema default "unknown" until admission pins it.
        ([(MOVIE_NAME, 2000)], "movie", SORTED_REMOTE),
        ([(f"{MOVIE_TOP}/{MOVIE_NAME}", 2000)], "movie", SORTED_REMOTE),
        ([(f"{EPISODE_TOP}/{EPISODE_NAME}", 2000)], "episode", UNSORTED_REMOTE),
        (
            [
                (f"{SEASON_TOP}/{EPISODE_NAME}", 2000),
                (f"{SEASON_TOP}/{NEXT_EPISODE}", 2000),
                (f"{SEASON_TOP}/{THIRD_EPISODE}", 2000),
            ],
            "season",
            SORTED_REMOTE,
        ),
        (
            [
                (f"{SEASON_TOP}/{EPISODE_NAME}", 2000),
                (f"{SEASON_TOP}/{NEXT_EPISODE}", 2000),
                (f"{SEASON_TOP}/{BONUS_ONE}", 2000),
                (f"{SEASON_TOP}/{BONUS_TWO}", 2000),
                (f"{SEASON_TOP}/{BONUS_THREE}", 2000),
            ],
            "mixed",
            SORTED_REMOTE,
        ),
    ],
)
@pytest.mark.anyio
async def test_unknown_stored_kind_is_resolved_from_the_pushed_files(
    tmp_path, transport, files, expect_kind, expect_remote
):
    """The push routes on the kind its bytes imply, exactly like the move stage."""
    store = StateStore(tmp_path / "state.db")
    try:
        blob = _blob_of(files)
        total = sum(size for _n, size in files)
        display = files[0][0].rsplit("/", 1)[0] if "/" in files[0][0] else files[0][0]
        coord = _push_coordinator(
            store,
            _push_cfg(),
            live=[_vps1_torrent(blob, name=display, size=total)],
            files=[],
        )
        ts = _row(
            store, blob, name=display, size=total, kind="unknown", state=State.WAITING_INDEXER
        )

        started, why = await coord.start_source_push(ts)
        assert started is True, why

        request = _request_of(transport["start_copy"])
        assert [f.name for f in request.files] == [n for n, _s in files]
        # The remote the resolved kind routes to — movie/season/mixed sorted,
        # a lone episode unsorted — never the default "unknown" one.
        assert request.target_remote == expect_remote
        expected = decide_seed_layout(
            kind=expect_kind,
            target_names=[n for n, _s in files],
            batches_total=0,
            pinned_seed_subdir="",
            racing_wrapper="",
            remote_default=SORTED_REMOTE,
            remote_unsorted=UNSORTED_REMOTE,
        )
        assert request.target_remote == expected.target_remote
        assert store.get(_hash_of(blob)).classification_kind == expect_kind
    finally:
        store.close()


@pytest.mark.anyio
async def test_push_pins_the_derived_kind_with_a_narrow_write(tmp_path, transport, monkeypatch):
    """The derived kind reaches the STORE, so later stages need not re-derive.

    Asserted on the row as re-read, not on the in-memory snapshot: the handover,
    the indexer retry and a restart all read it back. The write must stay
    narrow and version-guarded — a whole-row upsert of the snapshot the push
    read before its awaits would replay stale columns over any transition that
    raced it, and would invalidate the `seed_subdir` pin made right after.
    """
    store = StateStore(tmp_path / "state.db")
    writes = _narrow_writes(store, monkeypatch)
    try:
        blob = _wrapped_blob(MOVIE_TOP, [(MOVIE_NAME, 2000)])
        coord = _push_coordinator(
            store, _push_cfg(), live=[_vps1_torrent(blob, name=MOVIE_TOP, size=2000)], files=[]
        )
        ts = _row(
            store, blob, name=MOVIE_TOP, size=2000, kind="unknown", state=State.WAITING_INDEXER
        )
        revision = ts.version

        started, why = await coord.start_source_push(ts)
        assert started is True, why

        assert _request_of(transport["start_copy"]).target_remote == SORTED_REMOTE
        kinds = _kind_writes(writes)
        assert kinds == [{"classification_kind": "movie"}]
        assert writes.call_args_list[0].kwargs["expected_version"] == revision
        fresh = store.get(_hash_of(blob))
        assert fresh.classification_kind == "movie"
        # The kind write kept the snapshot's revision in sync, so the layout
        # pin of the same push still landed.
        assert fresh.seed_subdir == MOVIE_TOP
        assert fresh.version > revision
    finally:
        store.close()


@pytest.mark.anyio
async def test_push_keeps_a_kind_admission_already_pinned(tmp_path, transport, monkeypatch):
    """A real stored kind is final: no re-derivation, no churn, no rewrite."""
    store = StateStore(tmp_path / "state.db")
    writes = _narrow_writes(store, monkeypatch)
    seen = _classify_spy(monkeypatch)
    try:
        # An episode-shaped blob on a row admission classified as a season
        # pack: the pinned kind routes, exactly as the move stage's pin does.
        blob = _wrapped_blob(EPISODE_TOP, [(EPISODE_NAME, 2000)])
        coord = _push_coordinator(
            store,
            _push_cfg(),
            live=[_vps1_torrent(blob, name=EPISODE_TOP, size=2000, progress=0.2)],
            files=[TorrentFile(f"{EPISODE_TOP}/{EPISODE_NAME}", 2000, progress=1.0)],
        )
        ts = _row(
            store, blob, name=EPISODE_TOP, size=2000, kind="season", state=State.WAITING_INDEXER
        )

        started, why = await coord.start_source_push(ts)
        assert started is True, why

        assert _request_of(transport["start_copy"]).target_remote == SORTED_REMOTE
        assert seen == []
        assert _kind_writes(writes) == []
        assert store.get(_hash_of(blob)).classification_kind == "season"
    finally:
        store.close()


@pytest.mark.anyio
async def test_push_routes_unsorted_when_classification_fails(tmp_path, transport, monkeypatch):
    """Fail-closed: an unusable classification keeps today's routing and the push."""
    store = StateStore(tmp_path / "state.db")
    writes = _narrow_writes(store, monkeypatch)

    def _unavailable(_files, _cfg):
        raise RuntimeError("classifier unavailable")

    monkeypatch.setattr(coordinator_module, "classify", _unavailable)
    try:
        blob = _wrapped_blob(MOVIE_TOP, [(MOVIE_NAME, 2000)])
        coord = _push_coordinator(
            store, _push_cfg(), live=[_vps1_torrent(blob, name=MOVIE_TOP, size=2000)], files=[]
        )
        ts = _row(
            store, blob, name=MOVIE_TOP, size=2000, kind="unknown", state=State.WAITING_INDEXER
        )

        # No exception escapes into the caller, and no third routing branch:
        # the push still goes ahead with the answer it had before the fix.
        started, why = await coord.start_source_push(ts)
        assert started is True, why

        assert _request_of(transport["start_copy"]).target_remote == UNSORTED_REMOTE
        assert _kind_writes(writes) == []
        assert store.get(_hash_of(blob)).classification_kind == "unknown"
    finally:
        store.close()


def test_push_due_gates_one_remote_check_per_interval():
    """Cadence rides the persisted `updated_at`, so it survives a restart."""
    coord = make_coordinator()
    now = dt.datetime.now(dt.UTC)
    fresh = SourcePush(infohash="a" * 40, updated_at=now)
    aged = SourcePush(infohash="a" * 40, updated_at=now - dt.timedelta(seconds=180))
    naive = SourcePush(infohash="a" * 40, updated_at=now.replace(tzinfo=None))
    assert coord._push_due(fresh, now, 120.0) is False
    assert coord._push_due(aged, now, 120.0) is True
    # A stamp written by an older build without an offset is read as UTC.
    assert coord._push_due(naive, now, 120.0) is False


# --------------------------------------------------------------------------- #
# suppression while a push is live
# --------------------------------------------------------------------------- #


@pytest.mark.anyio
async def test_live_push_blocks_the_timeout_leech_and_the_reservation(tmp_path):
    """No second copy on VPS2: neither the leech nor an SSD budget fires."""
    store = StateStore(tmp_path / "state.db")
    try:
        cfg = _push_cfg(leech=True)
        blob = _single_blob(EPISODE_NAME, 2000)
        h = _hash_of(blob)
        _live_push(store, h, expires_in=dt.timedelta(minutes=30))
        past = dt.datetime.now(dt.UTC) - dt.timedelta(
            seconds=cfg.cross_seed.indexer_wait_seconds + 60
        )
        _row(
            store,
            blob,
            name=EPISODE_TOP,
            size=2000,
            kind="episode",
            state=State.WAITING_INDEXER,
            indexer_first_queried_at=past,
            indexer_attempts=40,
        )
        coord = _push_coordinator(store, cfg, live=[], files=[])
        coord.start_source_push = AsyncMock(return_value=(False, "push refused: cap reached"))
        calls: list[bool] = []

        async def _miss(**kwargs):
            calls.append(bool(kwargs.get("attempt_prowlarr")))
            # The leech branch would return the racing bytes here; with a live
            # push it must never even run.
            return None

        original = coordinator_module.pick_ssd_source_for_racing
        coordinator_module.pick_ssd_source_for_racing = _miss
        try:
            await coord._pick_and_admit(
                store.get(h), _vps1_torrent(blob, name=EPISODE_TOP, size=2000), []
            )
        finally:
            coordinator_module.pick_ssd_source_for_racing = original

        assert calls == [True], "the direct (leech) re-pick must not run"
        fresh = store.get(h)
        assert fresh.state == State.WAITING_INDEXER
        assert fresh.force_direct == 0, "the VPS2 leech must not be armed behind a push"
        coord._ssd_try_reserve.assert_not_awaited()
        assert coord._ssd_reserved == {}, "a push must not leak a reservation"
    finally:
        store.close()


@pytest.mark.anyio
async def test_live_push_reserves_nothing_in_admit_resolved(tmp_path):
    """The reserve->QUEUED tail parks instead of booking SSD (nothing leaks)."""
    store = StateStore(tmp_path / "state.db")
    try:
        blob = _single_blob(EPISODE_NAME, 2000)
        h = _hash_of(blob)
        _live_push(store, h, expires_in=dt.timedelta(minutes=30))
        coord = _push_coordinator(store, _push_cfg(), live=[], files=[])
        coord._park_for_indexer_retry = MagicMock()
        coord.transition = MagicMock()
        ts = _row(store, blob, name=EPISODE_TOP, size=2000, kind="episode", state=State.NEW)

        await coord._admit_resolved(ts, blob=blob, size_bytes=2000, display_name=EPISODE_TOP)

        coord._ssd_try_reserve.assert_not_awaited()
        assert coord._ssd_reserved == {}
        coord.transition.assert_not_called()
        coord._park_for_indexer_retry.assert_called_once_with(ts, reason=PUSH_PARK_REASON)
    finally:
        store.close()


@pytest.mark.anyio
async def test_no_live_push_admit_resolved_still_reserves(tmp_path):
    """Inert default: with no checkpoint the SSD reservation runs as today."""
    store = StateStore(tmp_path / "state.db")
    try:
        blob = _single_blob(EPISODE_NAME, 2000)
        h = _hash_of(blob)
        coord = _push_coordinator(store, _push_cfg(), live=[], files=[])
        coord.transition = MagicMock(side_effect=lambda t, s, **k: setattr(t, "state", s))
        ts = _row(store, blob, name=EPISODE_TOP, size=2000, kind="episode", state=State.NEW)

        await coord._admit_resolved(ts, blob=blob, size_bytes=2000, display_name=EPISODE_TOP)

        coord._ssd_try_reserve.assert_awaited_once_with(h, coord._ssd_estimate_for_new(2000))
        assert coord.transition.call_args[0][1] == State.QUEUED
    finally:
        store.close()


@pytest.mark.anyio
async def test_live_push_does_not_fail_the_row_at_max_age(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        cfg = _push_cfg(check_s=90)
        blob = _single_blob(EPISODE_NAME, 2000)
        h = _hash_of(blob)
        past = dt.datetime.now(dt.UTC) - dt.timedelta(
            seconds=cfg.cross_seed.indexer_wait_seconds + 60
        )
        _live_push(store, h, expires_in=dt.timedelta(minutes=30))
        _row(
            store,
            blob,
            name=EPISODE_TOP,
            size=2000,
            kind="episode",
            state=State.WAITING_INDEXER,
            indexer_first_queried_at=past,
            indexer_attempts=40,
        )
        coord = _push_coordinator(store, cfg, live=[], files=[])

        coord._park_for_indexer_retry(store.get(h), reason="indexer miss")

        fresh = store.get(h)
        assert fresh.state == State.WAITING_INDEXER, "a live push must not be failed at max age"
        assert fresh.indexer_first_queried_at == past, "the normal ceiling clock is kept"
        assert fresh.indexer_next_retry_at is not None
        assert fresh.indexer_next_retry_at <= dt.datetime.now(dt.UTC) + dt.timedelta(seconds=150)
    finally:
        store.close()


@pytest.mark.anyio
async def test_same_content_sibling_defers_to_a_row_that_owns_a_live_push(tmp_path):
    """A push row parked as WAITING_INDEXER counts as an OWNER, not a waiter.

    The push row here is the YOUNGER one: without the push the age election
    hands control to the querying row (owner None), so a non-None owner can
    only come from the live checkpoint.
    """
    store = StateStore(tmp_path / "state.db")
    try:
        # Same content (same name + size) under a different infohash: the
        # piece length is inside the info dict, so it moves the hash only.
        push_blob = _single_blob(EPISODE_NAME, 2000, piece_length=32768)
        sibling_blob = _single_blob(EPISODE_NAME, 2000)
        push_h = _hash_of(push_blob)
        assert push_h != _hash_of(sibling_blob)
        base = dt.datetime.now(dt.UTC)
        sibling = _row(
            store,
            sibling_blob,
            name=EPISODE_TOP,
            size=2000,
            kind="episode",
            state=State.WAITING_INDEXER,
            created_at=base,
        )
        _row(
            store,
            push_blob,
            name=EPISODE_TOP,
            size=2000,
            kind="episode",
            state=State.WAITING_INDEXER,
            created_at=base + dt.timedelta(minutes=1),
        )
        coord = _push_coordinator(store, _push_cfg(), live=[], files=[])
        # Inert default: the younger parked row is nobody's owner.
        assert coord._live_push_hashes() == set()
        assert coord._inflight_same_content(sibling) is None

        _live_push(store, push_h, expires_in=dt.timedelta(minutes=30))
        coord._inflight_snapshot = None
        coord._live_push_cache = None

        owner = coord._inflight_same_content(sibling)
        assert owner is not None
        assert owner.source_infohash == push_h
        # And the recorded-deferral view agrees, or the waiter re-scans.
        coord._inflight_snapshot = None
        assert coord._deferred_owner_live(push_h) is True

        # Past its deadline the record is not live: the scan forgets it.
        store.clear_push(push_h)
        _live_push(store, push_h, expires_in=-dt.timedelta(seconds=1))
        coord._inflight_snapshot = None
        coord._live_push_cache = None
        assert coord._inflight_same_content(sibling) is None
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# the sweep: progress, handover, deadline
# --------------------------------------------------------------------------- #


@pytest.mark.anyio
async def test_sweep_does_nothing_while_the_switch_is_off(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        coord = make_coordinator(store)
        coord.cfg = _push_cfg(enabled=False)
        store.list_pushes = MagicMock()
        await coord.push_sweep()
        store.list_pushes.assert_not_called()
    finally:
        store.close()


@pytest.mark.anyio
async def test_sweep_notes_progress_and_leaves_the_row_parked(tmp_path, transport):
    store = StateStore(tmp_path / "state.db")
    try:
        blob = _wrapped_blob(EPISODE_TOP, [(EPISODE_NAME, 2000), (NEXT_EPISODE, 3000)])
        h = _hash_of(blob)
        coord = _push_coordinator(
            store,
            _push_cfg(),
            live=[_vps1_torrent(blob, name=EPISODE_TOP, size=5000)],
            files=[TorrentFile(f"{EPISODE_TOP}/{EPISODE_NAME}", 2000, progress=1.0)],
        )
        coord._push_due = MagicMock(return_value=True)
        ts = _row(
            store, blob, name=EPISODE_TOP, size=5000, kind="episode", state=State.WAITING_INDEXER
        )
        assert (await coord.start_source_push(ts))[0] is True

        transport["remote_progress"].return_value = (1, 2)
        await coord.push_sweep()

        push = store.get_push(h)
        assert (push.files_present, push.files_total) == (1, 2)
        assert push.state == PushState.RUNNING
        assert store.get(h).state == State.WAITING_INDEXER
        # Metadata only: one listing of the wanted names, no content read.
        args = transport["remote_progress"].await_args
        assert args.args[1] == UNSORTED_REMOTE
        assert [f.name for f in args.args[2]] == [
            f"{EPISODE_TOP}/{EPISODE_NAME}",
            f"{EPISODE_TOP}/{NEXT_EPISODE}",
        ]
    finally:
        store.close()


@pytest.mark.anyio
async def test_sweep_hands_a_finished_push_to_the_existing_admission(tmp_path, transport):
    """All files on the remote -> verifying -> the all-remote path -> QUEUED."""
    store = StateStore(tmp_path / "state.db")
    try:
        blob = _wrapped_blob(EPISODE_TOP, [(EPISODE_NAME, 2000)])
        h = _hash_of(blob)
        coord = _push_coordinator(
            store,
            _push_cfg(),
            live=[_vps1_torrent(blob, name=EPISODE_TOP, size=2000)],
            files=[TorrentFile(f"{EPISODE_TOP}/{EPISODE_NAME}", 2000, progress=1.0)],
        )
        coord._push_due = MagicMock(return_value=True)
        ts = _row(
            store, blob, name=EPISODE_TOP, size=2000, kind="episode", state=State.WAITING_INDEXER
        )
        assert (await coord.start_source_push(ts))[0] is True

        transport["remote_progress"].return_value = (1, 1)
        # The EXISTING evidence path and the EXISTING all-remote shortcut —
        # the push re-implements neither.
        coord._fuse_content_evidence = AsyncMock(return_value=("present", blob, 1, 1))
        coord._blob_fully_remote = AsyncMock(return_value=True)

        await coord.push_sweep()

        row = store.get(h)
        assert row.state == State.QUEUED
        coord._blob_fully_remote.assert_awaited()
        coord._ssd_try_reserve.assert_not_awaited()
        assert coord._ssd_reserved == {}
        push = store.get_push(h)
        assert push.state == PushState.DONE
        assert push.files_present == 1
        assert store.push_is_live(h) is False
    finally:
        store.close()


@pytest.mark.anyio
async def test_sweep_keeps_verifying_when_the_seed_path_lags(tmp_path, transport):
    store = StateStore(tmp_path / "state.db")
    try:
        blob = _single_blob(EPISODE_NAME, 2000)
        h = _hash_of(blob)
        coord = _push_coordinator(
            store,
            _push_cfg(),
            live=[_vps1_torrent(blob, name=EPISODE_TOP, size=2000)],
            files=[TorrentFile(EPISODE_NAME, 2000, progress=1.0)],
        )
        coord._push_due = MagicMock(return_value=True)
        ts = _row(
            store, blob, name=EPISODE_TOP, size=2000, kind="episode", state=State.WAITING_INDEXER
        )
        assert (await coord.start_source_push(ts))[0] is True

        transport["remote_progress"].return_value = (1, 1)
        coord._fuse_content_evidence = AsyncMock(return_value=("partial", None, 1, 2))

        await coord.push_sweep()

        assert store.get_push(h).state == PushState.VERIFYING
        assert store.get(h).state == State.WAITING_INDEXER
        coord._ssd_try_reserve.assert_not_awaited()
    finally:
        store.close()


@pytest.mark.anyio
async def test_deadline_fails_the_checkpoint_and_today_resumes(tmp_path, transport):
    store = StateStore(tmp_path / "state.db")
    try:
        cfg = _push_cfg(deadline=600)
        blob = _single_blob(EPISODE_NAME, 2000)
        h = _hash_of(blob)
        past = dt.datetime.now(dt.UTC) - dt.timedelta(
            seconds=cfg.cross_seed.indexer_wait_seconds + 60
        )
        # A live-in-state, expired-in-fact checkpoint (what a copy that never
        # finished leaves behind): the record is RUNNING but the deadline is
        # already past, so `push_is_live` is False and suppression has lifted.
        _live_push(store, h, expires_in=-dt.timedelta(seconds=30))
        _row(
            store,
            blob,
            name=EPISODE_TOP,
            size=2000,
            kind="episode",
            state=State.WAITING_INDEXER,
            indexer_first_queried_at=past,
            indexer_attempts=40,
        )
        coord = _push_coordinator(store, cfg, live=[], files=[])
        assert store.push_is_live(h) is False
        assert store.get_push(h).state == PushState.RUNNING

        await coord.push_sweep()

        push = store.get_push(h)
        assert push.state == PushState.FAILED
        assert "deadline" in push.last_error

        # The next tick behaves exactly as today: an expired window FAILS.
        coord._park_for_indexer_retry(store.get(h), reason="indexer miss")
        assert store.get(h).state == State.FAILED
    finally:
        store.close()


@pytest.mark.anyio
async def test_sweep_settles_a_push_whose_row_disappeared(tmp_path, transport):
    store = StateStore(tmp_path / "state.db")
    try:
        blob = _single_blob(EPISODE_NAME, 2000)
        h = _hash_of(blob)
        _live_push(store, h, expires_in=dt.timedelta(minutes=30))
        coord = _push_coordinator(store, _push_cfg(), live=[], files=[])
        store.delete(h)

        await coord.push_sweep()

        assert store.get_push(h).state == PushState.ABANDONED
        assert store.push_is_live(h) is False
    finally:
        store.close()


@pytest.mark.anyio
async def test_sweep_rederives_names_after_a_restart(tmp_path, transport):
    """No in-memory wanted set (fresh process): rebuild it from the blob."""
    store = StateStore(tmp_path / "state.db")
    try:
        blob = _wrapped_blob(EPISODE_TOP, [(EPISODE_NAME, 2000)])
        h = _hash_of(blob)
        _live_push(store, h, expires_in=dt.timedelta(minutes=30))
        store.set_racing_blob(h, blob)
        # A restart: the row exists (no in-memory blob) and the checkpoint
        # does, but `_push_wanted` is empty.
        store.upsert(
            TorrentState(source_infohash=h, source_name=EPISODE_TOP, state=State.WAITING_INDEXER)
        )
        coord = _push_coordinator(
            store,
            _push_cfg(),
            live=[_vps1_torrent(blob, name=EPISODE_TOP, size=2000)],
            files=[TorrentFile(f"{EPISODE_TOP}/{EPISODE_NAME}", 2000, progress=0.5)],
        )
        coord._push_due = MagicMock(return_value=True)
        coord._push_wanted = {}

        await coord.push_sweep()

        args = transport["remote_progress"].await_args
        assert [f.name for f in args.args[2]] == [f"{EPISODE_TOP}/{EPISODE_NAME}"]
        assert coord._push_wanted[h]
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# illegal park states + auto precedence
# --------------------------------------------------------------------------- #


@pytest.mark.anyio
async def test_queued_row_is_preempted_before_the_park(tmp_path, transport):
    """Its reservation and half-done dest entry must be gone before parking."""
    store = StateStore(tmp_path / "state.db")
    try:
        blob = _single_blob(EPISODE_NAME, 2000)
        h = _hash_of(blob)
        coord = _push_coordinator(
            store, _push_cfg(), live=[_vps1_torrent(blob, name=EPISODE_TOP, size=2000)], files=[]
        )
        ts = _row(store, blob, name=EPISODE_TOP, size=2000, kind="episode", state=State.QUEUED)
        coord._ssd_reserved = {h: 2000}
        entry = Torrent(
            hash=h,
            name=EPISODE_TOP,
            category="racing",
            save_path=str(coord.cfg.ssd.path / EPISODE_TOP),
            size_bytes=2000,
            state="downloading",
            progress=0.3,
        )
        deleted: list[tuple[str, bool]] = []
        coord.dest_client.list_torrents = AsyncMock(return_value=[entry])

        async def _delete(torrent_hash: str, *, delete_files: bool = False):
            deleted.append((torrent_hash.lower(), delete_files))

        coord.dest_client.delete = AsyncMock(side_effect=_delete)

        started, why = await coord.start_source_push(ts)

        assert started is True, why
        assert store.get(h).state == State.WAITING_INDEXER
        assert h not in coord._ssd_reserved, "the reservation must be released, not leaked"
        assert deleted == [(h, True)], "the SSD-owned partial must be removed"
    finally:
        store.close()


@pytest.mark.anyio
async def test_downloading_row_is_refused(tmp_path, transport):
    """No half-doing: an active download is the operator's call, not ours."""
    store = StateStore(tmp_path / "state.db")
    try:
        blob = _single_blob(EPISODE_NAME, 2000)
        h = _hash_of(blob)
        coord = _push_coordinator(
            store, _push_cfg(), live=[_vps1_torrent(blob, name=EPISODE_TOP, size=2000)], files=[]
        )
        ts = _row(store, blob, name=EPISODE_TOP, size=2000, kind="episode", state=State.DOWNLOADING)

        started, why = await coord.start_source_push(ts)

        assert started is False
        assert "already downloading" in why
        assert store.get_push(h) is None
        assert store.push_is_live(h) is False
        transport["start_copy"].assert_not_awaited()
    finally:
        store.close()


def _prowlarr_miss_recorder() -> tuple[AsyncMock, list[bool]]:
    """Stand in for the cross-seed picker: never a decision, records the flag."""
    calls: list[bool] = []

    async def _miss(**kwargs):
        calls.append(bool(kwargs.get("attempt_prowlarr")))
        return None

    return _miss, calls


async def _run_timeout_pick(coord, ts, st):
    original = coordinator_module.pick_ssd_source_for_racing
    pick, calls = _prowlarr_miss_recorder()
    coordinator_module.pick_ssd_source_for_racing = pick
    try:
        await coord._pick_and_admit(ts, st, [])
    finally:
        coordinator_module.pick_ssd_source_for_racing = original
    return calls


@pytest.mark.anyio
async def test_auto_off_never_pushes_and_the_leech_still_runs(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        cfg = _push_cfg(auto=False, leech=True)
        blob = _single_blob(EPISODE_NAME, 2000)
        h = _hash_of(blob)
        past = dt.datetime.now(dt.UTC) - dt.timedelta(
            seconds=cfg.cross_seed.indexer_wait_seconds + 60
        )
        _row(
            store,
            blob,
            name=EPISODE_TOP,
            size=2000,
            kind="episode",
            state=State.WAITING_INDEXER,
            indexer_first_queried_at=past,
        )
        coord = _push_coordinator(
            store, cfg, live=[_vps1_torrent(blob, name=EPISODE_TOP, size=2000)], files=[]
        )
        coord.start_source_push = AsyncMock()

        calls = await _run_timeout_pick(
            coord, store.get(h), _vps1_torrent(blob, name=EPISODE_TOP, size=2000)
        )

        coord.start_source_push.assert_not_awaited()
        assert calls == [True, False], "today's racing-torrent leech is unchanged"
        assert store.get(h).force_direct == 1
    finally:
        store.close()


@pytest.mark.anyio
async def test_auto_on_pushes_before_leeching(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        cfg = _push_cfg(auto=True, leech=True)
        blob = _single_blob(EPISODE_NAME, 2000)
        h = _hash_of(blob)
        past = dt.datetime.now(dt.UTC) - dt.timedelta(
            seconds=cfg.cross_seed.indexer_wait_seconds + 60
        )
        _row(
            store,
            blob,
            name=EPISODE_TOP,
            size=2000,
            kind="episode",
            state=State.WAITING_INDEXER,
            indexer_first_queried_at=past,
        )
        coord = _push_coordinator(
            store, cfg, live=[_vps1_torrent(blob, name=EPISODE_TOP, size=2000)], files=[]
        )
        coord.start_source_push = AsyncMock(return_value=(True, "pushing 1 file(s) from VPS1"))

        calls = await _run_timeout_pick(
            coord, store.get(h), _vps1_torrent(blob, name=EPISODE_TOP, size=2000)
        )

        coord.start_source_push.assert_awaited_once()
        assert coord.start_source_push.await_args.kwargs["reason"] == "auto"
        assert calls == [True], "the push won: the VPS2 leech never ran"
        assert store.get(h).force_direct == 0
    finally:
        store.close()


@pytest.mark.anyio
async def test_auto_on_falls_back_to_the_leech_when_the_push_refuses(tmp_path):
    store = StateStore(tmp_path / "state.db")
    try:
        cfg = _push_cfg(auto=True, leech=True)
        blob = _single_blob(EPISODE_NAME, 2000)
        h = _hash_of(blob)
        past = dt.datetime.now(dt.UTC) - dt.timedelta(
            seconds=cfg.cross_seed.indexer_wait_seconds + 60
        )
        _row(
            store,
            blob,
            name=EPISODE_TOP,
            size=2000,
            kind="episode",
            state=State.WAITING_INDEXER,
            indexer_first_queried_at=past,
        )
        coord = _push_coordinator(
            store, cfg, live=[_vps1_torrent(blob, name=EPISODE_TOP, size=2000)], files=[]
        )
        coord.start_source_push = AsyncMock(return_value=(False, "push refused: nothing complete"))

        calls = await _run_timeout_pick(
            coord, store.get(h), _vps1_torrent(blob, name=EPISODE_TOP, size=2000)
        )

        assert calls == [True, False], "a refused push must leave today's fallback intact"
        assert store.get(h).force_direct == 1
    finally:
        store.close()


@pytest.mark.anyio
async def test_push_sweep_runs_from_the_poller_step(tmp_path):
    """The sweep rides the poller tick, next to the manual-move sweep."""
    coord = make_coordinator()
    coord.push_sweep = AsyncMock()
    coord.scan_watch = AsyncMock(return_value=[])
    coord._list_source_torrents = AsyncMock(return_value=[])
    coord._poll_source_racing = AsyncMock(return_value=[])
    coord._sweep_manual_fuse_adoptions = AsyncMock()
    coord._sweep_manual_moved_content = AsyncMock()
    coord._drain_intake = AsyncMock(return_value=0)
    await coord._poll_source_step()
    coord.push_sweep.assert_awaited_once()
