"""Dead-copy detection on the push lane, and the DEDICATED dial it runs on.

Fictional names only ("Example.Show.S01E01.1080p.WEB-DL-GROUP", "seedremote"),
as everywhere in this repo — nothing here is read from a log file or from
`debug-config.toml`.

The production failure these tests pin, in one paragraph: three copies were
launched in 22 seconds, every later request was refused `3/3 push(es) already
running`, and across 1.8 h there were 64 polls reading `has 0/1 file(s)` and
exactly one success. Nothing could tell a dead copy from a slow one, so a slot
could be squatted for the full 6 h `timeout_seconds`. Two fixes, one lane:

* `vps1_push.push_liveness` reads the copy's OWN rclone log on VPS1 (one
  bounded shell: `test -f`, `wc -c`, `tail -c` — never a glob, never the
  mount) and the sweep fails a provably dead checkpoint on the very poll that
  sees it, so the slot frees; `deadline_at` stays the backstop, and silence —
  normal for a multi-GB upload to a Telegram-backed remote — never kills;
* preflight, the file-list write and that log read run on a DEDICATED dial
  (`run_shell_dedicated` / `write_remote_file_dedicated`), never a pooled
  member, so the push lane can neither queue behind a racing .torrent fetch
  nor starve one. Pinned by a `_lease` counter that stays at zero across a
  full start-and-poll cycle.

Plus the shipped defaults that stopped three 4-way unthrottled copies eating
the uplink the races are won on: `transfers` 2, `max_concurrent_pushes` 1,
`bwlimit` still "" (the trap is documented, not defaulting it away), and
`poll_seconds` / `timeout_seconds` deliberately untouched.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from conftest import make_coordinator
from test_coordinator_push import (
    EPISODE_NAME,
    EPISODE_TOP,
    MOVIE_NAME,
    _Dial,
    _hash_of,
    _live_push,
    _push_cfg,
    _push_coordinator,
    _row,
    _single_blob,
    _vps1_torrent,
)

from racing_sync import coordinator_push, vps1_push
from racing_sync.config import AppConfig, DelugeSFTPConfig, RclonePushConfig
from racing_sync.runtime import Runtime
from racing_sync.sftp_source import SFTPExporter, _SFTPConnection
from racing_sync.state import State, StateStore, TorrentState
from racing_sync.state_schema import PushState
from racing_sync.vps1_push import (
    _SIZE_TAG,
    PushCheck,
    PushFile,
    PushRequest,
    build_copy_command,
    preflight,
    push_liveness,
    start_copy,
)

ROOT = Path(__file__).parent.parent
LOG_PATH = "/tmp/racing-sync-push-ab12cd34ef-1762000000.log"
LIST_PATH = "/tmp/racing-sync-push-ab12cd34ef-1762000000.lst"

#: A quiet, in-flight 40 GB copy: rclone has opened the log and said nothing.
_SILENT_TAIL = "2026-11-21 09:12:01 INFO : Example.Show.S01E01.1080p.WEB-DL-GROUP.mkv: Copied (new)"
_FATAL_TAIL = (
    "2026-11-21 09:12:01 ERROR : Example.Show.S01E01.1080p.WEB-DL-GROUP.mkv: "
    "Final commit failed: Write to disk failed\n"
    "Failed to copy with 1 errors: forced shutdown"
)
_SUMMARY_TAIL = (
    "2026-11-21 09:58:02 INFO : Transferred: 0 / 0 Bytes, 0%, 0 / s, 0 checks, 0 bytes\n"
    "2026-11-21 09:58:02 INFO : Elapsed time: 46m1s"
)


# --------------------------------------------------------------------------- #
# A: the probe itself — one bounded shell, four honest answers
# --------------------------------------------------------------------------- #


class _LogDial:
    """Answers the liveness probe from canned (status, stdout, stderr)."""

    def __init__(self, reply: tuple[int | None, str, str]):
        self.reply = reply
        self.commands: list[str] = []
        self.pooled_calls: list[str] = []

    def run_shell_dedicated(self, command: str, timeout: float = 30.0):
        self.commands.append(command)
        return self.reply

    # The pooled entry points exist ONLY so a test can prove they are unused.
    def run_shell(self, command: str, timeout: float = 30.0):
        self.pooled_calls.append(command)
        return (0, "", "")


def _probe(size_line: str, tail: str = "") -> tuple[int | None, str, str]:
    """The reply a log of `size_line` bytes with `tail` would give."""
    out = f"{_SIZE_TAG}{size_line}\n{tail}" if tail else f"{_SIZE_TAG}{size_line}\n"
    return (0, out, "")


class _LaneDial(_LogDial):
    """A dedicated dial for the whole lane: probes, list write, launch.

    Replies are chosen by substring, exactly like a real VPS1 would answer,
    and the pooled entry points are recorded so a test can prove the lane
    never used them.
    """

    def __init__(self, reply: tuple[int | None, str, str] | None = None):
        super().__init__(reply or (0, "", ""))
        self.written: list[tuple[str, bytes]] = []
        self.launched: list[str] = []

    def run_shell_dedicated(self, command: str, timeout: float = 30.0):
        self.commands.append(command)
        if "command -v" in command:
            return (0, "/usr/bin/rclone\n", "")
        if "listremotes" in command:
            return (0, "seedremote:\n", "")
        return self.reply

    def write_remote_file_dedicated(self, path: str, data: bytes) -> bool:
        self.written.append((path, data))
        return True

    def launch_detached(self, command: str) -> bool:
        self.launched.append(command)
        return True


def _request(files: tuple[PushFile, ...]) -> PushRequest:
    return PushRequest(
        infohash="ab12cd34ef56ab78cd90ef12ab34cd56ef78ab90",
        group_key="example-key",
        content_name="Example Show S01E01",
        src_dir="/data/racing/Example.Show.S01E01.1080p.WEB-DL-GROUP",
        target_remote="seedremote:Unsorted",
        files=files,
    )


@pytest.mark.anyio
async def test_the_probe_is_one_bounded_read_of_our_own_log():
    """Cheap and bounded: our log path, a size and a tail. Nothing else.

    No glob, no listing, no mount, no whole-file read — the rclone log is a
    job log (not content), and a multi-hour copy must not be pulled ashore.
    """
    dial = _LogDial(_probe("1024", _SILENT_TAIL))
    await push_liveness(ssh=dial, log_path=LOG_PATH, age_s=600.0)

    assert len(dial.commands) == 1
    command = dial.commands[0]
    assert f"test -f {LOG_PATH}" in command
    assert f"wc -c < {LOG_PATH}" in command
    assert f"tail -c {vps1_push._LIVENESS_TAIL_BYTES} {LOG_PATH}" in command
    # Exactly one file, named absolutely: never a wildcard, never a directory
    # walk, never the seed remote.
    assert "*" not in command and "?" not in command
    for forbidden in ("ls ", "find ", "lsf", "rclone", "/mnt"):
        assert forbidden not in command
    # And it went out on the dedicated dial, not a pooled member.
    assert dial.pooled_calls == []


@pytest.mark.anyio
async def test_probe_command_quotes_a_path_a_caller_could_not_have_meant():
    command = vps1_push._liveness_probe_command("/tmp/our own 'log'.log")
    assert command.count("'") % 2 == 0
    assert "/tmp/our own 'log'.log" in command.replace("'\"'\"", "")


@pytest.mark.anyio
async def test_no_log_at_all_past_the_grace_is_missing():
    dial = _LogDial((0, f"{_SIZE_TAG}absent\n", ""))
    verdict, reason = await push_liveness(ssh=dial, log_path=LOG_PATH, age_s=900.0)
    assert verdict == "missing"
    assert LOG_PATH in reason and "never appeared" in reason


@pytest.mark.parametrize("age_s", [60.0, None])
@pytest.mark.anyio
async def test_no_log_yet_inside_the_grace_is_still_alive(age_s):
    """A copy that has not opened its log in its first seconds is not dead."""
    dial = _LogDial((0, f"{_SIZE_TAG}absent\n", ""))
    verdict, reason = await push_liveness(ssh=dial, log_path=LOG_PATH, age_s=age_s)
    assert verdict == "alive"
    assert reason


@pytest.mark.anyio
async def test_an_empty_log_past_the_grace_is_missing():
    dial = _LogDial(_probe("0"))
    verdict, reason = await push_liveness(ssh=dial, log_path=LOG_PATH, age_s=900.0)
    assert verdict == "missing"
    assert "still empty" in reason


@pytest.mark.anyio
async def test_grace_is_the_callers_to_widen():
    dial = _LogDial((0, f"{_SIZE_TAG}absent\n", ""))
    verdict, _why = await push_liveness(ssh=dial, log_path=LOG_PATH, age_s=900.0, grace_s=1800.0)
    assert verdict == "alive"


@pytest.mark.anyio
async def test_a_fatal_line_is_failed_and_carries_that_line():
    dial = _LogDial(_probe("2048", _FATAL_TAIL))
    verdict, reason = await push_liveness(ssh=dial, log_path=LOG_PATH, age_s=900.0)
    assert verdict == "failed"
    # The LAST fatal statement (the one that describes the job), scrubbed flat.
    assert "Failed to copy with 1 errors" in reason
    assert "\n" not in reason and len(reason) <= 300


@pytest.mark.anyio
async def test_a_fatal_line_is_scrubbed_and_capped():
    nasty = "ERROR : \x00\x1b[31m" + ("x" * 5000) + "\n"
    dial = _LogDial(_probe("9999", nasty))
    verdict, reason = await push_liveness(ssh=dial, log_path=LOG_PATH, age_s=900.0)
    assert verdict == "failed"
    assert "\x00" not in reason and "\x1b" not in reason and "\n" not in reason
    assert len(reason) <= 300


@pytest.mark.parametrize(
    "line",
    [
        "ERROR : something",
        "Fatal error: cannot continue",
        "panic: runtime error: index out of range",
        "rclone exited with exit status 1",
        "Failed to copy with 1 errors",
    ],
)
@pytest.mark.anyio
async def test_the_fatal_set_is_small_and_documented(line):
    dial = _LogDial(_probe("512", f"INFO : fine so far\n{line}"))
    verdict, _why = await push_liveness(ssh=dial, log_path=LOG_PATH, age_s=900.0)
    assert verdict == "failed", line


@pytest.mark.anyio
async def test_a_clean_exit_summary_is_finished():
    dial = _LogDial(_probe("4096", _SUMMARY_TAIL))
    verdict, reason = await push_liveness(ssh=dial, log_path=LOG_PATH, age_s=3000.0)
    assert verdict == "finished"
    assert "closing summary" in reason


@pytest.mark.anyio
async def test_silence_is_not_death_a_big_upload_stays_alive():
    """THE rule: an in-flight multi-GB copy says nothing and must not be killed.

    A Telegram-backed remote creates the object only when the upload completes,
    so 40 minutes of an unchanged log is normal progress, not a dead job.
    """
    dial = _LogDial(_probe("16384", "INFO : Starting `rclone copy`"))
    verdict, reason = await push_liveness(ssh=dial, log_path=LOG_PATH, age_s=2400.0, grace_s=300.0)
    assert verdict == "alive"
    assert reason == ""


@pytest.mark.parametrize(
    "reply",
    [
        (None, "", "TimeoutError: Timeout opening channel"),
        (1, "", "sh: syntax error"),
        (0, "some unrelated shell chatter\n", ""),
        (0, f"{_SIZE_TAG}not-a-number\n", ""),
    ],
    ids=["no-answer", "probe-failed", "no-size-line", "garbage-size"],
)
@pytest.mark.anyio
async def test_cannot_tell_is_always_alive(reply):
    """A judgement this probe cannot make must never cost a copy its slot."""
    dial = _LogDial(reply)
    verdict, reason = await push_liveness(ssh=dial, log_path=LOG_PATH, age_s=6000.0)
    assert verdict == "alive"
    assert reason


@pytest.mark.anyio
async def test_a_dial_without_the_dedicated_runner_is_not_borrowed():
    """No pooled fallback: a dial missing the dedicated half says nothing."""

    class _PooledOnly:
        def __init__(self) -> None:
            self.pooled: list[str] = []

        def run_shell(self, command: str, timeout: float = 30.0):
            self.pooled.append(command)
            return (0, f"{_SIZE_TAG}absent\n", "")

    only = _PooledOnly()
    verdict, reason = await push_liveness(ssh=only, log_path=LOG_PATH, age_s=6000.0)
    assert verdict == "alive"
    assert "dedicated" in reason
    assert only.pooled == []


@pytest.mark.anyio
async def test_an_unusable_log_path_is_never_handed_to_a_shell():
    dial = _LogDial(_probe("10"))
    for bad in ("", "/tmp/x; rm -rf /", "-x.log", "/tmp/a\nb.log"):
        verdict, reason = await push_liveness(ssh=dial, log_path=bad, age_s=900.0)
        assert verdict == "alive", bad
    assert dial.commands == []


# --------------------------------------------------------------------------- #
# B: the sweep — a dead copy loses its slot on the poll that proves it
# --------------------------------------------------------------------------- #


@pytest.fixture
def lane(monkeypatch):
    """The push lane's four remote steps as mocks with mutable canned answers.

    Same shape as `tests/test_coordinator_push.py`'s `transport` fixture, plus
    `push_liveness`, so a test decides what the log said and counts the reads.
    """

    def _ok_launch(request, **_kwargs):
        return PushCheck(
            True,
            "",
            list_path=LIST_PATH,
            log_path=LOG_PATH,
            files_total=len(request.files),
        )

    def _ok_preflight(request, **_kwargs):
        return PushCheck(True, "", files_total=len(request.files))

    holder = {
        "preflight": AsyncMock(side_effect=_ok_preflight),
        "start_copy": AsyncMock(side_effect=_ok_launch),
        "remote_progress": AsyncMock(return_value=(0, 1)),
        "push_liveness": AsyncMock(return_value=("alive", "")),
    }
    monkeypatch.setattr(coordinator_push, "preflight", holder["preflight"])
    monkeypatch.setattr(coordinator_push, "start_copy", holder["start_copy"])
    monkeypatch.setattr(coordinator_push, "remote_progress", holder["remote_progress"])
    monkeypatch.setattr(coordinator_push, "push_liveness", holder["push_liveness"])
    return holder


async def _start(store: StateStore, coord, blob: bytes) -> str:
    """Launch one push through the mocked lane; return its infohash.

    The row is parked on WAITING_INDEXER first (what an indexer miss leaves),
    and the checkpoint must carry the log path the sweep is going to read.
    """
    h = _hash_of(blob)
    ts = _row(store, blob, name=EPISODE_TOP, size=2000, kind="episode", state=State.WAITING_INDEXER)
    coord._push_due = MagicMock(return_value=True)
    started, why = await coord.start_source_push(ts)
    assert started is True, why
    assert store.get_push(h).log_path == LOG_PATH
    return h


def _lane_coordinator(store: StateStore, blob: bytes, **kw):
    coord = _push_coordinator(
        store,
        _push_cfg(**kw),
        live=[_vps1_torrent(blob, name=EPISODE_TOP, size=2000)],
        files=[],
    )
    return coord


@pytest.mark.anyio
async def test_missing_log_fails_the_checkpoint_and_frees_the_slot(tmp_path, lane):
    store = StateStore(tmp_path / "state.db")
    try:
        blob = _single_blob(EPISODE_NAME, 2000)
        h = _hash_of(blob)
        coord = _lane_coordinator(store, blob)
        await _start(store, coord, blob)
        assert store.push_is_live(h) is True

        lane["push_liveness"].return_value = (
            "missing",
            f"the rclone log at {LOG_PATH} never appeared",
        )
        await coord.push_sweep()

        push = store.get_push(h)
        assert push.state == PushState.FAILED
        assert "never appeared" in push.last_error
        assert store.push_is_live(h) is False
        # The row is untouched: the normal path picks it up on the next tick.
        assert store.get(h).state == State.WAITING_INDEXER
        assert lane["push_liveness"].await_count == 1
    finally:
        store.close()


@pytest.mark.anyio
async def test_a_freed_slot_does_not_refuse_the_next_request_for_a_full_cap(tmp_path, lane):
    """The exact production refusal: `push refused: 1/1 push(es) already running`.

    With the dead copy failed on the same sweep, the next request must get a
    slot, not that message.
    """
    store = StateStore(tmp_path / "state.db")
    try:
        dead = _single_blob(EPISODE_NAME, 2000)
        coord = _lane_coordinator(store, dead)
        await _start(store, coord, dead)

        lane["push_liveness"].return_value = (
            "failed",
            "rclone reported a fatal line: Failed to copy",
        )
        await coord.push_sweep()
        assert store.push_is_live(_hash_of(dead)) is False
        assert coord._live_push_hashes() == set()

        started, why = await coord.start_source_push(store.get(_hash_of(dead)))
        assert started is True, why
        assert "already running" not in why
    finally:
        store.close()


@pytest.mark.anyio
async def test_a_fatal_line_lands_in_last_error_verbatim_enough_to_debug(tmp_path, lane):
    store = StateStore(tmp_path / "state.db")
    try:
        blob = _single_blob(EPISODE_NAME, 2000)
        h = _hash_of(blob)
        coord = _lane_coordinator(store, blob)
        await _start(store, coord, blob)

        lane["push_liveness"].return_value = (
            "failed",
            "rclone reported a fatal line: Failed to copy with 1 errors: forced shutdown",
        )
        await coord.push_sweep()

        push = store.get_push(h)
        assert push.state == PushState.FAILED
        assert "Failed to copy with 1 errors" in push.last_error
        assert store.push_is_live(h) is False
    finally:
        store.close()


@pytest.mark.anyio
async def test_a_clean_exit_with_nothing_landed_is_freed(tmp_path, lane):
    """`finished` is the weakest verdict, so it needs the same answer twice."""
    store = StateStore(tmp_path / "state.db")
    try:
        blob = _single_blob(EPISODE_NAME, 2000)
        h = _hash_of(blob)
        coord = _lane_coordinator(store, blob)
        await _start(store, coord, blob)
        lane["push_liveness"].return_value = ("finished", "the copy wrote its closing summary")

        coord._push_due = MagicMock(return_value=True)
        await coord.push_sweep()
        assert store.get_push(h).state == PushState.RUNNING  # confirming, not killing
        assert coord._push_liveness_seen[h][1] == 1

        coord._push_due = MagicMock(return_value=True)
        await coord.push_sweep()
        assert store.get_push(h).state == PushState.FAILED
        assert "closing summary" in store.get_push(h).last_error
        assert store.push_is_live(h) is False
        assert lane["push_liveness"].await_count == 2
    finally:
        store.close()


@pytest.mark.anyio
async def test_a_silent_alive_copy_keeps_its_slot_and_its_deadline(tmp_path, lane):
    store = StateStore(tmp_path / "state.db")
    try:
        blob = _single_blob(EPISODE_NAME, 2000)
        h = _hash_of(blob)
        coord = _lane_coordinator(store, blob, deadline=600)
        await _start(store, coord, blob)
        deadline_before = store.get_push(h).deadline_at

        lane["remote_progress"].return_value = (0, 1)
        lane["push_liveness"].return_value = ("alive", "")
        coord._push_due = MagicMock(return_value=True)
        await coord.push_sweep()

        push = store.get_push(h)
        assert push.state == PushState.RUNNING
        assert store.push_is_live(h) is True
        # The deadline is untouched and remains the ONLY thing that ends this
        # wait: 64 polls of `0/1` are legal while the copy is alive.
        assert push.deadline_at == deadline_before
        assert lane["push_liveness"].await_count == 1
    finally:
        store.close()


@pytest.mark.anyio
async def test_progress_advancing_never_asks_the_log(tmp_path, lane):
    """Requirement: once files land, the poll reports life — skip the probe."""
    store = StateStore(tmp_path / "state.db")
    try:
        blob = _single_blob(EPISODE_NAME, 2000)
        h = _hash_of(blob)
        coord = _lane_coordinator(store, blob)
        await _start(store, coord, blob)
        store.note_push_progress(h, files_present=0, files_total=1)

        lane["remote_progress"].return_value = (0, 1)
        coord._push_due = MagicMock(return_value=True)
        await coord.push_sweep()
        assert lane["push_liveness"].await_count == 1  # nothing new -> asked once

        lane["remote_progress"].return_value = (1, 2)  # files landed
        coord._push_due = MagicMock(return_value=True)
        await coord.push_sweep()
        assert lane["push_liveness"].await_count == 1  # NOT consulted again
        assert store.get_push(h).state == PushState.RUNNING
    finally:
        store.close()


@pytest.mark.anyio
async def test_a_dead_summary_with_files_landed_does_not_fail_the_push(tmp_path, lane):
    """The caller decides: `finished` is only death when NOTHING arrived."""
    store = StateStore(tmp_path / "state.db")
    try:
        blob = _single_blob(EPISODE_NAME, 2000)
        h = _hash_of(blob)
        coord = _lane_coordinator(store, blob)
        await _start(store, coord, blob)
        store.note_push_progress(h, files_present=1, files_total=3)

        lane["remote_progress"].return_value = (1, 3)
        lane["push_liveness"].return_value = ("finished", "the copy wrote its closing summary")
        coord._push_due = MagicMock(return_value=True)
        await coord.push_sweep()

        assert store.get_push(h).state == PushState.RUNNING
        assert store.push_is_live(h) is True
    finally:
        store.close()


@pytest.mark.anyio
async def test_only_a_running_checkpoint_is_judged(tmp_path, lane):
    """A `verifying` push is the handover's business; a dead verdict must not
    undo a copy whose files are already on the remote."""
    store = StateStore(tmp_path / "state.db")
    try:
        blob = _single_blob(EPISODE_NAME, 2000)
        h = _hash_of(blob)
        coord = _lane_coordinator(store, blob)
        await _start(store, coord, blob)
        store.finish_push(h, PushState.VERIFYING)

        lane["remote_progress"].return_value = (0, 1)
        lane["push_liveness"].return_value = ("missing", "no log")
        coord._push_due = MagicMock(return_value=True)
        await coord.push_sweep()

        assert lane["push_liveness"].await_count == 0
        assert store.get_push(h).state == PushState.VERIFYING
    finally:
        store.close()


@pytest.mark.anyio
async def test_a_checkpoint_without_a_log_path_is_never_probed(tmp_path, lane):
    store = StateStore(tmp_path / "state.db")
    try:
        blob = _single_blob(EPISODE_NAME, 2000)
        h = _hash_of(blob)
        # A checkpoint from before the transport stamped its log.
        _live_push(store, h, expires_in=dt.timedelta(minutes=30), files_total=1)
        store.upsert(
            TorrentState(source_infohash=h, source_name=EPISODE_TOP, state=State.WAITING_INDEXER)
        )
        coord = _push_coordinator(store, _push_cfg(), live=[], files=[])
        coord._push_due = MagicMock(return_value=True)
        coord._push_wanted = {h: (PushFile(f"{EPISODE_TOP}/{EPISODE_NAME}", 2000),)}

        await coord.push_sweep()

        assert lane["push_liveness"].await_count == 0
        assert store.get_push(h).state == PushState.RUNNING
    finally:
        store.close()


@pytest.mark.anyio
async def test_a_liveness_read_that_throws_keeps_the_slot(tmp_path, lane):
    store = StateStore(tmp_path / "state.db")
    try:
        blob = _single_blob(EPISODE_NAME, 2000)
        h = _hash_of(blob)
        coord = _lane_coordinator(store, blob)
        await _start(store, coord, blob)
        lane["push_liveness"].side_effect = RuntimeError("the dial exploded")

        coord._push_due = MagicMock(return_value=True)
        await coord.push_sweep()

        assert store.get_push(h).state == PushState.RUNNING
        assert store.push_is_live(h) is True
    finally:
        store.close()


# --------------------------------------------------------------------------- #
# C: the whole lane on a dedicated dial — the pool is provably untouched
# --------------------------------------------------------------------------- #


def _sftp_cfg() -> DelugeSFTPConfig:
    return DelugeSFTPConfig(
        enabled=True,
        ssh_host="localhost",
        ssh_user="x",
        ssh_password="pwd",
        state_dir=Path("/home/x/.config/deluge/state"),
    )


def _live_member(cfg: DelugeSFTPConfig) -> _SFTPConnection:
    member = object.__new__(_SFTPConnection)
    member._cfg = cfg
    import threading

    member._lock = threading.RLock()
    member._closed = False
    client = MagicMock()
    client.get_transport().is_active.return_value = True
    member._client = client
    member._sftp = MagicMock()
    return member


class _VPS1:
    """A fake paramiko transport that answers the push lane's own commands."""

    def __init__(self, *, log_tail: str = "INFO : starting", size: int = 2048):
        self.commands: list[str] = []
        self.writes: list[tuple[str, bytes]] = []
        self._log_tail = log_tail
        self._size = size

    def client(self) -> MagicMock:
        client = MagicMock()
        client.get_transport().is_active.return_value = True
        channel = client.get_transport().open_session.return_value
        channel.get_id.return_value = 3
        channel.is_closed.return_value = False
        # The detached launch travels on a session channel, not on
        # `exec_command`: record it in the same list so a test sees the order.
        channel.exec_command.side_effect = lambda command, *_a, **_k: self.commands.append(command)
        client.exec_command.side_effect = self._exec
        sftp = MagicMock()
        sftp.get_channel.return_value = MagicMock()
        sftp.open.side_effect = self._open
        client.open_sftp.return_value = sftp
        return client

    def _open(self, path: str, *_args, **_kwargs) -> MagicMock:
        """An SFTP handle that records which bytes went to which path."""
        handle = MagicMock()

        def _write(data: bytes) -> int:
            self.writes.append((path, bytes(data)))
            return len(data)

        handle.write.side_effect = _write
        opened = MagicMock()
        opened.__enter__.return_value = handle
        return opened

    def _exec(self, command: str, *_args, **_kwargs):
        self.commands.append(command)
        stdin, out, err = MagicMock(), MagicMock(), MagicMock()
        out.channel.recv_exit_status.return_value = 0
        err.read.return_value = b""
        if "command -v" in command:
            out.read.return_value = b"/usr/bin/rclone\n"
        elif "listremotes" in command:
            out.read.return_value = b"seedremote:\n"
        elif _SIZE_TAG in command:
            out.read.return_value = f"{_SIZE_TAG}{self._size}\n{self._log_tail}\n".encode()
        else:
            out.read.return_value = b""
        return stdin, out, err


def _dedicated_exporter(monkeypatch, vps1: _VPS1) -> tuple[SFTPExporter, _SFTPConnection, list]:
    """A real exporter whose pool is instrumented, dialling into `vps1`."""
    cfg = _sftp_cfg()
    exporter = SFTPExporter(cfg, pool_size=3)
    member = _live_member(cfg)
    exporter._members = [member, _live_member(cfg), _live_member(cfg)]
    leases: list[str] = []

    def _counting_lease(_self, what: str):
        leases.append(what)
        raise AssertionError(f"the push lane must never lease a pool member: {what}")

    monkeypatch.setattr(SFTPExporter, "_lease", _counting_lease)
    monkeypatch.setattr("racing_sync.sftp_source._ipv4_socket", lambda *a, **k: MagicMock())
    monkeypatch.setattr("racing_sync.sftp_source._dial_client", lambda _cfg: vps1.client())
    return exporter, member, leases


def _push_transport_cfg() -> AppConfig:
    cfg = _push_cfg()
    cfg.source.deluge_sftp = _sftp_cfg()
    return cfg


@pytest.mark.anyio
async def test_a_full_start_and_poll_cycle_never_leases_a_pool_member(tmp_path, monkeypatch):
    """Preflight, the list write, the launch AND the log read: one dial each.

    The pooled members are instrumented to raise on a lease, so this passing at
    all is the proof; their own locks stay free and their transports stay
    untouched.
    """
    vps1 = _VPS1()
    exporter, member, leases = _dedicated_exporter(monkeypatch, vps1)
    monkeypatch.setattr("racing_sync.vps1_push._remote_has_bytes", AsyncMock(return_value=False))

    request = PushRequest(
        infohash="ab12cd34ef56ab78cd90ef12ab34cd56ef78ab90",
        group_key="example-key",
        content_name="Example Show S01E01",
        src_dir="/data/racing/Example.Show.S01E01.1080p.WEB-DL-GROUP",
        target_remote="seedremote:Unsorted",
        files=(PushFile(f"{EPISODE_TOP}/{EPISODE_NAME}", 2000),),
    )

    check = await preflight(
        request,
        ssh=exporter,
        cfg=MagicMock(),
        remote_name="seedremote",
        rclone_bin="/usr/bin/rclone",
    )
    assert check.ok, check.reason
    started = await start_copy(
        request, ssh=exporter, rclone_bin="/usr/bin/rclone", now=1762000000.0
    )
    assert started.ok, started.reason
    verdict, reason = await push_liveness(
        ssh=exporter, log_path=started.log_path, age_s=900.0, grace_s=300.0
    )
    assert verdict == "alive", reason

    assert leases == []
    # Three execs before the poll: the two preflight probes and the launch —
    # and the list went over SFTP on its own dial, never through a member.
    assert vps1.commands[0] == "command -v /usr/bin/rclone"
    assert vps1.commands[1] == "/usr/bin/rclone listremotes"
    assert vps1.commands[2].startswith("setsid /usr/bin/rclone copy ")
    assert vps1.commands[2].endswith("</dev/null >/dev/null 2>&1 &")
    assert "move" not in vps1.commands[2].split()
    assert vps1.writes == [(LIST_PATH, f"{EPISODE_TOP}/{EPISODE_NAME}\n".encode())]
    assert any(_SIZE_TAG in c for c in vps1.commands)  # the liveness read
    # Not one byte read through a pooled member, and every member is idle.
    member._client.exec_command.assert_not_called()
    member._sftp.open.assert_not_called()
    for pooled in exporter._members:
        assert pooled._lock.acquire(blocking=False) is True
        pooled._lock.release()


@pytest.mark.anyio
async def test_the_sweep_rides_the_dedicated_dial_and_frees_the_slot(tmp_path, monkeypatch, lane):
    """End to end with the REAL probe: a fatal line on VPS1's log frees the slot.

    `push_liveness` is put back (the `lane` fixture mocks it) so the sweep's
    read is a real dedicated call, and the answer is rclone's own failure.
    """
    store = StateStore(tmp_path / "state.db")
    try:
        blob = _single_blob(EPISODE_NAME, 2000)
        h = _hash_of(blob)
        coord = _lane_coordinator(store, blob)

        class _FatalDial(_Dial):
            def __init__(self) -> None:
                super().__init__()
                self.probes: list[str] = []

            def run_shell_dedicated(self, command: str, timeout: float = 30.0):
                if _SIZE_TAG in command:
                    self.probes.append(command)
                    return (0, f"{_SIZE_TAG}4096\n{_FATAL_TAIL}\n", "")
                return super().run_shell_dedicated(command, timeout)

        coord.sftp = _FatalDial()
        await _start(store, coord, blob)

        with patch.object(coordinator_push, "push_liveness", vps1_push.push_liveness):
            coord._push_due = MagicMock(return_value=True)
            await coord.push_sweep()

        assert len(coord.sftp.probes) == 1
        assert LOG_PATH in coord.sftp.probes[0] and "*" not in coord.sftp.probes[0]
        push = store.get_push(h)
        assert push.state == PushState.FAILED
        assert "Failed to copy with 1 errors" in push.last_error
        assert store.push_is_live(h) is False
        # The pooled half of the dial was never used, in either direction.
        assert coord.sftp.pooled_calls == []
    finally:
        store.close()


@pytest.mark.anyio
async def test_a_copy_younger_than_the_grace_is_not_judged_for_an_absent_log(
    tmp_path, monkeypatch, lane
):
    """The grace is real: a push started seconds ago keeps its slot.

    rclone opens `--log` as it starts, so an absent log at age 0 says nothing,
    and only the poll cadence (never a new key) widens the window.
    """
    store = StateStore(tmp_path / "state.db")
    try:
        blob = _single_blob(EPISODE_NAME, 2000)
        h = _hash_of(blob)
        coord = _lane_coordinator(store, blob)

        class _AbsentDial(_Dial):
            def __init__(self) -> None:
                super().__init__()
                self.probes: list[str] = []

            def run_shell_dedicated(self, command: str, timeout: float = 30.0):
                if _SIZE_TAG in command:
                    self.probes.append(command)
                    return (0, f"{_SIZE_TAG}absent\n", "")
                return super().run_shell_dedicated(command, timeout)

        coord.sftp = _AbsentDial()
        await _start(store, coord, blob)

        with patch.object(coordinator_push, "push_liveness", vps1_push.push_liveness):
            coord._push_due = MagicMock(return_value=True)
            await coord.push_sweep()

        assert len(coord.sftp.probes) == 1
        assert store.get_push(h).state == PushState.RUNNING
        assert store.push_is_live(h) is True
    finally:
        store.close()


@pytest.mark.anyio
async def test_a_dial_that_raises_refuses_with_a_reason_and_leaves_nothing_live(
    tmp_path, monkeypatch
):
    """A dedicated dial that dies is a refusal, never an exception, never a
    checkpoint that suppresses work for a copy that does not exist."""
    store = StateStore(tmp_path / "state.db")
    try:
        cfg = _push_transport_cfg()
        blob = _single_blob(EPISODE_NAME, 2000)
        coord = _push_coordinator(
            store, cfg, live=[_vps1_torrent(blob, name=EPISODE_TOP, size=2000)], files=[]
        )
        exporter = SFTPExporter(_sftp_cfg(), pool_size=1)
        exporter._members = [_live_member(_sftp_cfg())]
        coord.sftp = exporter
        monkeypatch.setattr(
            "racing_sync.vps1_push._remote_has_bytes", AsyncMock(return_value=False)
        )
        monkeypatch.setattr(
            "racing_sync.sftp_source._dial_client",
            MagicMock(side_effect=OSError("Timeout opening channel")),
        )

        ts = _row(
            store, blob, name=EPISODE_TOP, size=2000, kind="episode", state=State.WAITING_INDEXER
        )
        started, why = await coord.start_source_push(ts)

        assert started is False
        assert why.startswith("push refused")
        assert "Timeout opening channel" in why
        push = store.get_push(_hash_of(blob))
        assert push is not None and push.state == PushState.ABANDONED
        assert store.push_is_live(_hash_of(blob)) is False
        # The pool was never touched, so a racing fetch could not have been
        # starved by this attempt either.
        assert exporter._members[0]._client.exec_command.call_count == 0
    finally:
        store.close()


def test_dedicated_runner_reports_a_failed_dial_instead_of_raising(monkeypatch):
    """`run_shell_dedicated` degrades to (None, "", reason) like its pooled twin."""
    cfg = _sftp_cfg()
    with patch(
        "racing_sync.sftp_source._dial_client", side_effect=OSError("Timeout opening channel")
    ):
        status, out, err = SFTPExporter(cfg).run_shell_dedicated("rclone listremotes", 5.0)
        written = SFTPExporter(cfg).write_remote_file_dedicated(
            "/tmp/racing-sync-push-x.lst", b"a\n"
        )

    assert status is None and out == ""
    assert "Timeout opening channel" in err
    assert written is False


def test_dedicated_write_shares_the_pooled_safety_gate():
    """Same refusals as the pooled member: content-sized and unsafe payloads are out.

    Nothing here even dials — the gate is checked before the transport exists.
    """
    exporter = SFTPExporter(_sftp_cfg())
    with patch("racing_sync.sftp_source._dial_client", MagicMock()) as dial:
        for path, payload in (
            ("relative/x.lst", b"a\n"),
            ("-flag.lst", b"a\n"),
            ("/tmp/x; rm -rf /", b"a\n"),
            ("/tmp/x.lst", b""),
            ("/tmp/x.lst", b"x" * (5 * 1024 * 1024)),
        ):
            assert exporter.write_remote_file_dedicated(path, payload) is False
        dial.assert_not_called()


def test_dedicated_run_shell_refuses_an_empty_command_without_dialling():
    cfg = _sftp_cfg()
    with patch("racing_sync.sftp_source._dial_client", MagicMock()) as dial:
        assert SFTPExporter(cfg).run_shell_dedicated("   ") == (None, "", "empty command")
    dial.assert_not_called()


# --------------------------------------------------------------------------- #
# D: the shipped defaults (Fix 3) — conservative on purpose
# --------------------------------------------------------------------------- #


def test_shipped_push_defaults():
    push = RclonePushConfig()
    assert push.transfers == 2
    assert push.max_concurrent_pushes == 1
    assert push.bwlimit == ""
    # Deliberately unchanged: the cadence and the backstop ceiling.
    assert push.poll_seconds == 120
    assert push.timeout_seconds == 21600


def test_the_example_config_ships_the_same_numbers():
    rt = Runtime(AppConfig.from_toml(ROOT / "config.example.toml")).push
    assert rt.transfers == 2
    assert rt.max_concurrent_pushes == 1
    assert rt.bwlimit == ""
    assert rt.poll_seconds == 120
    assert rt.timeout_seconds == 21600


def test_an_absent_section_resolves_to_the_same_defaults():
    """`[source.rclone_push]` is optional, so the runtime view carries them too."""
    cfg = AppConfig.from_toml(ROOT / "tests" / "fixtures" / "config" / "minimal.toml")
    assert cfg.source.rclone_push is None
    rt = Runtime(cfg).push
    assert rt.transfers == 2
    assert rt.max_concurrent_pushes == 1


def test_the_conservative_default_reaches_the_command_line():
    cmd = build_copy_command(
        PushRequest(
            infohash="ab12cd34ef56ab78cd90ef12ab34cd56ef78ab90",
            group_key="k",
            content_name="Example Show S01E01",
            src_dir="/data/racing",
            target_remote="seedremote:Unsorted",
            files=(PushFile(EPISODE_NAME, 2000),),
        ),
        rclone_bin="rclone",
        transfers=Runtime(AppConfig.from_toml(ROOT / "config.example.toml")).push.transfers,
        bwlimit="",
        list_path=LIST_PATH,
        log_path=LOG_PATH,
    )
    tokens = cmd.split()
    assert tokens[tokens.index("--transfers") + 1] == "2"
    assert "--bwlimit" not in cmd  # "" stays unset, as documented


def test_the_uplink_trap_is_documented_in_the_example():
    text = (ROOT / "config.example.toml").read_text(encoding="utf-8")
    block = text[text.index("[client.rclone_push]") :]
    assert 'bwlimit = "30M"' in block
    assert "unthrottled copy" in block.lower() or "competes head-on" in block.lower()
    # The liveness note: a stalled copy no longer needs a giant ceiling.
    assert "frees the slot" in block or "frees the slot" in text
    assert "3600" in block


@pytest.mark.anyio
async def test_the_copy_is_still_a_copy_and_still_gated_after_the_defaults_change():
    """The guarantees this task must not weaken, re-pinned on the new defaults."""
    dial = _LaneDial()
    refused = await preflight(_request((PushFile("../escape.mkv", 2000),)), ssh=dial, cfg=None)
    assert not refused.ok and "no usable file names" in refused.reason
    assert dial.commands == []  # refused before a single remote call

    started = await start_copy(
        _request((PushFile(EPISODE_NAME, 2000),)), ssh=dial, rclone_bin="/usr/bin/rclone"
    )
    assert started.ok, started.reason
    assert dial.written == [(started.list_path, f"{EPISODE_NAME}\n".encode())]
    assert len(dial.launched) == 1
    command = dial.launched[0]
    tokens = command.split()
    # copy only, sanitised list file, and a no-transfer run that fails loudly.
    assert "move" not in tokens
    assert "copy" in tokens
    assert "--files-from-raw" in command
    assert "--error-on-no-transfer" in command
    assert "--ignore-existing" in command
    assert command.startswith("setsid ") and command.endswith("</dev/null >/dev/null 2>&1 &")


@pytest.mark.anyio
async def test_the_lane_stays_inert_when_the_switch_is_off(tmp_path):
    """No verdict, no dial, no shell: the whole lane still starts with config."""
    store = StateStore(tmp_path / "state.db")
    try:
        blob = _single_blob(MOVIE_NAME, 2000)
        h = _hash_of(blob)
        _live_push(store, h, expires_in=dt.timedelta(minutes=30))
        coord = make_coordinator(store)
        coord.cfg = _push_cfg(enabled=False)
        coord._push_dial = MagicMock(side_effect=AssertionError("no dial while off"))

        await coord.push_sweep()

        coord._push_dial.assert_not_called()
        assert store.get_push(h).state == PushState.RUNNING
    finally:
        store.close()
