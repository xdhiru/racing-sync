"""Tests for the VPS1 push transport (vps1_push.py + the SSH launch helpers).

Every name here is fictional ("Example.Show.S01E01.Example-Group…"): no real
release, show, group or tracker may appear in this repo.
"""

from __future__ import annotations

import shlex
import threading
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from racing_sync.config import DelugeSFTPConfig
from racing_sync.rclone_ops import RcloneResult
from racing_sync.sftp_source import SFTPExporter, _SFTPConnection
from racing_sync.vps1_push import (
    PushCheck,
    PushFile,
    PushRequest,
    _guard_copy_only,
    build_copy_command,
    derive_remote_name,
    preflight,
    remote_progress,
    sanitize_push_files,
    start_copy,
)

HASH = "ab12cd34ef56ab78cd90ef12ab34cd56ef78ab90"
SRC_DIR = "/data/downloads/Example.Show.S01E01.Example-Group"
TARGET = "seedremote:TV/Example Show (2020)"
LIST_PATH = "/tmp/racing-sync-push-ab12cd34ef-1762000000.lst"


def _files() -> tuple[PushFile, ...]:
    return (
        PushFile("Example.Show.S01E01.Example-Group.mkv", 4_000_000_000),
        PushFile("subs/Example.Show.S01E01.Example-Group.eng.srt", 42_000),
    )


def _request(**overrides) -> PushRequest:
    args: dict = {
        "infohash": HASH,
        "group_key": "example-group-key",
        "content_name": "Example Show S01E01",
        "src_dir": SRC_DIR,
        "target_remote": TARGET,
        "files": _files(),
    }
    args.update(overrides)
    return PushRequest(**args)


class FakeDial:
    """Records every command string; replies from canned substrings."""

    def __init__(self, replies: dict[str, tuple[int | None, str, str]] | None = None, **flags):
        self.commands: list[str] = []
        self.written: list[tuple[str, bytes]] = []
        self.launched: list[str] = []
        self._replies = replies or {}
        self.write_ok = flags.get("write_ok", True)
        self.launch_ok = flags.get("launch_ok", True)
        self.raise_on_shell = flags.get("raise_on_shell", False)

    def run_shell(self, command: str, timeout: float = 30.0):
        self.commands.append(command)
        if self.raise_on_shell:
            raise RuntimeError("ssh exploded")
        for needle, reply in self._replies.items():
            if needle in command:
                return reply
        return (0, "", "")

    def write_remote_file(self, path: str, data: bytes) -> bool:
        self.written.append((path, data))
        return self.write_ok

    def launch_detached(self, command: str) -> bool:
        self.launched.append(command)
        return self.launch_ok


def _ok_dial(**flags) -> FakeDial:
    """A VPS1 that has rclone and knows the remote (colon-spelled listing)."""
    replies = {
        "command -v": (0, "/usr/bin/rclone\n", ""),
        "listremotes": (0, "otherremote:\nseedremote:\n", ""),
    }
    return FakeDial(replies, **flags)


def _sftp_cfg(**overrides) -> DelugeSFTPConfig:
    args: dict = {
        "enabled": True,
        "ssh_host": "localhost",
        "ssh_user": "x",
        "ssh_password": "pwd",
        "state_dir": Path("/tmp"),
    }
    args.update(overrides)
    return DelugeSFTPConfig(**args)


def _live_member(cfg: DelugeSFTPConfig) -> _SFTPConnection:
    """A pool member with a working (mocked) transport, like tests/test_sftp.py."""
    m = object.__new__(_SFTPConnection)
    m._cfg = cfg
    m._lock = threading.RLock()
    m._closed = False
    client = MagicMock()
    client.get_transport().is_active.return_value = True
    m._client = client
    m._sftp = MagicMock()
    return m


def _fake_exec(stdout: bytes = b"", status: int = 0) -> tuple:
    stdin, out, err = MagicMock(), MagicMock(), MagicMock()
    out.read.return_value = stdout
    out.channel.recv_exit_status.return_value = status
    err.read.return_value = b""
    return stdin, out, err


# --------------------------------------------------------------------------- #
# the command line
# --------------------------------------------------------------------------- #


def test_build_copy_command_is_a_copy_and_never_a_move():
    cmd = build_copy_command(
        _request(),
        rclone_bin="/usr/bin/rclone",
        transfers=4,
        bwlimit="42M",
        list_path=LIST_PATH,
        log_path="/tmp/push.log",
    )
    tokens = shlex.split(cmd)
    assert "copy" in tokens
    assert "move" not in tokens
    assert tokens[1] == "/usr/bin/rclone"
    assert tokens[2] == "copy"
    # copy is the ONLY rclone verb that leaves VPS1 seeding: assert it twice.
    assert " --move" not in cmd
    assert "--files-from-raw" in tokens
    assert LIST_PATH in tokens
    assert "--ignore-existing" in tokens
    assert tokens[tokens.index("--transfers") + 1] == "4"
    assert tokens[tokens.index("--bwlimit") + 1] == "42M"
    assert tokens[tokens.index("--log") + 1] == "/tmp/push.log"
    assert tokens[tokens.index("--loglevel") + 1] == "INFO"
    assert "--error-on-no-transfer" in tokens
    # positionals come last, after the end-of-flags marker.
    assert tokens.index("--") < tokens.index(SRC_DIR)
    assert tokens[-7:] == ["--", SRC_DIR, TARGET, "</dev/null", ">/dev/null", "2>&1", "&"]


def test_build_copy_command_omits_bwlimit_when_unset():
    cmd = build_copy_command(
        _request(),
        rclone_bin="rclone",
        transfers=1,
        bwlimit="",
        list_path=LIST_PATH,
        log_path="/tmp/push.log",
    )
    assert "--bwlimit" not in cmd


def test_build_copy_command_detaches_the_job():
    cmd = build_copy_command(
        _request(),
        rclone_bin="rclone",
        transfers=4,
        bwlimit="",
        list_path=LIST_PATH,
        log_path="/tmp/push.log",
    )
    assert cmd.startswith("setsid ")
    assert cmd.endswith("</dev/null >/dev/null 2>&1 &")


def test_build_copy_command_quotes_spaces_single_quotes_and_a_semicolon():
    nasty = '/data/raced/it\'s a "test"; rm -rf /var/tmp/nope'
    cmd = build_copy_command(
        _request(src_dir=nasty, target_remote="seedremote:TV/Example Show (2020)"),
        rclone_bin="rclone",
        transfers=4,
        bwlimit="",
        list_path=LIST_PATH,
        log_path="/tmp/push.log",
    )
    # Never spliced: the raw text must not appear, and a shell round-trip has
    # to hand back the exact path as ONE positional token.
    assert nasty not in cmd
    tokens = shlex.split(cmd)
    assert ";" not in tokens
    assert tokens[-6] == nasty
    assert tokens[-5] == "seedremote:TV/Example Show (2020)"
    # `;` outside quotes would end the command; here it is data inside the
    # quoted positional, and the backgrounding `&` appears exactly once.
    assert tokens.count("&") == 1
    assert cmd.index(";") > cmd.index("-- ")


def test_guard_copy_only_rejects_a_move_and_accepts_a_movie_path():
    with pytest.raises(ValueError, match="move"):
        _guard_copy_only(["rclone", "move", "--transfers", "4"])
    # "movies" as a path fragment is not the verb.
    _guard_copy_only(["rclone", "copy", "/data/movies"])
    with pytest.raises(ValueError, match="not a copy"):
        _guard_copy_only(["rclone", "sync", "/data/movies"])


def test_build_copy_command_refuses_a_flag_looking_source():
    with pytest.raises(ValueError, match="starts with"):
        build_copy_command(
            _request(src_dir="--transfers-999"),
            rclone_bin="rclone",
            transfers=4,
            bwlimit="",
            list_path=LIST_PATH,
            log_path="/tmp/push.log",
        )


# --------------------------------------------------------------------------- #
# list validation (reuses rclone_ops)
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "bad",
    [
        "../outside",
        "Show/../../outside",
        "/etc/passwd",
        "C:/Windows/win.mkv",
        "--ignore-existing",
        "Example.Show.S01E01.mkv\n/etc/passwd",
        "Example.Show.S01E01\r.mkv",
        "\\var\\tmp\\nope.mkv",
        "",
    ],
)
def test_sanitize_push_files_rejects_dangerous_names(bad):
    accepted, reason = sanitize_push_files([PushFile("Example.Show.S01E01.mkv", 10), bad])
    assert reason, f"{bad!r} should have been refused"
    if bad:  # a control-char/traversal name must not survive as accepted
        assert all(f.name != bad for f in accepted)


def test_sanitize_push_files_rejects_a_lone_bad_entry():
    accepted, reason = sanitize_push_files(["../escape.mkv"])
    assert accepted == []
    assert "no usable file names" in reason


def test_sanitize_push_files_keeps_nested_names_and_sizes():
    accepted, reason = sanitize_push_files(
        [
            PushFile("Example Show/Season 01/Example.Show.S01E01.mkv", 42),
            "Example.Show.S01E02.mkv",
        ]
    )
    assert reason == ""
    assert [f.name for f in accepted] == [
        "Example Show/Season 01/Example.Show.S01E01.mkv",
        "Example.Show.S01E02.mkv",
    ]
    assert accepted[0].size_bytes == 42
    assert accepted[1].size_bytes == 0


def test_sanitize_push_files_dedupes_silently():
    accepted, reason = sanitize_push_files(
        [
            PushFile("Example.Show.S01E01.mkv", 1),
            PushFile("Example.Show.S01E01.mkv", 2),
        ]
    )
    assert reason == ""
    assert len(accepted) == 1
    assert accepted[0].size_bytes == 1


# --------------------------------------------------------------------------- #
# preflight
# --------------------------------------------------------------------------- #


@pytest.mark.anyio
async def test_preflight_refuses_an_empty_request_before_any_ssh_call():
    dial = _ok_dial()
    check = await preflight(_request(files=()), ssh=dial, cfg=MagicMock())
    assert not check.ok
    assert "no files" in check.reason
    assert dial.commands == []


@pytest.mark.anyio
async def test_preflight_refuses_rejected_names_before_any_ssh_call():
    dial = _ok_dial()
    check = await preflight(_request(files=(PushFile("../escape.mkv", 1),)), ssh=dial, cfg=None)
    assert not check.ok
    assert "rejected" in check.reason or "no usable" in check.reason
    assert dial.commands == []


@pytest.mark.anyio
async def test_preflight_refuses_shell_metacharacters_before_any_ssh_call():
    dial = _ok_dial()
    check = await preflight(_request(src_dir="/data/x; rm -rf /"), ssh=dial, cfg=None)
    assert not check.ok
    assert "metacharacter" in check.reason
    assert dial.commands == []

    check = await preflight(_request(target_remote="seedremote:$(evil)"), ssh=dial, cfg=None)
    assert not check.ok
    assert "target_remote" in check.reason


@pytest.mark.anyio
async def test_preflight_refuses_an_empty_or_flag_like_remote():
    dial = _ok_dial()
    check = await preflight(_request(target_remote="no-colon-here"), ssh=dial, cfg=None)
    assert not check.ok
    assert "derive" in check.reason
    assert dial.commands == []


@pytest.mark.anyio
async def test_preflight_needs_an_ssh_dial():
    check = await preflight(_request(), ssh=None, cfg=None)
    assert not check.ok
    assert "no SSH dial" in check.reason


@pytest.mark.anyio
async def test_preflight_refuses_when_vps1_has_no_rclone():
    dial = FakeDial({"command -v": (1, "", "not found")})
    check = await preflight(_request(), ssh=dial, cfg=None, rclone_bin="/usr/bin/rclone")
    assert not check.ok
    assert "not usable on VPS1" in check.reason
    assert "not found" in check.reason
    # failed fast: never asked for the remote list
    assert not any("listremotes" in c for c in dial.commands)


@pytest.mark.anyio
async def test_preflight_refuses_when_listremotes_lacks_the_remote():
    dial = FakeDial({"listremotes": (0, "otherremote:\nthird:\n", "")})
    check = await preflight(_request(), ssh=dial, cfg=None)
    assert not check.ok
    assert "has no remote seedremote:" in check.reason
    assert "otherremote:" in check.reason


@pytest.mark.anyio
async def test_preflight_accepts_a_bare_or_colon_remote_spelling():
    for listing in ("seedremote\n", "seedremote:\n"):
        dial = FakeDial({"listremotes": (0, listing, "")})
        check = await preflight(_request(), ssh=dial, cfg=None)
        assert check.ok, f"listing {listing!r}: {check.reason}"
        assert check.files_total == 2


@pytest.mark.anyio
async def test_preflight_uses_the_explicit_remote_name_and_derives_the_rest():
    assert derive_remote_name(TARGET, "named-remote") == "named-remote"
    assert derive_remote_name(TARGET) == "seedremote"
    assert derive_remote_name("seedremote:") == "seedremote"
    assert derive_remote_name("plain/path") == ""


@pytest.mark.anyio
async def test_preflight_refuses_when_the_remote_already_has_every_file(monkeypatch):
    async def _has(cfg, dest, files_from):
        assert dest == TARGET
        assert "Example.Show.S01E01.Example-Group.mkv" in files_from
        return True

    monkeypatch.setattr("racing_sync.vps1_push._remote_has_bytes", _has)
    check = await preflight(_request(), ssh=_ok_dial(), cfg=MagicMock())
    assert not check.ok
    assert "already on the remote" in check.reason


@pytest.mark.anyio
async def test_preflight_allows_a_partial_remote(monkeypatch):
    monkeypatch.setattr("racing_sync.vps1_push._remote_has_bytes", AsyncMock(return_value=False))
    check = await preflight(_request(), ssh=_ok_dial(), cfg=MagicMock())
    assert check.ok
    assert check.reason == ""


@pytest.mark.anyio
async def test_preflight_skips_the_remote_check_without_a_config(monkeypatch):
    probe = AsyncMock(return_value=True)
    monkeypatch.setattr("racing_sync.vps1_push._remote_has_bytes", probe)
    check = await preflight(_request(), ssh=_ok_dial(), cfg=None)
    assert check.ok
    probe.assert_not_awaited()


@pytest.mark.anyio
async def test_preflight_never_raises_on_an_unreachable_vps1():
    dial = FakeDial(raise_on_shell=True)
    check = await preflight(_request(), ssh=dial, cfg=None)
    assert not check.ok
    assert "ssh exploded" in check.reason

    dial2 = FakeDial({"command -v": (None, "", "TimeoutError: sftp busy")})
    check2 = await preflight(_request(), ssh=dial2, cfg=None)
    assert not check2.ok
    assert "sftp busy" in check2.reason


@pytest.mark.anyio
async def test_preflight_survives_a_failing_remote_listing(monkeypatch):
    boom = AsyncMock(side_effect=RuntimeError("rclone missing locally"))
    monkeypatch.setattr("racing_sync.vps1_push._remote_has_bytes", boom)
    check = await preflight(_request(), ssh=_ok_dial(), cfg=MagicMock())
    assert check.ok  # unknown remote state is not a refusal


# --------------------------------------------------------------------------- #
# start_copy
# --------------------------------------------------------------------------- #


@pytest.mark.anyio
async def test_start_copy_writes_the_list_and_launches_the_copy():
    dial = _ok_dial()
    check = await start_copy(
        _request(),
        ssh=dial,
        rclone_bin="/usr/bin/rclone",
        transfers=4,
        bwlimit="42M",
        now=1762000000.4,
    )
    assert check.ok, check.reason
    assert check.list_path == LIST_PATH
    assert check.log_path == "/tmp/racing-sync-push-ab12cd34ef-1762000000.log"
    assert check.files_total == 2
    assert [p for p, _ in dial.written] == [LIST_PATH]
    assert (
        dial.written[0][1]
        == b"\n".join(
            (
                b"Example.Show.S01E01.Example-Group.mkv",
                b"subs/Example.Show.S01E01.Example-Group.eng.srt",
            )
        )
        + b"\n"
    )
    # One launch, the copy line, and no content ever crossed the dial.
    assert len(dial.launched) == 1
    assert dial.launched[0].startswith("setsid /usr/bin/rclone copy ")
    assert "move" not in shlex.split(dial.launched[0])


@pytest.mark.anyio
async def test_start_copy_refuses_before_touching_ssh_when_nothing_survives():
    dial = _ok_dial()
    check = await start_copy(_request(files=(PushFile("../escape.mkv", 1),)), ssh=dial)
    assert not check.ok
    assert dial.written == []
    assert dial.launched == []


@pytest.mark.anyio
async def test_start_copy_cleans_the_list_when_the_launch_fails():
    dial = _ok_dial(launch_ok=False)
    check = await start_copy(_request(), ssh=dial, now=1762000000.0)
    assert not check.ok
    assert "did not accept the detached rclone copy" in check.reason
    assert any(c == f"rm -f {LIST_PATH}" for c in dial.commands)
    # The delete is OUR scratch file only — never VPS1's content.
    assert all("rm " not in c or "-f /tmp/racing-sync-push-" in c for c in dial.commands)


@pytest.mark.anyio
async def test_start_copy_reports_a_failed_write_without_launching():
    dial = _ok_dial(write_ok=False)
    check = await start_copy(_request(), ssh=dial)
    assert not check.ok
    assert "file list write" in check.reason
    assert dial.launched == []


# --------------------------------------------------------------------------- #
# launch_detached: dedicated transport, never a pool lease
# --------------------------------------------------------------------------- #


class _LaunchOnlyProxy:
    """Writes via a fake; the launch goes to the REAL exporter."""

    def __init__(self, target: SFTPExporter):
        self._target = target
        self.written: list[tuple[str, bytes]] = []
        self.launched: list[str] = []

    def run_shell(self, command: str, timeout: float = 30.0):
        return (0, "", "")

    def write_remote_file(self, path: str, data: bytes) -> bool:
        self.written.append((path, data))
        return True

    def launch_detached(self, command: str) -> bool:
        self.launched.append(command)
        return self._target.launch_detached(command)


def _fake_channel(client: MagicMock) -> MagicMock:
    """A client whose session channel opens cleanly."""
    client.get_transport().is_active.return_value = True
    channel = client.get_transport().open_session.return_value
    channel.get_id.return_value = 7
    channel.is_closed.return_value = False
    return channel


@pytest.mark.anyio
async def test_push_launch_uses_a_dedicated_transport_and_never_leases(monkeypatch):
    cfg = _sftp_cfg()
    exporter = SFTPExporter(cfg, pool_size=2)
    members = [_live_member(cfg), _live_member(cfg)]
    exporter._members = members
    member_clients = [m._client for m in members]

    leases: list[str] = []

    def _no_lease(_self, what: str):
        leases.append(what)
        raise AssertionError(f"the push must not lease a pool member: {what}")

    monkeypatch.setattr(SFTPExporter, "_lease", _no_lease)
    # The pool is shut down: a detached launch still has to work.
    exporter.close()
    assert exporter._shutdown is True

    dedicated = MagicMock()
    channel = _fake_channel(dedicated)

    proxy = _LaunchOnlyProxy(exporter)
    with (
        patch("racing_sync.sftp_source._ipv4_socket"),
        patch("racing_sync.sftp_source.paramiko.SSHClient", return_value=dedicated) as cls,
    ):
        check = await start_copy(_request(), ssh=proxy, transfers=4, now=1762000000.0)

    assert check.ok, check.reason
    assert leases == []
    assert proxy.launched and channel.exec_command.call_args[0][0] == proxy.launched[0]
    # A brand-new transport, not one of the pooled members.
    assert cls.call_count == 1
    assert dedicated not in member_clients
    # No pool member was revived by the launch either.
    assert all(m._closed for m in exporter._members)


def test_launch_detached_closes_the_channel_without_waiting():
    cfg = _sftp_cfg()
    exporter = SFTPExporter(cfg, pool_size=1)
    exporter._members = [_live_member(cfg)]
    leased: list[str] = []
    exporter._lease = lambda what: (
        leased.append(what) or (_ for _ in ()).throw(AssertionError("must not lease"))
    )

    client = MagicMock()
    channel = _fake_channel(client)

    with (
        patch("racing_sync.sftp_source._ipv4_socket"),
        patch("racing_sync.sftp_source.paramiko.SSHClient", return_value=client),
    ):
        assert exporter.launch_detached("setsid rclone copy x y </dev/null >/dev/null 2>&1 &")

    assert leased == []
    channel.exec_command.assert_called_once_with(
        "setsid rclone copy x y </dev/null >/dev/null 2>&1 &"
    )
    # The whole point: never read the exit status (that would block until the
    # multi-hour copy finished), just drop the session.
    channel.recv_exit_status.assert_not_called()
    channel.close.assert_called_once_with()
    client.close.assert_called_once()


@pytest.mark.parametrize("failure", ["dial", "no-transport", "no-session", "exec-refused"])
def test_launch_detached_returns_false_and_never_raises(failure):
    client = MagicMock()
    if failure == "no-transport":
        client.get_transport.return_value = None
    elif failure == "no-session":
        client.get_transport().open_session.return_value = None
    elif failure == "exec-refused":
        channel = _fake_channel(client)
        channel.exec_command.side_effect = RuntimeError("exec refused")

    dial_fails = failure == "dial"
    with (
        patch("racing_sync.sftp_source._ipv4_socket"),
        patch(
            "racing_sync.sftp_source.paramiko.SSHClient",
            side_effect=RuntimeError("host down") if dial_fails else MagicMock(return_value=client),
        ),
    ):
        assert SFTPExporter(_sftp_cfg()).launch_detached("setsid true &") is False

    if not dial_fails:
        # Whatever failed, the throwaway transport is always dropped.
        client.close.assert_called_once()


def test_launch_detached_refuses_an_empty_command():
    with patch("racing_sync.sftp_source.paramiko.SSHClient") as cls:
        assert SFTPExporter(_sftp_cfg()).launch_detached("   ") is False
        assert SFTPExporter(_sftp_cfg()).launch_detached("") is False
    cls.assert_not_called()


def test_member_launch_detached_also_dials_its_own_transport():
    cfg = _sftp_cfg()
    member = _live_member(cfg)
    client = MagicMock()
    channel = _fake_channel(client)
    with (
        patch("racing_sync.sftp_source._ipv4_socket"),
        patch("racing_sync.sftp_source.paramiko.SSHClient", return_value=client) as cls,
    ):
        assert member.launch_detached("setsid true &") is True
    assert cls.call_count == 1
    channel.exec_command.assert_called_once_with("setsid true &")
    assert member._client is not client  # the pooled transport stayed untouched


# --------------------------------------------------------------------------- #
# run_shell / write_remote_file on the SSH layer
# --------------------------------------------------------------------------- #


@pytest.mark.anyio
async def test_preflight_probes_through_a_leased_pool_member():
    cfg = _sftp_cfg()
    exporter = SFTPExporter(cfg, pool_size=1)
    member = _live_member(cfg)
    exporter._members = [member]
    member._client.exec_command.side_effect = [
        _fake_exec(b"/usr/bin/rclone\n", status=0),
        _fake_exec(b"seedremote:\n", status=0),
    ]
    with patch("racing_sync.vps1_push._remote_has_bytes", AsyncMock(return_value=False)):
        check = await preflight(
            _request(), ssh=exporter, cfg=MagicMock(), rclone_bin="/usr/bin/rclone"
        )
    assert check.ok, check.reason
    asked = [c.args[0] for c in member._client.exec_command.call_args_list]
    assert asked == ["command -v /usr/bin/rclone", "/usr/bin/rclone listremotes"]
    # The lease is gone again: probes never pin a member.
    assert member._lock.acquire(blocking=False) is True
    member._lock.release()


def test_write_remote_file_goes_over_sftp_not_a_shell():
    cfg = _sftp_cfg()
    exporter = SFTPExporter(cfg, pool_size=1)
    member = _live_member(cfg)
    exporter._members = [member]
    assert exporter.write_remote_file("/tmp/racing-sync-push-abcd.lst", b"a.mkv\n") is True
    member._client.exec_command.assert_not_called()
    handle = member._sftp.open.return_value
    handle.__enter__.return_value.write.assert_called_once_with(b"a.mkv\n")


def test_connect_drops_a_transport_dialed_after_the_member_closed(monkeypatch):
    """A dial that lands after close() must not install a leaked transport."""
    fresh = MagicMock()
    member = _live_member(_sftp_cfg())
    member._client = None
    member._sftp = None

    def _late_dial(cfg):
        # Stand in for close() winning the race while this dial was running.
        member._closed = True
        return fresh

    monkeypatch.setattr("racing_sync.sftp_source._dial_client", _late_dial)
    with pytest.raises(Exception, match="closed during dial"):
        member.connect()
    fresh.close.assert_called_once()
    fresh.open_sftp.assert_not_called()
    assert member._client is None


def test_run_shell_returns_status_and_closes_streams():
    m = _live_member(_sftp_cfg())
    stdin, stdout, stderr = _fake_exec(b"seedremote:\n", status=0)
    m._client.exec_command.return_value = (stdin, stdout, stderr)

    status, out, err = m.run_shell("rclone listremotes", 5.0)
    assert (status, out, err) == (0, "seedremote:\n", "")
    m._client.exec_command.assert_called_once_with("rclone listremotes", timeout=5.0)
    for stream in (stdin, stdout, stderr):
        stream.close.assert_called_once_with()
    stdout.channel.close.assert_called_once_with()


def test_run_shell_never_raises_on_a_wedged_transport():
    m = _live_member(_sftp_cfg())
    m._client.exec_command.side_effect = OSError("channel timeout")
    status, _out, err = m.run_shell("rclone listremotes")
    assert status is None
    assert "command failed to run" in err


def test_run_shell_reports_a_busy_member_instead_of_queuing():
    m = _live_member(_sftp_cfg())
    held = threading.Event()
    release = threading.Event()

    def _hold():
        m._lock.acquire()
        held.set()
        release.wait(30)
        m._lock.release()

    holder = threading.Thread(target=_hold, daemon=True)
    holder.start()
    assert held.wait(10)
    try:
        with patch("racing_sync.sftp_source._SFTP_LOCK_TIMEOUT", 0.1):
            status, _out, err = m.run_shell("rclone listremotes")
    finally:
        release.set()
        holder.join(timeout=10)
    assert status is None
    assert "sftp busy" in err


def test_run_shell_reconnects_a_dropped_member_first():
    m = _live_member(_sftp_cfg())
    m._client.get_transport.return_value = None
    dialed = {"n": 0}

    def _connect():
        dialed["n"] += 1
        m._client = MagicMock()
        m._client.exec_command.return_value = _fake_exec(b"ok", status=0)

    m.connect = _connect  # type: ignore[method-assign]
    status, out, _err = m.run_shell("stat -f /data")
    assert (status, out) == (0, "ok")
    assert dialed["n"] == 1


def test_exporter_run_shell_and_write_use_one_lease_each():
    cfg = _sftp_cfg()
    exporter = SFTPExporter(cfg, pool_size=1)
    member = _live_member(cfg)
    exporter._members = [member]
    member._client.exec_command.return_value = _fake_exec(b"seedremote:\n", status=0)

    assert exporter.run_shell("rclone listremotes") == (0, "seedremote:\n", "")
    assert member._lock.acquire(blocking=False) is True
    member._lock.release()

    assert exporter.write_remote_file("/tmp/x.lst", b"a.mkv\n") is True
    member._sftp.open.assert_called_once_with("/tmp/x.lst", "wb")
    member._sftp.chmod.assert_called_once_with("/tmp/x.lst", 0o600)
    assert member._lock.acquire(blocking=False) is True
    member._lock.release()


def test_write_remote_file_refuses_unsafe_paths_and_oversize_payloads():
    m = _live_member(_sftp_cfg())
    assert m.write_remote_file("relative/x.lst", b"a") is False
    assert m.write_remote_file("-flag.lst", b"a") is False
    assert m.write_remote_file("/tmp/x; rm -rf /", b"a") is False
    assert m.write_remote_file("/tmp/x.lst", b"") is False
    assert m.write_remote_file("/tmp/x.lst", b"x" * (5 * 1024 * 1024)) is False
    m._sftp.open.assert_not_called()


def test_write_remote_file_reports_a_failed_write():
    m = _live_member(_sftp_cfg())
    m._sftp.open.side_effect = OSError("permission denied")
    assert m.write_remote_file("/tmp/x.lst", b"a.mkv\n") is False


# --------------------------------------------------------------------------- #
# progress: metadata only
# --------------------------------------------------------------------------- #


@pytest.mark.anyio
async def test_remote_progress_counts_partial_nested_listings(monkeypatch):
    seen: dict = {}

    async def _fake_run(cfg, cmd, **kwargs):
        seen["cmd"] = list(cmd)
        return RcloneResult(
            returncode=0,
            stdout=(
                "./Example Show/Season 01/Example.Show.S01E01.mkv\n"
                "Example Show/Season 01/\n"
                "Example Show/Season 01/Example.Show.S01E02.mkv\n"
            ),
            stderr="",
            duration=0.1,
        )

    monkeypatch.setattr("racing_sync.vps1_push.run_rclone", _fake_run)
    files = [
        PushFile("Example Show/Season 01/Example.Show.S01E01.mkv", 1),
        PushFile("Example Show/Season 01/Example.Show.S01E02.mkv", 1),
        PushFile("Example Show/Season 01/Example.Show.S01E03.mkv", 1),
    ]
    present, total = await remote_progress(MagicMock(), TARGET, files)
    assert (present, total) == (2, 3)
    # Metadata-only listing, run locally against the remote: never the mount.
    assert seen["cmd"][1:] == ["lsf", "-R", "--files-only", TARGET]
    assert "size" not in " ".join(seen["cmd"])


@pytest.mark.anyio
async def test_remote_progress_is_zero_total_zero_for_an_empty_request():
    assert await remote_progress(MagicMock(), TARGET, []) == (0, 0)


@pytest.mark.anyio
async def test_remote_progress_degrades_to_no_progress_on_failure(monkeypatch):
    monkeypatch.setattr(
        "racing_sync.vps1_push.run_rclone",
        AsyncMock(side_effect=RuntimeError("rclone not here")),
    )
    assert await remote_progress(MagicMock(), TARGET, _files()) == (0, 2)

    monkeypatch.setattr(
        "racing_sync.vps1_push.run_rclone",
        AsyncMock(return_value=RcloneResult(returncode=1, stdout="", stderr="denied", duration=0)),
    )
    assert await remote_progress(MagicMock(), TARGET, _files()) == (0, 2)


@pytest.mark.anyio
async def test_remote_progress_refuses_a_flag_looking_remote_without_spawning():
    probe = AsyncMock()
    with patch("racing_sync.vps1_push.run_rclone", probe):
        assert await remote_progress(MagicMock(), "--config=/x", _files()) == (0, 2)
    probe.assert_not_awaited()


def test_push_check_shape():
    check = PushCheck(False, "one line")
    assert (check.ok, check.reason, check.list_path, check.log_path, check.files_total) == (
        False,
        "one line",
        "",
        "",
        0,
    )
    assert PushCheck(True).reason == ""
