"""Launching a push must either PROVE the copy exists or say why it does not.

Fictional names only ("Example.Show.S01E01.1080p.WEB-DL-GROUP", "seedremote"),
as everywhere in this repo — nothing here is read from a log file or from
`debug-config.toml`.

The production failure this lane was blind to: 28 `push launched` lines, 282
polls reading `has 0/1 file(s)`, ZERO completed copies, and a manual
`/push_source` that answered "push started" while VPS1 showed no rclone process
in `ps aux` and no traffic. The wrapper ended `… </dev/null >/dev/null 2>&1 &`,
so every word the LOGIN SHELL wrote (`setsid: command not found`, `rclone:
unknown long flag '--error-on-no-transfer'`) was discarded, and rclone's own
`--log` only appears once rclone actually starts. Three candidate causes were
equally consistent with the evidence and the code could not tell them apart.
Pinned here, in that order:

* the wrapper's stdout/stderr are APPENDED to the job's own log, so a
  shell-level failure is an operator-visible reason instead of silence;
* preflight asks VPS1 which session-surviving wrapper it has (`setsid`, else
  `nohup`, else REFUSE — never launch blind) and what its own rclone version
  is, and adds `--error-on-no-transfer` only for >= 1.58 (unknown version costs
  the push an optional nicety, never a fatal flag; VPS2's version proves
  nothing about VPS1's);
* the same probe reads the LOGGING-FLAG SPELLINGS out of rclone's own help
  (`rclone help logging`, else `rclone help flags`) — `--log-file`, `--log-level`
  (the pre-1.55 `--loglevel` only when it is the sole spelling) — and
  `build_copy_command` will emit nothing outside that whitelist. A bare `--log`
  is what caused all of the above: VPS1's v1.69.2 answered 28 launches with
  `Error: unknown flag: --log` and died in 0.4 s, so every flag is now PROBED
  and an unproven one is OMITTED (the wrapper's redirect still puts the
  post-mortem in the log), never invented;
* a few seconds after the exec was accepted, ONE bounded proof of life decides:
  `pgrep -f` on this job's unique list path, else the job log's own words read
  by the SAME fatal table `push_liveness` uses — a non-empty log is EVIDENCE,
  not proof of life. Alive, dead-with-a-reason, or `alive (unproven)`; a process
  sighting always beats the log, and missing evidence never kills a
  possibly-live copy;
* a dead launch leaves NO live checkpoint (the slot frees on the spot instead
  of squatting `timeout_seconds`) and its reason reaches the operator verbatim;
* two dead launches in a row take that hash out of the automatic paths until
  the window passes, so a permanently broken VPS1 does not spend SSH per poll.

Every call still rides the DEDICATED dial: a `_lease` that raises is wired into
the exporter, so this whole file passing at all is the proof.
"""

from __future__ import annotations

import shlex
import time
from unittest.mock import AsyncMock, MagicMock

import pytest
from conftest import make_coordinator
from test_coordinator_push import (
    EPISODE_NAME,
    EPISODE_TOP,
    _hash_of,
    _push_cfg,
    _push_coordinator,
    _row,
    _single_blob,
    _vps1_torrent,
)
from test_push_liveness import _VPS1, _dedicated_exporter

from racing_sync import coordinator_push, vps1_push
from racing_sync.state import State, StateStore
from racing_sync.state_schema import PushState
from racing_sync.vps1_push import (
    PushCheck,
    PushFile,
    PushRequest,
    _launch_proof_command,
    _pgrep_pattern,
    build_copy_command,
    preflight,
    start_copy,
)

HASH = "ab12cd34ef56ab78cd90ef12ab34cd56ef78ab90"
NOW = 1762000000.0
SRC_DIR = "/data/racing/Example.Show.S01E01.1080p.WEB-DL-GROUP"
TARGET = "seedremote:Unsorted"
LIST_PATH = f"/tmp/racing-sync-push-ab12cd34ef-{int(NOW)}.lst"
LOG_PATH = f"/tmp/racing-sync-push-ab12cd34ef-{int(NOW)}.log"


def _files() -> tuple[PushFile, ...]:
    return (PushFile(f"{EPISODE_TOP}/{EPISODE_NAME}", 4_000_000_000),)


def _request(**overrides) -> PushRequest:
    args: dict = {
        "infohash": HASH,
        "group_key": "example-key",
        "content_name": "Example Show S01E01",
        "src_dir": SRC_DIR,
        "target_remote": TARGET,
        "files": _files(),
    }
    args.update(overrides)
    return PushRequest(**args)


def _cap(
    *,
    setsid: str = "yes",
    nohup: str = "yes",
    version: str | None = "rclone v1.66.0",
    log_file: bool = True,
    log_level: bool = True,
    loglevel_alias: bool = False,
    help_block: bool = True,
) -> tuple[int | None, str, str]:
    """A canned answer to the wrapper/version/logging-flag probe.

    `version=None` is the version question saying nothing; `help_block=False`
    is the help call erroring or answering nothing (which the lane reads as "no
    logging flag exists", never as "invent one"); the three booleans pick which
    SPELLINGS this rclone lists, exactly as a real one does.
    """
    lines = [f"SYNC-PUSH-CAP:setsid={setsid}", f"SYNC-PUSH-CAP:nohup={nohup}"]
    lines.append("SYNC-PUSH-CAP:rclone-version=")
    if version:
        lines.append(version)
    if help_block:
        help_lines = ["Logging:"]
        if log_file:
            help_lines.append("      --log-file string                Log to file")
            help_lines.append(
                "      --log-file-max-size SizeSuffix   Max size of log before rotation"
            )
        if log_level:
            help_lines.append("      --log-level Level                Log level DEBUG|INFO|NOTICE")
        if loglevel_alias:
            help_lines.append(
                "      --loglevel Level                 Deprecated alias for --log-level"
            )
        lines.append("SYNC-PUSH-CAP:log-help-begin")
        lines.extend(help_lines)
        lines.append("SYNC-PUSH-CAP:log-help-end")
    return (0, "\n".join(lines) + "\n", "")


def _proof(
    *, pgrep: str = "found", log: str = "absent", lines: tuple[str, ...] = ()
) -> tuple[int | None, str, str]:
    """A canned answer to the launch proof: its two verdicts and any log words."""
    body = f"SYNC-PUSH-PROOF:pgrep={pgrep}\nSYNC-PUSH-PROOF:log={log}\n"
    body += "SYNC-PUSH-PROOF:log-begin\n"
    for line in lines:
        body += f"{line}\n"
    return (0, body, "")


class _LaunchDial:
    """A dedicated dial for the launch lane, with the two probes under test.

    Answers by substring and records everything asked, so a test can see the
    order the lane works in and prove the pooled half was never used.
    """

    def __init__(
        self,
        *,
        cap: tuple[int | None, str, str] | None = None,
        proof: tuple[int | None, str, str] | None = None,
        launch_ok: bool = True,
    ):
        self.commands: list[str] = []
        self.written: list[tuple[str, bytes]] = []
        self.launched: list[str] = []
        self.pooled_calls: list[str] = []
        self._cap = cap if cap is not None else _CAP_REPLY
        self._proof = proof if proof is not None else _PROOF_REPLY
        self.launch_ok = launch_ok

    def run_shell_dedicated(self, command: str, timeout: float = 30.0):
        self.commands.append(command)
        if "SYNC-PUSH-CAP" in command:
            return self._cap
        if "SYNC-PUSH-PROOF" in command:
            return self._proof
        if "command -v" in command:
            return (0, "/usr/bin/rclone\n", "")
        if "listremotes" in command:
            return (0, "seedremote:\n", "")
        return (0, "", "")

    def write_remote_file_dedicated(self, path: str, data: bytes) -> bool:
        self.written.append((path, data))
        return True

    def launch_detached(self, command: str) -> bool:
        self.launched.append(command)
        return self.launch_ok

    # Present only so a test can prove the lane never reached for them.
    def run_shell(self, command: str, timeout: float = 30.0):
        self.pooled_calls.append(command)
        return (0, "", "")

    def write_remote_file(self, path: str, data: bytes) -> bool:
        self.pooled_calls.append(path)
        return True


_CAP_REPLY = _cap()
_PROOF_REPLY = _proof(pgrep="found")


def _launch(**kwargs) -> str:
    """One built command line, with the probe results the caller decides."""
    args: dict = {
        "rclone_bin": "/usr/bin/rclone",
        "transfers": 2,
        "bwlimit": "",
        "list_path": LIST_PATH,
        "log_path": LOG_PATH,
    }
    args.update(kwargs)
    return build_copy_command(_request(), **args)


# --------------------------------------------------------------------------- #
# 1. the launcher's own words are never discarded again
# --------------------------------------------------------------------------- #


def test_the_wrapper_appends_the_shells_output_to_the_job_log():
    cmd = _launch()
    # The exact shape: stdin dropped, the shell's stdout AND stderr appended to
    # the file the app already reads, the job backgrounded.
    assert cmd.endswith(f"</dev/null >> {LOG_PATH} 2>&1 &")
    assert f" >> {LOG_PATH} 2>&1" in cmd
    # Nothing points at the void any more: `>/dev/null` is gone, while the
    # stdin redirect (`</dev/null`) stays, because that one is about hangups.
    assert ">/dev/null" not in cmd
    assert "</dev/null" in cmd


def test_the_wrapper_is_still_a_copy_with_an_end_of_flags_marker():
    cmd = _launch()
    tokens = cmd.split()
    assert "copy" in tokens
    assert "move" not in tokens
    assert "--" in tokens
    assert tokens.index("--") < tokens.index(SRC_DIR)
    assert tokens[-8:] == [
        "--",
        SRC_DIR,
        TARGET,
        "</dev/null",
        ">>",
        LOG_PATH,
        "2>&1",
        "&",
    ]
    # A shell round-trip hands the positionals back as data, not as commands.
    parsed = shlex.split(cmd)
    assert parsed[-8:-5] == ["--", SRC_DIR, TARGET]
    assert parsed[0] == "setsid"


# --------------------------------------------------------------------------- #
# 2. adapt the wrapper and the flags to what VPS1 actually has
# --------------------------------------------------------------------------- #


@pytest.mark.anyio
async def test_setsid_present_preflight_hands_the_wrapper_on():
    dial = _LaunchDial(cap=_cap(setsid="yes", nohup="no"))
    check = await preflight(_request(), ssh=dial, cfg=None)
    assert check.ok, check.reason
    assert check.detacher == "setsid"
    started = await start_copy(
        _request(),
        ssh=dial,
        rclone_bin="/usr/bin/rclone",
        now=NOW,
        detacher=check.detacher,
        error_on_no_transfer=check.error_on_no_transfer,
        proof_delay_s=0.0,
    )
    assert started.ok, started.reason
    assert dial.launched[0].startswith("setsid /usr/bin/rclone copy ")


@pytest.mark.anyio
async def test_setsid_absent_falls_back_to_nohup_still_session_surviving():
    dial = _LaunchDial(cap=_cap(setsid="no", nohup="yes"))
    check = await preflight(_request(), ssh=dial, cfg=None)
    assert check.ok, check.reason
    assert check.detacher == "nohup"
    started = await start_copy(
        _request(),
        ssh=dial,
        rclone_bin="/usr/bin/rclone",
        now=NOW,
        detacher=check.detacher,
        proof_delay_s=0.0,
    )
    assert started.ok, started.reason
    cmd = dial.launched[0]
    assert cmd.startswith("nohup /usr/bin/rclone copy ")
    # The fallback keeps every other rule: detached, and never swallowing the
    # shell's words.
    assert cmd.endswith(f"</dev/null >> {LOG_PATH} 2>&1 &")
    assert ">/dev/null" not in cmd


@pytest.mark.anyio
async def test_neither_wrapper_is_a_refusal_that_names_both_and_launches_nothing():
    dial = _LaunchDial(cap=_cap(setsid="no", nohup="no"))
    check = await preflight(_request(), ssh=dial, cfg=None)
    assert not check.ok
    assert "setsid" in check.reason and "nohup" in check.reason
    assert "blind" in check.reason
    assert dial.launched == []
    assert dial.written == []


@pytest.mark.anyio
async def test_a_wrapper_probe_that_answers_nothing_is_still_not_a_blind_launch():
    """A dial that will not say is treated exactly like "neither exists"."""
    dial = _LaunchDial(cap=(0, "just some shell noise\n", ""))
    check = await preflight(_request(), ssh=dial, cfg=None)
    assert not check.ok
    assert "setsid" in check.reason and "nohup" in check.reason

    dead = _LaunchDial(cap=(None, "", "Timeout opening channel"))
    check = await preflight(_request(), ssh=dead, cfg=None)
    assert not check.ok
    assert "Timeout opening channel" in check.reason


@pytest.mark.parametrize(
    "version, expect_flag",
    [
        ("rclone v1.66.0", True),
        ("rclone v1.58.0", True),
        ("rclone v1.58.0-beta.1", True),
        ("rclone v1.57.0", False),
        ("rclone v1.53.3-12-g0d1e2f3a", False),
        ("totally garbage output", False),
        ("", False),
    ],
)
@pytest.mark.anyio
async def test_the_optional_flag_is_only_ever_added_for_an_rclone_that_knows_it(
    version, expect_flag
):
    dial = _LaunchDial(cap=_cap(version=version))
    check = await preflight(_request(), ssh=dial, cfg=None)
    # An unknown or old version costs the push a nicety, never the push.
    assert check.ok, check.reason
    assert check.error_on_no_transfer is expect_flag
    started = await start_copy(
        _request(),
        ssh=dial,
        rclone_bin="/usr/bin/rclone",
        now=NOW,
        detacher=check.detacher,
        error_on_no_transfer=check.error_on_no_transfer,
        proof_delay_s=0.0,
    )
    assert started.ok, started.reason
    assert ("--error-on-no-transfer" in dial.launched[0]) is expect_flag


def test_the_version_gate_is_a_version_gate_not_a_string_match():
    assert vps1_push._rclone_version_tuple("rclone v1.66.0") == (1, 66)
    assert vps1_push._rclone_version_tuple("rclone v1.58.0-3-gabc") == (1, 58)
    assert vps1_push._rclone_version_tuple("no numbers here") is None
    assert vps1_push._rclone_version_tuple("") is None
    assert vps1_push._supports_error_on_no_transfer((1, 58)) is True
    assert vps1_push._supports_error_on_no_transfer((1, 57)) is False
    assert vps1_push._supports_error_on_no_transfer(None) is False


def test_a_caller_that_never_probed_keeps_the_historical_command_line():
    """`build_copy_command` is pure: the defaults are the old behaviour."""
    cmd = _launch()
    assert cmd.startswith("setsid ") and "--error-on-no-transfer" in cmd


# --------------------------------------------------------------------------- #
# 2b. the LOGGING FLAGS are read off rclone itself, never guessed at
# --------------------------------------------------------------------------- #


#: What VPS1 actually wrote, 28 times, when the shipped line said `--log`: the
#: verbatim shape of the usage dump a flag it does not know produces.
USAGE_ERROR = (
    "Error: unknown flag: --log",
    "Usage:",
    "  rclone copy source:path dest:path [flags]",
    "",
    "Run 'rclone copy --help' for more information.",
)


def test_the_capability_probe_asks_rclone_for_its_own_flag_words():
    """Self-description, not a version table: `help logging`, else `help flags`.

    Bounded, tagged, and never fatal in itself: `|| true` means a rclone without
    either topic answers with an empty block instead of failing the probe, and
    the block is bracketed by this probe's own markers so nothing else in the
    channel can be read as flag names.
    """
    command = vps1_push._capability_probe_command("/usr/bin/rclone")
    assert "/usr/bin/rclone help logging" in command
    assert "|| /usr/bin/rclone help flags" in command
    assert "SYNC-PUSH-CAP:log-help-begin" in command
    assert "SYNC-PUSH-CAP:log-help-end" in command
    # Bounded (no flood) and harmless if the whole thing fails.
    assert f"head -c {vps1_push._CAP_HELP_MAX_BYTES}" in command
    assert "|| true" in command
    # And it stayed ONE shell, on the same probe the wrappers are asked about.
    assert command.count("SYNC-PUSH-CAP:log-help-begin") == 1
    assert "help flags &&" not in command


@pytest.mark.parametrize(
    "help_lines, expected",
    [
        # A modern rclone: both spellings, and the neighbouring `--log-file-*`
        # flag proves the match is on NAMES, not substrings.
        (
            "Logging:\n      --log-file string\n      --log-file-max-size SizeSuffix\n"
            "      --log-level Level\n",
            ("--log-file", "--log-level"),
        ),
        # A pre-1.55 build: only the alias, so the alias is what ships.
        ("      --log-file string\n      --loglevel Level\n", ("--log-file", "--loglevel")),
        # Both present: the modern spelling wins, the alias is never emitted.
        (
            "      --log-file string\n      --log-level Level\n      --loglevel Level\n",
            ("--log-file", "--log-level"),
        ),
        # Only the alias is DEFINED, and the description mentions the modern
        # name: a definition at the start of a line is what counts, so the
        # pre-1.55 build gets its own spelling and nothing else.
        (
            "      --log-file string\n      --loglevel Level  deprecated alias for --log-level\n",
            ("--log-file", "--loglevel"),
        ),
        # Only the file flag exists: no level flag is invented to go with it.
        ("      --log-file string\n", ("--log-file", None)),
        # Only the LEVEL exists (a build with something odd about it): say so.
        ("      --log-level Level\n", (None, "--log-level")),
        # The longer flag alone is NOT the shorter one.
        ("      --log-file-max-size SizeSuffix\n      --log-level-by-path Level\n", (None, None)),
        # The help call errored / printed nothing: BOTH unsupported.
        ("", (None, None)),
        ("Error: unknown help topic. Run '--help'.\n", (None, None)),
        ("   \n  \n", (None, None)),
    ],
)
def test_the_flag_names_come_from_the_help_words_and_nowhere_else(help_lines, expected):
    assert vps1_push._detect_log_flags(help_lines) == expected


def test_a_probe_that_never_showed_its_help_block_supports_nothing():
    """Missing evidence reads as "no such flag", which is the safe direction."""
    assert vps1_push._detect_log_flags(
        vps1_push._parse_capability_output(_cap()[1]).get("log-help")
    ) == (
        "--log-file",
        "--log-level",
    )
    caps = vps1_push._parse_capability_output(_cap(help_block=False)[1])
    assert "log-help" not in caps
    assert vps1_push._detect_log_flags(caps.get("log-help", "")) == (None, None)
    # The wrapper answers still came through: only the help is missing.
    assert caps["setsid"] == "yes" and caps["version"] == "rclone v1.66.0"


@pytest.mark.anyio
async def test_the_help_block_never_becomes_the_version_answer():
    """The version window closes at the help marker, whatever the help contains."""
    out = (
        "SYNC-PUSH-CAP:setsid=yes\nSYNC-PUSH-CAP:nohup=yes\nSYNC-PUSH-CAP:rclone-version=\n"
        "SYNC-PUSH-CAP:log-help-begin\n      --log-level Level   since v1.55.0\n"
        "SYNC-PUSH-CAP:log-help-end\n"
    )
    caps = vps1_push._parse_capability_output(out)
    assert "version" not in caps
    assert vps1_push._detect_log_flags(caps.get("log-help", "")) == (None, "--log-level")


@pytest.mark.parametrize(
    "cap_kwargs, expected_flags",
    [
        # The two spellings a real 1.69 lists: the line uses them, in full.
        ({}, ("--log-file", "--log-level")),
        # Only `--log-file` exists → NO level flag is emitted at all.
        ({"log_level": False}, ("--log-file",)),
        # Neither exists → neither is emitted, and the redirect still carries
        # the post-mortem to the file the poll reads.
        ({"log_file": False, "log_level": False}, ()),
        # The help call itself failed → the same, and the push still went out.
        ({"help_block": False}, ()),
        # The pre-1.55 alias, and only the alias → the alias ships.
        ({"log_level": False, "loglevel_alias": True}, ("--log-file", "--loglevel")),
    ],
)
@pytest.mark.anyio
async def test_the_launch_emits_exactly_the_logging_flags_vps1_reported(cap_kwargs, expected_flags):
    dial = _LaunchDial(cap=_cap(**cap_kwargs))
    check = await preflight(_request(), ssh=dial, cfg=None)
    assert check.ok, check.reason
    started = await start_copy(
        _request(),
        ssh=dial,
        rclone_bin="/usr/bin/rclone",
        now=NOW,
        detacher=check.detacher,
        error_on_no_transfer=check.error_on_no_transfer,
        log_file_flag=check.log_file_flag,
        log_level_flag=check.log_level_flag,
        proof_delay_s=0.0,
    )
    # A rclone with no logging flag at all is a REASON to omit it, never a
    # reason to refuse the push.
    assert started.ok, started.reason
    cmd = dial.launched[0]
    for flag in ("--log-file", "--log-level", "--loglevel"):
        assert (flag in shlex.split(cmd)) is (flag in expected_flags), flag
    # THE BUG: bare `--log ` can never be on the line, in any outcome.
    assert "--log " not in cmd
    assert "--log" not in shlex.split(cmd)
    # And whatever the flags, the wrapper's own redirect is still there: that is
    # the mechanism that makes omitting one harmless — and the reason this bug
    # was ever reported, since it is where `Error: unknown flag: --log` was read.
    assert cmd.endswith(f"</dev/null >> {LOG_PATH} 2>&1 &")
    assert f" >> {LOG_PATH} 2>&1" in cmd


@pytest.mark.anyio
async def test_preflight_hands_the_probed_spellings_to_the_caller():
    """What the help text said is what the CHECK carries, None and all."""
    modern = await preflight(_request(), ssh=_LaunchDial(cap=_cap()), cfg=None)
    assert (modern.log_file_flag, modern.log_level_flag) == ("--log-file", "--log-level")

    file_only = await preflight(_request(), ssh=_LaunchDial(cap=_cap(log_level=False)), cfg=None)
    assert (file_only.log_file_flag, file_only.log_level_flag) == ("--log-file", None)

    mute = await preflight(_request(), ssh=_LaunchDial(cap=_cap(help_block=False)), cfg=None)
    assert (mute.log_file_flag, mute.log_level_flag) == (None, None)

    alias = await preflight(
        _request(), ssh=_LaunchDial(cap=_cap(log_level=False, loglevel_alias=True)), cfg=None
    )
    assert (alias.log_file_flag, alias.log_level_flag) == ("--log-file", "--loglevel")


@pytest.mark.anyio
async def test_start_copy_defaults_are_a_valid_line_for_an_unprobed_caller():
    """A caller that never probed must still never ship `--log`."""
    dial = _LaunchDial()
    started = await start_copy(_request(), ssh=dial, rclone_bin="/usr/bin/rclone", now=NOW)
    assert started.ok, started.reason
    cmd = dial.launched[0]
    assert "--log-file" in shlex.split(cmd) and "--log-level" in shlex.split(cmd)
    assert "--log" not in shlex.split(cmd)
    # And the check reports the flags the line ACTUALLY carries.
    assert (started.log_file_flag, started.log_level_flag) == ("--log-file", "--log-level")


@pytest.mark.anyio
async def test_a_caller_that_hands_over_a_bad_flag_has_it_dropped_not_launched():
    """The whitelist is the last word, whoever the caller is."""
    dial = _LaunchDial()
    started = await start_copy(
        _request(),
        ssh=dial,
        rclone_bin="/usr/bin/rclone",
        now=NOW,
        log_file_flag="--log",
        log_level_flag="--loglevel",
        proof_delay_s=0.0,
    )
    assert started.ok, started.reason
    cmd = dial.launched[0]
    assert "--log " not in cmd and "--log" not in shlex.split(cmd)
    # The alias IS a real spelling, so it survives; `--log` is not, so it does
    # not — and the check echoes what shipped, not what was asked for.
    assert "--loglevel" in shlex.split(cmd)
    assert (started.log_file_flag, started.log_level_flag) == (None, "--loglevel")


# --------------------------------------------------------------------------- #
# 3. prove the copy is alive, or say so within seconds
# --------------------------------------------------------------------------- #


def test_the_proof_pattern_cannot_match_the_probe_that_runs_it():
    """`pgrep -f` would otherwise find its own shell and always say "alive"."""
    pattern = _pgrep_pattern(LIST_PATH)
    assert pattern not in LIST_PATH  # the dots are bracketed
    command = _launch_proof_command(LIST_PATH, LOG_PATH)
    assert pattern in command
    assert LIST_PATH not in pattern  # the probe text is not the pattern
    # One bounded read: our own two scratch files, no glob, no mount, no content.
    for forbidden in ("ls ", "find ", "lsf", "rclone", "/mnt", "*"):
        assert forbidden not in command


@pytest.mark.anyio
async def test_pgrep_finds_the_job_and_the_launch_is_good_with_no_caveat():
    dial = _LaunchDial(proof=_proof(pgrep="found", log="absent"))
    started = await start_copy(_request(), ssh=dial, now=NOW, proof_delay_s=0.0)
    assert started.ok, started.reason
    assert started.reason == ""
    assert started.list_path == LIST_PATH and started.log_path == LOG_PATH
    assert dial.written == [(LIST_PATH, f"{EPISODE_TOP}/{EPISODE_NAME}\n".encode())]


@pytest.mark.anyio
async def test_no_pgrep_but_a_log_with_bytes_is_alive():
    """pgrep missing/unsupported must never be read as death; rclone spoke."""
    for answer in ("missing", "error"):
        dial = _LaunchDial(
            proof=_proof(pgrep=answer, log="bytes", lines=("INFO : Starting `rclone copy`",))
        )
        started = await start_copy(_request(), ssh=dial, now=NOW, proof_delay_s=0.0)
        assert started.ok, f"{answer}: {started.reason}"
        # Proven alive by the log: no "unproven" caveat rides along.
        assert started.reason == ""


@pytest.mark.anyio
async def test_no_process_and_no_log_words_is_a_failed_launch_with_a_reason():
    dial = _LaunchDial(
        proof=_proof(pgrep="notfound", log="absent", lines=("sh: setsid: command not found",))
    )
    started = await start_copy(_request(), ssh=dial, now=NOW, proof_delay_s=0.0)
    assert not started.ok
    assert "VPS1 accepted the launch but no copy is running" in started.reason
    # The reason names BOTH things it checked …
    assert _pgrep_pattern(LIST_PATH) in started.reason
    assert LOG_PATH in started.reason
    # … and quotes what the launcher wrote, so the operator sees the cause.
    assert "setsid: command not found" in started.reason
    assert started.list_path == "" and started.log_path == ""
    # Our own scratch list is the only thing deleted, and only after a launch
    # that provably never ran.
    assert any(c == f"rm -f {LIST_PATH}" for c in dial.commands)
    assert all("rm " not in c or "-f /tmp/racing-sync-push-" in c for c in dial.commands)


@pytest.mark.anyio
async def test_a_clean_pgrep_miss_with_an_empty_log_is_the_same_dead_launch():
    dial = _LaunchDial(proof=_proof(pgrep="notfound", log="empty"))
    started = await start_copy(_request(), ssh=dial, now=NOW, proof_delay_s=0.0)
    assert not started.ok
    assert "no copy is running" in started.reason
    assert "nothing was ever written to it" in started.reason


@pytest.mark.anyio
async def test_pgrep_unavailable_and_no_log_yet_is_unproven_not_killed():
    """Missing evidence never kills a possibly-live copy: the poll decides."""
    for answer in ("missing", "error"):
        dial = _LaunchDial(proof=_proof(pgrep=answer, log="absent"))
        started = await start_copy(_request(), ssh=dial, now=NOW, proof_delay_s=0.0)
        assert started.ok is True, f"{answer} must not be read as death"
        assert "unproven" in started.reason
        # Nothing cleaned up: the copy stays, the checkpoint is still stamped.
        assert started.list_path == LIST_PATH and started.log_path == LOG_PATH
        assert not any(c.startswith("rm ") for c in dial.commands)


@pytest.mark.anyio
async def test_a_proof_that_cannot_reach_vps1_is_unproven_not_dead():
    dial = _LaunchDial(proof=(None, "", "Timeout opening channel"))
    started = await start_copy(_request(), ssh=dial, now=NOW, proof_delay_s=0.0)
    assert started.ok is True
    assert "cannot tell yet" in started.reason
    assert "Timeout opening channel" in started.reason


# --------------------------------------------------------------------------- #
# 3b. a NON-EMPTY LOG IS EVIDENCE, NOT PROOF OF LIFE
# --------------------------------------------------------------------------- #


@pytest.mark.anyio
async def test_a_usage_error_in_the_log_kills_the_launch_instead_of_reporting_it_alive():
    """THE second bug: rclone wrote its complaint and died in 0.4 s; the proof
    saw bytes and said "alive", `start_copy` said ok, the checkpoint squatted the
    slot and the operator was told "push started". Now the same bytes are a
    refusal, seconds later, quoting the line that says why.
    """
    dial = _LaunchDial(proof=_proof(pgrep="notfound", log="bytes", lines=USAGE_ERROR))
    started = await start_copy(_request(), ssh=dial, now=NOW, proof_delay_s=0.0)

    assert started.ok is False
    # The reason NAMES the flag line — the operator reads the cause, not a mood.
    assert "Error: unknown flag: --log" in started.reason
    assert "fatal line" in started.reason
    assert started.list_path == "" and started.log_path == ""
    # Nothing left running for the checkpoint to describe: our scratch list is
    # the only thing removed.
    assert any(c == f"rm -f {LIST_PATH}" for c in dial.commands)
    assert all("rm " not in c or "-f /tmp/racing-sync-push-" in c for c in dial.commands)


@pytest.mark.anyio
async def test_a_fatal_log_line_is_death_even_when_pgrep_could_not_have_helped():
    """pgrep missing/errored says nothing about the job; rclone's words do."""
    for answer in ("missing", "error", "notfound", ""):
        dial = _LaunchDial(
            proof=_proof(pgrep=answer, log="bytes", lines=("Fatal: failed to create file system",))
        )
        started = await start_copy(_request(), ssh=dial, now=NOW, proof_delay_s=0.0)
        assert started.ok is False, answer
        assert "Fatal: failed to create file system" in started.reason


@pytest.mark.anyio
async def test_pgrep_finding_the_job_outranks_a_fatal_looking_log():
    """PROCESS EVIDENCE BEATS LOG EVIDENCE, always.

    A long run's first lines can hold a retry that reads like a give-up while
    the copy is uploading merrily; one rclone process on VPS1 is worth more than
    anything its log says, so this launch stays alive and stays checkpointed.
    """
    dial = _LaunchDial(proof=_proof(pgrep="found", log="bytes", lines=USAGE_ERROR))
    started = await start_copy(_request(), ssh=dial, now=NOW, proof_delay_s=0.0)
    assert started.ok is True, started.reason
    assert started.reason == ""
    assert started.list_path == LIST_PATH and started.log_path == LOG_PATH
    assert not any(c.startswith("rm ") for c in dial.commands)


@pytest.mark.anyio
async def test_a_stats_block_and_a_quiet_start_are_still_a_running_copy():
    """Progress-shaped words are NOT fatal words: only the table kills.

    pgrep is deliberately answering "notfound" here, so what saves these launches
    is the log's own content, not a process sighting: an in-flight copy, a copy
    that has already written its closing summary (a small push really can finish
    inside the proof's few seconds — the poll and the progress check are the ones
    that know whether bytes landed), and a copy mid-file are all left running.
    """
    for lines in (
        ("INFO : Starting `rclone copy` with 2 threads",),
        (
            "Transferred:        4.2 GiB / 40.0 GiB, 11%, 12.4 MiB/s, ETA 48m2s",
            "Checks:               1 / 1, 100%",
            "Elapsed time:      4m21.3s",
        ),
        ("Transferred: 1 / 1, 100%", "Errors: 0"),
        ("Example.Show.S01E01.1080p.WEB-DL-GROUP.mkv: Copied (new)",),
    ):
        dial = _LaunchDial(proof=_proof(pgrep="notfound", log="bytes", lines=lines))
        started = await start_copy(_request(), ssh=dial, now=NOW, proof_delay_s=0.0)
        assert started.ok is True, lines
        assert started.log_path == LOG_PATH
        assert not any(c.startswith("rm ") for c in dial.commands)


@pytest.mark.anyio
async def test_a_log_that_cannot_be_read_is_alive_and_stays_alive():
    """The fail-safe direction, three ways: no words, no verdict, no killing.

    `log=bytes` with nothing captured (a head that could not read the file), a
    proof whose log tag never arrived, and a reply that is pure shell noise: all
    three leave a possibly-running multi-GB copy alone, checkpoint intact.
    """
    unreadable = [
        _proof(pgrep="missing", log="bytes"),  # non-empty, but nothing was read
        _proof(pgrep="missing", log="empty"),
        (0, "sh: syntax error near unexpected token\n", ""),
        (0, "SYNC-PUSH-PROOF:log-begin\nError: unknown flag: --log\n", ""),  # no log= tag
    ]
    for reply in unreadable:
        dial = _LaunchDial(proof=reply)
        started = await start_copy(_request(), ssh=dial, now=NOW, proof_delay_s=0.0)
        assert started.ok is True, reply
        assert started.list_path == LIST_PATH and started.log_path == LOG_PATH
        assert not any(c.startswith("rm ") for c in dial.commands)


def test_the_launch_and_the_poll_share_one_fatal_table():
    """The same words mean the same thing to `_confirm_launch` and `push_liveness`.

    One table (`_FATAL_MARKERS`), one classifier (`_log_verdict`): a launch proof
    that disagreed with the poll about what a fatal line is would either kill a
    live copy or keep a dead one squatting its slot.
    """
    for line in ("Error: unknown flag: --log", "Failed to copy with 1 errors", "panic: boom"):
        assert vps1_push._log_verdict(line) == ("failed", line)
        assert vps1_push._fatal_log_line(line) == line
    assert vps1_push._log_verdict("Transferred: 1 / 1, 100%") == (
        "finished",
        "Transferred: 1 / 1, 100%",
    )
    assert vps1_push._log_verdict("INFO : nothing to transfer") == ("alive", "")
    # Silence, and near-silence, are never death to either reader. And `Errors:`
    # (the stats key) is NOT `Error:` (the fatal prefix) — a clean closing
    # summary stays a summary.
    assert vps1_push._log_verdict("") == ("alive", "")
    assert vps1_push._log_verdict("Errors: 0")[0] == "finished"


def test_only_the_probes_own_lines_are_read_as_answers():
    """A job log can never talk itself into a verdict.

    Past the `log-begin` marker everything is content: a line shaped like the
    probe's own `pgrep=found` tag is quoted, not believed.
    """
    spoofed = (
        "SYNC-PUSH-PROOF:pgrep=notfound\nSYNC-PUSH-PROOF:log=absent\n"
        "SYNC-PUSH-PROOF:log-begin\nSYNC-PUSH-PROOF:pgrep=found\n"
        "  \x00 some control-char noise \x1b[31m \x00\n"
    )
    pgrep, log_state, captured = vps1_push._parse_proof_output(spoofed)
    assert (pgrep, log_state) == ("notfound", "absent")
    assert captured and all("\x00" not in line and "\x1b" not in line for line in captured)


@pytest.mark.anyio
async def test_the_proof_waits_off_any_lease_and_is_bounded(monkeypatch):
    """The confirmation sleep is the push's own, cancellable, and capped."""
    slept: list[float] = []

    async def _no_sleep(_delay: float) -> None:
        slept.append(_delay)

    monkeypatch.setattr(vps1_push.asyncio, "sleep", _no_sleep)
    dial = _LaunchDial()
    await start_copy(_request(), ssh=dial, now=NOW, proof_delay_s=900.0)
    assert slept == [vps1_push._LAUNCH_PROOF_DELAY_MAX_S], "the wait stays bounded"
    slept.clear()
    await start_copy(_request(), ssh=dial, now=NOW)
    assert slept and slept[0] <= vps1_push._LAUNCH_PROOF_DELAY_MAX_S


# --------------------------------------------------------------------------- #
# 4. the operator hears the truth, and the slot is free again
# --------------------------------------------------------------------------- #


def _manual_coordinator(store: StateStore, dial: _LaunchDial, monkeypatch):
    """A coordinator with the REAL launch path and a VPS1 dial under test.

    Only preflight is stood in for (its own behaviour is pinned above) and the
    proof's wait is zeroed — the delay is a shipped constant read at call time,
    so a test can drive the real `start_copy` without sleeping. Every write,
    launch and proof still goes through `vps1_push` and `dial`.
    """
    monkeypatch.setattr(vps1_push, "_LAUNCH_PROOF_DELAY_S", 0.0)
    monkeypatch.setattr(
        coordinator_push,
        "preflight",
        AsyncMock(return_value=PushCheck(True, "", files_total=1)),
    )
    blob = _single_blob(EPISODE_NAME, 2000)
    coord = _push_coordinator(
        store, _push_cfg(), live=[_vps1_torrent(blob, name=EPISODE_TOP, size=2000)], files=[]
    )
    coord.sftp = dial
    ts = _row(store, blob, name=EPISODE_TOP, size=2000, kind="episode", state=State.WAITING_INDEXER)
    return coord, ts, _hash_of(blob)


@pytest.mark.anyio
async def test_a_dead_launch_returns_the_proof_reason_verbatim_and_no_checkpoint(
    tmp_path, monkeypatch
):
    store = StateStore(tmp_path / "state.db")
    try:
        dial = _LaunchDial(
            proof=_proof(pgrep="notfound", log="absent", lines=("rclone: unknown flag",))
        )
        coord, ts, h = _manual_coordinator(store, dial, monkeypatch)

        started, why = await coord.start_source_push(ts, reason="manual")

        assert started is False
        # NOT "push started": the transport's own line, word for word.
        assert why.startswith("push refused: VPS1 accepted the launch but no copy is running")
        assert "rclone: unknown flag" in why
        # A failed launch leaves no live checkpoint, so the slot is free NOW
        # instead of squatted for `timeout_seconds`.
        assert store.push_is_live(h) is False
        assert store.get_push(h).state == PushState.FAILED
        assert store.get_push(h).last_error == why
        # The row was never parked: the normal path picks it up untouched.
        assert store.get(h).state == State.WAITING_INDEXER
        assert h not in getattr(coord, "_push_wanted", {})  # nothing remembered
    finally:
        store.close()


@pytest.mark.anyio
async def test_an_unproven_launch_starts_and_says_so_in_the_reply(tmp_path, monkeypatch):
    store = StateStore(tmp_path / "state.db")
    try:
        dial = _LaunchDial(proof=_proof(pgrep="missing", log="absent"))
        coord, ts, h = _manual_coordinator(store, dial, monkeypatch)

        started, why = await coord.start_source_push(ts, reason="manual")

        assert started is True, why
        assert "unproven" in why, "the reply must not pretend the copy was proven"
        assert store.push_is_live(h) is True
        # The checkpoint is stamped with this job's own scratch pair, so the
        # poll has a log to read: the proof never claimed death.
        push = store.get_push(h)
        assert push.log_path.startswith("/tmp/racing-sync-push-") and push.log_path.endswith(".log")
        assert push.list_path == push.log_path[: -len(".log")] + ".lst"
    finally:
        store.close()


@pytest.mark.anyio
async def test_two_dead_launches_stop_the_third_retry_until_the_window_passes(
    tmp_path, monkeypatch
):
    """A permanently broken VPS1 must not cost SSH on every poll."""
    store = StateStore(tmp_path / "state.db")
    try:
        dial = _LaunchDial(proof=_proof(pgrep="notfound", log="absent"))
        coord, ts, h = _manual_coordinator(store, dial, monkeypatch)

        for _attempt in (1, 2):
            started, why = await coord.start_source_push(ts, reason="auto")
            assert started is False and "no copy is running" in why
        assert coord._push_launch_failures[h][0] == 2  # type: ignore[attr-defined]

        # Third try: refused before a single remote call, so nothing is spent.
        spent = len(dial.commands) + len(dial.launched)
        started, why = await coord.start_source_push(ts, reason="auto")
        assert started is False
        assert "failed to start the last 2 push(es)" in why
        assert len(dial.commands) + len(dial.launched) == spent

        # The pressure sweep rides the same entry point, so it is covered too.
        started, why = await coord.start_source_push(ts, reason="pressure")
        assert started is False
        assert len(dial.commands) + len(dial.launched) == spent

        # An operator's tap is never deferred: they asked, VPS1 gets asked.
        started, why = await coord.start_source_push(ts, reason="manual")
        assert started is False and "no copy is running" in why
        assert len(dial.commands) + len(dial.launched) > spent

        # Once the window passes the automatic paths try again.
        count, _armed, seen, why_text = coord._push_launch_failures[h]  # type: ignore
        coord._push_launch_failures[h] = (count, time.monotonic() - 1.0, seen, why_text)
        spent = len(dial.commands) + len(dial.launched)
        started, why = await coord.start_source_push(ts, reason="auto")
        assert started is False and "no copy is running" in why
        assert len(dial.commands) + len(dial.launched) > spent
    finally:
        store.close()


@pytest.mark.anyio
async def test_a_copy_that_really_starts_ends_the_failure_streak(tmp_path, monkeypatch):
    store = StateStore(tmp_path / "state.db")
    try:
        dial = _LaunchDial(proof=_proof(pgrep="notfound", log="absent"))
        coord, ts, h = _manual_coordinator(store, dial, monkeypatch)
        await coord.start_source_push(ts, reason="auto")
        await coord.start_source_push(ts, reason="auto")
        assert coord._push_launch_throttled(h)  # type: ignore[attr-defined]

        # VPS1 gets fixed (the proof now finds the process): the next auto
        # attempt is refused only until the streak is remembered as ended.
        dial._proof = _proof(pgrep="found")
        coord._push_launch_failures[h] = (2, 0.0, time.monotonic(), "old")  # type: ignore
        started, why = await coord.start_source_push(ts, reason="auto")
        assert started is True, why
        assert h not in coord._push_launch_failures  # type: ignore[attr-defined]
    finally:
        store.close()


@pytest.mark.anyio
async def test_a_launch_that_logged_its_own_usage_error_frees_the_slot_immediately(
    tmp_path, monkeypatch
):
    """The whole incident, end to end, at the operator's tap.

    rclone printed `Error: unknown flag: --log` and was gone in 0.4 s. Before
    this fix the proof read "log has bytes", `start_copy` returned ok, the
    checkpoint stayed live and the slot was squatted for the whole
    `timeout_seconds` while `/push_source` said "push started". Now the same
    four seconds produce a refusal, the FAILED record, and the fatal line as the
    reply — on the path the caller already uses for a dead proof.
    """
    store = StateStore(tmp_path / "state.db")
    try:
        dial = _LaunchDial(proof=_proof(pgrep="notfound", log="bytes", lines=USAGE_ERROR))
        coord, ts, h = _manual_coordinator(store, dial, monkeypatch)

        started, why = await coord.start_source_push(ts, reason="manual")

        assert started is False
        assert "Error: unknown flag: --log" in why, why
        assert store.push_is_live(h) is False, "the slot is free on the spot"
        push = store.get_push(h)
        assert push.state == PushState.FAILED
        assert push.last_error == why
        # Never a live checkpoint, never a squatting `log_path` for the poll to
        # follow: nothing is running on VPS1 and nothing claims otherwise.
        assert push.list_path == "" and push.log_path == ""
        # And the reason is quoted to the operator verbatim, not paraphrased.
        assert why.startswith("push refused:")
    finally:
        store.close()


@pytest.mark.anyio
async def test_a_fatal_log_line_counts_towards_the_hammer_throttle(tmp_path, monkeypatch):
    """A usage error that repeats is a broken lane, not a transient: two in a
    row and the automatic paths leave this hash alone."""
    store = StateStore(tmp_path / "state.db")
    try:
        dial = _LaunchDial(proof=_proof(pgrep="notfound", log="bytes", lines=USAGE_ERROR))
        coord, ts, h = _manual_coordinator(store, dial, monkeypatch)
        for _attempt in (1, 2):
            started, why = await coord.start_source_push(ts, reason="auto")
            assert started is False and "Error: unknown flag: --log" in why
        assert coord._push_launch_throttled(h)  # type: ignore[attr-defined]
        spent = len(dial.commands) + len(dial.launched)
        started, why = await coord.start_source_push(ts, reason="auto")
        assert started is False and "failed to start the last 2 push(es)" in why
        assert len(dial.commands) + len(dial.launched) == spent
    finally:
        store.close()


def test_the_launch_failure_map_stays_bounded():
    """A long-lived daemon must not grow with the number of pushes ever run."""
    coord = make_coordinator()
    memo: dict = {}
    coord._push_launch_failures = memo
    for i in range(500):
        coord._note_push_launch_failure(f"{i:040x}", "push refused: no copy is running")
    assert len(memo) <= coordinator_push._PUSH_LAUNCH_FAIL_CAP
    # Expired streaks are pruned on insert, the way `_failed_late_cross_seeds`
    # and the pressure defer are, so a fixed box is not remembered forever.
    memo["c" * 40] = (1, 0.0, time.monotonic() - 10_000.0, "old")
    coord._note_push_launch_failure("d" * 40, "push refused: no copy is running")
    assert "c" * 40 not in memo


# --------------------------------------------------------------------------- #
# the whole launch lane stays off the pooled members
# --------------------------------------------------------------------------- #


@pytest.mark.anyio
async def test_preflight_the_launch_and_the_proof_never_lease_a_pool_member(monkeypatch):
    """Four probes, one launch, one proof: all on throwaway transports.

    `_lease` raises, so this test passing at all is the proof that a push
    cannot queue behind — or starve — a racing .torrent fetch, even on the
    path that reports a dead launch.
    """
    vps1 = _VPS1(proof=_proof(pgrep="notfound", log="absent")[1])
    exporter, member, leases = _dedicated_exporter(monkeypatch, vps1)
    monkeypatch.setattr("racing_sync.vps1_push._remote_has_bytes", AsyncMock(return_value=False))

    request = _request()
    check = await preflight(
        request,
        ssh=exporter,
        cfg=MagicMock(),
        rclone_bin="/usr/bin/rclone",
        remote_name="seedremote",
    )
    assert check.ok, check.reason
    started = await start_copy(
        request,
        ssh=exporter,
        rclone_bin="/usr/bin/rclone",
        now=NOW,
        detacher=check.detacher,
        error_on_no_transfer=check.error_on_no_transfer,
        log_file_flag=check.log_file_flag,
        log_level_flag=check.log_level_flag,
        proof_delay_s=0.0,
    )
    assert not started.ok
    assert "no copy is running" in started.reason

    assert leases == []
    order = [c for c in vps1.commands]
    assert order[0] == "command -v /usr/bin/rclone"
    assert "SYNC-PUSH-CAP" in order[1]
    assert order[2] == "/usr/bin/rclone listremotes"
    assert order[3].startswith("setsid /usr/bin/rclone copy ")
    assert "SYNC-PUSH-PROOF" in order[4]
    # The dead launch's cleanup (our scratch list only) also stayed dedicated.
    assert any(c.startswith("rm -f ") for c in order[4:])
    member._client.exec_command.assert_not_called()
    member._sftp.open.assert_not_called()
    for pooled in exporter._members:
        assert pooled._lock.acquire(blocking=False) is True
        pooled._lock.release()
