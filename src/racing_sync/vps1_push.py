"""The push transport: ask VPS1 to copy content straight onto the seed remote.

Why this exists: the normal chain leeches a fresh copy on VPS2's SSD and then
rclone-moves it to the remote, so bytes VPS1 already holds complete travel
over the internet a second time. A push hands VPS1 one detached
``rclone copy --files-from-raw …`` instead; VPS2 then seeds from the remote
without ever re-downloading.

Contract rules this module enforces by construction:

* **copy, never move.** ``build_copy_command`` refuses a command that is not
  a copy (and no code path here can pass ``move``): deleting VPS1's files
  would stop its seed mid-race and destroy the ratio the release was raced
  for. Deletion stays the janitor's job. The only remote delete is our own
  throwaway file list, after a launch that never started.
* **the launcher never talks into a void.** A detached job has no stdout, so
  the WRAPPER's own stdout/stderr go into the job's log file, never to
  ``/dev/null``: when the login shell itself fails (`setsid: command not
  found`, an unknown rclone long flag, a quoting disaster) that text is the
  only evidence there is, and it must land in the one file this module and
  its caller already read. See ``build_copy_command`` — this rule exists
  because 28 launches reported success while 282 polls saw `0/1` and nothing
  at all had run on VPS1.
* **a flag is only ever emitted because VPS1 said it has it.** The same
  incident came from ONE wrong character: rclone spells its log flag
  ``--log-file`` and this module sent ``--log``, so every launch died printing
  a usage error in 0.4 s. The logging-flag spellings are therefore PROBED from
  rclone's own self-description (`_capability_probe_command`) and
  `build_copy_command` will physically not emit a spelling outside its
  whitelist. Dropping an unsupported flag costs nothing — the wrapper's
  redirect already puts the post-mortem in the log `push_liveness` reads —
  while inventing one is fatal, which is also why a launch whose own first log
  lines already say "give up" is reported DEAD on the spot
  (`_confirm_launch`) instead of "push started".
* **metadata only.** Nothing here reads or hashes content — not over SSH,
  not against the rclone mount or VPS2's SSD. Progress is one name+size
  listing (``rclone lsjson --files-only -R``) against the remote: completion
  stays name coverage, and the sizes are metadata the remote already
  publishes, not a read of the file.
* **no remote paths from untrusted text.** ``target_remote`` always comes
  from ``seed_layout.decide_seed_layout`` (or is handed over already
  computed). Names inside the list are validated by ``rclone_ops``' own
  files-from checks; release/show/group names never reach a path — remote
  scratch files are keyed by the infohash alone.
* **no cadence.** Every function here is one-shot. Polling intervals, state
  records and the concurrency cap belong to the coordinator, and this module
  does not import it (nor ``asyncio.sleep``).

All remote calls go through a DEDICATED SSH dial built from the same config as
the torrent-fetch dial (``source.deluge_sftp``): ``run_shell_dedicated``,
``write_remote_file_dedicated`` and ``launch_detached``. None of them leases a
pooled member, so a push can never queue behind — or be starved by — a racing
.torrent fetch, and can never steal one of those members from the race.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import shlex
import time
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path

# Reuse rclone_ops' validation and the recursive remote check: the push and
# the move stage must reject and verify exactly the same name shapes.
from .rclone_ops import (
    _norm_listed_path,
    _remote_has_bytes,
    _validate_files_from_entries,
    run_rclone,
)

log = logging.getLogger(__name__)

__all__ = [
    "PushCheck",
    "PushFile",
    "PushProgress",
    "PushRequest",
    "build_copy_command",
    "parse_remote_sizes",
    "preflight",
    "push_byte_totals",
    "push_liveness",
    "remote_progress",
    "sanitize_push_files",
    "start_copy",
]

# Remote scratch dir for the throwaway list/log pair. Fixed, world-writable,
# and wiped by the janitor with the rest of /tmp — the push never creates
# directories on VPS1.
_REMOTE_TMP_DIR = "/tmp"

# A file list is one line per torrent file; anything past this is a caller
# mistake (content never belongs in a push argument).
_MAX_LIST_ENTRIES = 5000

# Outer bounds for a single remote call. The SSH layer has its own timeouts;
# these exist so a wedged pool lease can never hold a push coroutine (the
# abandoned worker then fails fast on the pool's own lease budget, which is
# the behaviour sftp_source.py documents for every other caller).
_PROBE_TIMEOUT_S = 30.0
_WRITE_TIMEOUT_S = 60.0
_LIST_TIMEOUT_S = 120.0
#: The liveness read is deliberately the cheapest thing this module does
#: remotely: one shell on one small file. A slow read must degrade to "alive",
#: never to a killed copy, so its budget is short and its failure is benign.
_LIVENESS_TIMEOUT_S = 20.0

# How much slack past an inner timeout before the coroutine gives up.
_WAIT_MARGIN_S = 20.0

# Shell metacharacters we refuse outright in interpolated paths. `shlex.quote`
# already neutralises them; refusing is the belt that keeps a hostile-looking
# path from ever reaching a command line (and `\` and control chars have no
# business in a POSIX path).
_SHELL_METACHARS = (";", "`", "$", "\\", "\n", "\r")

_HEX_RE = re.compile(r"[0-9a-f]")

#: How many bytes of the log the liveness probe reads (the END of it: a dead
#: job says so in its last lines, and a multi-hour log is never pulled whole).
_LIVENESS_TAIL_BYTES = 8192

#: How long a push may look "not started yet" before its absent/empty log
#: counts as death. A couple of polls at the shipped 120 s cadence, so a slow
#: VPS1 (or an rclone that has not opened its log the instant the poll lands)
#: is never read as a dead job. The caller may widen it with the cadence.
_LIVENESS_GRACE_S = 300.0

#: rclone's own failure vocabulary, kept SMALL on purpose: every one of these
#: is a line rclone writes for a run that has given up (an `ERROR`/`Fatal`
#: prefix, a Go panic, a child exit status, a `Failed to <verb>` summary, or
#: cobra's `Error:` line for a run that never even STARTED — an unknown flag,
#: an unparsable command line, a bad config. That last one is the production
#: case: `Error: unknown flag: --log`, printed 0.4 s after a "successful"
#: launch, 28 times over).
#: Note the case matters, and `Errors:` (rclone's stats key, see
#: `_FINISHED_MARKERS`) deliberately does NOT match `Error:`: the colon has to
#: follow "Error" directly, so a normal closing summary stays evidence of a
#: finished run rather than a dead one.
#: Anything else in the tail is silence, and silence is NORMAL here.
_FATAL_MARKERS = ("ERROR", "Error:", "Fatal", "panic:", "exit status", "Failed to")

#: rclone's stats-summary block keys. At `--log-level INFO` with rclone's own
#: default `--stats-log-level` (error) this block is written ONCE, as the run
#: exits, which is exactly the evidence `finished` needs; if a build did log
#: the periodic blocks, their text differs poll to poll and the caller's
#: two-same-verdicts rule never fires, so a slow copy still survives. An error
#: summary (`Failed to copy with N errors`) is NOT here: it carries the `Failed
#: to` fatal marker and is reported as `failed`, which says more.
_FINISHED_MARKERS = ("Transferred:", "Errors:", "Checks:", "Elapsed time:")

#: Prefix the liveness probe's own output is tagged with, so a log line that
#: happens to look like a number can never be mistaken for the size it reports.
_SIZE_TAG = "SYNC-LIVENESS-SIZE:"

#: The probe's stand-in size for "there is no log file at all".
_SIZE_ABSENT = -1

#: Control characters in a log line (scrubbed before anything is stored).
_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]+")

#: What may wrap the copy so it survives the SSH session ending, in preference
#: order. `setsid` gives the job its own session (no controlling terminal to
#: hang up); `nohup` at least ignores SIGHUP, which is good enough to keep a
#: transfer running. Neither on VPS1 is a REFUSAL, never a blind launch.
_DETACHER_CANDIDATES = ("setsid", "nohup")

#: Tag prefix of the capability probe's own output lines, so a stray line of
#: shell noise can never be read as an answer (and so a test double can match
#: this probe by substring without colliding with `command -v rclone`).
_CAP_TAG = "SYNC-PUSH-CAP:"

#: Markers bracketing the help text the capability probe quotes back, so the
#: logging-flag names are read from rclone's OWN self-description and nothing
#: else in the channel can be mistaken for them.
_CAP_HELP_BEGIN = f"{_CAP_TAG}log-help-begin"
_CAP_HELP_END = f"{_CAP_TAG}log-help-end"

#: Key `log-help` is filed under in the parsed capability answers.
_CAP_HELP_KEY = "log-help"

#: How much help text may cross the channel: `rclone help logging` is a couple
#: of KiB and `rclone help flags` (the fallback) tens, and both are cut short
#: here, because a probe must never be able to flood a push.
_CAP_HELP_MAX_BYTES = 65536

#: The ONLY logging-flag spellings `build_copy_command` may ever emit, split by
#: the ROLE each plays (a level flag handed a path would be read as a level,
#: which is the same class of bug as the one below). `--log-file` is rclone's
#: "write my log here", `--log-level` its modern level flag and `--loglevel`
#: the pre-1.55 alias, accepted only when a build reports no other spelling.
#: A bare `--log` — the flag that killed 28 launches — is on no list here, so
#: it cannot be emitted even by a caller that asks for it by name.
_LOG_FILE_FLAG = "--log-file"
_LOG_LEVEL_FLAG = "--log-level"
_LOG_LEVEL_ALIAS = "--loglevel"
_LOG_FILE_FLAG_CHOICES = (_LOG_FILE_FLAG,)
_LOG_LEVEL_FLAG_CHOICES = (_LOG_LEVEL_FLAG, _LOG_LEVEL_ALIAS)


def _help_flag_pattern(name: str) -> re.Pattern[str]:
    """A regex matching one rclone flag DEFINITION, at the start of a line.

    rclone's help prints each flag on its own line, optionally after a short
    form (`  -v, --verbose  …`), so anchoring on the line start is what makes
    this a match on the flag's DEFINITION and not on a description that happens
    to mention it — the deprecated `--loglevel` line is literally described as
    "alias for --log-level", and a build that only has the alias must not be
    read as having the modern spelling. The trailing guard ends the NAME, so
    `--log-file-max-size` is never a match for `--log-file`.

    Strict on purpose: a build whose help this cannot read comes back as
    "unsupported", which omits a flag (harmless) rather than inventing one
    (fatal).
    """
    return re.compile(rf"^[ \t]*(?:-\w[ \t]*,[ \t]*)?{re.escape(name)}(?![-_0-9A-Za-z])", re.M)


#: Flag names as rclone defines them in its help, matched by NAME and by
#: position — never as a substring of a longer flag or of a description.
_LOG_FILE_HELP_RE = _help_flag_pattern("--log-file")
_LOG_LEVEL_HELP_RE = _help_flag_pattern("--log-level")
_LOG_LEVEL_ALIAS_HELP_RE = _help_flag_pattern("--loglevel")

#: The lowest rclone that knows `--error-on-no-transfer` (1.58, 2021-05).
#: VPS2's version proves nothing about VPS1's, so this is probed, never assumed.
_MIN_RCLONE_NO_TRANSFER_ERROR = (1, 58)

_VERSION_RE = re.compile(r"v(\d+)\.(\d+)")

#: Tag prefix of the launch proof's output lines (see `_launch_proof_command`).
_PROOF_TAG = "SYNC-PUSH-PROOF:"

#: How long to wait after the launch before asking whether the job is really
#: there. Short enough that a dead lane answers the operator in seconds — the
#: incident this closes reported "push started" and then said nothing more for
#: the whole `timeout_seconds`; long enough that rclone has had time to open
#: its own `--log-file` on a busy box, and long enough for a run that died on
#: its own command line to have written its complaint.
_LAUNCH_PROOF_DELAY_S = 4.0

#: Hard ceiling on that wait: this runs inline in the push's own task, and a
#: mis-set caller must never make a push stall for minutes.
_LAUNCH_PROOF_DELAY_MAX_S = 20.0

#: Budget of the proof's single shell (one pgrep, two tests and a bounded
#: head). Transport trouble is read as "unknown", never as "dead".
_PROOF_TIMEOUT_S = 20.0

#: How many leading log lines the proof may quote into its failure reason.
_PROOF_LOG_LINES = 12


@dataclass(frozen=True)
class PushFile:
    """One file the push must carry, named relative to VPS1's `src_dir`."""

    name: str
    size_bytes: int


class PushProgress(tuple):
    """One poll's metadata progress: a 2-tuple of FILE counts.

    `(files_present, files_total)` exactly as `remote_progress` always
    returned — completion is still name coverage — and the byte counters ride
    along as attributes because they come from the SAME listing: rclone's
    `lsjson` publishes each object's size, so being byte-aware costs no second
    remote call and no content read.

    A tuple subclass rather than a dataclass on purpose: every caller (and
    every existing test) unpacks `present, total = await remote_progress(...)`,
    and a poll that cannot answer about bytes still compares equal to `(0, n)`.
    """

    def __new__(
        cls,
        files_present: int,
        files_total: int,
        *,
        bytes_present: int = 0,
        bytes_total: int = 0,
    ) -> PushProgress:
        obj = super().__new__(cls, (_count(files_present), _count(files_total)))
        obj.files_present = _count(files_present)
        obj.files_total = _count(files_total)
        # Clamped and non-negative: a byte counter is only ever an observation
        # of "this much has landed", never a signed quantity.
        obj.bytes_present = _count(bytes_present)
        obj.bytes_total = _count(bytes_total)
        return obj


def _count(value: object) -> int:
    """A metadata counter as a non-negative int; anything unreadable is 0."""
    try:
        return max(0, int(value or 0))  # type: ignore[arg-type]
    except (TypeError, ValueError, OverflowError):
        return 0


@dataclass(frozen=True)
class PushRequest:
    """Everything a single push needs to know about one content group.

    `src_dir` is the tree on VPS1 (the racing client's content dir) and
    `target_remote` the seed remote produced by ``decide_seed_layout``;
    `files` are `PushFile`s whose names are relative to `src_dir`.
    `content_name`/`group_key` ride along for the caller's logging and state
    records only — this module never puts them in a path or a command.
    """

    infohash: str
    group_key: str
    content_name: str
    src_dir: str
    target_remote: str
    files: tuple[PushFile, ...]


@dataclass(frozen=True)
class PushCheck:
    """One pass/fail answer plus the values a checkpoint wants.

    `reason` is a single operator-readable line ("" when ok). The optional
    tail is what `start_copy` hands back so the caller can store
    `list_path` / `log_path` / `files_total` without re-deriving them;
    `src_dir` and `target_remote` are already on the request.

    `detacher` / `error_on_no_transfer` are what `preflight` LEARNED about
    VPS1 and what `start_copy` must therefore build: the wrapper the operator
    actually has, and the optional flag only when their rclone accepts it.
    They default to the historical behaviour, so a caller that never probed
    (or an older test double) still gets the old command line. When a
    successful `start_copy` could not PROVE the copy is alive it says so in
    `reason` — the push still went ahead, and the poll decides later.

    `log_file_flag` / `log_level_flag` are the same lesson applied to the two
    flags that decide WHERE rclone writes its own post-mortem: what VPS1's
    rclone says it accepts, with `None` meaning "this build does not have it —
    emit nothing". They default to the verified-correct spellings, so a caller
    that never probed still gets a VALID line, and `build_copy_command` refuses
    any spelling outside its whitelist, so a wrong flag is not possible.
    """

    ok: bool
    reason: str = ""
    list_path: str = ""
    log_path: str = ""
    files_total: int = 0
    detacher: str = "setsid"
    error_on_no_transfer: bool = True
    log_file_flag: str | None = _LOG_FILE_FLAG
    log_level_flag: str | None = _LOG_LEVEL_FLAG


def _fail(reason: str) -> PushCheck:
    return PushCheck(False, _one_line(reason))


def _one_line(text: object) -> str:
    """Collapse to one readable line, bounded so a log/DB field stays sane."""
    flat = " ".join(str(text or "").split())
    return flat[:400]


def _shape_refusal(text: str, label: str) -> str:
    """Refusals `shlex.quote` CANNOT fix: empties, line breaks, flag-looks."""
    if not text.strip():
        return f"push refused: {label} is empty"
    if text != text.strip():
        return f"push refused: {label} has surrounding whitespace: {text!r}"
    if any(c in text for c in ("\x00", "\n", "\r")):
        return f"push refused: {label} contains a control character"
    if text.startswith("-"):
        # `--` on the command line already shields positionals; refusing here
        # too keeps a `-`-leading value out of any caller that re-parses it.
        return f"push refused: {label} starts with '-' (reads as an rclone flag): {text!r}"
    return ""


def _unsafe_path_reason(value: str, label: str) -> str:
    """Strict gate (preflight): refuse anything a shell could reinterpret.

    Quoting would neutralise `;`, `$`, a backtick or a backslash, but a path
    that needs them is not a path this daemon should be handing to a remote
    shell at all — so the push declines and the normal chain runs instead.
    """
    text = str(value or "")
    reason = _shape_refusal(text, label)
    if reason:
        return reason
    for meta in _SHELL_METACHARS:
        if meta in text:
            return f"push refused: {label} contains the shell metacharacter {meta!r}"
    return ""


def _arg_refusal(value: str, label: str) -> str:
    """Command-builder gate: only what quoting cannot repair (see `_shape_refusal`)."""
    return _shape_refusal(str(value or ""), label)


def _entry_name_size(raw: object) -> tuple[str, int]:
    """(name, size) of one entry: accept str, PushFile, or name/size objects."""
    if isinstance(raw, str):
        return raw, 0
    name = getattr(raw, "name", None)
    if name is None:
        return "", 0
    size = getattr(raw, "size_bytes", None)
    if size is None:
        size = getattr(raw, "size", 0)
    try:
        size_int = max(0, int(size or 0))
    except (TypeError, ValueError):
        size_int = 0
    return str(name), size_int


def sanitize_push_files(entries: Iterable[object]) -> tuple[list[PushFile], str]:
    """Filter candidate names through rclone_ops' files-from validation.

    The list handed to ``--files-from-raw`` is read by rclone on VPS1 as
    literal relative paths, so traversal, absolute, control-character and
    ``-``-leading entries are refused before anything is written or launched.
    Returns (accepted, reason): `reason` is non-empty as soon as a single
    entry was dropped or nothing survived — the caller must then refuse the
    push rather than send a partial list (a silently missing file is a
    permanently incomplete seed).
    """
    accepted: list[PushFile] = []
    seen: set[str] = set()
    rejected: list[str] = []
    for raw in entries or ():
        name, size = _entry_name_size(raw)
        if not name:
            rejected.append("empty entry")
            continue
        # rclone_ops normalises backslashes for its checks but the POSIX
        # target still receives them verbatim, and a `\`-leading name is a
        # Windows-absolute path: refuse it here (that gap is POSIX-specific).
        if name.startswith("\\"):
            rejected.append(f"{name!r}: backslash-lead (Windows-absolute)")
            continue
        try:
            _validate_files_from_entries([name])
        except ValueError as e:
            rejected.append(_one_line(e))
            continue
        if name.startswith("-"):
            rejected.append(f"{name!r}: starts with '-'")
            continue
        norm = _norm_listed_path(name)
        if not norm or norm in seen:
            # Same file listed twice (a duplicate name is not a lost file,
            # so this is the one silent skip).
            continue
        seen.add(norm)
        accepted.append(PushFile(name=name, size_bytes=size))
    if len(accepted) > _MAX_LIST_ENTRIES:
        return [], (
            f"push refused: {len(accepted)} files exceeds the {_MAX_LIST_ENTRIES}-entry "
            "list cap (wrong tree?)"
        )
    if not accepted:
        detail = f" ({_one_line('; '.join(rejected[:3]))})" if rejected else ""
        return [], f"push refused: no usable file names for the list{detail}"
    if rejected:
        return (
            accepted,
            f"push refused: {len(rejected)} of {len(rejected) + len(accepted)} "
            f"entries rejected: {_one_line('; '.join(rejected[:3]))}",
        )
    return accepted, ""


def derive_remote_name(target_remote: str, remote_name: str = "") -> str:
    """The rclone remote NAME the push may use, or "" when there is none.

    An explicit `remote_name` (``[client.rclone_push].remote_name``) wins;
    otherwise it is the part of `target_remote` before the first ``:`` —
    exactly the derivation the config documents. Only the name is reused,
    never the path, and a wrong name fails preflight instead of writing into
    a bucket nobody seeds.
    """
    explicit = str(remote_name or "").strip().rstrip(":")
    if explicit:
        return explicit
    prefix = str(target_remote or "").strip()
    if ":" not in prefix:
        return ""
    return prefix.split(":", 1)[0].strip()


def _remote_listing_matches(line: str, name: str) -> bool:
    """True when one `rclone listremotes` line names `name` (colon or not)."""
    listed = str(line or "").strip()
    if not listed:
        return False
    return listed.rstrip(":") == name.rstrip(":")


def _push_scratch_paths(request: PushRequest, now: float | None = None) -> tuple[str, str]:
    """(list, log) paths on VPS1, keyed by the infohash and the start epoch.

    Deliberately name-free: a release title in a /tmp path would put the
    content in the racing box's process list and logs for no reason.
    """
    raw = str(getattr(request, "infohash", "") or "").lower()
    digest = "".join(c for c in raw if _HEX_RE.fullmatch(c))[:10] or "unknown"
    try:
        epoch = int(now if now is not None else time.time())
    except (TypeError, ValueError):
        epoch = int(time.time())
    stem = f"{_REMOTE_TMP_DIR}/racing-sync-push-{digest}-{max(0, epoch)}"
    return f"{stem}.lst", f"{stem}.log"


def _usable_log_flag(value: object, choices: tuple[str, ...], label: str) -> str | None:
    """The probed logging flag, or None when it is not a spelling we know.

    THE POINT OF THIS FUNCTION: `build_copy_command` cannot emit a flag that is
    not on `choices`, so the one wrong character that cost 28 launches
    (`--log`, which rclone has never had) is impossible HERE, whatever a
    caller — probed, unprobed, or a mock from a test — hands in. An unknown
    spelling is dropped, not raised: rclone then logs to the wrapper's redirect
    instead of the file it names, which is harmless, while refusing a push over
    a cosmetic flag would be the harm.
    """
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text in choices:
        return text
    log.warning(
        "push: not emitting the unsupported rclone %s flag %r (want %s)",
        label,
        text,
        " or ".join(choices),
    )
    return None


def build_copy_command(
    request: PushRequest,
    *,
    rclone_bin: str,
    transfers: int,
    bwlimit: str,
    list_path: str,
    log_path: str,
    detacher: str = "setsid",
    error_on_no_transfer: bool = True,
    log_file_flag: str | None = _LOG_FILE_FLAG,
    log_level_flag: str | None = _LOG_LEVEL_FLAG,
) -> str:
    """The one detached command line that runs the copy on VPS1.

    Everything interpolated goes through `shlex.quote`, positionals sit after
    a ``--`` end-of-flags marker (the same convention as rclone_ops' move: it
    shields a ``-``-leading path from being read as a flag), and the whole
    thing is wrapped so the job survives the SSH session dying.

    ``--ignore-existing`` keeps a re-push cheap and, above all, never
    overwrites. The two LOGGING FLAGS are not hardcoded strings any more:
    `log_file_flag` / `log_level_flag` carry what VPS1's own rclone reported
    having (`_capability_probe_command` → `preflight`), `None` means "this
    build does not have it — emit nothing", and the defaults are the
    verified-correct spellings `--log-file` / `--log-level`, so a caller that
    never probed still gets a valid line. `_usable_log_flag` then refuses any
    spelling outside the whitelist, which is what makes a wrong flag impossible
    rather than unlikely: this line used to read ``--log <path> --loglevel
    INFO``, rclone has never had a bare ``--log``, and VPS1 answered every one
    of 28 launches with `Error: unknown flag: --log` and died in 0.4 s.

    DROPPING AN UNSUPPORTED FLAG IS SAFE, AND HERE IS WHY: the wrapper already
    redirects the job's OWN stdout/stderr into that very path
    (``… </dev/null >> <log> 2>&1 &``), so even with NO rclone logging flag at
    all the post-mortem still lands in the file `push_liveness` and the launch
    proof read — rclone's level/usage output goes to stderr, and stderr is that
    log. That redirect is the mechanism that PRODUCED this bug report: it is
    where `Error: unknown flag: --log` was found in the first place. What a
    missing `--log-file` costs is rclone's own timestamped, `--log-level`
    filtered detail, nothing more; what an invented one costs is the copy.

    WHY THE SHELL'S OWN OUTPUT GOES INTO THAT LOG, NOT /dev/null: this exact
    incident. The wrapper used to end ``</dev/null >/dev/null 2>&1 &``, so every
    byte the LOGIN SHELL wrote — `setsid: command not found`, `rclone: unknown
    long flag '--error-on-no-transfer'`, a quoting failure, even a /tmp that
    would not open — vanished, while rclone's log file only ever appears once
    rclone itself starts. 28 launches were reported as successes, 282
    polls read `has 0/1 file(s)`, and nothing could say which of the three
    candidate causes was real (no setsid, an rclone too old for the flag, a job
    killed when the exec channel closed). Redirecting the wrapper into the job's
    log makes a shell-level failure land in the one file this module already
    reads, so the launch either has evidence or has a reason.

    `detacher` is the wrapper `preflight` PROVED to exist on VPS1 (`setsid`, or
    the session-surviving fallback `nohup`); `error_on_no_transfer` adds the
    zero-transfer-is-an-error flag only for an rclone new enough to accept it —
    an unsupported flag is fatal, while omitting a nicety is harmless. Both keep
    the historical behaviour as their default, and this function stays pure and
    deterministic: it never probes and never waits on anything.
    """
    src = str(getattr(request, "src_dir", "") or "")
    dst = str(getattr(request, "target_remote", "") or "")
    limit = str(bwlimit or "")
    wrapper = str(detacher or "setsid").strip()
    for value, label in (
        (src, "src_dir"),
        (dst, "target_remote"),
        (str(rclone_bin or ""), "rclone_bin"),
        (str(list_path or ""), "list_path"),
        (str(log_path or ""), "log_path"),
        (wrapper, "detacher"),
    ):
        reason = _arg_refusal(value, label)
        if reason:
            raise ValueError(reason)
    # The wrapper is spliced into the command UNQUOTED (it has to stay a
    # command word, not an argument), so it gets the strict gate that rejects
    # anything a shell could reinterpret, plus the whitelist of what preflight
    # can actually have found.
    strict = _unsafe_path_reason(wrapper, "detacher")
    if strict or wrapper not in _DETACHER_CANDIDATES:
        raise ValueError(
            f"push refused: {wrapper!r} is not a usable session-surviving wrapper "
            f"(want {' or '.join(_DETACHER_CANDIDATES)})"
        )
    # bwlimit is rclone syntax ("42M", "8M,24:00-08:00"), not a path: only a
    # line break would corrupt the command, and quoting cannot undo that.
    if any(c in limit for c in ("\n", "\r", "\x00")):
        raise ValueError("push refused: bwlimit contains a control character")
    if not src.strip() or not dst.strip():
        raise ValueError("push refused: src_dir and target_remote are both required")

    try:
        workers = int(transfers)
    except (TypeError, ValueError):
        # Garbage falls back to the SHIPPED default, not to rclone's: one copy
        # must never grab the whole uplink the races are run on.
        workers = 2
    workers = max(1, min(64, workers))

    argv: list[str] = [
        str(rclone_bin),
        # COPY, not move: VPS1 keeps seeding. The guard below proves it.
        "copy",
        "--files-from-raw",
        str(list_path),
        "--ignore-existing",
        "--transfers",
        str(workers),
    ]
    if limit.strip():
        argv += ["--bwlimit", limit.strip()]
    # The logging flags VPS1's own rclone reported having — each looked up in
    # the whitelist for its ROLE (a level flag handed a path would be read as a
    # level), each omitted when the probe said it does not exist. See the
    # docstring: the wrapper's `>> <log> 2>&1` is what makes omitting one safe.
    file_flag = _usable_log_flag(log_file_flag, _LOG_FILE_FLAG_CHOICES, "log file")
    level_flag = _usable_log_flag(log_level_flag, _LOG_LEVEL_FLAG_CHOICES, "log level")
    if file_flag:
        argv += [file_flag, str(log_path)]
    if level_flag:
        argv += [level_flag, "INFO"]
    if error_on_no_transfer is not False:
        argv += [
            # Fail loudly when the copy transfers nothing: an rc=0 that moved zero
            # files would look like a finished push. Already-complete pushes are
            # refused earlier by preflight, so a no-transfer here is real trouble.
            # Needs rclone >= 1.58 ON VPS1, which `preflight` now probes instead of
            # assuming (VPS2's version says nothing about VPS1's); an old rclone
            # treats the unknown flag as a fatal error, so it is only ever added
            # when the probe proves support.
            "--error-on-no-transfer",
        ]

    _guard_copy_only(argv)
    quoted = " ".join(shlex.quote(token) for token in argv)
    # `--` before the positionals, then the detach wrapper: it gives the copy
    # its own session (setsid) or at least immunity to SIGHUP (nohup),
    # </dev/null drops stdin and the trailing & lets the login shell exit, so
    # closing our channel cannot kill the transfer. The shell's stdout/stderr
    # are APPENDED to the job's own log (see above): rclone writes that log with
    # its `--log-file` when the probe says it has one, so this only ever adds the
    # wrapper's words — and, with no logging flag at all, ALL of rclone's own
    # words — to the same file, and a zero-byte log stays the "rclone never
    # spoke" evidence the poll reads.
    return (
        f"{wrapper} {quoted} -- {shlex.quote(src)} {shlex.quote(dst)} "
        f"</dev/null >> {shlex.quote(log_path)} 2>&1 &"
    )


def _guard_copy_only(argv: Sequence[str]) -> None:
    """Defence in depth: the argv must say `copy` and must never say `move`.

    A `move` would delete VPS1's files as they land on the remote — stopping
    its seed mid-race and destroying the ratio. Token-level (not substring)
    so a legitimate `/data/movies` path still passes.
    """
    tokens = [str(t).strip().lower() for t in argv]
    if any(t == "move" or t.startswith("--move") or t.startswith("-m") for t in tokens):
        raise ValueError("push refused: the rclone command looks like a move (would delete VPS1)")
    if "copy" not in tokens:
        raise ValueError("push refused: the rclone command is not a copy")
    # Defence in depth for the flag that caused the incident: `--log` is not an
    # rclone flag at all (`--log-file` is), and one invented long flag makes
    # rclone print a usage block and exit before transferring anything. The
    # whitelist in `_usable_log_flag` means this line is unreachable by
    # construction; it is here so that a future caller that assembles its own
    # argv is refused out loud rather than launching 28 dead copies.
    if any(t in ("--log", "--logfile") for t in tokens):
        raise ValueError("push refused: --log is not an rclone flag (the spelling is --log-file)")


#: The dial attribute every push shell call goes through. Deliberately NOT the
#: pooled `run_shell`: preflight and the liveness read used to lease members,
#: so on a busy VPS1 they died with `Timeout opening channel` while competing
#: with the racing .torrent fetches — the exact opposite of what the race needs.
_SHELL_ATTR = "run_shell_dedicated"
_WRITE_ATTR = "write_remote_file_dedicated"


def _no_dial_reason(attr: str) -> str:
    """Why the dial cannot run this lane's own kind of call (no prefix).

    Returned bare, because it is embedded in other refusals (and in a
    `push_liveness` "cannot tell" note) as often as it is a refusal itself.
    """
    return (
        f"the SSH dial to VPS1 offers no dedicated runner ({attr}); the pooled "
        "members are the racing path's and the push must not share them "
        "([client.deluge_sftp])"
    )


async def _remote_shell(
    ssh: object, command: str, timeout_s: float, *, attr: str = _SHELL_ATTR
) -> tuple[int | None, str, str]:
    """One DEDICATED-dial remote probe off the event loop; never raises.

    A dial without `attr` is a refusal (the reason is returned in stderr, the
    way every other failure here is), never a silent fallback onto a pooled
    member: a push that had to queue behind a .torrent fetch is the bug this
    exists to fix.
    """
    run = getattr(ssh, attr, None)
    if not callable(run):
        return (None, "", _no_dial_reason(attr))
    try:
        return await asyncio.wait_for(
            asyncio.to_thread(run, command, timeout_s),
            timeout=timeout_s + _WAIT_MARGIN_S,
        )
    except asyncio.CancelledError:
        raise
    except Exception as e:  # noqa: BLE001
        return (None, "", f"{type(e).__name__}: {e}")


def _capability_probe_command(rclone_bin: str) -> str:
    """ONE shell that says what VPS1 can actually run a detached copy with.

    Answers four questions with tagged lines, so a stray word of shell noise
    cannot be read as an answer:

    * does `setsid` exist? does `nohup`? (the wrapper the launch may use);
    * what rclone does VPS1 have? `rclone version` first, `rclone --version`
      as the fallback for a build that predates the subcommand, each cut to
      one line so a long `--version` banner never floods the channel.
    * WHICH LOGGING FLAGS DOES ITS RCLONE ACTUALLY ACCEPT? Asked of rclone
      itself — `rclone help logging`, falling back to `rclone help flags` for a
      build without that topic — quoted back between this probe's own begin/end
      markers and cut to `_CAP_HELP_MAX_BYTES`. NOT asked of a version table:
      the incident this closes was one invented character (`--log`), and a
      hardcoded guess about a build we have never met is the same mistake in
      slower clothing. Reading the names out of the help text also settles
      `--log-level` vs the pre-1.55 `--loglevel` alias for free, which a version
      comparison only approximates.

    Everything here is best-effort by construction: a probe that cannot answer
    is missing tags, which the caller reads as "unknown" (and unknown means
    refuse the LAUNCH path or omit the optional flag, never launch blind). For
    the logging flags specifically, no answer means BOTH are unsupported:
    omitting a logging flag is harmless (the wrapper already redirects the
    job's own stdout/stderr into the log file), inventing one is fatal.
    """
    binary = shlex.quote(str(rclone_bin or "rclone"))
    parts: list[str] = []
    for name in _DETACHER_CANDIDATES:
        parts.append(
            f"if command -v {name} >/dev/null 2>&1; then "
            f"echo '{_CAP_TAG}{name}=yes'; else echo '{_CAP_TAG}{name}=no'; fi"
        )
    parts.append(f"echo '{_CAP_TAG}rclone-version='")
    parts.append(f"{binary} version 2>/dev/null | head -n 1")
    parts.append(f"{binary} --version 2>/dev/null | head -n 1")
    # The help dump last, so a stray version-shaped line inside it can never be
    # read as the version answer (`_parse_capability_output` closes the version
    # window the moment this marker appears). `|| true` keeps a rclone that has
    # neither topic — or is not on PATH at all — from making the whole probe
    # exit non-zero: an empty block is the honest "no answer", not a failure.
    parts.append(f"echo '{_CAP_HELP_BEGIN}'")
    parts.append(
        f"{{ {binary} help logging 2>/dev/null || {binary} help flags 2>/dev/null || true; }} "
        f"2>/dev/null | head -c {_CAP_HELP_MAX_BYTES}"
    )
    parts.append(f"echo '{_CAP_HELP_END}'")
    return "; ".join(parts)


def _parse_capability_output(text: object) -> dict[str, str]:
    """{setsid/nohup: "yes"|"no", version: the first line that looks like one,
    log-help: the rclone help block the probe quoted back}.

    Only this module's own tagged lines are trusted; anything else in the
    output is noise. A missing key means VPS1 did not answer that question —
    which for `log-help` (absent, empty, or a help call that printed nothing)
    `preflight` reads as "neither logging flag exists", the safe direction.
    """
    found: dict[str, str] = {}
    after_version_tag = False
    in_help = False
    help_lines: list[str] = []
    for line in str(text or "").splitlines():
        flat = line.strip()
        if flat.startswith(_CAP_TAG):
            key, sep, value = flat[len(_CAP_TAG) :].partition("=")
            key = key.strip().lower()
            if not sep:
                # A bare marker: the help block's begin/end. The begin also
                # shuts the version window, so help text is never a version.
                if key == "log-help-begin":
                    in_help = True
                    after_version_tag = False
                elif key == "log-help-end":
                    in_help = False
                continue
            if sep and key == "rclone-version":
                after_version_tag = True
                if _VERSION_RE.search(value):
                    found["version"] = value.strip()
                continue
            if sep and key in _DETACHER_CANDIDATES and value.strip().lower() in ("yes", "no"):
                found[key] = value.strip().lower()
            continue
        if in_help:
            if flat:
                help_lines.append(flat)
            continue
        if after_version_tag and "version" not in found and _VERSION_RE.search(flat):
            found["version"] = flat
    if help_lines:
        found[_CAP_HELP_KEY] = "\n".join(help_lines)
    return found


def _detect_log_flags(help_text: object) -> tuple[str | None, str | None]:
    """(`log_file_flag`, `log_level_flag`) from rclone's own help words.

    Read from the flag DEFINITIONS `rclone help logging` / `rclone help flags`
    prints — matched by NAME and at the start of a line (`_help_flag_pattern`),
    never from a version table and never as a substring: `--log-file-max-size`
    is not `--log-file`, and the `--loglevel` line is described as "alias for
    --log-level" without that making the modern spelling exist. `--loglevel` is
    accepted only as the ALIAS it is: when — and only when — `--log-level` is
    absent from the definitions.

    Anything unproven comes back as None, meaning "emit nothing", which is the
    safe direction on both counts: no logging flag is harmless because the
    wrapper redirects the job's stdout/stderr into the very log file this
    module reads, while one wrong flag makes rclone print a usage block and
    die before transferring a byte.
    """
    text = str(help_text or "")
    if not text.strip():
        return None, None
    file_flag = _LOG_FILE_FLAG if _LOG_FILE_HELP_RE.search(text) else None
    if _LOG_LEVEL_HELP_RE.search(text):
        level_flag: str | None = _LOG_LEVEL_FLAG
    elif _LOG_LEVEL_ALIAS_HELP_RE.search(text):
        level_flag = _LOG_LEVEL_ALIAS
    else:
        level_flag = None
    return file_flag, level_flag


def _rclone_version_tuple(text: object) -> tuple[int, int] | None:
    """(major, minor) from rclone's own version words, None when unparsable.

    `v1.66.0`, `v1.62.1-32-gabcdef`, `rclone v1.58.0-beta.1` and a bare
    `v1.58` all read fine; garbage reads as unknown, which costs the push
    nothing but one optional flag.
    """
    match = _VERSION_RE.search(str(text or ""))
    if not match:
        return None
    try:
        return (int(match.group(1)), int(match.group(2)))
    except (TypeError, ValueError):
        return None


def _supports_error_on_no_transfer(version: tuple[int, int] | None) -> bool:
    """The version gate for the one OPTIONAL flag: known-new-enough only.

    Unknown is deliberately False: `--error-on-no-transfer` turns a zero-byte
    rc=0 into an error, which is a nicety, while a flag an old rclone does not
    know aborts the copy where it stands. Omitting a nicety is harmless;
    including a fatal flag is not.
    """
    return version is not None and version >= _MIN_RCLONE_NO_TRANSFER_ERROR


def _detacher_refusal(caps: dict[str, str], status: int | None, out: str, err: str) -> str:
    """Why the launch may not go out blind, naming both wrappers we asked for."""
    if status is None:
        said = f"the probe did not run ({_one_line(err) or 'no answer from VPS1'})"
    elif status != 0:
        said = f"the probe exited {status} ({_one_line(err or out)})"
    elif not caps:
        said = f"the probe answered {_one_line(out) or 'nothing at all'}"
    else:
        said = ", ".join(f"{name}={caps.get(name, 'unknown')}" for name in _DETACHER_CANDIDATES)
    return (
        f"push refused: VPS1 has no usable session-surviving wrapper "
        f"(asked for {' or '.join(_DETACHER_CANDIDATES)}; {said}) — the copy "
        "would die with the SSH session, so the push is refused instead of launched blind"
    )


async def preflight(
    request: PushRequest,
    *,
    ssh: object | None,
    cfg: object | None = None,
    remote_name: str = "",
    rclone_bin: str = "rclone",
    timeout_s: float = _PROBE_TIMEOUT_S,
) -> PushCheck:
    """Answer "can this push start on VPS1, right now?" — one line per refusal.

    Cheap checks first (nothing, then paths, then names), SSH probes next,
    and the remote listing last. Every step is best-effort: an unreachable
    VPS1 or an unlistable remote is a failed check with a reason, never an
    exception, because the caller falls back to the normal chain.

    The probes run on the DEDICATED dial (see `_remote_shell`): preflight
    used to lease pooled members, so a saturated racing path turned it into
    `could not list VPS1 rclone remotes: Timeout opening channel` — a refusal
    that cost the push nothing but read as "push unavailable" forever. The
    third one exists because the launch used to be a guess: it asks VPS1 which
    session-surviving wrapper it has, what its own rclone version is, and — read
    out of rclone's own `help logging` / `help flags` words rather than a version
    table — WHICH SPELLINGS its logging flags have. All of it goes on the
    returned check for `start_copy` (and `build_copy_command`) to obey, because
    a flag this rclone does not have kills the copy where it stands, and the 28
    dead launches of the incident were one character of exactly that.
    """
    files = list(getattr(request, "files", ()) or ())
    if not files:
        return _fail("push refused: the request carries no files")
    accepted, why = sanitize_push_files(files)
    if not accepted:
        return _fail(why)
    if why:
        # Partial lists are refused outright: the missing names would never be
        # seeded, and a silently short list looks like a complete push.
        return _fail(why)

    src = str(getattr(request, "src_dir", "") or "")
    dst = str(getattr(request, "target_remote", "") or "")
    for value, label in ((src, "src_dir"), (dst, "target_remote")):
        reason = _unsafe_path_reason(value, label)
        if reason:
            return _fail(reason)

    name = derive_remote_name(dst, remote_name)
    if not name:
        return _fail(
            f"push refused: cannot derive an rclone remote name from {dst!r} "
            "(set remote_name or use remote:path)"
        )
    reason = _unsafe_path_reason(str(rclone_bin or ""), "rclone_bin")
    if reason:
        return _fail(reason)

    if ssh is None:
        return _fail("push refused: no SSH dial to the racing box (source.deluge_sftp)")

    # rclone must exist on VPS1 — this daemon never runs the copy itself.
    status, out, err = await _remote_shell(
        ssh, f"command -v {shlex.quote(str(rclone_bin))}", timeout_s
    )
    if status != 0:
        return _fail(
            f"push refused: rclone {str(rclone_bin)!r} not usable on VPS1 "
            f"(exit {status}): {_one_line(err or out)}"
        )

    # ... and VPS1 must be able to run it DETACHED, with flags it understands.
    # One more dedicated probe, because the launch is exactly where guessing
    # costs an incident: `setsid` missing, an rclone too old for
    # `--error-on-no-transfer` and — the one that actually happened — a logging
    # flag this rclone does not spell all look like a successful launch whose
    # copy never existed. The detacher is mandatory (refuse without it); the
    # version and the flag names only gate OPTIONAL flags (unknown is an
    # omission, never a refusal, and never an invention either).
    status, out, err = await _remote_shell(ssh, _capability_probe_command(rclone_bin), timeout_s)
    caps = _parse_capability_output(out) if status is not None else {}
    detacher = next((name for name in _DETACHER_CANDIDATES if caps.get(name) == "yes"), "")
    if not detacher:
        return _fail(_detacher_refusal(caps, status, out, err))
    version_text = caps.get("version", "")
    version = _rclone_version_tuple(version_text)
    error_on_no_transfer = _supports_error_on_no_transfer(version)
    # The logging flags rclone SAYS it has. No help block (the call errored, or
    # answered nothing) means neither: `_detect_log_flags` returns (None, None),
    # which `build_copy_command` reads as "emit no logging flag at all" — safe,
    # because the wrapper's `>> <log> 2>&1` already puts this run's post-mortem
    # in the file the poll reads. That is the whole asymmetry this probe exists
    # to enforce: omitting a flag costs detail, inventing one costs the copy.
    log_file_flag, log_level_flag = _detect_log_flags(caps.get(_CAP_HELP_KEY, ""))
    flags_text = ", ".join(f or "(none)" for f in (log_file_flag, log_level_flag))
    # Once per push, and only at debug: the operator reads this when a launch
    # is refused or a copy never starts, and must not have to guess what the
    # wrapper was built from.
    log.debug(
        "push preflight: VPS1 wrapper=%s, rclone version=%s, --error-on-no-transfer=%s, "
        "logging flags=%s",
        detacher,
        _one_line(version_text) or "unknown",
        "included" if error_on_no_transfer else "omitted",
        flags_text,
    )

    # ... and it must know the remote under that name.
    status, out, err = await _remote_shell(
        ssh, f"{shlex.quote(str(rclone_bin))} listremotes", timeout_s
    )
    if status != 0:
        return _fail(
            f"push refused: could not list VPS1 rclone remotes (exit {status}): "
            f"{_one_line(err or out)}"
        )
    lines = [line.strip() for line in (out or "").splitlines() if line.strip()]
    if not any(_remote_listing_matches(line, name) for line in lines):
        return _fail(
            f"push refused: VPS1's rclone has no remote {name}: "
            f"(offers {_one_line(', '.join(lines[:8])) or 'nothing'})"
        )

    if cfg is not None:
        # Last: the remote must not already hold every wanted file. That push
        # would transfer nothing and, worse, hide a state bug (the coordinator
        # thinks the content still needs seeding).
        try:
            already = await _remote_has_bytes(cfg, dst, [f.name for f in accepted])
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            already = False
            log.warning("push preflight: remote listing failed for %s: %s", dst, e)
        if already:
            return _fail(
                f"push refused: all {len(accepted)} files are already on the remote "
                f"({dst}) — nothing to copy"
            )

    return PushCheck(
        True,
        "",
        files_total=len(accepted),
        detacher=detacher,
        error_on_no_transfer=error_on_no_transfer,
        log_file_flag=log_file_flag,
        log_level_flag=log_level_flag,
    )


async def start_copy(
    request: PushRequest,
    *,
    ssh: object,
    rclone_bin: str = "rclone",
    transfers: int = 2,
    bwlimit: str = "",
    now: float | None = None,
    write_timeout_s: float = _WRITE_TIMEOUT_S,
    detacher: str = "setsid",
    error_on_no_transfer: bool = True,
    log_file_flag: str | None = _LOG_FILE_FLAG,
    log_level_flag: str | None = _LOG_LEVEL_FLAG,
    proof_delay_s: float | None = None,
) -> PushCheck:
    """Write the file list on VPS1, launch the detached copy and PROVE it ran.

    On success the PushCheck carries `list_path` / `log_path` / `files_total`
    for the caller's checkpoint. Nothing here verifies the TRANSFER: that is
    `remote_progress`, `push_liveness` and the caller's deadline — what it does
    verify, a few seconds after the launch, is that a copy EXISTS
    (`_confirm_launch`), because an accepted exec request was not proof: the
    production run this closes logged 28 `push launched` lines with 282 polls
    reading `has 0/1 file(s)` and zero completed copies, and a manual tap
    answered "push started" while VPS1 showed no process and no traffic.

    A proven dead launch is a refusal that leaves NOTHING behind to clean up
    beyond our own scratch list (the only delete this module issues), so the
    caller settles no live checkpoint and the slot frees immediately. A launch
    this check cannot judge reports `alive (unproven)` in `reason` and stays
    successful — the poll, with its grace, decides later.

    DEAD includes the launch that wrote its own death sentence: a non-empty log
    is not proof of life, it is evidence, and `_confirm_launch` reads it with
    `push_liveness`'s fatal-line table (`_fatal_log_line`). That is precisely
    the incident this closes — rclone answered the bad `--log` flag with
    `Error: unknown flag: --log`, a usage block and exit in 0.4 s, the proof
    saw bytes and called it "alive", `start_copy` returned ok, the checkpoint
    squatted a slot for `timeout_seconds` and the operator was told "push
    started". Now the same reason comes back as a refusal within seconds.

    The list goes out over SFTP on the DEDICATED dial and the copy starts on
    its own throwaway transport, so neither can be starved by (or starve) a
    racing .torrent fetch — the `sftp: write of /tmp/racing-sync-push-….lst
    failed: TimeoutError()` refusals in the production log were pooled writes.
    `detacher`, `error_on_no_transfer` and the two `log_*_flag` names carry what
    `preflight` learned, so the wrapper is one VPS1 has and every flag is one
    its rclone reports, rather than anything this daemon hoped for; a caller
    that never probed gets the verified-correct spellings (`_usable_log_flag`
    drops any name outside the whitelist, so a wrong flag cannot be emitted
    even by accident).
    """
    files = list(getattr(request, "files", ()) or ())
    accepted, why = sanitize_push_files(files)
    if not accepted or why:
        return _fail(why or "push refused: the request carries no files")

    list_path, log_path = _push_scratch_paths(request, now)
    payload = ("\n".join(f.name for f in accepted) + "\n").encode("utf-8")
    # Normalise HERE as well as in `build_copy_command` (idempotent: a value it
    # accepts is already on its whitelist) so the check we hand back reports the
    # flags the launched line actually carries, not whatever we were told.
    file_flag = _usable_log_flag(log_file_flag, _LOG_FILE_FLAG_CHOICES, "log file")
    level_flag = _usable_log_flag(log_level_flag, _LOG_LEVEL_FLAG_CHOICES, "log level")

    try:
        command = build_copy_command(
            request,
            rclone_bin=rclone_bin,
            transfers=transfers,
            bwlimit=bwlimit,
            list_path=list_path,
            log_path=log_path,
            detacher=detacher,
            error_on_no_transfer=error_on_no_transfer,
            log_file_flag=file_flag,
            log_level_flag=level_flag,
        )
    except ValueError as e:
        return _fail(str(e))

    write = getattr(ssh, _WRITE_ATTR, None)
    if not callable(write):
        return _fail(f"push refused: {_no_dial_reason(_WRITE_ATTR)}")
    try:
        written = await asyncio.wait_for(
            asyncio.to_thread(write, list_path, payload),
            timeout=write_timeout_s + _WAIT_MARGIN_S,
        )
    except asyncio.CancelledError:
        raise
    except Exception as e:  # noqa: BLE001
        return _fail(
            f"push refused: could not write the file list on VPS1: {type(e).__name__}: {e}"
        )
    if not written:
        return _fail(f"push refused: VPS1 rejected the file list write at {list_path}")

    launch = getattr(ssh, "launch_detached", None)
    if not callable(launch):
        await _drop_list_file(ssh, list_path)
        return _fail("push refused: the SSH dial cannot launch a detached command")
    try:
        started = await asyncio.wait_for(
            asyncio.to_thread(launch, command), timeout=_WRITE_TIMEOUT_S
        )
    except asyncio.CancelledError:
        raise
    except Exception as e:  # noqa: BLE001
        started = False
        log.warning("push launch failed on VPS1: %s: %s", type(e).__name__, e)
    if not started:
        cleaned, clean_reason = await _drop_list_file(ssh, list_path)
        tail = "" if cleaned else f"; stale list cleanup failed ({clean_reason})"
        return _fail(
            f"push refused: VPS1 did not accept the detached rclone copy (see {log_path}){tail}"
        )

    # The exec request was accepted; that is NOT the copy existing. Ask once,
    # a few seconds later, on the same dedicated dial.
    verdict, note = await _confirm_launch(
        ssh=ssh, list_path=list_path, log_path=log_path, delay_s=proof_delay_s
    )
    if verdict == "dead":
        cleaned, clean_reason = await _drop_list_file(ssh, list_path)
        tail = "" if cleaned else f"; stale list cleanup failed ({clean_reason})"
        log.warning("push launch died: %s: %s", log_path, note)
        return _fail(f"{note}{tail}")
    log.info(
        "push launched: %d files, list %s, log %s (%s)",
        len(accepted),
        list_path,
        log_path,
        note or "proof: alive",
    )
    return PushCheck(
        True,
        # Only an unproven launch has something to say on a success; the
        # caller puts it in the operator's reply, and the poll decides.
        note if verdict == "unproven" else "",
        list_path=list_path,
        log_path=log_path,
        files_total=len(accepted),
        detacher=detacher,
        error_on_no_transfer=error_on_no_transfer,
        log_file_flag=file_flag,
        log_level_flag=level_flag,
    )


async def _drop_list_file(ssh: object, list_path: str) -> tuple[bool, str]:
    """Best-effort `rm -f` of OUR scratch list after a launch that never ran.

    The only delete this module can issue on VPS1, and it only ever touches
    the daemon's own /tmp file — content deletion stays the janitor's job.
    """
    reason = _unsafe_path_reason(list_path, "list_path")
    if reason:
        return False, reason
    status, _out, err = await _remote_shell(
        ssh, f"rm -f {shlex.quote(list_path)}", _PROBE_TIMEOUT_S
    )
    if status == 0:
        return True, ""
    return False, _one_line(err or f"exit {status}")


def _scrub_log_line(line: object) -> str:
    """One safe, bounded, operator-readable line taken from the rclone log."""
    flat = _CONTROL_RE.sub(" ", str(line or ""))
    return " ".join(flat.split())[:200]


def _pgrep_pattern(list_path: str) -> str:
    """The list path as a pattern our OWN probe cannot match.

    `pgrep -f` searches every process's full command line — and the command
    line of the shell running this very probe contains the path it is looking
    for, so a plain pattern would always "find" the copy and prove nothing.
    Spelling the last dot as a bracket expression keeps the regex matching
    rclone's real command line (`…/x.lst`) while the probe's own text
    (`…/x[.]lst`) no longer matches it. The list path is unique per push —
    `_push_scratch_paths` keys it by infohash and the start epoch precisely so
    it can serve as this job's fingerprint.
    """
    text = str(list_path or "")
    head, sep, tail = text.rpartition(".")
    return f"{head}[.]{tail}" if sep else text


def _launch_proof_command(list_path: str, log_path: str) -> str:
    """One bounded shell that answers "did the copy we just launched survive?".

    Two independent pieces of evidence, each on its own tagged line, plus the
    first lines of the job log for the reason:

    * `pgrep -f` on this job's unique list path — but only read as an answer
      when pgrep RAN: `command -v pgrep` first, and pgrep's own exit status
      separates "found" (0) from "looked and did not find" (1) from "could not
      do it" (2+: a syntax error, a `-f` this pgrep does not implement). The
      last case must never be read as a dead copy.
    * the job log: `test -s` (non-empty), `test -f` (there but empty) or
      neither, then its first `_PROOF_LOG_LINES` lines. Since the wrapper
      appends the login shell's output to this very file, a shell-level failure
      makes it non-empty — and since rclone prints a run that never STARTED the
      same way (`Error: unknown flag: …`, then a usage dump, then exit 0.4 s
      later), those lines are read as EVIDENCE by `_confirm_launch` rather than
      as proof of life. "Has bytes" on its own only ever meant "something wrote
      here", which is exactly how 28 dead launches got reported as "push
      started".

    Never a glob, never a directory walk, never the mount, never content.
    """
    pattern = shlex.quote(_pgrep_pattern(list_path))
    log_file = shlex.quote(str(log_path))
    return "; ".join(
        [
            "if command -v pgrep >/dev/null 2>&1; then "
            f"pgrep -f -- {pattern} >/dev/null 2>&1; rc=$?; "
            f"if [ $rc -eq 0 ]; then echo '{_PROOF_TAG}pgrep=found'; "
            f"elif [ $rc -eq 1 ]; then echo '{_PROOF_TAG}pgrep=notfound'; "
            f"else echo '{_PROOF_TAG}pgrep=error'; fi; "
            f"else echo '{_PROOF_TAG}pgrep=missing'; fi",
            f"if test -s {log_file}; then echo '{_PROOF_TAG}log=bytes'; "
            f"elif test -f {log_file}; then echo '{_PROOF_TAG}log=empty'; "
            f"else echo '{_PROOF_TAG}log=absent'; fi",
            f"echo '{_PROOF_TAG}log-begin'",
            f"if test -f {log_file}; then head -n {_PROOF_LOG_LINES} {log_file} 2>/dev/null; fi",
        ]
    )


def _parse_proof_output(text: object) -> tuple[str, str, list[str]]:
    """(pgrep answer, log state, first captured log lines) — "" means unknown.

    Only the probe's OWN tagged lines are trusted, and only the ones ahead of
    the `log-begin` marker: past that marker everything — including a line that
    happens to look like a tag — is captured log content, so a job log can
    never talk its way into a verdict. Anything else in the output is noise.
    """
    pgrep = ""
    log_state = ""
    captured: list[str] = []
    collecting = False
    for line in str(text or "").splitlines():
        flat = line.strip()
        if not collecting and flat.startswith(_PROOF_TAG):
            key, sep, value = flat[len(_PROOF_TAG) :].partition("=")
            key = key.strip()
            if not sep:
                collecting = key == "log-begin"
                continue
            answer = value.strip().lower()
            if key == "pgrep" and answer in ("found", "notfound", "error", "missing"):
                pgrep = answer
            elif key == "log" and answer in ("bytes", "empty", "absent"):
                log_state = answer
            elif key == "log-begin":
                collecting = True
            continue
        if collecting:
            scrubbed = _scrub_log_line(line)
            if scrubbed:
                captured.append(scrubbed)
    return pgrep, log_state, captured[:_PROOF_LOG_LINES]


def _proof_dead_reason(
    list_path: str, log_path: str, pgrep: str, log_state: str, captured: list[str]
) -> str:
    """A failed launch, said in one line: what was checked and what it showed."""
    checks = (
        f"no process matching {_pgrep_pattern(list_path)!r} (pgrep answered {pgrep or 'nothing'}) "
        f"and the job log {log_path} is {log_state or 'unreadable'}"
    )
    if captured:
        return (
            f"push refused: VPS1 accepted the launch but no copy is running "
            f"({checks}); the log says: {' | '.join(captured[:_PROOF_LOG_LINES])}"
        )
    return (
        f"push refused: VPS1 accepted the launch but no copy is running "
        f"({checks}) and nothing was ever written to it"
    )


def _proof_log_death_reason(log_path: str, pgrep: str, fatal: str) -> str:
    """A launch that wrote its own death sentence, said in one readable line.

    The fatal line comes FIRST on purpose: `_fail` caps a reason at 400
    characters, and an operator's reply is worth nothing if the one line that
    explains the failure — `Error: unknown flag: --log` — is what got trimmed.
    Same wording as `push_liveness`'s "failed" verdict, so the phrase an
    operator searches the logs for is the same on both paths.
    """
    return (
        f"push refused: the copy died at once, rclone reported a fatal line: {fatal} "
        f"(job log {log_path}, pgrep answered {pgrep or 'nothing'})"
    )


async def _confirm_launch(
    *, ssh: object, list_path: str, log_path: str, delay_s: float | None
) -> tuple[str, str]:
    """Did the detached copy we just launched actually start? ("alive"|"unproven"|"dead")

    One short confirmation, a few seconds after the launch, on the push's own
    dedicated dial:

    * `("alive", note)` — pgrep found this job's process (process evidence beats
      log evidence, always), OR the job log has bytes that do NOT read as a
      give-up (rclone opened it, or the login shell wrote its failure there —
      either way something ran and the poll can read it).
    * `("dead", reason)` — pgrep ran cleanly and did not find it, and the log is
      absent or empty: nothing is running and nothing would ever be written, and
      the reason names both checks and quotes the first log lines if any. OR the
      log has bytes whose own words are a fatal line by `push_liveness`'s table
      (`_log_verdict`): a non-empty log is NOT proof of life, it is rclone
      talking, and `Error: unknown flag: --log` is it saying it never started.
      That reason — the fatal line first — is what the operator then sees.
    * `("unproven", note)` — anything else, and deliberately NOT death: no
      dial, a wedged or unparsable answer, a pgrep that is not installed or
      errored with no log to fall back on, a log that cannot be READ (absent,
      empty or unreadable content). The existing `push_liveness` poll,
      with its grace window, decides later; missing evidence never kills a
      possibly-live copy, and only a recognised fatal line ever does.

    The wait is an `asyncio.sleep` on the push's own task — bounded,
    cancellable, and holding no pool lease (nothing here leases a member). No
    `delay_s` means the shipped one, read here at call time rather than bound
    into a default argument, so the constant stays the single knob.
    """
    resolved = _LAUNCH_PROOF_DELAY_S if delay_s is None else delay_s
    try:
        delay = max(0.0, min(_LAUNCH_PROOF_DELAY_MAX_S, float(resolved)))
    except (TypeError, ValueError):
        delay = _LAUNCH_PROOF_DELAY_S
    if delay:
        await asyncio.sleep(delay)

    unsafe = _unsafe_path_reason(list_path, "list_path") or _unsafe_path_reason(
        log_path, "log_path"
    )
    if unsafe:
        return "unproven", f"{unsafe} (the launch cannot be checked, so it stays alive)"

    status, out, err = await _remote_shell(
        ssh, _launch_proof_command(list_path, log_path), _PROOF_TIMEOUT_S
    )
    if status is None:
        return (
            "unproven",
            f"cannot tell yet: {_one_line(err) or 'no answer from VPS1'} "
            "(the poll will judge the copy)",
        )

    pgrep, log_state, captured = _parse_proof_output(out)
    if pgrep == "found":
        # PROCESS EVIDENCE BEATS LOG EVIDENCE: a copy VPS1 says is running wins
        # over anything its log says, fatal lines included (a long run can hold
        # a retry that reads like a give-up). Checked first, always.
        return "alive", f"VPS1 has the copy running ({_pgrep_pattern(list_path)})"
    if log_state == "bytes":
        # NOT "a non-empty log means life" any more — that was the second half of
        # the incident. Bytes are EVIDENCE, and the first lines are rclone's own
        # words, so they get judged by the SAME fatal table the poll uses
        # (`_log_verdict` → `_fatal_log_line`). rclone answered the bad flag with
        # `Error: unknown flag: --log` plus a usage dump and exited in 0.4 s: the
        # log had bytes, `start_copy` said ok, the checkpoint squatted a slot and
        # the operator read "push started". Now that same log is a refusal, in
        # the same seconds, on the path that already frees the checkpoint.
        # Only `failed` kills. A closing `finished` summary is left ALONE: a
        # genuinely tiny copy can complete inside this delay, and the poll plus
        # the progress check are the ones that know whether bytes landed.
        verdict, fatal = _log_verdict("\n".join(captured))
        if verdict == "failed":
            return "dead", _proof_log_death_reason(log_path, pgrep, fatal)
        return "alive", f"the job log at {log_path} already has content"
    if pgrep == "notfound" and log_state in ("absent", "empty"):
        return "dead", _proof_dead_reason(list_path, log_path, pgrep, log_state, captured)
    if not pgrep and not log_state:
        return (
            "unproven",
            f"cannot tell yet: the proof answered {_one_line(out) or 'nothing at all'} "
            "(the poll will judge the copy)",
        )
    return (
        "unproven",
        f"alive (unproven): pgrep said {pgrep or 'nothing'} and the log at {log_path} is "
        f"{log_state or 'unreadable'}; the poll will judge it",
    )


def _liveness_probe_command(log_path: str) -> str:
    """The ONE bounded shell that judges a copy: does it exist, how big, tail?

    Reads only our own job log — `test -f`, `wc -c`, `tail -c` on one quoted
    path. Never a glob, never a directory listing, never the rclone mount, and
    never more than the last `_LIVENESS_TAIL_BYTES` of a log a multi-hour copy
    can grow long. The size line carries our own tag so a log line that looks
    like a number can never be read as one.
    """
    quoted = shlex.quote(log_path)
    return (
        f"if test -f {quoted}; then "
        f"printf '%s%s\\n' '{_SIZE_TAG}' \"$(wc -c < {quoted} 2>/dev/null)\"; "
        f"tail -c {_LIVENESS_TAIL_BYTES} {quoted}; "
        f"else printf '%sabsent\\n' '{_SIZE_TAG}'; fi"
    )


def _parse_liveness_output(text: object) -> tuple[bool, int, str]:
    """(usable, size_bytes, tail) from the probe's own output.

    `usable` False means the answer was not the shape we asked for, which the
    caller reads as "unknown" — never as "dead". `size_bytes` is
    `_SIZE_ABSENT` when the log file does not exist and 0 when it is empty.
    """
    lines = str(text or "").splitlines()
    for index, line in enumerate(lines):
        flat = line.strip()
        if not flat.startswith(_SIZE_TAG):
            continue
        value = flat[len(_SIZE_TAG) :].strip()
        tail = "\n".join(lines[index + 1 :])
        if value == "absent":
            return True, _SIZE_ABSENT, tail
        try:
            return True, max(0, int(value)), tail
        except (TypeError, ValueError):
            return False, 0, ""
    return False, 0, ""


def _fatal_log_line(tail: str) -> str:
    """The LAST rclone line saying this run is over with a problem, "" if none.

    Last, not first: a tail can still hold a transient retry from earlier in a
    run that is going fine, and the most recent statement is the one that
    describes the job.
    """
    found = ""
    for line in tail.splitlines():
        flat = _scrub_log_line(line)
        if any(marker in flat for marker in _FATAL_MARKERS):
            found = flat
    return found


def _finished_log_line(tail: str) -> str:
    """The rclone summary line a run writes as it exits, "" when there is none.

    Deliberately the stats keys only (see `_FINISHED_MARKERS`): a per-file
    `Copied (new)` line is NOT evidence, because a two-file copy writes one of
    those while the second file is still in flight.
    """
    found = ""
    for line in tail.splitlines():
        flat = _scrub_log_line(line)
        if any(marker in flat for marker in _FINISHED_MARKERS):
            found = flat
    return found


def _log_verdict(tail: str) -> tuple[str, str]:
    """rclone's own words, judged by ONE table: ("failed"|"finished"|"alive", line).

    The single classifier behind both readers of a job log — `push_liveness`
    (a poll, reading the END of a log that may have run for hours) and
    `_confirm_launch` (a launch proof, reading its FIRST lines seconds after the
    exec was accepted). The same `_FATAL_MARKERS` decide both, so "this line
    means the run gave up" cannot mean one thing to the poll and another to the
    launch — and a launch that died printing a usage error is caught by the
    proof instead of waiting for the first poll to notice an empty remote.

    Nothing here is a second opinion about liveness: a job that is merely SILENT
    — or merely slow, or merely unreadable — is `alive`, always. Only a
    recognised fatal line is death.
    """
    fatal = _fatal_log_line(tail)
    if fatal:
        return "failed", fatal
    summary = _finished_log_line(tail)
    if summary:
        return "finished", summary
    return "alive", ""


async def push_liveness(
    *,
    ssh: object,
    log_path: str,
    age_s: float | None = None,
    grace_s: float = _LIVENESS_GRACE_S,
) -> tuple[str, str]:
    """Is the detached copy behind `log_path` still a job worth waiting for?

    One bounded shell on VPS1 (see `_liveness_probe_command`), over the push's
    own dedicated dial, at most once per push per poll. It reads rclone's log —
    the job's post-mortem, never content — and answers `(verdict, reason)`:

    * `("alive", note)` — anything this function cannot prove: no dial, a
      wedged channel, a non-zero probe exit, output it cannot parse, an absent
      or empty log INSIDE the grace window, or plain silence. The note is for
      the log; the caller keeps the slot.
    * `("missing", reason)` — no log file at all, or a log that is still empty,
      past the grace: the copy never started or died before writing anything.
    * `("failed", reason)` — a fatal line in the tail (`ERROR`, `Error:`,
      `Fatal`, `panic:`, `exit status`, `Failed to`), quoted scrubbed and
      length-capped. THE SAME TABLE the launch proof reads
      (`_confirm_launch` → `_log_verdict`), so a copy that died printing its
      complaint seconds after the launch is refused there rather than here.
    * `("finished", reason)` — the tail holds rclone's closing stats summary
      and no fatal line. The run ended; only the caller knows whether anything
      landed, so it must treat this as death ONLY when no file arrived (a
      summary plus landed files is a job that is about to be handed over).

    WHY SILENCE IS NOT DEATH: the remotes seeded here are Telegram-backed, and
    such a remote creates the destination object only when a large upload
    COMPLETES. A multi-GB copy therefore writes nothing at `--log-level INFO`
    for the better part of an hour and then everything at once — the production
    window this fix was written for saw exactly one `1/1` success 46 minutes
    after launch, after 64 polls of `0/1`. This exists to catch a job that is
    provably gone, never one that is merely slow.

    `age_s` is how long the copy has been running; `None` (unknown) is read as
    "still inside the grace", so an absent log can never be judged from a
    checkpoint without a start stamp. `grace_s` is a couple of polls at the
    shipped cadence; the caller widens it to its own cadence.
    """
    path = str(log_path or "")
    unsafe = _unsafe_path_reason(path, "log_path")
    if unsafe:
        return "alive", f"{unsafe} (the log cannot be read, so the copy stays alive)"

    status, out, err = await _remote_shell(ssh, _liveness_probe_command(path), _LIVENESS_TIMEOUT_S)
    if status is None:
        # Transport trouble says nothing about the copy: VPS1 may be happily
        # uploading while its SSH channel is wedged. Never free a slot on one.
        return "alive", f"liveness unknown: {_one_line(err) or 'no answer from VPS1'}"
    if status != 0:
        return "alive", f"liveness probe exited {status}: {_one_line(err or out)}"

    usable, size, tail = _parse_liveness_output(out)
    if not usable:
        return "alive", "liveness probe answer was unreadable"

    try:
        grace = max(0.0, float(grace_s))
    except (TypeError, ValueError):
        grace = _LIVENESS_GRACE_S
    try:
        age: float | None = None if age_s is None else max(0.0, float(age_s))
    except (TypeError, ValueError):
        age = None
    past_grace = age is not None and age >= grace
    age_text = f"{int(age)}s elapsed" if age is not None else "elapsed age unknown"
    note = f"{age_text}, {int(grace)}s grace"

    if size == _SIZE_ABSENT:
        if not past_grace:
            return "alive", f"no rclone log at {path} yet ({note})"
        return (
            "missing",
            f"the rclone log at {path} never appeared ({note}): the copy did not "
            "start or died before writing anything",
        )
    if size == 0:
        if not past_grace:
            return "alive", f"the rclone log at {path} is still empty ({note})"
        return (
            "missing",
            f"the rclone log at {path} is still empty ({note}): the copy started "
            "and wrote nothing, so it is not running",
        )

    verdict, line = _log_verdict(tail)
    if verdict == "failed":
        return "failed", f"rclone reported a fatal line: {line}"
    if verdict == "finished":
        return "finished", f"the copy wrote its closing summary and nothing since: {line}"
    return ("alive", "")


async def remote_progress(
    cfg: object,
    target_remote: str,
    files: Iterable[PushFile],
    *,
    timeout_s: float = _LIST_TIMEOUT_S,
) -> PushProgress:
    """Metadata-only progress on the seed remote: file counts AND byte totals.

    One recursive ``lsjson --files-only -R`` against the *remote* (never the
    fuse mount, never VPS2's SSD, never a content read, never a hash). The
    listing is name+size in a single pass, so a mid-copy poll is byte-aware
    at no extra remote cost: the completion check stays NAME coverage (the
    previous behaviour), and the sizes are what the coordinator's throughput
    estimate is derived from.

    Why bytes at all: ``lsf`` can only answer "is the name there yet", which
    for the common single-file release reads `0/1` for the whole copy. The
    sizes are the only progress signal a one-file push has.
    """
    accepted = [f for f in (files or ()) if str(getattr(f, "name", "") or "")]
    want = {_norm_listed_path(str(f.name)) for f in accepted}
    want.discard("")
    total = len(want)
    if not total:
        return PushProgress(0, 0)
    remote = str(target_remote or "").strip()
    if not remote or remote.startswith("-"):
        return PushProgress(0, total)
    try:
        result = await run_rclone(
            cfg,  # type: ignore[arg-type]
            [_local_rclone_binary(cfg), "lsjson", "--files-only", "-R", remote],
            timeout=timeout_s,
            tree=None,
        )
    except asyncio.CancelledError:
        raise
    except Exception as e:  # noqa: BLE001
        log.warning("push progress: remote listing of %s failed: %s", remote, e)
        return PushProgress(0, total)
    if getattr(result, "returncode", 1) != 0:
        log.warning(
            "push progress: remote listing of %s exited %s",
            remote,
            getattr(result, "returncode", "?"),
        )
        return PushProgress(0, total)
    sizes = parse_remote_sizes(getattr(result, "stdout", ""))
    listed = set(sizes)
    listed.discard("")
    present = len(want & listed)
    landed, expected = push_byte_totals(accepted, sizes)
    return PushProgress(present, total, bytes_present=landed, bytes_total=expected)


def parse_remote_sizes(text: object) -> dict[str, int]:
    """{normalized remote path: size in bytes} from ``lsjson`` output.

    Metadata the remote already publishes — rclone's own JSON, one entry per
    object, with `Path` and `Size`. Nothing is read, hashed or guessed:

    * malformed / truncated / non-JSON output ⇒ {} (no crash, no bytes);
    * a directory entry, a non-object entry, a missing/blank path ⇒ skipped;
    * a missing, non-numeric, negative or non-finite `Size` ⇒ that entry is
      skipped rather than counted as 0: an unknown size must never be
      invented, and a bogus 0 would drag a byte delta down;
    * paths are normalized exactly like every other listing comparison in
      this module (`_norm_listed_path`), so ``./A/B/ep.mkv`` and
      ``A/B/ep.mkv`` are one object.
    """
    raw = str(text or "").strip()
    if not raw:
        return {}
    try:
        decoded = json.loads(raw)
    except (ValueError, TypeError):
        return {}
    if not isinstance(decoded, list):
        return {}
    sizes: dict[str, int] = {}
    for entry in decoded:
        if not isinstance(entry, dict):
            continue
        if entry.get("IsDir") is True:
            continue
        path = _norm_listed_path(str(entry.get("Path") or entry.get("Name") or ""))
        if not path:
            continue
        value = entry.get("Size")
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            continue
        try:
            size = int(value)
        except (TypeError, ValueError, OverflowError):
            continue
        if size < 0:
            continue
        sizes[path] = size
    return sizes


def push_byte_totals(files: Sequence[PushFile], sizes: dict[str, int]) -> tuple[int, int]:
    """(landed_bytes, expected_bytes) for one push, from metadata alone.

    `expected` is the sum of the wanted files' own sizes — and it is 0 (i.e.
    "unknowable", callers must not build a deadline on it) as soon as ANY
    wanted file carries no size, because a partial expectation would read as
    a copy that is further along than it is.

    `landed` counts only the WANTED names, and only those whose size the
    listing actually reported; an object the push never asked for is somebody
    else's bytes and is ignored. A name that is present but whose size is
    unknown to the parser contributes nothing.
    """
    if not files or not sizes:
        return 0, 0
    expected = 0
    landed = 0
    for item in files:
        name, size = _entry_name_size(item)
        norm = _norm_listed_path(name)
        if not norm:
            continue
        if size <= 0:
            return 0, 0
        expected += size
        # A landed object may be SMALLER than expected (the copy is mid-file
        # on a remote that publishes partial objects). Take what is there,
        # never more than the file's own size: an over-count could push a
        # delta upwards on nothing and train the throughput estimate.
        found = sizes.get(norm)
        if found is not None:
            landed += min(int(found), size)
    return landed, expected


def _local_rclone_binary(cfg: object) -> str:
    """This host's rclone binary (progress lists run here, not on VPS1).

    Falls back to a bare `rclone` for partially built configs/doubles; the
    configured absolute path is what production uses, and `run_rclone`
    validates it the same way the move stage does.
    """
    raw: object = None
    try:
        from .runtime import Runtime

        raw = Runtime(cfg).transfer.binary  # type: ignore[arg-type]
    except Exception:
        raw = None
    if isinstance(raw, (str, Path)) and str(raw).strip():
        return str(raw)
    return "rclone"
