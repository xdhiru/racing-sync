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
* **metadata only.** Nothing here reads or hashes content — not over SSH,
  not against the rclone mount or VPS2's SSD. Progress is a name comparison
  against ``rclone lsf -R --files-only``.
* **no remote paths from untrusted text.** ``target_remote`` always comes
  from ``seed_layout.decide_seed_layout`` (or is handed over already
  computed). Names inside the list are validated by ``rclone_ops``' own
  files-from checks; release/show/group names never reach a path — remote
  scratch files are keyed by the infohash alone.
* **no cadence.** Every function here is one-shot. Polling intervals, state
  records and the concurrency cap belong to the coordinator, and this module
  does not import it (nor ``asyncio.sleep``).

All remote calls go through the SSH dial already configured for torrent
fetches (``source.deluge_sftp``); ``launch_detached`` uses its own dedicated
transport so a multi-hour copy never occupies — and never gets orphaned by —
a pooled member.
"""

from __future__ import annotations

import asyncio
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
    "PushRequest",
    "build_copy_command",
    "preflight",
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

# How much slack past an inner timeout before the coroutine gives up.
_WAIT_MARGIN_S = 20.0

# Shell metacharacters we refuse outright in interpolated paths. `shlex.quote`
# already neutralises them; refusing is the belt that keeps a hostile-looking
# path from ever reaching a command line (and `\` and control chars have no
# business in a POSIX path).
_SHELL_METACHARS = (";", "`", "$", "\\", "\n", "\r")

_HEX_RE = re.compile(r"[0-9a-f]")


@dataclass(frozen=True)
class PushFile:
    """One file the push must carry, named relative to VPS1's `src_dir`."""

    name: str
    size_bytes: int


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
    """

    ok: bool
    reason: str = ""
    list_path: str = ""
    log_path: str = ""
    files_total: int = 0


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


def build_copy_command(
    request: PushRequest,
    *,
    rclone_bin: str,
    transfers: int,
    bwlimit: str,
    list_path: str,
    log_path: str,
) -> str:
    """The one detached command line that runs the copy on VPS1.

    Everything interpolated goes through `shlex.quote`, positionals sit after
    a ``--`` end-of-flags marker (the same convention as rclone_ops' move: it
    shields a ``-``-leading path from being read as a flag), and the whole
    thing is wrapped so the job survives the SSH session dying.

    ``--ignore-existing`` keeps a re-push cheap and, above all, never
    overwrites; ``--log``/``--loglevel INFO`` leaves the post-mortem on VPS1
    because a detached process has no stdout anyone can read.
    """
    src = str(getattr(request, "src_dir", "") or "")
    dst = str(getattr(request, "target_remote", "") or "")
    limit = str(bwlimit or "")
    for value, label in (
        (src, "src_dir"),
        (dst, "target_remote"),
        (str(rclone_bin or ""), "rclone_bin"),
        (str(list_path or ""), "list_path"),
        (str(log_path or ""), "log_path"),
    ):
        reason = _arg_refusal(value, label)
        if reason:
            raise ValueError(reason)
    # bwlimit is rclone syntax ("42M", "8M,24:00-08:00"), not a path: only a
    # line break would corrupt the command, and quoting cannot undo that.
    if any(c in limit for c in ("\n", "\r", "\x00")):
        raise ValueError("push refused: bwlimit contains a control character")
    if not src.strip() or not dst.strip():
        raise ValueError("push refused: src_dir and target_remote are both required")

    try:
        workers = int(transfers)
    except (TypeError, ValueError):
        workers = 4
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
    argv += [
        "--log",
        str(log_path),
        "--loglevel",
        "INFO",
        # Fail loudly when the copy transfers nothing: an rc=0 that moved zero
        # files would look like a finished push. Already-complete pushes are
        # refused earlier by preflight, so a no-transfer here is real trouble.
        # (Needs rclone >= 1.58 on VPS1 — the same flag rclone_ops' move uses.)
        "--error-on-no-transfer",
    ]

    _guard_copy_only(argv)
    quoted = " ".join(shlex.quote(token) for token in argv)
    # `--` before the positionals, then the detach wrapper: setsid gives the
    # copy its own session, </dev/null drops stdin, and the trailing & lets
    # the login shell exit so closing our channel cannot SIGHUP the transfer.
    return f"setsid {quoted} -- {shlex.quote(src)} {shlex.quote(dst)} </dev/null >/dev/null 2>&1 &"


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


async def _remote_shell(ssh: object, command: str, timeout_s: float) -> tuple[int | None, str, str]:
    """One leased remote probe off the event loop; never raises."""
    run = getattr(ssh, "run_shell", None)
    if not callable(run):
        return (None, "", "no SSH dial to the racing box")
    try:
        return await asyncio.wait_for(
            asyncio.to_thread(run, command, timeout_s),
            timeout=timeout_s + _WAIT_MARGIN_S,
        )
    except asyncio.CancelledError:
        raise
    except Exception as e:  # noqa: BLE001
        return (None, "", f"{type(e).__name__}: {e}")


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

    return PushCheck(True, "", files_total=len(accepted))


async def start_copy(
    request: PushRequest,
    *,
    ssh: object,
    rclone_bin: str = "rclone",
    transfers: int = 4,
    bwlimit: str = "",
    now: float | None = None,
    write_timeout_s: float = _WRITE_TIMEOUT_S,
) -> PushCheck:
    """Write the file list on VPS1 and launch the detached copy; never wait.

    On success the PushCheck carries `list_path` / `log_path` / `files_total`
    for the caller's checkpoint. Nothing here verifies the transfer: that is
    `remote_progress` and the caller's deadline.
    """
    files = list(getattr(request, "files", ()) or ())
    accepted, why = sanitize_push_files(files)
    if not accepted or why:
        return _fail(why or "push refused: the request carries no files")

    list_path, log_path = _push_scratch_paths(request, now)
    payload = ("\n".join(f.name for f in accepted) + "\n").encode("utf-8")

    try:
        command = build_copy_command(
            request,
            rclone_bin=rclone_bin,
            transfers=transfers,
            bwlimit=bwlimit,
            list_path=list_path,
            log_path=log_path,
        )
    except ValueError as e:
        return _fail(str(e))

    write = getattr(ssh, "write_remote_file", None)
    if not callable(write):
        return _fail("push refused: the SSH dial cannot write remote files")
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
    log.info("push launched: %d files, list %s, log %s", len(accepted), list_path, log_path)
    return PushCheck(
        True,
        "",
        list_path=list_path,
        log_path=log_path,
        files_total=len(accepted),
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


async def remote_progress(
    cfg: object,
    target_remote: str,
    files: Iterable[PushFile],
    *,
    timeout_s: float = _LIST_TIMEOUT_S,
) -> tuple[int, int]:
    """Metadata-only (present, total) file counts on the seed remote.

    One recursive ``lsf -R --files-only`` against the *remote* (never the
    fuse mount, never VPS2's SSD, never a content read) compared by
    normalized relative name, so nested paths (``Show/Season/ep.mkv``)
    count individually. Sizes travel with the listing's caller: the
    completion check is name coverage, and the byte-level verification stays
    in the caller's verifying stage.
    """
    accepted = [f for f in (files or ()) if str(getattr(f, "name", "") or "")]
    want = {_norm_listed_path(str(f.name)) for f in accepted}
    want.discard("")
    total = len(want)
    if not total:
        return 0, 0
    remote = str(target_remote or "").strip()
    if not remote or remote.startswith("-"):
        return 0, total
    try:
        result = await run_rclone(
            cfg,  # type: ignore[arg-type]
            [_local_rclone_binary(cfg), "lsf", "-R", "--files-only", remote],
            timeout=timeout_s,
            tree=None,
        )
    except asyncio.CancelledError:
        raise
    except Exception as e:  # noqa: BLE001
        log.warning("push progress: remote listing of %s failed: %s", remote, e)
        return 0, total
    if getattr(result, "returncode", 1) != 0:
        log.warning(
            "push progress: remote listing of %s exited %s",
            remote,
            getattr(result, "returncode", "?"),
        )
        return 0, total
    listed = {
        _norm_listed_path(line) for line in str(getattr(result, "stdout", "") or "").splitlines()
    }
    listed.discard("")
    return len(want & listed), total


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
