"""Coordinator wiring for the "push from VPS1" job (`vps1_push` transport).

Why this exists: when no Prowlarr cross-seed turns up in time, the normal
chain leeches the same content again on VPS2's SSD from the private swarm.
A push instead asks VPS1 — which already holds the bytes — for one detached
``rclone copy`` onto the very remote VPS2 seeds from, so the bytes cross the
internet once. From there the EXISTING machinery finishes the work: this
module only starts the copy and keeps the pipeline from doing competing work
while it runs.

Contract this module keeps:

* **inert by default.** Every entry point and every suppression guard is
  gated on `_push_switch_on()` (the literal configured ``enabled = true``,
  never a truthy stand-in) and, for guards, on `_push_is_live(hash)` (the
  store answering exactly ``True``). With the section absent or off nothing
  in the admission path changes, not even the queries it issues.
* **metadata only.** Names, sizes and remote listings. No content is ever
  read or hashed — not over SSH, not against the rclone mount, not on SSD.
* **one shared layout rule.** The target comes from
  ``seed_layout.decide_seed_layout`` with ``batches_total=0``, i.e. an
  episode pushed from VPS1 lands exactly where the same episode moved from
  VPS2 would; that is the whole point of the extraction. The kind it routes
  on is the one the pushed names classify to — a pushed row is parked before
  admission, so its stored ``classification_kind`` is still the schema
  default ``"unknown"`` — and that derived kind is pinned on the row so the
  stages after the push agree with where the bytes actually went.
* **clean refusals.** `start_source_push` returns (started, one-line reason)
  instead of raising, because its callers (a Telegram command, an /act
  button, the indexer-timeout fallback, the janitor-pressure sweep) must all
  be able to fall back to today's behaviour.
* **a slot cannot be squatted.** A poll that sees nothing new asks the copy's
  own rclone log whether the job is still there (`vps1_push.push_liveness`),
  and a provably dead copy is FAILED on that same sweep so the next release
  gets the slot; `timeout_seconds` stays the backstop for anything the log
  cannot prove, and a slow-but-silent big upload is never killed.
* **a launch that never started is said out loud, seconds later.**
  `start_copy` confirms its own launch (`vps1_push._confirm_launch`: this
  job's unique list path in `pgrep`, else the job log's own words judged by
  `push_liveness`'s fatal-line table — a non-empty log is EVIDENCE, not proof
  of life) and a dead one comes back as a refusal — so the "a failed attempt
  leaves no live checkpoint" rule below frees the slot at once instead of
  squatting it for `timeout_seconds`, and the operator's `/push_source` reply
  carries the reason (with the launcher's own captured words) verbatim. What
  the check cannot judge is reported as `alive (unproven)` and left to the
  poll, because missing evidence never kills a possibly-live copy.
* **a broken VPS1 lane is not hammered.** After
  `_PUSH_LAUNCH_FAIL_LIMIT` launch/proof failures in a row for the same
  infohash, the automatic paths (indexer-timeout auto push, janitor-pressure
  sweep) leave that hash alone until the window expires, so a permanently
  broken box does not spend a preflight, a list write, a launch and a proof on
  every poll. A human tap is never deferred: `/push_source` always tries.
* **never on the pooled members.** Preflight, the file-list write and the log
  read all go through the dial's DEDICATED runners, so the push lane can
  neither queue behind a racing .torrent fetch nor starve one.
* **no live checkpoint outlives a failed attempt.** Every branch after
  `claim_push` settles the record (abandoned/failed/done), so a broken push
  can never leave the pipeline suppressing work for nothing.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import logging
import math
import time
from collections.abc import Iterable, Sequence

from .clients.abstract import COMPLETE_THRESHOLD
from .content_keys import content_key
from .coordinator_errors import AbandonedError
from .io_bounds import offload, rpc
from .rclone_ops import _norm_listed_path
from .seed_layout import SeedLayout, decide_seed_layout
from .state import State, TorrentState
from .state_schema import LIVE_PUSH_STATES, PushState
from .torrent_layout import blob_files, common_top, is_flat
from .vps1_push import (
    PushFile,
    PushRequest,
    preflight,
    push_liveness,
    remote_progress,
    sanitize_push_files,
    start_copy,
)

# Keep the historic logger name so log output is unchanged by the split.
log = logging.getLogger("racing_sync.coordinator")

#: Park reason while a push owns the content (see `_push_park_row`). The
#: indexer park path recognises it only through `push_is_live`, so the row
#: re-arms on the push poll cadence instead of hitting the indexer-wait FAILED.
PUSH_PARK_REASON = "push from VPS1 in flight"

#: Row states a push may never take over: VPS2 is already working on the
#: bytes (or already seeding them), so a copy behind its back is wasted
#: traffic at best and a half-moved tree at worst.
_PUSH_UNTOUCHABLE = frozenset({State.DOWNLOADING, State.MOVING, State.RE_ADDING, State.DONE})

#: States that may be abandoned the way the manual /inject preempt does
#: before parking: an SSD attempt that has not produced bytes yet.
_PUSH_PREEMPTABLE = frozenset({State.QUEUED, State.WAITING_DISK})

#: States the handover (evidence -> `_admit_fuse_existing`) may drive.
_PUSH_ADMITTABLE = frozenset({State.NEW, State.WAITING_INDEXER, State.WAITING_DISK})

#: Bound for the in-memory "what this push must deliver" name sets, so a
#: long-lived process cannot grow with the number of pushes ever run.
_PUSH_WANTED_CAP = 500

#: Fallback for a corrupt/absent push cadence or ceiling (seconds).
#: Both equal the documented model defaults of [source.rclone_push] / [client.rclone_push].
_DEFAULT_POLL_S = 120.0
_DEFAULT_TIMEOUT_S = 21600

#: How long a refused pressure push is left alone, as a MULTIPLE of the push
#: poll cadence (no new duration knob — the cadence is already the push's
#: clock). Long enough that a push which can never run (VPS1 holds nothing
#: complete) does not hot-loop every poll; short enough that a refusal which
#: was only "cap full" / a preflight blip is retried within minutes.
_PRESSURE_BACKOFF_MULTIPLIER = 4.0
#: Cap for the refusal map, mirroring `_PUSH_WANTED_CAP`: a long-lived
#: process must not grow with the number of refusals ever seen. Expired
#: entries are pruned on every insert; past the cap the oldest go first.
_PRESSURE_BACKOFF_CAP = 200

#: How many IDENTICAL dead verdicts free a slot. `missing` (no log, or a log
#: nothing ever wrote to) and `failed` (rclone's own fatal line) are conclusive
#: on the first read. `finished` rests on a stats-summary line in the log tail,
#: which is the weakest evidence of the three, so it must repeat: a copy that
#: is really still running either has no summary yet or writes a NEW one whose
#: text differs (elapsed time, byte counts), so its verdict never repeats and
#: it is never killed for being slow.
_LIVENESS_DEATH_COUNTS = {"missing": 1, "failed": 1, "finished": 2}

#: Floor for the "not started yet" grace, in polls: a couple of them, and never
#: under 300 s, so a fast cadence cannot turn a slow rclone start into a
#: false death. No new config key — this is derived from `poll_seconds`.
_LIVENESS_GRACE_POLLS = 2.0
_LIVENESS_GRACE_FLOOR_S = 300.0

#: Bound for the verdict memo, mirroring `_PUSH_WANTED_CAP`.
_LIVENESS_SEEN_CAP = 200

#: Consecutive launch/proof failures for ONE infohash before the automatic
#: paths stop trying it. A copy that VPS1 accepted and did not start is a
#: broken lane, not a transient: every attempt costs a preflight, a version
#: probe, a list write, a launch and a proof, all of it SSH the racing path
#: needs. One retry after a dead launch is fair (a wedged channel, a box
#: mid-reboot); a second one in a row says the lane is broken, and a THIRD
#: attempt would only spend SSH on a VPS1 that has already failed to run the
#: copy twice.
_PUSH_LAUNCH_FAIL_LIMIT = 2

#: Cap for the launch-failure counter, mirroring `_PRESSURE_BACKOFF_CAP` — and
#: pruned the same way `_failed_late_cross_seeds` is: expired entries dropped
#: on every insert, oldest shed first past the cap, so a long-lived daemon
#: never grows with the number of pushes ever run.
_PUSH_LAUNCH_FAIL_CAP = 200

#: How long a hash stays skipped once the counter arms, as a MULTIPLE of the
#: push cadence with a floor (no new config key — the push's own clock is the
#: unit). Longer than the refusal defer, and about a different question: a
#: refusal like "cap full" or "nothing complete yet" is worth another look next
#: poll, while a launch that died is only worth another look after the operator
#: had a chance to notice.
_PUSH_LAUNCH_FAIL_MULTIPLIER = 10.0
_PUSH_LAUNCH_FAIL_FLOOR_S = 600.0

# ---- measured push throughput (no config key: it is OBSERVED) ------------ #

#: Smoothing weight for the byte-delta EMA. Under 0.5 so one unlucky poll
#: (a remote that publishes the whole object at once) cannot outvote the
#: history of every real copy this daemon has run.
_PUSH_THROUGHPUT_ALPHA = 0.3

#: Hard ceiling on ONE sample, in bytes/second (500 MiB/s). The point is not
#: to model a fast link — it is that a copy which "delivered" a terabyte in a
#: poll is a listing artefact, and feeding it through unclamped would make the
#: estimate a rounding error and the push deadline effectively zero.
_PUSH_THROUGHPUT_MAX_BPS = 500.0 * 1024.0 * 1024.0

#: The documented CONSERVATIVE speed used only by `push_seconds_estimate` when
#: nothing has been measured yet: 5 MiB/s is slower than any link this daemon
#: has ever pushed over, so an unmeasured push is always estimated LONG. The
#: indexer-wait deadline never uses it: unknown means no push term at all.
_PUSH_THROUGHPUT_DEFAULT_BPS = 5.0 * 1024.0 * 1024.0

#: Cap for the "this copy was judged dead" memory, mirroring
#: `_PRESSURE_BACKOFF_CAP`: bounded, oldest shed first.
_PUSH_THROUGHPUT_DEAD_CAP = 200

#: How much of the push poll cadence the indexer deadline keeps as slack:
#: TWO polls. A push is only ever observed at a poll boundary, so its real
#: finish time can be a full cadence later than the last sample said, plus
#: one cadence of handover/verification before the bytes are actually VPS2's.
#: No new config key — `poll_seconds` is already the push's clock.
_PUSH_DEADLINE_MARGIN_POLLS = 2.0
#: Floor for that slack, so a fast cadence cannot make the margin vanish.
_PUSH_DEADLINE_MARGIN_FLOOR_S = 300.0


def _one_line(text: object) -> str:
    flat = " ".join(str(text or "").split())
    return flat[:300]


def _optional_count(value: object) -> int | None:
    """A metadata counter a poll could answer, or None when it could not.

    None means "no observation", which the checkpoint stores as "keep the
    last reading" — a 0 would be a lie about a copy that is mid-flight, and a
    lie that goes DOWN is what drags a byte-delta estimate to nothing.
    """
    if value is None:
        return None
    try:
        count = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError, OverflowError):
        return None
    return count if count >= 0 else None


class PushMixin:
    """Push-from-VPS1 job control (duck-typed ``self``; a `Coordinator` base)."""

    # ---- switch + liveness: strict on purpose ---------------------------- #

    def _push_switch_on(self) -> bool:
        """`[source.rclone_push].enabled` read as the CONFIGURED bool only.

        Deliberately not `bool(self.rt.push.enabled)`: the coordinator test
        doubles hand out `MagicMock` configs, and a MagicMock attribute is
        truthy — a truthiness test would let this feature (and every guard
        below) leak into runs that never opted in. The raw field must read
        exactly ``True``, which is also what "section absent" fails to do.
        """
        try:
            section = getattr(getattr(self, "cfg", None), "source", None)
            section = getattr(section, "rclone_push", None)
        except Exception:
            return False
        return getattr(section, "enabled", None) is True

    def _push_auto_enabled(self) -> bool:
        """Prefer a push over the VPS2 leech when the indexer window expires."""
        if not self._push_switch_on():
            return False
        try:
            section = getattr(getattr(self.cfg, "source", None), "rclone_push", None)
            return getattr(section, "auto_on_prowlarr_timeout", None) is True
        except Exception:
            return False

    def _push_pressure_on(self) -> bool:
        """May the janitor's pressure verdict start a push at all?

        Two switches, both read exactly: the master `enabled` (the strict
        config-side check below, so a MagicMock double can never opt in by
        accident) and `push_on_janitor_pressure` through the typed view,
        which already folds the master switch in and reads False when the
        section is absent. Nothing else calls this, so with either off the
        sweep returns before it lists a row or probes the disk.
        """
        if not self._push_switch_on():
            return False
        try:
            return self.rt.push.push_on_janitor_pressure is True
        except Exception:
            return False

    def _push_is_live(self, infohash: str) -> bool:
        """True only while the store says a push for this hash is live.

        The single question every suppression guard asks. Fails closed:
        switch off, no store, no method, an error or anything but a literal
        ``True`` all mean "no push", so today's behaviour is the default.
        """
        if not self._push_switch_on():
            return False
        try:
            probe = getattr(getattr(self, "store", None), "push_is_live", None)
            if not callable(probe):
                return False
            return probe(infohash) is True
        except Exception:
            return False

    def _live_push_hashes(self) -> set[str]:
        """Hashes owning a live checkpoint (state live AND deadline alive).

        Mirrors `StateStore.push_is_live` for the batch case, one SELECT per
        `_INFLIGHT_SNAPSHOT_S` window instead of one per candidate row, and
        no SELECT at all while the switch is off.
        """
        if not self._push_switch_on():
            return set()
        from .coordinator_admit_retry import _INFLIGHT_SNAPSHOT_S

        now_mono = time.monotonic()
        cache = getattr(self, "_live_push_cache", None)
        if isinstance(cache, tuple) and len(cache) == 2:
            try:
                if now_mono - float(cache[0]) < _INFLIGHT_SNAPSHOT_S:
                    return set(cache[1])
            except Exception:
                pass
        hashes: set[str] = set()
        try:
            rows = self.store.list_pushes(states=LIVE_PUSH_STATES)
            now = dt.datetime.now(dt.UTC)
            for p in rows or []:
                try:
                    if p.state not in LIVE_PUSH_STATES:
                        continue
                    if p.deadline_at is not None and now >= p.deadline_at:
                        continue
                    h = (p.infohash or "").strip().lower()
                    if h:
                        hashes.add(h)
                except Exception:
                    continue
        except Exception:
            hashes = set()
        try:
            self._live_push_cache = (now_mono, set(hashes))
        except Exception:
            pass
        return hashes

    def _push_has_live_owner(self, infohash: str) -> bool:
        """Group-dedupe view: does this hash own a live push right now?"""
        h = (infohash or "").strip().lower()
        if not h:
            return False
        return h in self._live_push_hashes()

    # ---- typed settings (each guarded) ---------------------------------- #

    def _push_poll_seconds(self) -> float:
        """Progress-poll cadence; `[...rclone_push].poll_seconds` is the floor."""
        try:
            return float(self.rt.push.poll_seconds)
        except Exception:
            return _DEFAULT_POLL_S

    def _push_timeout(self) -> dt.timedelta:
        """The push's own ceiling; `[...rclone_push].timeout_seconds` bounds it."""
        try:
            return dt.timedelta(seconds=int(self.rt.push.timeout_seconds))
        except Exception:
            return dt.timedelta(seconds=_DEFAULT_TIMEOUT_S)

    def push_deadline_margin_seconds(self) -> float | None:
        """Slack the indexer wait keeps for a push: two poll cadences.

        Derived from `poll_seconds` alone (no new key), floored so a fast
        cadence cannot make it vanish. None — never a small number — when the
        cadence itself is unreadable, which makes the caller drop the push
        term instead of guessing at the margin.
        """
        try:
            return max(
                _PUSH_DEADLINE_MARGIN_FLOOR_S,
                _PUSH_DEADLINE_MARGIN_POLLS * float(self._push_poll_seconds()),
            )
        except Exception:
            return None

    def _push_max_concurrent(self) -> int:
        try:
            return max(1, int(self.rt.push.max_concurrent_pushes))
        except Exception:
            return 1

    def _push_rclone_bin(self) -> str:
        try:
            return str(self.rt.push.rclone_bin or "rclone")
        except Exception:
            return "rclone"

    def _push_remote_name(self) -> str:
        try:
            return str(self.rt.push.remote_name or "")
        except Exception:
            return ""

    def _push_dial(self) -> object | None:
        """The DEDICATED SSH dial the push runs on (`self.sftp`), or None.

        `rt.push.ssh_dial` re-checks the config side (an enabled section is
        validated against an enabled `deluge_sftp`); the object is the pooled
        exporter the coordinator already holds, but the push only ever uses
        its non-pooled half — `run_shell_dedicated` / `write_remote_file_
        dedicated` / `launch_detached`. A dial that cannot answer on its own
        transport is refused here rather than quietly sharing a member with
        the racing .torrent fetches (see `sftp_source`).
        """
        try:
            if self.rt.push.ssh_dial is None:
                return None
        except Exception:
            return None
        dial = getattr(self, "sftp", None)
        if dial is None:
            return None
        for attr in ("run_shell_dedicated", "write_remote_file_dedicated"):
            if not callable(getattr(dial, attr, None)):
                return None
        return dial

    # ---- entry point ---------------------------------------------------- #

    async def start_source_push(
        self, ts: TorrentState, *, reason: str = "manual"
    ) -> tuple[bool, str]:
        """Ask VPS1 to copy the bytes it holds straight onto the seed remote.

        Returns ``(started, one-line reason)``. Started means a detached copy
        is running on VPS1, the checkpoint is live (so the admission path
        suppresses its own competing work) and the row is parked waiting for
        its own bytes. Anything else is a refusal that changes nothing.
        """
        try:
            return await self._start_source_push(ts, reason)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            log.warning("push: unexpected failure (%s)", e)
            return False, f"push refused: unexpected {type(e).__name__}: {_one_line(e)}"

    async def _start_source_push(self, ts: TorrentState, reason: str) -> tuple[bool, str]:
        h = (getattr(ts, "source_infohash", "") or "").strip().lower()
        display = (getattr(ts, "source_name", "") or h[:10])[:60]

        # (a) master switch.
        if not self._push_switch_on():
            return False, "push refused: rclone_push.enabled = false"
        if len(h) != 40:
            return False, "push refused: the row has no VPS1 infohash to key the push on"
        if reason != "manual":
            # A lane that died on this hash twice in a row is not worth another
            # preflight, list write, launch and proof this poll. Checked BEFORE
            # any remote work (and before the claim), so the throttle costs
            # nothing; it only ever defers the automatic paths — an operator's
            # tap always tries.
            deferred = self._push_launch_throttled(h)
            if deferred:
                return False, deferred
        store = getattr(self, "store", None)
        if store is None or not callable(getattr(store, "claim_push", None)):
            return False, "push refused: no state store to keep the checkpoint in"
        # Rows VPS2 is already working (or already seeding) are refused before
        # anything is claimed or launched: no history, no scratch files, no
        # copy competing with a download that is already paid for.
        state = getattr(ts, "state", None)
        if state in _PUSH_UNTOUCHABLE:
            label = getattr(state, "value", state)
            return False, f"push refused: VPS2 is already {label} for this content"
        if self._push_is_live(h):
            return False, "push refused: a push for this group is already running"

        # (b) the LIVE VPS1 torrent — the push copies what the racing client
        # holds now, so its own view is the only proof worth acting on.
        live = await self._push_source_torrent(h)
        if live is None:
            return False, f"push refused: VPS1 no longer has {display}"

        blob = await self._push_racing_blob(h, ts)
        if not blob:
            return False, "push refused: no .torrent bytes for this hash (file names unknown)"

        # (c)+(e) the files VPS1 can actually deliver.
        files, why = await self._push_complete_files(h, live, blob)
        if not files:
            return False, why

        # (d) VPS1's download dir: the parent the blob-relative names live under.
        src_dir = self._push_src_dir(live, blob)
        if not src_dir:
            return False, "push refused: VPS1 reports no usable download directory"

        # (f) THE SAME layout the move stage would choose for these names.
        layout = await self._push_seed_layout(ts, files)
        target_remote = str(getattr(layout, "target_remote", "") or "")
        if not target_remote:
            return False, "push refused: no seed remote configured ([remote] / rclone)"

        # Cap first (a push is bulk traffic on the link the race needs), then
        # (g) the atomic claim: one conditional upsert, so two taps — or the
        # auto path racing a manual one — can only ever launch one copy.
        cap = self._push_max_concurrent()
        running = len(self._live_push_hashes())
        if running >= cap:
            return False, f"push refused: {running}/{cap} push(es) already running"
        ssh = self._push_dial()
        if ssh is None:
            return False, (
                "push refused: no dedicated SSH dial to VPS1 "
                "([client.deluge_sftp] enabled, run_shell_dedicated)"
            )

        request = PushRequest(
            infohash=h,
            group_key=self._push_group_key(ts),
            content_name=display,
            src_dir=src_dir,
            target_remote=target_remote,
            files=tuple(files),
        )
        expected_bytes = self._push_expected_bytes(files)
        deadline_at = dt.datetime.now(dt.UTC) + self._push_timeout()
        try:
            claimed = await offload(
                store.claim_push,
                h,
                group_key=request.group_key,
                content_name=request.content_name,
                src_dir=src_dir,
                target_remote=target_remote,
                deadline_at=deadline_at,
                files_total=len(files),
                bytes_total=expected_bytes,
            )
        except Exception as e:  # noqa: BLE001
            return False, f"push refused: could not claim the checkpoint ({_one_line(e)})"
        if claimed is None:
            return False, "push refused: a push for this group is already running"
        # This hash is live now: drop the memoized live set (rather than
        # planting an empty one) so the concurrency cap and the group-dedupe
        # owner check re-read the table on their next question.
        try:
            self._live_push_cache = None
        except Exception:
            pass

        check = await preflight(
            request,
            ssh=ssh,
            cfg=getattr(self, "cfg", None),
            remote_name=self._push_remote_name(),
            rclone_bin=self._push_rclone_bin(),
        )
        if not check.ok:
            # Nothing ever started: history, not a live suppression.
            await self._push_settle(store, h, PushState.ABANDONED, check.reason)
            return False, check.reason

        # What preflight PROVED VPS1 has, obeyed when the line is built: the
        # wrapper that exists there, the optional flag only for an rclone that
        # accepts it, and the SPELLINGS of the two logging flags that rclone's
        # own help text reported. Whitelisted, because a launch is exactly where
        # an unproven token costs an incident — the 28 dead copies of the last
        # one were one wrong character (`--log`, where rclone says `--log-file`)
        # — and a caller that never probed keeps the verified defaults, which
        # `build_copy_command` still filters through its own whitelist.
        detacher = str(getattr(check, "detacher", "") or "").strip()
        if detacher not in ("setsid", "nohup"):
            detacher = "setsid"
        started = await start_copy(
            request,
            ssh=ssh,
            rclone_bin=self._push_rclone_bin(),
            transfers=self._push_transfers(),
            bwlimit=self._push_bwlimit(),
            detacher=detacher,
            error_on_no_transfer=getattr(check, "error_on_no_transfer", True) is not False,
            log_file_flag=getattr(check, "log_file_flag", "--log-file"),
            log_level_flag=getattr(check, "log_level_flag", "--log-level"),
        )
        if not started.ok:
            await self._push_settle(store, h, PushState.FAILED, started.reason)
            self._note_push_launch_failure(h, started.reason)
            return False, started.reason

        # A launch that runs is a fresh opinion about this group: forget that
        # an earlier copy of it was ever judged dead, so its byte deltas are
        # eligible to train the throughput estimate again.
        self._push_throughput_alive(h)
        # The copy is detached and running: stamp the scratch paths + counters
        # the transport generated, then hand the row over. A launch that
        # started ends the failure streak, so a lane that was broken and got
        # fixed is eligible again at once.
        self._forget_push_launch_failure(h)
        try:
            await offload(
                store.start_push,
                h,
                group_key=request.group_key,
                content_name=request.content_name,
                src_dir=src_dir,
                target_remote=target_remote,
                list_path=started.list_path,
                log_path=started.log_path,
                deadline_at=deadline_at,
                files_total=started.files_total or len(files),
                bytes_total=expected_bytes,
            )
        except Exception as e:  # noqa: BLE001
            log.warning("push: could not stamp the checkpoint for %s: %s", h[:10], e)
        self._remember_wanted(h, files)
        # Hand the row to the push (parked, on the poll cadence). If the row
        # cannot be parked cleanly the copy still runs on VPS1, but nothing
        # may be suppressed for it: settle the checkpoint as abandoned so the
        # pipeline picks up exactly where it was today.
        parked, park_why = await self._push_park_row(ts)
        if not parked:
            await self._push_settle(store, h, PushState.ABANDONED, park_why)
            return False, park_why
        # An unproven launch says so in the transport's own one-liner, and that
        # line goes ON the reply: "it started" and "we could not prove it
        # started" must not read the same to the operator. Nothing is killed on
        # missing evidence — `push_liveness` judges it later.
        note = f" ({_one_line(started.reason)})" if started.reason else ""
        if note:
            log.warning(
                "push: %s started unproven: %s",
                h[:10],
                _one_line(started.reason),
            )
        log.info(
            "push from VPS1 started for %s (%d file(s) -> %s; %s)",
            display,
            len(files),
            target_remote,
            reason or "manual",
        )
        return True, f"pushing {len(files)} file(s) from VPS1 to {target_remote}{note}"

    # ---- step helpers --------------------------------------------------- #

    def _push_group_key(self, ts: TorrentState) -> str:
        try:
            name, size = content_key(
                getattr(ts, "source_name", "") or "", int(getattr(ts, "total_bytes", 0) or 0)
            )
            return f"{name}|{size}"
        except Exception:
            return ""

    @staticmethod
    def _push_expected_bytes(files: Sequence[PushFile]) -> int:
        """The payload this push owes the remote, in bytes, from the torrent.

        Stamped on the checkpoint at launch so `bytes_total` is meaningful
        from the first poll. 0 (unknowable) whenever any wanted file carries
        no size — a partial sum would read as a copy that is nearly done,
        and a push estimate must never be built on an invented denominator.
        """
        total = 0
        try:
            for item in files or ():
                size = getattr(item, "size_bytes", None)
                if size is None:
                    size = getattr(item, "size", 0)
                size = int(size or 0)
                if size <= 0:
                    return 0
                total += size
        except (TypeError, ValueError, AttributeError):
            return 0
        return total

    def _push_transfers(self) -> int:
        try:
            return int(self.rt.push.transfers)
        except Exception:
            # The shipped default, never rclone's: a copy must not take the
            # uplink the races are run on just because a setting was unreadable.
            return 2

    def _push_bwlimit(self) -> str:
        try:
            return str(self.rt.push.bwlimit or "")
        except Exception:
            return ""

    async def _push_source_torrent(self, infohash: str):
        """The live VPS1 `Torrent` for this hash, or None."""
        try:
            live = await self._list_source_torrents()
        except Exception as e:  # noqa: BLE001
            log.info("push: cannot list VPS1 torrents: %s", _one_line(e))
            return None
        for t in live or []:
            try:
                found = (getattr(t, "infohash", "") or getattr(t, "hash", "") or "").lower()
                if found == infohash:
                    return t
            except Exception:
                continue
        return None

    async def _push_racing_blob(self, infohash: str, ts: TorrentState) -> bytes | None:
        """The racing torrent's own .torrent bytes: the file-name authority.

        Same order the rest of the pipeline uses (row copy -> blob table ->
        export). Bytes that do not decode to THIS infohash are rejected:
        these names go to a remote shell and then onto the seed remote, so
        they must be this torrent's names.
        """
        candidates: list[bytes] = []
        for raw in (
            getattr(ts, "_blob", None),
            getattr(ts, "cross_seed_blob", None),
        ):
            if isinstance(raw, (bytes, bytearray)) and raw:
                candidates.append(bytes(raw))
        store = getattr(self, "store", None)
        for attr in ("get_racing_blob", "get_blob"):
            getter = getattr(store, attr, None)
            if not callable(getter):
                continue
            try:
                raw = await offload(getter, infohash)
            except Exception:
                raw = None
            if isinstance(raw, (bytes, bytearray)) and raw:
                candidates.append(bytes(raw))
        for blob in candidates:
            if self._blob_matches_infohash(blob, infohash) is True:
                return blob
        try:
            fetched = await self._load_racing_torrent_bytes(infohash)
        except Exception as e:  # noqa: BLE001
            log.debug("push: blob fetch failed for %s: %s", infohash[:10], e)
            return None
        if not fetched or self._blob_matches_infohash(bytes(fetched), infohash) is not True:
            return None
        # Keep it: the later re-add needs these very bytes.
        try:
            if store is not None and callable(getattr(store, "set_racing_blob", None)):
                await offload(store.set_racing_blob, infohash, bytes(fetched))
        except Exception:
            pass
        return bytes(fetched)

    def _push_expected_files(self, blob: bytes) -> list[tuple[str, int]]:
        out: list[tuple[str, int]] = []
        for entry in blob_files(blob) or []:
            name = str(getattr(entry, "name", "") or "").strip()
            if not name:
                continue
            try:
                size = max(0, int(getattr(entry, "size_bytes", 0) or 0))
            except (TypeError, ValueError):
                size = 0
            out.append((name, size))
        return out

    async def _push_client_files(self, infohash: str) -> list | None:
        """The racing client's per-file entries, or None when it cannot tell."""
        client = getattr(self, "source_client", None)
        getter = getattr(client, "get_torrent_files", None)
        if not callable(getter):
            return None
        try:
            files = await rpc(
                getter(infohash), getattr(self, "cfg", None), "push source file progress"
            )
        except Exception as e:  # noqa: BLE001
            log.debug("push: no per-file progress for %s: %s", infohash[:10], e)
            return None
        return list(files or []) or None

    async def _push_complete_files(
        self, infohash: str, live, blob: bytes
    ) -> tuple[list[PushFile], str]:
        """The blob's names, intersected with what VPS1 provably HOLDS.

        Completeness, never size: Deluge and qBittorrent preallocate every
        file of a torrent, so on a partial torrent `size_bytes` already reads
        as the final size and a size check would happily push holes onto the
        seed remote. The only proof is the client's per-file progress
        (Deluge `file_progress`, qBittorrent's `/torrents/files` `progress`),
        both normalised to 0..1 on `TorrentFile.progress`. Therefore:

        * client reports the whole torrent complete (torrent-level progress,
          i.e. it has hashed every piece) -> the full blob list is pushable;
        * partial torrent -> the per-file list is mandatory, and only entries
          at `COMPLETE_THRESHOLD` count. No list at all, or a list with no
          complete entry ("no meaningful subset"), refuses the push outright
          rather than guessing: an empty/partial list is a permanently
          incomplete seed. A subset that is real but small is allowed and
          bounded by the push deadline — the handover only admits the row when
          the seed path itself verifies every expected file.
        """
        expected = self._push_expected_files(blob)
        if not expected:
            return [], "push refused: the .torrent bytes carry no file list"
        try:
            whole = bool(live.is_complete())
        except Exception:
            whole = False
        if whole:
            return self._sanitize_push_files([PushFile(n, s) for n, s in expected])

        client_files = await self._push_client_files(infohash)
        if not client_files:
            return [], (
                "push refused: VPS1's copy is incomplete and the client reports no "
                "per-file progress (a size check would lie)"
            )
        by_name, by_base = self._push_file_index(client_files)
        complete: list[PushFile] = []
        for name, size in expected:
            entry = self._push_match_entry(name, size, by_name, by_base)
            if entry is None:
                continue
            try:
                progress = float(getattr(entry, "progress", 0.0) or 0.0)
            except (TypeError, ValueError):
                progress = 0.0
            if progress >= COMPLETE_THRESHOLD:
                complete.append(PushFile(name, size))
        if not complete:
            return [], ("push refused: VPS1's copy is incomplete and no file of it is complete yet")
        return self._sanitize_push_files(complete)

    @staticmethod
    def _sanitize_push_files(files: Sequence[PushFile]) -> tuple[list[PushFile], str]:
        accepted, why = sanitize_push_files(files)
        if not accepted:
            return [], _one_line(why)
        if why:
            # The transport's own rule: a list that lost an entry is a list
            # with a permanently missing file, so never send half a release.
            return [], _one_line(why)
        return accepted, ""

    @staticmethod
    def _push_file_index(client_files: Iterable[object]) -> tuple[dict, dict]:
        """(by-normalized-name, by-(basename,size)) for VPS1's own file list."""
        by_name: dict[str, object] = {}
        by_base: dict[tuple[str, int], list] = {}
        for entry in client_files or ():
            name = _norm_listed_path(str(getattr(entry, "name", "") or ""))
            if not name:
                continue
            by_name.setdefault(name.lower(), entry)
            try:
                size = int(getattr(entry, "size_bytes", 0) or 0)
            except (TypeError, ValueError):
                size = 0
            by_base.setdefault((name.rsplit("/", 1)[-1].lower(), size), []).append(entry)
        return by_name, by_base

    @staticmethod
    def _push_match_entry(name: str, size: int, by_name: dict, by_base: dict) -> object | None:
        """VPS1's entry for a blob-relative name, tolerant of the root folder.

        A client may spell the same file with or without the torrent's top
        folder, so the lookup tries the exact path, then each suffix of it,
        then a unique (basename, size) pair. Never a prefix match: that
        could pair ``a/b.mkv`` with some other tree's ``b.mkv``.
        """
        norm = _norm_listed_path(name)
        if not norm:
            return None
        parts = norm.split("/")
        for cut in range(len(parts)):
            found = by_name.get("/".join(parts[cut:]).lower())
            if found is not None:
                return found
        hits = by_base.get((parts[-1].lower(), int(size or 0)), [])
        return hits[0] if len(hits) == 1 else None

    def _push_src_dir(self, live, blob: bytes) -> str:
        """VPS1's download dir: the parent the blob-relative names resolve under.

        Deluge's `save_path` IS the download dir. The qBittorrent adapter
        already resolves `content_path` into that dir (see
        `clients.qbittorrent._torrent_from_qb`); walking up here is the belt
        for a client that reports the content itself — the top folder of a
        wrapped torrent or the single media file — because copying from
        `<dir>/<Top>` with names that already carry `<Top>` would look for
        `<Top>/<Top>/…` and silently transfer nothing. The row's own
        `TorrentState.save_path` is never used: it names VPS2's SSD staging
        dir, not VPS1's tree.
        """
        raw = str(getattr(live, "save_path", "") or "").strip()
        if not raw:
            return ""
        posix = raw.replace("\\", "/").rstrip("/")
        if not posix or posix in ("/", "."):
            return ""
        names = [n for n, _s in self._push_expected_files(blob)]
        base = posix.rsplit("/", 1)[0] if "/" in posix else ""
        leaf = posix.rsplit("/", 1)[-1]
        parent = (base or "/") if base else ""
        top = common_top(names)
        if top and leaf == top:
            return parent
        if leaf in {_norm_listed_path(n).rsplit("/", 1)[-1] for n in names}:
            return parent
        return posix

    async def _push_seed_layout(self, ts: TorrentState, files: Sequence[PushFile]) -> SeedLayout:
        """`decide_seed_layout` with `batches_total=0` — the move stage's rule.

        Mirrors `MoveMixin._seed_layout_for` (same wrapper pre-check, same
        `seed_subdir` pinning) with the push's one difference: there are no
        SSD batches, so `batches_total=0` and the names come from the blob.
        An episode therefore lands in `remote_unsorted` with the same pinned
        top folder as an episode moved from VPS2 would.

        The kind is the one the pushed bytes imply, never the one the row
        happens to carry (see `_push_resolve_kind`): a pushed row is parked in
        WAITING_INDEXER BEFORE admission, so its stored `classification_kind`
        is the schema default "unknown", and routing on that would send every
        push — a movie included — to the unsorted remote.
        """
        names = [f.name for f in files]
        kind = self._push_resolve_kind(ts, files)
        pinned = str(getattr(ts, "seed_subdir", "") or "").strip()
        racing_wrapper = ""
        if not common_top(names) and is_flat(names) and not pinned:
            racing_wrapper = await self._racing_wrapper_folder(ts)
        layout = decide_seed_layout(
            kind=kind,
            target_names=names,
            batches_total=0,
            pinned_seed_subdir=pinned,
            racing_wrapper=racing_wrapper,
            remote_default=self._push_remote_default(),
            remote_unsorted=self._push_remote_unsorted(),
        )
        await self._pin_seed_subdir(ts, layout.seed_subdir)
        return layout

    def _push_resolve_kind(self, ts: TorrentState, files: Sequence[PushFile]) -> str:
        """The routing kind for this push, deriving and pinning it when needed.

        `classification_kind` is only pinned by admission/move
        (`coordinator_admit_fuse` / `coordinator_move`), so a row that is
        waiting for an indexer — which is exactly what a push takes over —
        still carries the schema default "unknown". `route_kind_is_sorted`
        reads that as unsorted, so the push would misroute the content the
        row's own bytes describe. Derive the kind from the very file list
        being pushed, with the classifier the fuse path uses, and write it
        back so the later stages agree with where the bytes went instead of
        re-deriving from a different listing.

        Fail-closed: nothing usable (a raise, an empty list, "unknown") keeps
        today's answer — "unknown", i.e. the unsorted remote — and a row that
        already carries a real kind is never reclassified.
        """
        stored = str(getattr(ts, "classification_kind", "") or "").strip().lower()
        if stored not in ("", "unknown"):
            return stored
        derived = self._push_classify_kind(ts, files)
        if not derived:
            return "unknown"
        self._push_pin_kind(ts, derived)
        return derived

    def _push_classify_kind(self, ts: TorrentState, files: Sequence[PushFile]) -> str:
        """The classifier's kind for these names, "" when it has no usable answer."""
        try:
            # Patched in tests as racing_sync.coordinator.classify; resolve
            # through the coordinator namespace (the fuse/move precedent).
            from . import coordinator as _coordinator

            derived = str(_coordinator.classify(files, getattr(self, "cfg", None)).kind or "")
        except Exception as e:  # noqa: BLE001
            log.debug(
                "push layout: no classification for %s, routing as unknown: %s",
                str(getattr(ts, "source_infohash", ""))[:10],
                e,
            )
            return ""
        derived = derived.strip().lower()
        return "" if derived == "unknown" else derived

    def _push_pin_kind(self, ts: TorrentState, kind: str) -> None:
        """Pin a derived kind on the row; best-effort, never raises into the push.

        Narrow and version-guarded, exactly like `_pin_seed_subdir` right
        below: this runs after awaits, and a whole-row upsert would replay a
        stale snapshot over a concurrent transition (and invalidate its own
        revision). Losing the race only means the next stage re-derives.
        """
        try:
            ts.classification_kind = kind
            store = getattr(self, "store", None)
            if not callable(getattr(store, "update_columns", None)):
                return
            rev = store.update_columns(
                ts.source_infohash, {"classification_kind": kind}, expected_version=ts.version
            )
            if rev:
                ts.version = rev
        except Exception:  # noqa: BLE001
            pass

    def _push_remote_default(self) -> str:
        try:
            return str(self.rt.transfer.remote_default or "")
        except Exception:
            return ""

    def _push_remote_unsorted(self) -> str:
        try:
            return str(self.rt.transfer.remote_unsorted or "")
        except Exception:
            return ""

    async def _push_park_row(self, ts: TorrentState) -> tuple[bool, str]:
        """Hand the row over to the push: parked WAITING_INDEXER on the cadence.

        Legal straight from NEW/WAITING_INDEXER. A QUEUED/WAITING_DISK row may
        NOT enter WAITING_INDEXER (see `state_schema.ALLOWED`), so it first
        gets the abandon the manual /inject preempt performs — reservation
        released through the same helper, dest entry removed — and then the
        FAILED -> NEW walk; when that is not possible cleanly the caller
        refuses, because a half-parked row would both download and be
        suppressed. `transition` is never allowed to raise into the caller.
        """
        state = getattr(ts, "state", None)
        if state in _PUSH_UNTOUCHABLE:
            label = getattr(state, "value", state)
            return False, f"push refused: VPS2 is already {label} for this content"
        if state in _PUSH_PREEMPTABLE:
            try:
                await self._injectfuse_preempt_ssd(ts)
                self.transition(
                    ts,
                    State.FAILED,
                    error="push from VPS1 took over; the SSD attempt was abandoned",
                )
                self.transition(ts, State.NEW)
            except (ValueError, AbandonedError) as e:
                return False, f"push refused: could not abandon the SSD attempt ({_one_line(e)})"
        try:
            self._park_for_indexer_retry(ts, reason=PUSH_PARK_REASON)
        except (ValueError, AbandonedError) as e:
            return False, f"push refused: could not park the row ({_one_line(e)})"
        return True, ""

    async def _push_settle(
        self, store, infohash: str, state: PushState, last_error: str = ""
    ) -> None:
        """Best-effort finish; never raises (checkpoints are not ledgers)."""
        try:
            if callable(getattr(store, "finish_push", None)):
                await offload(store.finish_push, infohash, state, last_error=last_error)
        except Exception as e:  # noqa: BLE001
            log.warning("push: could not settle %s to %s: %s", infohash[:10], state.value, e)
        # The table changed either way: let the next question re-read it
        # instead of honouring a stale (suppressing) memo.
        try:
            self._live_push_cache = None
        except Exception:
            pass
        if state not in LIVE_PUSH_STATES:
            self._forget_wanted(infohash)
            self._forget_liveness(infohash)

    def _remember_wanted(self, infohash: str, files: Sequence[PushFile]) -> None:
        """What this push is accountable for (names, never content)."""
        try:
            cache = getattr(self, "_push_wanted", None)
            if not isinstance(cache, dict):
                cache = {}
                self._push_wanted = cache  # type: ignore[attr-defined]
            cache[(infohash or "").lower()] = tuple(files)
            if len(cache) > _PUSH_WANTED_CAP:
                for old in list(cache.keys())[: len(cache) - _PUSH_WANTED_CAP]:
                    cache.pop(old, None)
        except Exception:
            pass

    def _forget_wanted(self, infohash: str) -> None:
        """Drop the wanted-name set for a push that is over."""
        try:
            cache = getattr(self, "_push_wanted", None)
            if isinstance(cache, dict):
                cache.pop((infohash or "").lower(), None)
        except Exception:
            pass

    # ---- launch-failure throttle (do not hammer a broken lane) ----------- #

    def _push_launch_fail_window(self) -> float:
        """The skip window (seconds) once a hash has failed to START repeatedly."""
        try:
            return max(
                _PUSH_LAUNCH_FAIL_FLOOR_S,
                float(self._push_poll_seconds()) * _PUSH_LAUNCH_FAIL_MULTIPLIER,
            )
        except Exception:
            return _PUSH_LAUNCH_FAIL_FLOOR_S

    def _push_launch_throttled(self, infohash: str) -> str:
        """A refusal line while this hash is being left alone; "" when it may try.

        Deliberately NOT the pressure refusal defer (`_push_pressure_backed_off`)
        that already exists: that one parks ANY refusal — cap full, nothing
        complete, a preflight blip — for a few polls, and only the pressure
        sweep consults it. This one counts consecutive failures to get a copy
        RUNNING (launch refused, or the proof found no process and no log),
        which means the VPS1 lane itself is broken, and it gates both automatic
        paths from inside `start_source_push` so the indexer-timeout auto push
        is covered next to the sweep. Two different questions, two maps; a
        manual tap answers to neither.
        """
        h = (infohash or "").strip().lower()
        if not h:
            return ""
        try:
            memo = getattr(self, "_push_launch_failures", None)
            if not isinstance(memo, dict):
                return ""
            entry = memo.get(h)
            if not isinstance(entry, tuple) or len(entry) != 4:
                return ""
            count, armed, seen, why = entry
            now = time.monotonic()
            armed_at = float(armed)
            if armed_at > 0.0 and now < armed_at:
                left = int(armed_at - now) + 1
                return (
                    f"push refused: VPS1 failed to start the last {int(count)} push(es) for "
                    f"this group ({_one_line(why)}); not trying again for another {left}s"
                )
            # Window passed (or never armed and now stale): the streak is
            # forgotten, so the next attempt starts from zero.
            if armed_at <= 0.0 and now - float(seen) < self._push_launch_fail_window():
                return ""
            memo.pop(h, None)
            return ""
        except Exception:
            return ""

    def _note_push_launch_failure(self, infohash: str, why: str) -> int:
        """Count one launch/proof failure; arm the skip window at the limit.

        Bounded and pruned exactly like `_note_pressure_refusal` /
        `_failed_late_cross_seeds`: expired streaks dropped on every insert,
        oldest shed past the cap. Never raises — this is bookkeeping on the way
        to a refusal, not the refusal itself.
        """
        h = (infohash or "").strip().lower()
        if not h:
            return 0
        try:
            memo = getattr(self, "_push_launch_failures", None)
            if not isinstance(memo, dict):
                memo = {}
                self._push_launch_failures = memo  # type: ignore[attr-defined]
            now = time.monotonic()
            window = self._push_launch_fail_window()
            for stale in [k for k, v in memo.items() if now - float(v[2]) >= window]:
                memo.pop(stale, None)
            prior = memo.get(h)
            count = int(prior[0]) + 1 if isinstance(prior, tuple) and len(prior) == 4 else 1
            armed = now + window if count >= _PUSH_LAUNCH_FAIL_LIMIT else 0.0
            memo[h] = (count, armed, now, _one_line(why))
            if len(memo) > _PUSH_LAUNCH_FAIL_CAP:
                for oldest in list(memo.keys())[: len(memo) - _PUSH_LAUNCH_FAIL_CAP]:
                    memo.pop(oldest, None)
            if armed > 0.0:
                log.warning(
                    "push: %s failed to start %d times in a row (%s); the automatic paths "
                    "leave it alone for %ds",
                    h[:10],
                    count,
                    _one_line(why),
                    int(window),
                )
            else:
                log.debug("push: %s failed to start (%s)", h[:10], _one_line(why))
            return count
        except Exception:
            return 1

    def _forget_push_launch_failure(self, infohash: str) -> None:
        """A copy that really started ends the streak: eligible again at once."""
        try:
            memo = getattr(self, "_push_launch_failures", None)
            if isinstance(memo, dict):
                memo.pop((infohash or "").strip().lower(), None)
        except Exception:
            pass

    # ---- per-poll sweep ------------------------------------------------- #

    async def push_sweep(self) -> None:
        """Advance every live push checkpoint; hand finished rows to admission.

        Runs once per poll next to the manual-move sweep and does nothing at
        all while the switch is off. Progress is one metadata-only
        ``rclone lsjson --files-only -R`` per push against the seed remote —
        never the fuse mount, never VPS2's SSD, never a content read — so at
        most one remote listing per push per `poll_seconds`, and that single
        listing answers both questions: which names have landed (completion
        stays name coverage) and how many BYTES of them have (the file counts
        alone read `0/1` for the whole of a single-file copy). When
        the remote holds every expected file the push goes `verifying` and
        the EXISTING evidence path (`_fuse_content_evidence` ->
        `_admit_fuse_existing` -> the all-remote shortcut) admits the row
        straight away instead of waiting for the next sweep tick. When a poll
        sees NOTHING new on a `running` checkpoint, the sweep also reads the
        copy's own rclone log (`push_liveness`, one bounded read on the
        dedicated dial) and fails a provably dead copy right here, so its slot
        frees on the same sweep instead of squatting it for the whole
        `timeout_seconds`. Past the deadline the checkpoint is `failed`, which
        switches the suppression off and leaves the row exactly where today's
        behaviour picks up — and the deadline stays the backstop for every
        verdict this probe cannot make (a silent-but-alive big upload).
        """
        if not self._push_switch_on():
            return
        store = getattr(self, "store", None)
        lister = getattr(store, "list_pushes", None)
        if not callable(lister):
            return
        try:
            pushes = await offload(lister, LIVE_PUSH_STATES)
        except Exception as e:  # noqa: BLE001
            log.warning("push sweep: cannot list checkpoints: %s", e)
            return
        if not isinstance(pushes, list) or not pushes:
            return
        interval = self._push_poll_seconds()
        now = dt.datetime.now(dt.UTC)
        for push in pushes:
            try:
                if getattr(push, "state", None) not in LIVE_PUSH_STATES:
                    continue
                # The deadline and the row's existence are checked every tick
                # (cheap indexed reads): suppression must lift the moment the
                # push dies. Only the remote listing obeys the cadence.
                await self._push_check_one(
                    store, push, now, poll=self._push_due(push, now, interval)
                )
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                key = _one_line(getattr(push, "infohash", "?"))[:10]
                log.warning("push sweep: check failed for %s: %s", key, e)

    def _push_due(self, push, now: dt.datetime, interval: float) -> bool:
        """Cadence gate: at most one remote check per push per interval.

        `updated_at` is the persisted stamp every note/finish/claim bumps, so
        the window survives a restart (the first pass after booting always
        checks, which is exactly what a detached copy needs).
        """
        try:
            updated = push.updated_at
            if updated is None:
                return True
            if updated.tzinfo is None:
                updated = updated.replace(tzinfo=dt.UTC)
            return (now - updated).total_seconds() >= interval
        except Exception:
            return True

    async def _push_check_one(self, store, push, now: dt.datetime, *, poll: bool) -> None:
        """One push checkpoint: expire it, poll it, or hand it over."""
        h = (getattr(push, "infohash", "") or "").strip().lower()
        if not h:
            return
        deadline = getattr(push, "deadline_at", None)
        if deadline is not None and now >= deadline:
            seconds = int((now - (push.started_at or now)).total_seconds())
            await self._push_settle(
                store,
                h,
                PushState.FAILED,
                f"the copy did not finish within the push deadline ({seconds}s elapsed)",
            )
            log.warning(
                "push: deadline passed for %s after %ds; the row falls back to the normal path",
                h[:10],
                max(0, seconds),
            )
            return
        ts = None
        try:
            ts = await offload(store.get, h, include_blob=False)
        except Exception:
            ts = None
        if ts is None:
            # Forgotten row: nothing left to suppress, nothing to hand over.
            await self._push_settle(store, h, PushState.ABANDONED, "the row is gone (forgotten?)")
            return
        if ts.state == State.DONE:
            # Another actor finished the job (a manual /inject, a fuse adopt);
            # the push has served its purpose.
            await self._push_settle(store, h, PushState.DONE)
            return
        target = str(getattr(push, "target_remote", "") or "")
        if not target:
            await self._push_settle(
                store, h, PushState.FAILED, "the checkpoint has no target remote"
            )
            return
        if not poll:
            # Not due for a remote listing yet: the deadline/row checks above
            # already ran, which is all suppression needs this tick.
            return
        files = await self._push_wanted_files(push, ts)
        if not files:
            # Cannot name the expectation, so no proof is possible: leave it
            # live and let the deadline fail it (the row keeps today's
            # behaviour the moment suppression lifts).
            return
        landed_before = int(getattr(push, "files_present", 0) or 0)
        bytes_before = int(getattr(push, "bytes_present", 0) or 0)
        progress = await remote_progress(getattr(self, "cfg", None), target, files)
        present, total = progress[0], progress[1]
        # The same listing that names the files also publishes their sizes, so
        # the byte counters are free metadata. A poll that cannot answer about
        # bytes (no `lsjson` sizes, a mocked/legacy 2-tuple, an unreadable
        # remote) passes None and leaves the stored reading alone — 0 would
        # look like a copy that went backwards and drag the estimate down.
        bytes_now = _optional_count(getattr(progress, "bytes_total", None))
        landed_bytes = _optional_count(getattr(progress, "bytes_present", None))
        if not bytes_now:
            landed_bytes = None
        try:
            await offload(
                store.note_push_progress,
                h,
                files_present=int(present or 0),
                files_total=int(total or len(files)),
                bytes_present=landed_bytes,
                bytes_total=bytes_now,
            )
        except Exception as e:  # noqa: BLE001
            log.debug("push: could not note progress for %s: %s", h[:10], e)
        log.debug(
            "push: %s has %d/%d file(s) (%s B/%s B) on %s",
            h[:10],
            int(present or 0),
            int(total or 0),
            "-" if landed_bytes is None else int(landed_bytes),
            "-" if bytes_now is None else int(bytes_now),
            target,
        )
        if landed_bytes is not None and bytes_now:
            # Δbytes / Δseconds between two polls of THIS checkpoint is the
            # only throughput measurement the push lane can make without
            # guessing: the stamp it compares against is the one the previous
            # note wrote.
            self._note_push_throughput_sample(
                h, landed_bytes - bytes_before, self._push_note_gap_seconds(push, now)
            )
        if total > 0 and present >= total:
            await self._push_handoff(store, h, ts, present, total)
            return
        # Nothing new landed. That is NOT yet evidence of a dead copy (a
        # Telegram-backed remote shows nothing until a big upload completes),
        # so ask the one thing that can prove it: the job's own log.
        if int(present or 0) > landed_before or (
            landed_bytes is not None and landed_bytes > bytes_before
        ):
            # Progress is progress: the copy is alive by definition, and the
            # log read is skipped outright this poll.
            self._forget_liveness(h)
            return
        await self._push_liveness(store, push, h, now, present=int(present or 0))

    @staticmethod
    def _push_note_gap_seconds(push, now: dt.datetime) -> float | None:
        """Seconds since this checkpoint last reported progress (or None).

        `updated_at` is the stamp every `note_push_progress` writes, so this is
        the elapsed time BETWEEN two polls of the same record — the exact
        denominator a byte delta needs. None (a missing/unsane stamp) means the
        sample cannot be computed and must be dropped, never guessed at.
        """
        try:
            updated = push.updated_at
            if updated is None:
                return None
            if updated.tzinfo is None:
                updated = updated.replace(tzinfo=dt.UTC)
            gap = (now - updated).total_seconds()
            return gap if gap > 0 else None
        except Exception:
            return None

    # ---- measured push throughput (the deadline's only speed opinion) ---- #

    def _measured_push_throughput_bps(self) -> float | None:
        """The EMA of observed push speed, or None while it is UNKNOWN.

        None is a real answer with teeth: callers must treat it as "no
        measurement", never as "fast". Only two or more plausible byte deltas
        from live pushes ever produce a number (see
        `_note_push_throughput_sample`), and nothing here consults a
        configured speed — the link this daemon races on is not a constant.
        """
        value = getattr(self, "_push_throughput_bps", None)
        try:
            if value is None:
                return None
            speed = float(value)
        except (TypeError, ValueError):
            return None
        if speed <= 0.0 or not math.isfinite(speed):
            return None
        return speed

    def _note_push_throughput_sample(
        self, infohash: str, delta_bytes: int, delta_seconds: float | None
    ) -> None:
        """Fold one live-push byte delta into the throughput estimate.

        Rules, all of them fail-closed (a sample that is not obviously real
        simply does not count):

        * `delta_bytes <= 0` or a missing/non-positive interval ⇒ ignored. A
          poll that saw nothing land carries no information, and the
          byte-aware poll of a Telegram-backed remote sees exactly that for
          most of an upload.
        * a hash `push_liveness` judged dead is ignored, and the sample it
          just contributed is rolled back (`_push_throughput_retract`): a
          copy that died printing a usage error must not train the estimate
          to infinity on its few bytes-per-second noise.
        * the sample is CLAMPED to `_PUSH_THROUGHPUT_MAX_BPS` before it is
          used, so one absurd reading (a remote-side cache flush landing a
          whole tree in one poll) cannot outvote every real measurement.
        * the first valid sample only SEEDS the estimate — it stays None.
          The second turns it into an exponential moving average, so a
          single lucky poll is never a number the deadline trusts.
        """
        h = (infohash or "").strip().lower()
        try:
            if delta_bytes is None or delta_seconds is None:
                return
            step_bytes = int(delta_bytes)
            step_seconds = float(delta_seconds)
            if step_bytes <= 0 or step_seconds <= 0.0:
                return
            sample = float(step_bytes) / step_seconds
            if not math.isfinite(sample) or sample <= 0.0:
                return
            sample = min(sample, _PUSH_THROUGHPUT_MAX_BPS)
            if h and h in getattr(self, "_push_throughput_dead", ()):
                return
            snapshot = (
                h,
                getattr(self, "_push_throughput_bps", None),
                getattr(self, "_push_throughput_seed", None),
            )
            seed = snapshot[2]
            ema = snapshot[1]
            if ema is None and seed is None:
                self._push_throughput_seed = sample
                self._push_throughput_bps = None
                self._push_throughput_snapshot = snapshot
                log.debug("push: throughput seed %.0f B/s from %s", sample, h[:10])
                return
            base = float(ema) if ema is not None else float(seed)
            self._push_throughput_bps = (
                base * (1.0 - _PUSH_THROUGHPUT_ALPHA) + sample * _PUSH_THROUGHPUT_ALPHA
            )
            self._push_throughput_seed = None
            self._push_throughput_snapshot = snapshot
            log.debug(
                "push: throughput now %.0f B/s (sample %.0f B/s)", self._push_throughput_bps, sample
            )
        except Exception as e:  # noqa: BLE001
            log.debug("push: throughput sample ignored (%s)", _one_line(e))

    def _push_throughput_retract(self, infohash: str) -> None:
        """Undo the last folded-in sample when its copy is judged dead.

        One deep: the snapshot is the state immediately before the most recent
        accepted sample, and only the hash that produced it may consume it.
        The hash is then remembered (bounded) so no later delta from that same
        checkpoint trains the estimate either.
        """
        h = (infohash or "").strip().lower()
        if not h:
            return
        try:
            dead = getattr(self, "_push_throughput_dead", None)
            if not isinstance(dead, set):
                dead = set()
                self._push_throughput_dead = dead  # type: ignore[attr-defined]
            dead.add(h)
            if len(dead) > _PUSH_THROUGHPUT_DEAD_CAP:
                for stale in list(dead)[: len(dead) - _PUSH_THROUGHPUT_DEAD_CAP]:
                    dead.discard(stale)
            snapshot = getattr(self, "_push_throughput_snapshot", None)
            if not isinstance(snapshot, tuple) or len(snapshot) != 3 or snapshot[0] != h:
                return
            _who, ema, seed = snapshot
            self._push_throughput_bps = ema
            self._push_throughput_seed = seed
            self._push_throughput_snapshot = None
            log.info(
                "push: throughput sample from %s dropped; the copy was judged dead",
                h[:10],
            )
        except Exception:
            pass

    def _push_throughput_alive(self, infohash: str) -> None:
        """A fresh launch resets the dead-hash memory for this group."""
        h = (infohash or "").strip().lower()
        if not h:
            return
        try:
            dead = getattr(self, "_push_throughput_dead", None)
            if isinstance(dead, set):
                dead.discard(h)
            self._push_throughput_snapshot = None
        except Exception:
            pass

    def push_term_for_deadline(self, size_bytes: object) -> tuple[float, float, float] | None:
        """`(push_seconds, margin_seconds, throughput_bps)` or None for "don't".

        The ONLY form the indexer wait may consult a push speed through, and
        it answers with a real triple exactly when all three inputs are known:

        * a MEASURED throughput (`_measured_push_throughput_bps`, i.e. two or
          more plausible live samples) — the conservative fallback speed is
          deliberately not allowed here, because inventing a speed is how a
          wait gets shortened for a copy nobody has timed yet;
        * a positive size for the row being waited on;
        * a readable poll cadence to derive the margin from.

        Anything else — a half-built config, a row with no bytes, a daemon
        that has not finished two polls yet — returns None and the caller
        keeps today's projection-only deadline. Fail-closed in both
        directions: None never shortens AND never lengthens a wait.
        """
        try:
            speed = self._measured_push_throughput_bps()
            if speed is None:
                return None
            size = int(size_bytes or 0)  # type: ignore[arg-type]
            if size <= 0:
                return None
            margin = self.push_deadline_margin_seconds()
            if margin is None:
                return None
            needed = float(size) / float(speed)
            if not math.isfinite(needed) or not math.isfinite(margin):
                return None
            return needed, margin, speed
        except Exception:
            return None

    def push_seconds_estimate(self, size_bytes: object) -> float | None:
        """How long a push of `size_bytes` would take, or None when unknowable.

        `size / throughput`, with the divisor falling back to
        `_PUSH_THROUGHPUT_DEFAULT_BPS` — the documented conservative (slow)
        speed — whenever no measurement exists. The fallback is deliberately
        SLOW, so an unknown speed means a LONG estimate and never a small one,
        and an unknown size means None: no size is never a "quick push".

        The indexer-wait deadline does NOT use the fallback (it refuses the
        push term outright while the estimate is unknown); this helper is the
        readable answer for operators and cards.
        """
        try:
            size = int(size_bytes or 0)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return None
        if size <= 0:
            return None
        speed = self._measured_push_throughput_bps()
        if speed is None:
            speed = _PUSH_THROUGHPUT_DEFAULT_BPS
        try:
            return float(size) / float(speed)
        except (TypeError, ValueError, ZeroDivisionError):
            return None

    async def _push_liveness(
        self, store, push, infohash: str, now: dt.datetime, *, present: int
    ) -> None:
        """Consult the copy's own rclone log and free the slot when it is dead.

        The production failure this answers: a stalled copy was invisible, so
        it squatted one of the (few) push slots for the whole `timeout_seconds`
        — 3 slots, 3 dead copies, and 64 polls of `has 0/1 file(s)` for a single
        success. `push_liveness` turns "provably gone" into a FAILED checkpoint
        on the very sweep that sees it, so the next release gets the slot.

        Rules kept strict here (see `push_liveness` for the verdict table):
        * only a `running` checkpoint is judged — `verifying` is the handover's
          business and a dead verdict must not undo a finished copy;
        * `alive` (including "could not tell": no dial, a wedged channel) keeps
          the slot: the `deadline_at` ceiling stays the backstop;
        * `finished` while files HAVE landed is not a failure — the poll above
          is what hands the row over, and it gets the next tick to agree;
        * the weakest verdict (`finished`) must repeat before anything is freed.
        """
        if getattr(push, "state", None) != PushState.RUNNING:
            return
        log_path = str(getattr(push, "log_path", "") or "")
        if not log_path:
            # A checkpoint from before the transport stamped its log: nothing
            # to read, so nothing to prove. The deadline still bounds it.
            return
        ssh = self._push_dial()
        if ssh is None:
            log.debug("push: no dedicated dial for %s; keeping the slot", infohash[:10])
            return
        age = self._push_age_seconds(push, now)
        grace = max(_LIVENESS_GRACE_FLOOR_S, _LIVENESS_GRACE_POLLS * self._push_poll_seconds())
        try:
            verdict, reason = await push_liveness(
                ssh=ssh, log_path=log_path, age_s=age, grace_s=grace
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            # The read is a judgement call, never a pipeline failure: an
            # unexpected error must not cost a running copy its slot.
            log.debug("push: liveness read failed for %s: %s", infohash[:10], e)
            return
        if verdict == "alive":
            self._forget_liveness(infohash)
            if reason:
                log.debug("push: %s kept running (%s)", infohash[:10], _one_line(reason))
            return
        if verdict == "finished" and present > 0:
            self._forget_liveness(infohash)
            log.info(
                "push: %s reported a closing summary with %d file(s) landed; leaving it "
                "to the poll instead of failing it",
                infohash[:10],
                present,
            )
            return
        count = self._note_liveness(infohash, (verdict, reason))
        needed = _LIVENESS_DEATH_COUNTS.get(verdict, 1)
        if count < needed:
            log.info(
                "push: %s looks %s (%s); confirming next poll",
                infohash[:10],
                verdict,
                _one_line(reason),
            )
            return
        self._forget_liveness(infohash)
        # A copy this probe judged dead must not have trained the throughput
        # estimate, and its hash is remembered so no later delta from the same
        # checkpoint can either (see `_push_throughput_retract`).
        self._push_throughput_retract(infohash)
        await self._push_settle(
            store, infohash, PushState.FAILED, f"the copy is dead ({_one_line(reason)})"
        )
        log.warning(
            "push: %s is %s (%s); the slot is free again and the row falls back to the normal path",
            infohash[:10],
            verdict,
            _one_line(reason),
        )

    @staticmethod
    def _push_age_seconds(push, now: dt.datetime) -> float | None:
        """How long this copy has been running, or None when the stamp is lost."""
        try:
            started = push.started_at
            if started is None:
                return None
            if started.tzinfo is None:
                started = started.replace(tzinfo=dt.UTC)
            return max(0.0, (now - started).total_seconds())
        except Exception:
            return None

    def _note_liveness(self, infohash: str, key: tuple[str, str]) -> int:
        """How many polls in a row this exact dead verdict has been seen.

        Bounded like `_push_wanted` (oldest shed first) so a long-lived daemon
        cannot grow with the number of pushes ever run. Never raises: this is
        bookkeeping on the way to a judgement, not the judgement itself.
        """
        try:
            memo = getattr(self, "_push_liveness_seen", None)
            if not isinstance(memo, dict):
                memo = {}
                self._push_liveness_seen = memo  # type: ignore[attr-defined]
            entry = memo.get(infohash)
            count = int(entry[1]) + 1 if isinstance(entry, tuple) and entry[0] == key else 1
            memo[infohash] = (key, count)
            if len(memo) > _LIVENESS_SEEN_CAP:
                for stale in list(memo.keys())[: len(memo) - _LIVENESS_SEEN_CAP]:
                    memo.pop(stale, None)
            return count
        except Exception:
            return 1

    def _forget_liveness(self, infohash: str) -> None:
        """Drop the verdict memo: progress, a handover or an end means restart."""
        try:
            memo = getattr(self, "_push_liveness_seen", None)
            if isinstance(memo, dict):
                memo.pop((infohash or "").lower(), None)
        except Exception:
            pass

    async def _push_wanted_files(self, push, ts: TorrentState) -> list[PushFile]:
        """The names this push must deliver, as recorded at launch.

        After a restart the in-memory set is gone and the list is re-derived
        from the blob — every file when VPS1 now reports the torrent
        complete, else its complete subset. That subset can only have grown,
        so a copy that really finished is still recognised; if it grew past
        what was ever copied, the deadline fails the push and the row falls
        back, which is the safe direction.
        """
        h = (getattr(push, "infohash", "") or "").lower()
        cached = None
        try:
            cache = getattr(self, "_push_wanted", None)
            if isinstance(cache, dict):
                cached = cache.get(h)
        except Exception:
            cached = None
        if cached:
            return list(cached)
        live = await self._push_source_torrent(h)
        if live is None:
            return []
        blob = await self._push_racing_blob(h, ts)
        if not blob:
            return []
        files, _why = await self._push_complete_files(h, live, blob)
        if files:
            self._remember_wanted(h, files)
        return files

    async def _push_handoff(
        self, store, infohash: str, ts: TorrentState, present: int, total: int
    ) -> None:
        """Remote says it is all there: verify through the existing path, admit."""
        await self._push_settle(store, infohash, PushState.VERIFYING)
        display = (getattr(ts, "source_name", "") or infohash[:10])[:60]
        if getattr(ts, "state", None) not in _PUSH_ADMITTABLE:
            # Already past admission (a sibling row won, an operator injected
            # it): the bytes are on the seed remote, so the push is done and
            # this row's own machinery carries it from here.
            await self._push_settle(store, infohash, PushState.DONE)
            return
        try:
            status, blob, _p, _t = await self._fuse_content_evidence(ts)
        except Exception as e:  # noqa: BLE001
            log.warning("push: verify failed for %s: %s", display, e)
            return
        if status != "present" or not blob:
            # rclone's own listing saw every name; the seed path not agreeing
            # yet is mount/dir-cache lag. Stay `verifying` and re-check on the
            # next interval — the deadline still bounds the wait.
            log.info(
                "push: %d/%d file(s) on the remote for %s but the seed path is not "
                "verified yet (mount lag?); re-checking next interval",
                present,
                total,
                display,
            )
            return
        log.info("push complete for %s (%d file(s)); admitting from the seed path", display, total)
        try:
            await self._admit_fuse_existing(ts, bytes(blob))
        except (ValueError, AbandonedError) as e:
            log.warning(
                "push: could not admit %s from the seed path (%s); keeping it under verification",
                display,
                _one_line(e),
            )
            return
        await self._push_settle(store, infohash, PushState.DONE)

    # ---- pressure-triggered starts (the janitor's verdict) --------------- #

    async def _push_pressure_verdict(self) -> tuple[bool, str, int | None]:
        """Ask the janitor's OWN pressure function: (pressured, verdict, free).

        No second threshold and no re-derived arithmetic — "pressured" here
        is literally `bursting or urgent`, the same verdict that shortens the
        janitor's cadence and lets it reclaim MOVING rows, produced by
        `_cleanup_pressure` with the `[cleanup]` section and the free bytes.
        Free space comes from the janitor's cached probe
        (`_last_source_free_bytes`); only when it has never probed does this
        probe once itself. Unknown free space is NOT pressured: guessing at
        the disk is how a half-filled box starts a copy it cannot finish, so
        the sweep fails closed exactly like the janitor's deletes do.
        """
        try:
            free = getattr(self, "_last_source_free_bytes", None)
        except Exception:
            free = None
        if free is None:
            try:
                free = await self._source_free_bytes()
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                log.debug("pressure push: VPS1 free-space probe failed: %s", _one_line(e))
                free = None
            try:
                self._last_source_free_bytes = free
            except Exception:
                pass
        if free is None:
            return False, "unknown free space", None
        try:
            _arrivals, _bytes_rate, _projected, urgent, bursting = self._cleanup_pressure(
                getattr(self.rt.cleanup, "config", None), free
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            log.debug("pressure push: no janitor verdict (%s)", _one_line(e))
            return False, "no verdict", free
        verdict = "+".join(name for name, on in (("bursting", bursting), ("urgent", urgent)) if on)
        return bool(bursting or urgent), verdict or "calm", free

    async def _pressure_push_rows(self) -> list[TorrentState]:
        """Indexer-miss WAITING_INDEXER rows a pressure push may take over.

        The park's own schedule is what says "indexer miss": a row parked on
        the public-export timer never queried the download-target indexers
        (no cross-seed is missing there), and a watch-dir drop answers to its
        own election instead of the indexer clock. Operator-held rows
        (`skipped`) get no automatic work. Sorted by size DESCENDING — under
        pressure the scarce push slots should free the most bytes, the same
        instinct as the janitor's biggest-first ordering below the low
        watermark.
        """
        try:
            lister = getattr(getattr(self, "store", None), "list_by_state", None)
            if not callable(lister):
                return []
            rows = await offload(lister, State.WAITING_INDEXER)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            log.debug("pressure push: cannot list parked rows: %s", _one_line(e))
            return []
        out: list[TorrentState] = []
        for ts in rows or []:
            try:
                if getattr(ts, "state", None) != State.WAITING_INDEXER:
                    continue
                if getattr(ts, "indexer_next_retry_at", None) is None:
                    continue
                if getattr(ts, "public_export_next_retry_at", None) is not None:
                    continue
                if getattr(ts, "skipped", 0):
                    continue
                if len((getattr(ts, "source_infohash", "") or "").strip()) != 40:
                    continue
                probe = getattr(self, "_is_watch_row", None)
                if callable(probe) and probe(ts) is True:
                    continue
            except Exception:
                continue
            out.append(ts)
        try:
            out.sort(key=lambda t: int(getattr(t, "total_bytes", 0) or 0), reverse=True)
        except Exception:
            pass
        return out

    def _push_pressure_backoff_seconds(self) -> float:
        """TTL for a refused pressure push: a multiple of the push cadence."""
        try:
            return max(60.0, float(self._push_poll_seconds()) * _PRESSURE_BACKOFF_MULTIPLIER)
        except Exception:
            return _DEFAULT_POLL_S * _PRESSURE_BACKOFF_MULTIPLIER

    def _push_pressure_backed_off(self, infohash: str) -> bool:
        """True while a refusal for this hash is still being deferred."""
        try:
            memo = getattr(self, "_push_pressure_backoff", None)
            if not isinstance(memo, dict):
                return False
            expiry = memo.get(infohash)
            if expiry is None:
                return False
            if time.monotonic() >= float(expiry):
                memo.pop(infohash, None)
                return False
            return True
        except Exception:
            return False

    def _note_pressure_refusal(self, infohash: str, why: str) -> None:
        """Defer one refused hash so a push that can never run does not hot-loop.

        Bounded the way `_failed_late_cross_seeds` is: expired entries are
        pruned on every insert and the map is capped, shedding the oldest
        first (dict insertion order). Never raises — the defer is bookkeeping.
        """
        try:
            memo = getattr(self, "_push_pressure_backoff", None)
            if not isinstance(memo, dict):
                memo = {}
                self._push_pressure_backoff = memo  # type: ignore[attr-defined]
            now = time.monotonic()
            for stale in [k for k, v in memo.items() if now >= float(v)]:
                memo.pop(stale, None)
            memo[infohash] = now + self._push_pressure_backoff_seconds()
            if len(memo) > _PRESSURE_BACKOFF_CAP:
                for stale in list(memo.keys())[: len(memo) - _PRESSURE_BACKOFF_CAP]:
                    memo.pop(stale, None)
            log.debug("pressure push: refused %s (%s)", infohash[:10], _one_line(why))
        except Exception:
            pass

    async def pressure_push_sweep(self) -> int:
        """Start pushes NOW when the janitor says VPS1 is running out of room.

        Returns how many pushes started; does nothing at all (no probe, no
        row listing) unless `push_on_janitor_pressure` is on next to the
        master switch. Why this exists: a release that found no cross-seed
        parks on the indexer window (up to
        `[acquire].indexer_wait_seconds`), so a burst of arrivals fills
        VPS1 while every one of those rows calmly waits for a Prowlarr hit
        that may never come, and the automation stalls until a human taps
        `/push_source`. The janitor already decides when space is critical —
        this consumes that decision.

        The trade is deliberate: pushing forfeits a late Prowlarr cross-seed
        as the *download* source for that row, which is the right call while
        the disk is failing, and the `tracker_map` supplement path (see
        `coordinator_reinject.py`) can still attach same-release torrents
        afterwards. Concurrency is the push's own cap: the sweep budget is
        `max_concurrent_pushes` minus what is already live, and a refusal
        backs off per hash instead of retrying every poll. Two separate
        deferrals apply, because they answer different questions: the existing
        refusal map (ANY refusal — cap full, nothing complete, a preflight
        blip — for a few polls, this sweep only) and the launch-failure streak
        checked inside `start_source_push` (a copy that never started, for the
        whole automatic lane including the indexer-timeout auto path).
        """
        if not self._push_pressure_on():
            return 0
        pressured, verdict, free = await self._push_pressure_verdict()
        if not pressured:
            return 0
        rows = await self._pressure_push_rows()
        if not rows:
            return 0
        slots = self._push_max_concurrent() - len(self._live_push_hashes())
        started = 0
        for ts in rows:
            if slots <= 0:
                break
            h = (getattr(ts, "source_infohash", "") or "").strip().lower()
            if not h or self._push_pressure_backed_off(h) or self._push_is_live(h):
                continue
            ok, why = await self.start_source_push(ts, reason="pressure")
            if ok:
                started += 1
                slots -= 1
            else:
                self._note_pressure_refusal(h, why)
        if started:
            log.info(
                "pressure push: janitor verdict %s (VPS1 free %d bytes) started %d push(es) "
                "for indexer-parked rows",
                verdict,
                int(free or 0),
                started,
            )
        return started

    # ---- suppression helpers ------------------------------------------- #

    def _park_behind_push(self, ts: TorrentState) -> None:
        """Park a row on the push cadence instead of admitting it to SSD.

        Rides the indexer park, whose expired-window branch re-arms on
        `poll_seconds` while a push is live, and keeps
        `indexer_first_queried_at` untouched so a push that dies still hits
        the normal ceiling on the very next park. Anything the transition
        schema forbids degrades to a narrow same-state park — this must never
        raise into the admission path.
        """
        try:
            self._park_for_indexer_retry(ts, reason=PUSH_PARK_REASON)
            return
        except (ValueError, AbandonedError) as e:
            log.info(
                "push: keeping %s parked without a transition (%s)",
                (getattr(ts, "source_infohash", "") or "")[:10],
                _one_line(e),
            )
        try:
            self._park_row(ts, PUSH_PARK_REASON, level=None)
        except Exception:
            pass

    def push_live_for(self, infohash: str) -> bool:
        """Public form of the suppression question (guards + operator views)."""
        return self._push_is_live(infohash)


__all__ = ["PUSH_PARK_REASON", "PushMixin"]
