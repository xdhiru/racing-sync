"""VPS1 cleanup janitor mixin (disk pressure + grace).

Split out of the coordinator god-file. Deletes racing torrents from VPS1
once VPS2 has secured the content (DONE past adaptive grace and idle, or
MOVING under disk pressure with bytes verified on SSD). Fail-closed
throughout: any doubt keeps VPS1 — except, when the operator opts in, the
activity keep alone lifts under real pressure (VPS1 is a scratch disk and
VPS2 takes over serving the peer).

Under an EMERGENCY (free space at/below `critical_watermark_free_bytes`, or a
newly discovered release that cannot fit) a fuse-VERIFIED DONE row stops
waiting out the grace period, and the janitor acts on its next wake instead of
its next cadence: timing only, never a safety gate.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import logging
import time
from pathlib import Path
from typing import Any

from .clients.abstract import Torrent
from .coordinator_content import cleanup_grace_seconds
from .coordinator_paths import _safe_ssd_join

# The 1 s wake slice the racing-blob warm-up loop already uses: reused here so
# the janitor's emergency wake follows the file's existing prompt-wakeup
# convention instead of inventing a second one.
from .coordinator_racing_blobs import _BULK_FETCH_SLEEP_STEP_S
from .io_bounds import chunked, offload, rpc
from .state import State, TorrentState

# Keep the historic logger name so log output is unchanged by the split.
log = logging.getLogger("racing_sync.coordinator")

#: How old the cached VPS1 listing may be before "still on VPS1" stops
#: being provable. The poller refreshes every tick (45 s TTL), so a view
#: older than this means the source poll is dead: the 🗄 tab must say
#: "listing unavailable" instead of guessing presence from a ghost.
_VPS1_LISTING_MAX_AGE_S = 120.0

#: Floor (seconds) for the emergency janitor spacing, which is otherwise
#: derived from `burst_janitor_interval_seconds / 5` (see
#: `_cleanup_emergency_spacing_seconds`). The floor only matters for a tiny
#: burst interval: every extra pass re-probes VPS1 free space and re-scans the
#: whole state, and both the cadence gate and the loop's sleep read this one
#: number, so an emergency wake can never degrade into a hot loop.
_EMERGENCY_WAKE_MIN_FLOOR_S = 15.0


class CleanupMixin:
    """VPS1 janitor (duck-typed ``self``: cfg/store/source_client/dest_client)."""

    def _cleanup_arrival_rates(self, now_mono: float) -> tuple[float, float]:
        """Rolling intake velocity: (releases/hour, bytes/hour) over 1 h.

        Spam-uploader bursts raise the rate; the janitor shortens deletion
        grace and switches to burst cadence/cap accordingly. Both arrival
        lists are pruned with the SAME cutoff so counts and bytes always
        describe the same window (bounded memory either way). Tolerant of
        object.__new__ test doubles (missing attributes).
        """
        cutoff = now_mono - 3600.0
        arrivals = 0.0
        try:
            times = getattr(self, "_arrival_times", None)
            if times:
                kept = [t for t in times if t >= cutoff]
                try:
                    self._arrival_times = kept
                except Exception:
                    pass
                arrivals = float(len(kept))
        except Exception:
            arrivals = 0.0
        bytes_rate = 0.0
        try:
            sizes = getattr(self, "_arrival_bytes", None)
            if sizes:
                kept_b = [e for e in sizes if e[0] >= cutoff]
                try:
                    self._arrival_bytes = kept_b
                except Exception:
                    pass
                bytes_rate = float(sum(int(e[1] or 0) for e in kept_b))
        except Exception:
            bytes_rate = 0.0
        return arrivals, bytes_rate

    def _note_cleanup_arrival(self, size_bytes: int) -> None:
        """Flag a newly discovered release that does not fit on VPS1.

        Compares the arrival against the free space the janitor last
        probed. Exceeding free-minus-safety (or plain free, whichever is
        stricter) means the disk will overflow before the normal hourly
        tick: warn once and mark the next pass urgent so it runs early and
        may reclaim MOVING rows. Never raises (discovery must not fail on
        bookkeeping).
        """
        try:
            last_free = getattr(self, "_last_source_free_bytes", None)
            if last_free is None:
                return
            free = int(last_free)
            size = int(size_bytes or 0)
            if size <= 0:
                return
            try:
                safety = int(getattr(self.rt.storage, "disk_safety_margin_bytes", 0) or 0)
            except (TypeError, ValueError):
                safety = 0
            if size > max(0, free - safety):
                self._cleanup_urgent = True
                # Wake the janitor loop on its next slice instead of up to
                # `burst_janitor_interval_seconds` later. A flag, not a task:
                # the janitor lane keeps owning its one-pass-at-a-time lock.
                self._cleanup_emergency_wake = True
                log.warning(
                    "cleanup: new release ~%d MB exceeds VPS1 free ~%d MB; "
                    "the next janitor pass will prioritise cleanup",
                    size // (1024 * 1024),
                    free // (1024 * 1024),
                )
        except Exception:
            return

    def _cleanup_arrivals_per_hour(self, now_mono: float) -> float:
        """Arrival-count half of `_cleanup_arrival_rates` (kept for callers)."""
        return self._cleanup_arrival_rates(now_mono)[0]

    def _cleanup_pressure(
        self, cfg: object, free_bytes: int | None
    ) -> tuple[float, float, float | None, bool, bool]:
        """Intake/disk pressure snapshot: (arrivals/h, bytes/h, projected
        hours-to-fill | None, urgent, bursting).

        Bursting (shorter cadence, bigger cap) when any of:
          - free space is below the low watermark;
          - a recent arrival did not fit (`_cleanup_urgent`);
          - arrivals/hour reached `burst_arrivals_per_hour`;
          - the recent bytes/hour fill the remaining space inside
            `fill_lookahead_hours` (0 disables this trigger).
        Unknown free space never projects (fail-closed: no guess).
        """
        arrivals, bytes_rate = self._cleanup_arrival_rates(time.monotonic())
        projected: float | None = None
        if free_bytes is not None and bytes_rate > 0:
            try:
                projected = float(free_bytes) / bytes_rate * 3600.0
            except (TypeError, ValueError, ZeroDivisionError):
                projected = None
        try:
            low = int(getattr(cfg, "low_watermark_free_bytes", 0) or 0)
            burst_arrivals = float(getattr(cfg, "burst_arrivals_per_hour", 8.0) or 0.0)
            lookahead = float(getattr(cfg, "fill_lookahead_hours", 0.0) or 0.0)
        except (TypeError, ValueError):
            low, burst_arrivals, lookahead = 0, 8.0, 0.0
        try:
            urgent = bool(getattr(self, "_cleanup_urgent", False))
        except Exception:
            urgent = False
        pressured = bool(free_bytes is not None and low > 0 and free_bytes < low)
        bursting = bool(
            pressured
            or urgent
            or arrivals >= burst_arrivals
            or (lookahead > 0 and projected is not None and projected < lookahead * 3600.0)
        )
        return arrivals, bytes_rate, projected, urgent, bursting

    # ---------------------------------------------------------------- emergency

    @staticmethod
    def _cleanup_emergency_trigger(cfg: object, free_bytes: int | None, urgent: bool) -> str | None:
        """Which emergency applies right now: `"critical"` / `"arrival-did-not-fit"` / None.

        The one emergency predicate (the cadence gate, the wake logic and the
        verdict all call this, so "pressured", "bursting" and "emergency" stay
        three distinct, honestly-named things):

          - `bursting` answers "tighten the cadence and the per-run cap";
          - `pressured` answers "lift the activity keep (opt-in)";
          - an EMERGENCY answers "VPS1 cannot land the next race right now" —
            the only state in which a fuse-VERIFIED DONE row skips the grace
            wait (and the idle-confirm fast lane that stands in for it).

        An emergency changes TIMING only: verification, protection, unknown
        stats, H&R minimums and the activity gate (still governed by
        `ignore_activity_under_pressure`) are untouched by it. A disabled
        watermark (`critical_watermark_free_bytes = 0`) and unknown free space
        never guess: only the arrival trigger can then apply.
        """
        try:
            crit = int(getattr(cfg, "critical_watermark_free_bytes", 0) or 0)
        except (TypeError, ValueError):
            crit = 0
        if crit > 0 and free_bytes is not None and free_bytes <= crit:
            return "critical"
        if bool(urgent):
            return "arrival-did-not-fit"
        return None

    def _cleanup_emergency_is_live(self, cfg: object) -> bool:
        """Emergency from what the janitor already knows — never a fresh probe.

        Reads the cached free-space reading (`_last_source_free_bytes`, written
        once per tick by `_maybe_cleanup_source`) plus the arrival flag, so the
        hot path (the loop's sleep) adds no VPS1 RPC.
        """
        try:
            free = getattr(self, "_last_source_free_bytes", None)
            urgent = bool(getattr(self, "_cleanup_urgent", False))
        except Exception:
            return False
        return self._cleanup_emergency_trigger(cfg, free, urgent) is not None

    def _cleanup_emergency_spacing_seconds(self, cfg: object) -> float:
        """Minimum spacing between emergency janitor passes (seconds).

        Derived from the cadence that already exists — one fifth of
        `burst_janitor_interval_seconds` (300 s burst -> 60 s), floored at
        `_EMERGENCY_WAKE_MIN_FLOOR_S` — so an emergency acts within about a
        minute instead of up to a whole burst window late, while a disk with
        nothing reclaimable still costs at most five extra scans per burst
        window. The gate and the loop's sleep share this number.
        """
        try:
            burst = int(getattr(cfg, "burst_janitor_interval_seconds", 300) or 300)
        except (TypeError, ValueError):
            burst = 300
        if burst < 60:
            burst = 60
        return max(burst / 5.0, _EMERGENCY_WAKE_MIN_FLOOR_S)

    def _cleanup_emergency_wake_seconds(self, cfg: object, normal_wake: float) -> float:
        """The janitor loop's next sleep: `normal_wake`, shortened if emergency.

        Never longer than the cadence the loop already chose, and never faster
        than the emergency spacing (same bound the cadence gate applies).
        """
        try:
            normal = float(normal_wake)
        except (TypeError, ValueError):
            return 300.0
        if not self._cleanup_emergency_is_live(cfg):
            return normal
        spacing = self._cleanup_emergency_spacing_seconds(cfg)
        return max(min(normal, spacing), _EMERGENCY_WAKE_MIN_FLOOR_S)

    async def _cleanup_emergency_wait(self, seconds: float) -> None:
        """Sleep the janitor cadence, waking early on an emergency signal.

        The wait is chopped into the same 1 s slices the racing-blob warm-up
        loop uses, so `_cleanup_emergency_wake` — set by another lane (the
        discovery path flagging a release that cannot fit, never a spawned
        task) — is honoured within a second instead of after the whole cadence.
        The flag is CONSUMED here: a pass was the point of the flag, and an
        uncleared one would make every later slice return instantly. How often
        emergency passes may actually run is bounded by
        `_cleanup_emergency_spacing_seconds` (the cadence gate refuses a pass
        closer than that), so this cannot become a hot loop. Also returns on a
        stop request, like every other sliced sleep in the daemon.
        """
        try:
            remaining = float(seconds)
        except (TypeError, ValueError):
            remaining = 300.0
        while remaining > 0 and not getattr(self, "_stop", False):
            if bool(getattr(self, "_cleanup_emergency_wake", False)):
                try:
                    self._cleanup_emergency_wake = False
                except Exception:
                    pass
                return
            step = min(_BULK_FETCH_SLEEP_STEP_S, remaining)
            try:
                await asyncio.sleep(step)
            except asyncio.CancelledError:
                raise
            remaining -= step

    # Report order for the "nothing eligible" breakdown (`_cleanup_blocked_reason`).
    _CLEANUP_BLOCK_ORDER = (
        "grace",
        "activity",
        "verification",
        "unknown stats",
        "protection",
        "minimums",
        "not pressured",
        "state",
    )

    def _cleanup_activity_override(self) -> bool:
        """`[cleanup].ignore_activity_under_pressure`, read through the runtime view.

        Never raises: a half-built config double (or a missing section) reads
        as "off", which is today's behaviour. The flag only unlocks the
        activity keep — every other gate stays fail-closed.
        """
        try:
            return self.rt.cleanup.ignore_activity_under_pressure is True
        except Exception:
            return False

    @staticmethod
    def _cleanup_pressured(cfg: object, free_bytes: int | None, bursting: bool) -> bool:
        """Real pressure for THIS run: bursting, or free below the low watermark.

        Deliberately not a new threshold: `bursting` is what `_cleanup_pressure`
        already decided for this tick (the janitor passes it down), and the
        watermark comparison is the same one the MOVING early path and the
        candidate sort use. Unknown free space is never pressure.
        """
        if bursting:
            return True
        try:
            low = int(getattr(cfg, "low_watermark_free_bytes", 0) or 0)
        except (TypeError, ValueError):
            low = 0
        return bool(low and free_bytes is not None and free_bytes < low)

    def _cleanup_note_pressure_override(
        self, ts: TorrentState, lively: list[Torrent], group_size: int, free_bytes: int | None
    ) -> None:
        """One INFO line per row the pressure override rescued from a keep."""
        leechers = 0
        for m in lively:
            try:
                leechers += max(0, int(m.num_leechers or 0))
            except (TypeError, ValueError, AttributeError):
                continue
        log.info(
            "pressure: deleting %s despite %d leecher(s)/upspeed (%d/%d racing, "
            "VPS1 free=%s); VPS2 takes over serving",
            (ts.source_name or "")[:60],
            leechers,
            len(lively),
            group_size,
            f"{free_bytes} B" if free_bytes is not None else "unknown",
        )

    def _cleanup_note_emergency_rescue(
        self,
        ts: TorrentState,
        free_bytes: int | None,
        trigger: str,
        age_s: float,
        grace_s: float,
    ) -> None:
        """One INFO line per row the emergency rescued from the grace wait.

        Same shape as the pressure-override note (one line per row, emitted
        only once the row really is a candidate), and it says exactly what was
        skipped so an operator reading the log can tell a politeness bypass
        from a lost safety gate.
        """
        log.info(
            "emergency: deleting %s after %ds of a %ds grace (VPS1 free=%s, trigger=%s); "
            "fuse-verified, grace and idle-confirm skipped",
            (ts.source_name or "")[:60],
            int(age_s),
            int(grace_s),
            f"{free_bytes} B" if free_bytes is not None else "unknown",
            trigger,
        )

    def _source_probe_path(self) -> str | None:
        try:
            raw = self.rt.intake.deluge_state_dir or self.rt.intake.sftp_state_dir
        except Exception:
            return None
        if raw is None:
            return None
        try:
            return raw.as_posix() if hasattr(raw, "as_posix") else str(raw)
        except Exception:
            return None

    async def _source_free_bytes(self) -> int | None:
        """Free bytes on the VPS1 racing filesystem, or None when unknown.

        Prefers the Deluge daemon's own `core.get_free_space`: it is one
        RPC, needs no SSH channel, and does not compete with the SFTP pool
        that .torrent fetches lease (a wedged transport used to blind the
        janitor exactly when disk pressure made deletions urgent). The
        SFTP statvfs/df probe stays as the fallback for daemons without
        the method. Unknown covers qBittorrent sources, reconnect
        failures and stat errors — callers degrade to time-only grace,
        never to zero.
        """
        path = self._source_probe_path()
        if path is not None:
            client = getattr(self, "source_client", None)
            probe = getattr(client, "get_free_space", None)
            if callable(probe):
                try:
                    free = await asyncio.wait_for(probe(path), timeout=15.0)
                except Exception as e:  # noqa: BLE001
                    log.debug("cleanup: VPS1 free-space RPC probe failed: %r", e)
                else:
                    if isinstance(free, int) and not isinstance(free, bool) and free >= 0:
                        return free
        sftp = getattr(self, "sftp", None)
        if sftp is None or path is None:
            return None
        try:
            free = await asyncio.wait_for(
                asyncio.to_thread(sftp.disk_free_bytes, path),
                timeout=15.0,
            )
        except Exception as e:  # noqa: BLE001
            # repr, not str: bare TimeoutError (the 15s budget above)
            # stringifies to "" and hides the diagnosis.
            log.warning("cleanup: VPS1 free-space probe failed: %r", e)
            return None
        return free if isinstance(free, int) and free >= 0 else None

    _CLEANUP_LAST_RUN_META_KEY = "cleanup_last_run"

    def _cleanup_last_run_wall(self) -> float:
        """Wall-clock epoch of the last janitor run (persisted across restarts).

        Memory first, store_meta second (a restart wipes memory but keeps
        the DB). Missing/corrupt reads as 0 (run). Legacy monotonic
        `_cleanup_last_run` values are ignored (meaningless after reboot).
        Never raises.
        """
        try:
            mem = float(getattr(self, "_cleanup_last_run_wall_ts", 0.0) or 0.0)
        except (TypeError, ValueError):
            mem = 0.0
        if mem > 0:
            return mem
        try:
            store = getattr(self, "store", None)
            get_meta = getattr(store, "get_meta", None)
            if callable(get_meta):
                raw = get_meta(self._CLEANUP_LAST_RUN_META_KEY)
                if raw:
                    return float(raw)
        except (TypeError, ValueError):
            pass
        except Exception:
            pass
        return 0.0

    def _remember_cleanup_run(self, now_wall: float) -> None:
        """Stamp the janitor run in memory and in store_meta. Never raises."""
        try:
            self._cleanup_last_run_wall_ts = now_wall
        except Exception:
            pass
        try:
            # Keep the legacy monotonic attr moving for any external reader.
            self._cleanup_last_run = time.monotonic()
        except Exception:
            pass
        try:
            store = getattr(self, "store", None)
            set_meta = getattr(store, "set_meta", None)
            if callable(set_meta):
                set_meta(self._CLEANUP_LAST_RUN_META_KEY, str(now_wall))
        except Exception:
            pass

    async def _maybe_cleanup_source(self, *, force: bool = False) -> dict | None:
        """Gate for the VPS1 cleanup janitor; no-op unless enabled.

        The cadence clock persists in store_meta, so a restart resumes the
        schedule instead of resetting it (a minutes-long reboot for an
        upgrade must not trigger an immediate run). A startup settle delay
        keeps the first minutes after boot delete-free while recovery and
        polls settle. Both use wall-clock epoch (monotonic clocks don't
        survive restarts). Times survive only as early as the oldest
        reading: memory, then meta, else 0 (run).

        Each tick probes VPS1 free space once and derives intake pressure
        from it: calm runs keep `janitor_interval_seconds`, pressured ones
        switch to the shorter `burst_janitor_interval_seconds`. The probe
        value is cached for the oversized-arrival check.

        `force` (operator `/cleanup`) skips ONLY the startup-delay and
        cadence gates: every safety, grace, H&R and blob-preservation rule
        in `_run_source_cleanup` still applies. An EMERGENCY (free space at or
        below `critical_watermark_free_bytes`, or an arrival that did not fit)
        also skips the cadence gate — bounded by `_cleanup_emergency_spacing_*`
        — because waiting a whole burst interval for a disk that cannot land
        the next race is the failure this path exists to avoid. Returns the run
        summary dict, or None when cleanup is disabled, unavailable (no seed
        entry), still settling, or not due.
        """
        cfg = getattr(self.rt.cleanup, "config", None)
        if cfg is None or not getattr(cfg, "enabled", False):
            return None
        if not self.rt.chain.adds_seed_entry:
            # The janitor proves the content is secured by re-verifying our
            # own seed entry. Without that link (disabled or adopt-only)
            # there is no such proof yet (a remote verifier is a later phase).
            if not getattr(self, "_cleanup_seed_skip_logged", False):
                self._cleanup_seed_skip_logged = True
                log.warning(
                    "cleanup janitor skipped: no seeding entry is added by this "
                    "chain — completed content cannot be verified yet"
                )
            return None
        try:
            interval = int(getattr(cfg, "janitor_interval_seconds", 3600) or 3600)
        except (TypeError, ValueError):
            interval = 3600
        if interval < 60:
            interval = 60
        try:
            burst_interval = int(getattr(cfg, "burst_janitor_interval_seconds", 300) or 300)
        except (TypeError, ValueError):
            burst_interval = 300
        if burst_interval < 60:
            burst_interval = 60
        try:
            startup_delay = float(getattr(cfg, "janitor_startup_delay_seconds", 300) or 0)
        except (TypeError, ValueError):
            startup_delay = 300.0
        if startup_delay < 0:
            startup_delay = 0.0
        now_mono = time.monotonic()
        try:
            gate_t0 = getattr(self, "_cleanup_gate_t0", None)
            if not isinstance(gate_t0, float):
                gate_t0 = now_mono
                self._cleanup_gate_t0 = gate_t0
        except Exception:
            gate_t0 = now_mono
        if not force and startup_delay > 0 and now_mono - gate_t0 < startup_delay:
            return None
        # One probe per tick: the pressure decision, the cadence gate and
        # `_run_source_cleanup` all share this reading (and the arrival
        # check compares new releases against it).
        free_bytes = await self._source_free_bytes()
        try:
            self._last_source_free_bytes = free_bytes
        except Exception:
            pass
        arrivals, bytes_rate, projected, urgent, bursting = self._cleanup_pressure(cfg, free_bytes)
        # Emergency (critical watermark, or an arrival that did not fit) on
        # THIS tick's single probe — the same predicate the verdict and the
        # loop wake use.
        emergency = self._cleanup_emergency_trigger(cfg, free_bytes, urgent)
        if emergency is not None:
            # The signal has reached the janitor lane: consume it now, whether
            # or not this tick actually runs. A spacing-refused pass must not
            # leave it armed, or every later sleep slice would return
            # instantly. The emergency LEVEL stays readable from `_cleanup_urgent`
            # and the watermark, so nothing is lost.
            try:
                self._cleanup_emergency_wake = False
            except Exception:
                pass
        effective = burst_interval if bursting else interval
        now_wall = time.time()
        since_last = now_wall - self._cleanup_last_run_wall()
        if not force and since_last < effective:
            if emergency is None:
                return None
            # Act now, not next tick: a starving disk waits out neither the
            # calm nor the burst cadence. Bounded by the emergency spacing, so
            # a run that keeps finding nothing eligible cannot spin.
            if since_last < self._cleanup_emergency_spacing_seconds(cfg):
                return None
            log.info(
                "cleanup janitor: emergency (%s) — running %ds early "
                "(cadence %ds, last pass %ds ago)",
                emergency,
                int(effective - since_last),
                int(effective),
                int(since_last),
            )
        self._remember_cleanup_run(now_wall)
        # A run is starting: it consumes the urgency flag (a still-cramped
        # disk re-trips the watermark trigger next tick) and with it the wake
        # signal, so no second pass is scheduled for the same pressure.
        try:
            self._cleanup_urgent = False
            self._cleanup_emergency_wake = False
        except Exception:
            pass
        return await self._run_source_cleanup(
            cfg,
            free_bytes=free_bytes,
            arrivals=arrivals,
            bytes_rate=bytes_rate,
            projected=projected,
            bursting=bursting,
            urgent=urgent,
        )

    async def _run_source_cleanup(
        self,
        cfg: object,
        *,
        free_bytes: int | None = None,
        arrivals: float | None = None,
        bytes_rate: float | None = None,
        projected: float | None = None,
        bursting: bool | None = None,
        urgent: bool | None = None,
    ) -> dict:
        """Delete VPS1 racing torrents whose content VPS2 fully secured.

        Two deletion classes (see [cleanup] docs): normal DONE rows past
        adaptive grace (or verifiably idle), and early MOVING rows under disk
        pressure (or when an arrival did not fit). Dry-run only logs.
        Fail-closed throughout: any doubt about bytes, health, identity or
        activity skips the row for this run.

        With `[cleanup].ignore_activity_under_pressure` the activity keep is
        skipped while THIS run reads pressure (bursting, or free below the low
        watermark): a live peer's download is cut short and VPS2 takes over
        serving it. That gate alone — verification, protection, unknown stats
        and blob preservation still keep the row whatever the disk says.

        The pressure values are optional so direct callers/tests can pass
        only `cfg`; the janitor passes its single probe down so this method
        never re-probes.

        Under an EMERGENCY (`_cleanup_emergency_trigger`: free space at/below
        `critical_watermark_free_bytes`, or an arrival that did not fit) a
        DONE row whose bytes are already fuse-VERIFIED becomes a candidate
        without waiting out the grace period or the idle-confirm fast lane —
        timing only; verification still runs first and the activity keep still
        needs `ignore_activity_under_pressure`.

        Returns a one-line-friendly summary (also on every early-return
        path): `{"deleted", "candidates", "dry_run", "grace_s", "free",
        "bursting", "reason"}` where `reason` is only set when nothing ran
        ("no rows", "nothing eligible (activity 17, grace 3)", ...; while an
        emergency is on the breakdown line says so:
        "nothing eligible (grace 19; emergency on)").
        """
        now_utc = dt.datetime.now(dt.UTC)
        if arrivals is None:
            # Direct call with no pressure snapshot: probe and derive it
            # here (the janitor always passes its single probe down).
            if free_bytes is None:
                free_bytes = await self._source_free_bytes()
                try:
                    self._last_source_free_bytes = free_bytes
                except Exception:
                    pass
            arrivals, bytes_rate, projected, urgent, bursting = self._cleanup_pressure(
                cfg, free_bytes
            )
        bytes_rate = float(bytes_rate or 0.0)
        bursting = bool(bursting)
        urgent = bool(urgent)
        # Emergency state for THIS run, from the same reading the cadence used
        # (never a second probe): None = no emergency, else the trigger name.
        emergency = self._cleanup_emergency_trigger(cfg, free_bytes, urgent)
        dry_run = bool(getattr(cfg, "dry_run", True))
        grace_s = cleanup_grace_seconds(cfg, free_bytes, arrivals)

        def _summary(**over: Any) -> dict:
            out = {
                "deleted": 0,
                "candidates": 0,
                "dry_run": dry_run,
                "grace_s": float(grace_s),
                "free": free_bytes,
                "bursting": bursting,
                "reason": "",
            }
            out.update(over)
            return out

        # Calm-and-roomy short-circuit: plenty of free space and a quiet
        # intake mean no deletion could help anything — skip the scan and
        # the deletes entirely (next tick re-evaluates). Unknown free space
        # never skips (fail-closed), and active H&R minimums still enforce
        # regardless of pressure. Never short-circuits a burst (projected
        # fill or a just-arrived release would otherwise be ignored).
        try:
            _high = float(getattr(cfg, "high_watermark_free_bytes", 0) or 0)
            _calm = float(getattr(cfg, "calm_arrivals_per_hour", 2.0) or 0.0)
            _min_ratio = float(getattr(cfg, "min_ratio", 0.0) or 0.0)
            _min_hours = float(getattr(cfg, "min_seed_hours", 0.0) or 0.0)
        except (TypeError, ValueError):
            _high, _calm, _min_ratio, _min_hours = 0.0, 2.0, 0.0, 0.0
        if (
            free_bytes is not None
            and _high > 0
            and free_bytes >= _high
            and arrivals <= _calm
            and not urgent
            and not bursting
            and _min_ratio <= 0
            and _min_hours <= 0
        ):
            log.info(
                "cleanup janitor: skipping run (VPS1 free=%d B >= high "
                "watermark, arrivals=%.1f/h calm)",
                free_bytes,
                arrivals,
            )
            return _summary(reason="VPS1 roomy and intake calm")
        low_free = int(getattr(cfg, "low_watermark_free_bytes", 0) or 0)
        if emergency == "critical":
            log.warning(
                "cleanup: VPS1 disk CRITICAL (%d bytes free); grace floored at minimum "
                "and fuse-verified DONE rows skip the grace wait",
                free_bytes,
            )
        try:
            cap_field = "burst_per_run_cap" if bursting else "per_run_cap"
            cap_default = 25 if bursting else 10
            cap = max(1, int(getattr(cfg, cap_field, cap_default) or cap_default))
        except (TypeError, ValueError):
            cap = 10
        try:
            eff_interval = max(
                60,
                min(
                    int(getattr(cfg, "burst_janitor_interval_seconds", 300) or 300),
                    int(getattr(cfg, "janitor_interval_seconds", 3600) or 3600),
                ),
            )
        except (TypeError, ValueError):
            eff_interval = 300
        log.info(
            "cleanup janitor: VPS1 free=%s arrivals=%.1f/h bytes=%.1f MB/h projected=%s grace=%ds",
            f"{free_bytes} B" if free_bytes is not None else "unknown",
            arrivals,
            bytes_rate / (1024 * 1024),
            f"{projected / 3600.0:.1f}h" if projected is not None else "unknown",
            int(grace_s),
        )
        if bursting:
            log.info(
                "cleanup janitor: burst mode (free=%s, arrivals=%.1f/h, "
                "bytes=%.1f MB/h, projected=%s, interval=%ds, cap=%d)",
                f"{free_bytes} B" if free_bytes is not None else "unknown",
                arrivals,
                bytes_rate / (1024 * 1024),
                f"{projected / 3600.0:.1f}h" if projected is not None else "unknown",
                eff_interval,
                cap,
            )

        try:
            rows = [
                ts for ts in await offload(self.store.all) if ts.state in (State.DONE, State.MOVING)
            ]
        except Exception as e:  # noqa: BLE001
            log.warning("cleanup: cannot list state rows: %s", e)
            return _summary(reason="cannot list state rows")
        if not rows:
            return _summary(reason="no rows")
        try:
            src_torrents = await rpc(
                self.source_client.list_torrents(category=self.rt.intake.category),
                getattr(self, "cfg", None),
                "source cleanup list_torrents",
            )
        except Exception as e:  # noqa: BLE001
            log.warning("cleanup: cannot list VPS1 torrents: %s", e)
            return _summary(reason="cannot list VPS1 torrents")
        by_hash = {t.infohash.lower(): t for t in src_torrents}

        candidates: list[tuple[float, float, TorrentState, list[Torrent]]] = []
        # What held the scan back: every keep path counts itself here, so an
        # empty run answers "why" instead of the bare "nothing eligible".
        blocked: dict[str, int] = {}

        def _blocked(key: str) -> None:
            try:
                blocked[key] = int(blocked.get(key, 0)) + 1
            except Exception:
                pass

        _sem = asyncio.Semaphore(4)

        async def _verdict_one(ts: TorrentState):
            async with _sem:
                try:
                    group = self._cleanup_group_for(ts, by_hash)
                    if not group:
                        return None
                    if self._cleanup_protected(ts):
                        _blocked("protection")
                        return None
                except asyncio.CancelledError:
                    raise
                except Exception as e:  # noqa: BLE001
                    log.warning(
                        "cleanup: group resolve failed for %s: %s", ts.source_infohash[:10], e
                    )
                    return None
                try:
                    return await self._cleanup_verdict(
                        ts,
                        group,
                        grace_s,
                        now_utc,
                        dry_run,
                        free_bytes,
                        urgent=urgent,
                        bursting=bursting,
                        emergency=emergency,
                        blocked=blocked,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as e:  # noqa: BLE001
                    log.warning("cleanup: verdict failed for %s: %s", ts.source_infohash[:10], e)
                    return None

        try:
            _verdicts = await asyncio.gather(*(_verdict_one(ts) for ts in rows))
        except Exception as e:  # noqa: BLE001
            log.warning("cleanup: verdict phase failed: %s", e)
            return _summary(reason="verdict phase failed")
        # Verdicts are per-row independent (narrow stamps on distinct rows;
        # dry-run mutates nothing): gather preserves row order here.
        candidates.extend(v for v in (_verdicts or []) if v is not None)

        if not candidates:
            reason = self._cleanup_blocked_reason(blocked, emergency=emergency is not None)
            log.info("cleanup janitor: %s", reason)
            return _summary(reason=reason)
        pressured = bool(free_bytes is not None and low_free and free_bytes < low_free)
        # Under disk pressure (or any burst) DONE rows go first: their
        # content is fully secured and normally already re-added, so they
        # reclaim space with the least risk. Within a class, biggest-first
        # (a finished 50GB pack beats ten 3GB episodes when the next race
        # needs a landing zone), then oldest. Calm and roomy keeps today's
        # oldest-first.
        candidates = self._cleanup_sort_candidates(candidates, pressured or bursting)

        deleted = 0
        try:
            # The 25 here is the model default too (they used to disagree):
            # an unreadable field must not silently double the documented
            # pause between deletes.
            _stagger = float(getattr(cfg, "delete_stagger_seconds", 25) or 0)
        except (TypeError, ValueError):
            _stagger = 25.0
        if _stagger < 0:
            _stagger = 0.0
        _cands = list(candidates)
        for _i, (_, _, ts, group) in enumerate(_cands):
            # The cap bounds real deletions; dry-run logs every candidate
            # so rehearsal output matches a live run instead of stopping
            # at phantom counts.
            if not dry_run and deleted >= cap:
                break
            # The verdict ran on a snapshot, and the stagger below leaves
            # time for a cancel, a retry or an operator re-add. Re-read
            # before deleting: a row that moved on (or was rewritten) still
            # needs its VPS1 copy, and its worker may be re-injecting now.
            try:
                _fresh = await asyncio.to_thread(
                    self.store.get, ts.source_infohash, include_blob=False
                )
            except Exception:
                _fresh = None
            try:
                _fresh_version = int(getattr(_fresh, "version", 0) or 0)
                _snap_version = int(getattr(ts, "version", 0) or 0)
            except (TypeError, ValueError, AttributeError):
                _fresh_version = _snap_version = 0
            if (
                _fresh is None
                or _fresh.state not in (State.DONE, State.MOVING)
                or _fresh_version != _snap_version
            ):
                log.info(
                    "cleanup: %s changed since its verdict (%s); keeping the VPS1 copy",
                    (ts.source_infohash or "")[:10],
                    getattr(getattr(_fresh, "state", None), "value", "row gone"),
                )
                continue
            # Deleting VPS1 loses any racing torrent that was never injected:
            # its .torrent bytes only ever come from VPS1. Persist them first;
            # a member whose bytes cannot be preserved vetoes this candidate.
            if not await self._cleanup_preserve_racing_blobs(ts, group, dry_run):
                continue
            try:
                ok = await self._delete_source_group(ts, group, cfg, dry_run)
            except Exception as e:  # noqa: BLE001
                log.warning("cleanup: delete failed for %s: %s", ts.source_infohash[:10], e)
                continue
            if ok:
                deleted += 1
                _more = not dry_run and _i + 1 < len(_cands) and deleted < cap
                if _more and _stagger > 0:
                    # Spread load: back-to-back remove_torrent(+data) calls
                    # spike deluged teardown + disk unlinking, which has
                    # coincided with autobrr injection timeouts on the same
                    # daemon. A short settle pause between groups costs
                    # nothing (hourly cadence) and keeps the daemon
                    # responsive. Cancellation propagates (shutdown).
                    try:
                        await asyncio.sleep(_stagger)
                    except asyncio.CancelledError:
                        raise
        log.info(
            "cleanup janitor: %s %d content group(s)%s",
            "would delete" if dry_run else "deleted",
            deleted,
            f" (cap {cap})" if deleted >= cap else "",
        )
        return _summary(deleted=deleted, candidates=len(candidates))

    # ---- operator commands + cleanup ETA -------------------------------

    @staticmethod
    def _cleanup_eta_compact(seconds: float) -> str:
        """Compact remaining time for the hint (``45m`` / ``5h``)."""
        try:
            s = max(0, int(seconds))
        except (TypeError, ValueError):
            return ""
        # Stay in minutes up to ~59m so a rounding edge never prints "60m".
        if s < 3540:
            return f"{max(1, round(s / 60))}m"
        return f"{max(1, round(s / 3600))}h"

    @staticmethod
    def _cleanup_error_text(e: object) -> str:
        """Scrubbed one-line error for a command reply (never raises)."""
        try:
            from .logging_setup import sanitize_log_text

            text = sanitize_log_text(str(e))
        except Exception:
            text = ""
        return (text or type(e).__name__)[:200]

    @staticmethod
    def _cleanup_preview_list(items: object, limit: int = 3, width: int = 60) -> str:
        """Bounded one-line preview (`a, b, c (+7 more)`) of untrusted names.

        Keep-warnings used to print only a count, so an operator staring at
        the same row for 80 runs could not tell WHICH file never arrived.
        Capped on three axes — item count, item width, and the joined text —
        and control characters are flattened so a path with a newline can
        never forge a second log line. Never raises.
        """
        try:
            from .logging_setup import strip_control_chars
        except Exception:  # pragma: no cover - import guard only
            strip_control_chars = None
        try:
            items = list(items or [])
        except Exception:
            return "?"
        shown: list[str] = []
        for item in items[:limit]:
            try:
                text = str(item)
                if strip_control_chars is not None:
                    text = strip_control_chars(text)
                text = text.strip().replace("\n", " ")
            except Exception:
                text = "?"
            shown.append((text or "?")[:width])
        out = ", ".join(shown)[:200]
        if len(items) > limit:
            out += f" (+{len(items) - limit} more)"
        return out or "?"

    @staticmethod
    def _cleanup_summary_text(summary: dict) -> str:
        """One-line `/cleanup` reply from a `_run_source_cleanup` summary."""
        try:
            deleted = int(summary.get("deleted", 0) or 0)
            candidates = int(summary.get("candidates", 0) or 0)
            dry = bool(summary.get("dry_run"))
            reason = str(summary.get("reason") or "")
            free = summary.get("free")
        except Exception:
            return "Cleanup ran."
        if reason:
            return f"🧹 Cleanup: nothing deleted — {reason}."
        verb = "would delete" if dry else "deleted"
        line = (
            f"🧹 Cleanup ({'dry-run' if dry else 'live'}): {verb} "
            f"{deleted} of {candidates} candidate(s)"
        )
        try:
            if free is not None:
                line += f" · VPS1 free {int(free) // (1024 * 1024)} MB"
        except (TypeError, ValueError):
            pass
        return line

    @classmethod
    def _cleanup_blocked_text(cls, blocked: dict[str, int]) -> str:
        """`activity 17, grace 3` from the keep counters; empty when nothing counted.

        Reported in `_CLEANUP_BLOCK_ORDER` (stable wording, not dict order) and
        length-capped: this travels into a Telegram reply, so it stays a
        diagnosis aid rather than a report.
        """
        try:
            counts = {str(k): int(v) for k, v in (blocked or {}).items() if int(v or 0) > 0}
        except (TypeError, ValueError, AttributeError):
            return ""
        if not counts:
            return ""
        keys = [k for k in cls._CLEANUP_BLOCK_ORDER if k in counts]
        keys += sorted(k for k in counts if k not in cls._CLEANUP_BLOCK_ORDER)
        return ", ".join(f"{k} {counts[k]}" for k in keys)[:160]

    def _cleanup_blocked_reason(self, blocked: dict[str, int], *, emergency: bool = False) -> str:
        """`nothing eligible`, extended with what actually blocked the scan.

        The summary contract is untouched — `reason` stays one plain string
        the operator line prints verbatim; the breakdown only adds to it.

        While an emergency is on, the line says so (`nothing eligible (grace
        19; emergency on)`): that exact report — verified rows held by the
        grace floor on a 16 GiB-free disk — used to be indistinguishable from
        "nothing was ready", so the state has to be readable from the one line
        it appears in. Still one line, still capped.
        """
        text = self._cleanup_blocked_text(blocked)
        if emergency:
            tail = "; emergency on"
            text = f"{text[: 160 - len(tail)]}{tail}" if text else "emergency on"
        return f"nothing eligible ({text})" if text else "nothing eligible"

    def _cleanup_remaining_seconds(self, ts: object) -> tuple[bool, float | None] | None:
        """(ready, seconds_left) for a DONE row, or None when not applicable.

        Mirrors the janitor's timing: adaptive grace from the last free-space
        probe plus the idle fast lane (an old `vps1_last_activity_at` stamp).
        Purely cosmetic — the authoritative gates still run at deletion.
        """
        try:
            if getattr(ts, "state", None) != State.DONE:
                return None
            cfg = getattr(self.rt.cleanup, "config", None)
            if cfg is None or not getattr(cfg, "enabled", False):
                return None
            if not self.rt.chain.adds_seed_entry:
                return None
        except Exception:
            return None
        try:
            arrivals = float(self._cleanup_arrival_rates(time.monotonic())[0] or 0.0)
        except Exception:
            arrivals = 0.0
        try:
            free = getattr(self, "_last_source_free_bytes", None)
        except Exception:
            free = None
        try:
            grace_s = float(cleanup_grace_seconds(cfg, free, arrivals))
        except Exception:
            return None
        completed = getattr(ts, "completed_at", None)
        anchor = completed or getattr(ts, "created_at", None)
        if anchor is None:
            return None
        now = dt.datetime.now(dt.UTC)
        try:
            age_s = (now - anchor).total_seconds()
        except Exception:
            return None
        if age_s >= grace_s:
            return (True, 0.0)
        # Idle fast lane: an old activity stamp proves the race finished.
        # Rows without a completion stamp are never fast-laned (matching
        # `_cleanup_verdict`: adoption age alone is not seed-time proof).
        if completed is not None:
            last = getattr(ts, "vps1_last_activity_at", None)
            if last is not None:
                try:
                    window_s = float(getattr(cfg, "idle_confirm_minutes", 45) or 0) * 60.0
                except (TypeError, ValueError):
                    window_s = 45.0 * 60.0
                try:
                    if window_s <= 0 or (now - last).total_seconds() >= window_s:
                        return (True, 0.0)
                except Exception:
                    pass
        return (False, max(0.0, grace_s - age_s))

    def _cleanup_hint(self, ts: object) -> str:
        """Best-effort "when will VPS1 lose this copy" line for a DONE row."""
        try:
            remaining = self._cleanup_remaining_seconds(ts)
        except Exception:
            return ""
        if remaining is None:
            return ""
        ready, seconds_left = remaining
        if ready:
            return "clean ready"
        eta = self._cleanup_eta_compact(seconds_left or 0.0)
        return f"clean ~{eta}" if eta else "clean pending"

    def cleanup_tab_enabled(self) -> bool:
        """True when the "still on VPS1" (🗄) category is meaningful.

        With `[cleanup].enabled` off the janitor never reclaims anything, so
        the tab would promise a countdown that cannot arrive: the button,
        the rows and a stored `vc` filter all go away instead. Returns a
        real bool so the Telegram side can fail closed on bare doubles.
        """
        try:
            cfg = getattr(getattr(getattr(self, "rt", None), "cleanup", None), "config", None)
            if cfg is None:
                cfg = getattr(getattr(self, "cfg", None), "cleanup", None)
            return bool(getattr(cfg, "enabled", False))
        except Exception:
            return False

    def vps1_present_hashes(self) -> set[str] | None:
        """Lowercase hashes the cached VPS1 listing currently reports.

        Pure cache read of the poller's TTL-cached racing-client view
        (`_source_torrents_cache`): no RPC, so the Telegram path can never
        add a VPS1 round trip of its own, and no exception escapes.

        Returns None when there is no listing to reason from (never polled,
        or older than `_VPS1_LISTING_MAX_AGE_S`, i.e. the source poll is
        dead). Callers must treat None as "unknown" — never as "nothing is
        present", which would silently hide rows that still hold files.
        """
        try:
            cache = getattr(self, "_source_torrents_cache", None)
            if cache is None:
                return None
            stamp = float(getattr(self, "_source_torrents_cached_at", 0.0) or 0.0)
            if stamp <= 0.0 or (time.monotonic() - stamp) > _VPS1_LISTING_MAX_AGE_S:
                return None
            out: set[str] = set()
            for torrent in cache or []:
                try:
                    h = str(getattr(torrent, "infohash", "") or getattr(torrent, "hash", ""))
                except Exception:
                    continue
                h = h.strip().lower()
                # Hex-only: a test double or a client object with a repr
                # where a hash should be must not pose as a presence proof.
                if 32 <= len(h) <= 64 and all(c in "0123456789abcdef" for c in h):
                    out.add(h)
            return out
        except Exception:
            return None

    @staticmethod
    def _cleanup_row_hashes(ts: object) -> set[str]:
        """Every hash a row claims on VPS1 (racing + cross-seed + injected)."""
        try:
            raw = [
                getattr(ts, "source_infohash", ""),
                getattr(ts, "dest_infohash", ""),
                getattr(ts, "cross_seed_infohash", ""),
                *(str(getattr(ts, "injected_private_hashes", "") or "").split(",")),
            ]
        except Exception:
            return set()
        out: set[str] = set()
        for h in raw:
            try:
                n = str(h or "").strip().lower()
            except Exception:
                continue
            if n:
                out.add(n)
        return out

    def cleanup_pending_rows(self) -> tuple[list[TorrentState], bool]:
        """DONE rows whose files are provably still on VPS1.

        Truth source is evidence, not a flag: a row belongs when it is
        DONE *and* at least one of its hashes is in the live VPS1 listing
        (`injected_private_hashes` counts — the racing copy is one member
        of the group, not the only one that can still hold bytes). There is
        no persisted "cleaned" marker and adding one would be a claim, so
        a row disappears the moment the source listing stops showing it.

        Sorted oldest grace anchor (`completed_at`, falling back to
        `created_at`) first, i.e. what the janitor reclaims next.

        Returns `(rows, listing_unavailable)`; with no fresh listing the
        rows are empty and the flag is True, so the caller shows a note
        instead of a fabricated or falsely-empty list. Cheap (cache read +
        one cached display query) and never raises.
        """
        try:
            if not self.cleanup_tab_enabled():
                return [], False
            present = self.vps1_present_hashes()
            if present is None:
                return [], True
            rows_fn = getattr(getattr(self, "store", None), "list_done_display", None)
            if not callable(rows_fn):
                return [], True
            rows = list(rows_fn() or [])
        except Exception:
            return [], True
        out: list[TorrentState] = []
        for ts in rows:
            try:
                if getattr(ts, "state", None) != State.DONE:
                    continue
                if not (self._cleanup_row_hashes(ts) & present):
                    continue
            except Exception:
                continue
            out.append(ts)
        out.sort(key=self._cleanup_grace_anchor_key)
        return out, False

    @staticmethod
    def _cleanup_grace_anchor_key(ts: object) -> float:
        """Epoch of the row's grace anchor; unknown stamps sort last."""
        try:
            anchor = getattr(ts, "completed_at", None) or getattr(ts, "created_at", None)
            if anchor is None:
                return float("inf")
            if getattr(anchor, "tzinfo", None) is None:
                anchor = anchor.replace(tzinfo=dt.UTC)
            return float(anchor.timestamp())
        except Exception:
            return float("inf")

    async def _cleanup_pending_summary(self) -> str:
        """One-line footer for the active card: completed rows awaiting VPS1 removal.

        The pinned list intentionally shows only in-flight rows; this keeps
        the cleanup countdown visible there without listing every DONE row.
        """
        try:
            cfg = getattr(self.rt.cleanup, "config", None)
            if cfg is None or not getattr(cfg, "enabled", False):
                return ""
            if not self.rt.chain.adds_seed_entry:
                return ""
        except Exception:
            return ""
        try:
            rows = await asyncio.to_thread(self.store.list_by_state, State.DONE)
        except Exception:
            return ""
        ready = 0
        pending = 0
        soon: float | None = None
        for ts in rows or []:
            remaining = self._cleanup_remaining_seconds(ts)
            if remaining is None:
                continue
            is_ready, seconds_left = remaining
            if is_ready:
                ready += 1
                continue
            pending += 1
            if seconds_left is not None and (soon is None or seconds_left < soon):
                soon = seconds_left
        total = ready + pending
        if not total:
            return ""
        parts = []
        if ready:
            parts.append(f"{ready} ready")
        if soon is not None:
            eta = self._cleanup_eta_compact(soon)
            if eta:
                parts.append(f"next in ~{eta}")
        return f"🧹 VPS1 cleanup: {total} done · " + " · ".join(parts)

    async def _cleanup_resolve_row(self, norm: str) -> tuple[object | None, str]:
        """Resolve one tracked row by full hash or 6+ hex prefix.

        DONE rows are invisible to `list_active_inflight`, so the prefix
        scan reads the full table (`store.all`). Returns (row, "") on a
        unique match, else (None, reply text).
        """
        store = getattr(self, "store", None)
        if store is None:
            return None, "Action failed: store not attached."
        if len(norm) == 40:
            try:
                row = await asyncio.to_thread(store.get, norm, include_blob=False)
            except Exception as e:  # noqa: BLE001
                return None, f"Lookup failed: {self._cleanup_error_text(e)}"
            if row is None:
                return None, f"No tracked torrent is {norm}."
            return row, ""
        if len(norm) < 6:
            return None, "Use /clean_<6+ hex chars> or the full 40-char hash."
        try:
            rows = await asyncio.to_thread(store.all)
        except Exception as e:  # noqa: BLE001
            return None, f"Lookup failed: {self._cleanup_error_text(e)}"
        want = [
            r
            for r in rows or []
            if (getattr(r, "source_infohash", "") or "").lower().startswith(norm)
        ]
        if len(want) == 1:
            return want[0], ""
        if len(want) > 1:
            return None, (
                f"/clean_{norm} matches {len(want)} torrents; send /clean_<full 40-char hash>."
            )
        return None, f"No tracked torrent starts with {norm!r}."

    async def _cleanup_now_command(self, message: Any = None) -> str:
        """Run one VPS1 cleanup pass now (operator `/cleanup`).

        Serialized with the janitor loop via `_janitor_lock`. `force`
        bypasses only the startup-delay/cadence gates: every safety, grace,
        H&R and blob-preservation rule still applies. Returns the reply
        text (`message` is accepted for handler-signature symmetry; the
        Telegram bot sends the reply).
        """
        try:
            cfg = getattr(self.rt.cleanup, "config", None)
        except Exception:
            cfg = None
        if cfg is None or not getattr(cfg, "enabled", False):
            return "Cleanup is disabled — set [cleanup] enabled = true first."
        try:
            if not self.rt.chain.adds_seed_entry:
                return (
                    "Cleanup is disabled for this chain: no seeding entry is "
                    "added, so completed content cannot be verified."
                )
        except Exception:
            return "Cleanup is unavailable: the chain is not loaded."
        lock = None
        try:
            loop_lock = getattr(self, "_loop_lock", None)
            if callable(loop_lock):
                lock = loop_lock("_janitor_lock")
        except Exception:
            lock = None
        try:
            if lock is None:
                summary = await self._maybe_cleanup_source(force=True)
            else:
                async with lock:
                    summary = await self._maybe_cleanup_source(force=True)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            return f"Cleanup failed: {self._cleanup_error_text(e)}"
        if summary is None:
            return "Cleanup did not run."
        return self._cleanup_summary_text(summary)

    async def _clean_task_command(self, target: str, message: Any = None) -> str:
        """Clean one completed row's VPS1 copy now (operator `/clean_<hash>`).

        Bypasses grace/idle on purpose (the operator asked for it) but keeps
        every safety check: DONE only, protected patterns, usable swarm
        stats, fuse byte verification, H&R minimums, no live race activity,
        and .torrent preservation for un-injected copies. The activity keep
        obeys `[cleanup].ignore_activity_under_pressure` exactly like the
        janitor's (the same pressure signal, no extra threshold): when the
        disk is short and VPS2 provably serves the content, VPS1 goes.
        Dry-run deletes nothing. Returns the reply text.
        """
        try:
            cfg = getattr(self.rt.cleanup, "config", None)
        except Exception:
            cfg = None
        if cfg is None or not getattr(cfg, "enabled", False):
            return "Cleanup is disabled — set [cleanup] enabled = true first."
        try:
            if not self.rt.chain.adds_seed_entry:
                return (
                    "Cleanup is disabled for this chain: no seeding entry is "
                    "added, so completed content cannot be verified."
                )
        except Exception:
            return "Cleanup is unavailable: the chain is not loaded."
        norm = str(target or "").strip().lower()
        if not norm or any(c not in "0123456789abcdef" for c in norm):
            return "Use /clean_<hash> (6+ hex chars, or the full 40-char hash)."
        row, err = await self._cleanup_resolve_row(norm)
        if row is None:
            return err
        title = str(getattr(row, "source_name", "") or norm[:10])[:60]
        try:
            state = getattr(row, "state", None)
            state_label = getattr(state, "value", "unknown")
        except Exception:
            state, state_label = None, "unknown"
        if state != State.DONE:
            return (
                f"{title} is {state_label} — only completed rows can be cleaned; "
                "MOVING copies clean automatically under pressure."
            )
        try:
            torrents = await rpc(
                self.source_client.list_torrents(category=self.rt.intake.category),
                getattr(self, "cfg", None),
                "source manual cleanup list_torrents",
            )
        except Exception as e:  # noqa: BLE001
            return f"Cleanup failed: {self._cleanup_error_text(e)}"
        by_hash: dict[str, Torrent] = {}
        for t in torrents or []:
            try:
                by_hash[t.infohash.lower()] = t
            except Exception:
                continue
        group = self._cleanup_group_for(row, by_hash)
        if not group:
            return f"No live VPS1 copy for {title} (already cleaned?)."
        dry_run = bool(getattr(cfg, "dry_run", True))
        if self._cleanup_protected(row):
            return f"Keeping VPS1: {title} matches a protected pattern."
        try:
            stats_unknown = any(self._cleanup_stats_unknown(m) for m in group)
        except Exception:
            stats_unknown = True
        if stats_unknown:
            return f"Keeping VPS1: {title} reports no usable VPS1 stats."
        try:
            healthy = await self._cleanup_fuse_healthy(row, dry_run)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            return (
                f"Keeping VPS1: fuse verification failed for {title} "
                f"({self._cleanup_error_text(e)})."
            )
        if not healthy:
            return f"Keeping VPS1: fuse copy of {title} is not verified."
        if not self._cleanup_minimums_met(group, cfg):
            return f"Keeping VPS1: H&R minimums are not met for {title}."
        try:
            active = [m for m in group if self._cleanup_member_active(m, cfg)]
        except Exception:
            # Unknowable activity is activity: fail closed.
            active = list(group)
        if active:
            # Same unlock as the janitor, same signal: the override only
            # applies while THIS run reads pressure (bursting, or free below
            # the low watermark). No extra probe and no second threshold —
            # the cached free-space reading is the one the janitor derived
            # its cadence from, and `_cleanup_pressure` answers for it.
            try:
                pressure_free = getattr(self, "_last_source_free_bytes", None)
            except Exception:
                pressure_free = None
            try:
                pressure_bursting = bool(self._cleanup_pressure(cfg, pressure_free)[4])
            except Exception:
                pressure_bursting = False
            if not (
                self._cleanup_activity_override()
                and self._cleanup_pressured(cfg, pressure_free, pressure_bursting)
            ):
                return f"Keeping VPS1: {title} still shows race activity."
            self._cleanup_note_pressure_override(row, active, len(group), pressure_free)
        try:
            preserved = await self._cleanup_preserve_racing_blobs(row, group, dry_run)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            return (
                f"Keeping VPS1: could not preserve .torrent bytes for {title} "
                f"({self._cleanup_error_text(e)})."
            )
        if not preserved:
            return f"Keeping VPS1: could not preserve .torrent bytes for {title}."
        if dry_run:
            return f"dry-run is on — would delete {len(group)} VPS1 torrent(s) for {title}."
        try:
            ok = await self._delete_source_group(row, group, cfg, False)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            return f"Cleanup failed for {title}: {self._cleanup_error_text(e)}"
        if not ok:
            return f"Delete failed for {title} — will retry next janitor run."
        return f"Deleted {len(group)} VPS1 torrent(s) for {title}."

    @staticmethod
    def _cleanup_sort_candidates(
        candidates: list[tuple[float, float, TorrentState, list[Torrent]]],
        pressured: bool,
    ) -> list[tuple[float, float, TorrentState, list[Torrent]]]:
        """Order deletion candidates for one janitor run.

        Calm and roomy: oldest-first (seed time preserved), biggest among
        equal ages. Under pressure or burst: DONE rows before MOVING rows
        (fully secured/re-added content first), then biggest-first, then
        oldest. Candidates are `(sort_key, size, ts, group)`.
        """

        def _key(c: tuple[float, float, TorrentState, list[Torrent]]):
            if pressured:
                return (0 if c[2].state == State.DONE else 1, -c[1], c[0])
            return (c[0], -c[1])

        return sorted(candidates, key=_key)

    def _cleanup_group_for(self, ts: TorrentState, by_hash: dict[str, Torrent]) -> list[Torrent]:
        """Live VPS1 torrents holding this row's content.

        Same normalized release name plus KNOWN and EQUAL total size on
        both sides. Unknown sizes never match: grouping a size-unknown
        member (or row) by name alone could delete a different release's
        only VPS1 copy. Only live client entries are returned —
        already-vanished torrents need no work.
        """
        from .content_keys import same_content as _same_content

        try:
            want_size = int(ts.total_bytes or 0)
        except (TypeError, ValueError):
            want_size = 0
        if not want_size:
            return []
        out: list[Torrent] = []
        for t in by_hash.values():
            try:
                got_size = int(t.size_bytes or 0)
            except (TypeError, ValueError):
                got_size = 0
            # Destructive grouping: known-and-equal sizes only.
            if not _same_content(
                t.name or "",
                got_size,
                ts.source_name or "",
                want_size,
                allow_unknown=False,
            ):
                continue
            if self.rt.chain.single_box and self._cleanup_member_seeding(t):
                # One client on both sides: this member IS the seed copy.
                log.info(
                    "cleanup: keeping %s — the entry seeds from the seed path",
                    (t.name or "")[:60],
                )
                continue
            out.append(t)
        return out

    def _cleanup_member_seeding(self, torrent: object) -> bool:
        """True when this client entry's save_path is (or may be) the seed copy."""
        try:
            from .path_ownership import PathOwner, classify_save_path

            save_path = str(getattr(torrent, "save_path", "") or "").strip()
            if not save_path:
                return False
            return classify_save_path(self.cfg, save_path) is PathOwner.FUSE
        except Exception:
            # Unknowable ownership must never become a delete.
            return True

    def _cleanup_protected(self, ts: TorrentState) -> bool:
        """True when the release matches a protected pattern (never delete)."""
        try:
            patterns = list(getattr(self.rt.cleanup.config, "protected_patterns", []) or [])
        except Exception:
            return False
        name = (ts.source_name or "").lower()
        for pat in patterns:
            try:
                text = str(pat).strip().lower()
                if text and text in name:
                    log.info(
                        "cleanup: %s matches protected pattern %r; keeping",
                        ts.source_name[:60],
                        pat,
                    )
                    return True
            except Exception:
                continue
        return False

    def _cleanup_member_active(self, t: Torrent, cfg: object) -> bool:
        """True when a VPS1 torrent shows race activity (do not delete).

        Upload rate at/above the threshold OR any leechers attached means the
        swarm is still working — or the client simply doesn't report stats
        (all zeros is "unknown", handled by the caller, not here).
        """
        try:
            thresh = int(getattr(cfg, "activity_upspeed_bps", 65536) or 0)
        except (TypeError, ValueError):
            thresh = 65536
        if thresh <= 0:
            # 0 disables the upspeed gate (matches the 0 = disabled
            # convention elsewhere): only attached leechers count.
            try:
                return int(t.num_leechers or 0) > 0
            except (TypeError, ValueError):
                return False
        try:
            return int(t.upspeed_bps or 0) >= thresh or int(t.num_leechers or 0) > 0
        except (TypeError, ValueError):
            return False

    @staticmethod
    def _cleanup_stats_unknown(t: Torrent) -> bool:
        """True when the member's self stats are all zero (unreported, not quiet).

        A daemon that reports nothing yields zeros — indistinguishable from
        idle, so fail closed and keep. Only SELF stats count (upspeed,
        uploaded, ratio, seed time): swarm counts (leechers/seeds) are
        observed signals, and any lifetime seeding history (even a second)
        proves the stats channel works. Never raises.
        """
        try:
            up = int(getattr(t, "upspeed_bps", 0) or 0)
            uploaded = int(getattr(t, "total_uploaded_bytes", 0) or 0)
            ratio = float(getattr(t, "ratio", 0.0) or 0.0)
            seed_s = float(getattr(t, "seeding_time_seconds", 0) or 0)
        except (TypeError, ValueError):
            return True
        return up <= 0 and uploaded <= 0 and ratio <= 0 and seed_s <= 0

    def _cleanup_minimums_met(self, group: list[Torrent], cfg: object) -> bool:
        """H&R minimums across every group member (0 = disabled).

        Conservative AND-semantics: every copy must satisfy ratio/seed-time,
        since each client entry carries its own tracker obligation. Unknown
        stats (<=0) fail closed when the corresponding minimum is enabled.
        """
        try:
            min_ratio = float(getattr(cfg, "min_ratio", 0.0) or 0.0)
        except (TypeError, ValueError):
            min_ratio = 0.0
        try:
            min_seed_s = float(getattr(cfg, "min_seed_hours", 0.0) or 0.0) * 3600.0
        except (TypeError, ValueError):
            min_seed_s = 0.0
        if min_ratio <= 0 and min_seed_s <= 0:
            return True
        for m in group:
            try:
                ratio = float(m.ratio or 0.0)
            except (TypeError, ValueError):
                ratio = 0.0
            try:
                seed_s = float(m.seeding_time_seconds or 0)
            except (TypeError, ValueError):
                seed_s = 0.0
            if min_ratio > 0 and ratio < min_ratio:
                return False
            if min_seed_s > 0 and seed_s < min_seed_s:
                return False
        return True

    async def _cleanup_verdict(
        self,
        ts: TorrentState,
        group: list[Torrent],
        grace_s: float,
        now_utc: dt.datetime,
        dry_run: bool,
        free_bytes: int | None,
        urgent: bool | None = None,
        bursting: bool = False,
        emergency: str | None = None,
        blocked: dict[str, int] | None = None,
    ) -> tuple[float, float, TorrentState, list[Torrent]] | None:
        """Decide deletion for one row; None = keep this run.

        Returns (completed_sort_key, size_bytes, ts, group) for candidates.
        Updates vps1_last_activity_at when swarm activity is observed (skipped
        in dry-run mode, which performs zero state mutations). `urgent`
        (None = read the live flag) lets the MOVING early path run when a
        newly discovered release did not fit, even above the low watermark.
        `bursting` is this tick's `_cleanup_pressure` verdict (never
        recomputed here); with `[cleanup].ignore_activity_under_pressure` on
        and pressure real, the activity keep below is skipped — that gate
        ONLY. Verification, H&R minimums, unknown stats, protected patterns,
        state and the blob preservation in the delete loop stay fail-closed,
        so a row that cannot be proven seeded on VPS2 is still kept whatever
        the disk says. Accepted trade: a live peer's download is cut short
        and VPS2 takes over serving it. `blocked` (optional) counts which
        keep fired, for the one-line "nothing eligible" breakdown.

        `emergency` is this run's `_cleanup_emergency_trigger` name (None =
        no emergency). It changes TIMING and nothing else: a fuse-VERIFIED
        DONE row stops waiting out the grace period (and no longer needs the
        idle-confirm fast lane) because the bytes are provably served by VPS2
        already and VPS1 has to land the next race. Rows that verification
        cannot prove are never rescued by it, the activity keep stays under
        `ignore_activity_under_pressure`, and MOVING keeps its own
        early-pressure logic untouched.
        """
        cfg = self.rt.cleanup.config

        def _keep(key: str) -> None:
            if blocked is None:
                return
            try:
                blocked[key] = int(blocked.get(key, 0)) + 1
            except Exception:
                pass

        # 1. Observe swarm activity first: any active member bumps the stamp
        #    and vetoes deletion this run (the race is still going).
        rescued: list[Torrent] = []
        lively = [m for m in group if self._cleanup_member_active(m, cfg)]
        if lively:
            if not dry_run:
                # Narrow stamp (no whole-row overwrite): worker fields
                # must survive the janitor's observation.
                try:
                    await asyncio.to_thread(
                        self.store.set_vps1_activity, ts.source_infohash, now_utc
                    )
                except Exception as e:  # noqa: BLE001
                    # The veto below still holds this run, but a lost stamp
                    # makes the row look quieter to the next pass.
                    log.warning(
                        "cleanup: could not stamp VPS1 activity for %s "
                        "(grace window may look stale): %s",
                        ts.source_infohash[:10],
                        e,
                    )
            # Pressure override: unlocks this keep alone, and only while the
            # janitor's own pressure signal is on for THIS run. The stamp
            # above is still recorded — the activity really happened.
            if not (
                self._cleanup_activity_override()
                and self._cleanup_pressured(cfg, free_bytes, bursting)
            ):
                log.debug(
                    "cleanup: %s still active on VPS1 (%d/%d racing); keeping",
                    ts.source_name[:60],
                    len(lively),
                    len(group),
                )
                _keep("activity")
                return None
            # Remembered, not announced yet: the line belongs at the moment
            # the row really becomes a candidate, so a later verification keep
            # never leaves a "deleting" claim sitting in the log.
            rescued = lively
        # 1b. Unknown self stats (stat-less client): never read as quiet.
        # A stat-less Deluge looks exactly like an idle swarm — deleting on
        # that evidence alone risks an H&R hit on a live race. Keep without
        # bumping the activity stamp (unknown is not activity). Pressure
        # never relaxes this one: an unreported swarm is unproven, not quiet.
        if any(self._cleanup_stats_unknown(m) for m in group):
            log.debug("cleanup: %s has unknown swarm stats; keeping", ts.source_name[:60])
            _keep("unknown stats")
            return None
        # 2. Opt-in H&R minimums (default off — same-account fuse seeding
        #    keeps the account compliant; see [cleanup] docs).
        if not self._cleanup_minimums_met(group, cfg):
            _keep("minimums")
            return None
        # 3. State-specific safety + timing.
        completed = ts.completed_at
        # Rows adopted without completed_at must still serve the grace period;
        # age them from created_at (the adoption instant).
        age_s = (
            (now_utc - (completed or ts.created_at)).total_seconds()
            if (completed or ts.created_at)
            else 0.0
        )
        # Set when the emergency (not the idle lane) is what made this row
        # eligible: the INFO belongs to the moment the row really becomes a
        # candidate, same as the pressure-override note below.
        emergency_rescued = False
        if ts.state == State.DONE:
            if not await self._cleanup_fuse_healthy(ts, dry_run):
                _keep("verification")
                return None
            if age_s < grace_s:
                # Without a completion stamp the idle fast lane proves only
                # that VPS1 has been quiet, not that the seed has served the
                # grace period on VPS2 (recovery adopts DONE rows this way).
                # The same proof is missing for an emergency bypass — an
                # adopted row was never observed racing on VPS2 at all — so it
                # stays blocked by grace whatever the disk says.
                if completed is None:
                    _keep("grace")
                    return None
                if emergency is not None:
                    # Emergency: the box cannot land the next race. The bytes
                    # are fuse-VERIFIED (the gate above passed), so VPS2 already
                    # serves them; only the politeness delay is dropped. The
                    # activity, unknown-stats, minimums and protection keeps
                    # ran above and are NOT bypassed.
                    emergency_rescued = True
                elif not self._cleanup_idle_confirmed(ts, group, now_utc, cfg):
                    _keep("grace")
                    return None
        elif ts.state == State.MOVING:
            # Early pressure path: bytes are 100% on the VPS2 SSD (MOVING is
            # only entered after full completion) but not yet moved. Allowed
            # when VPS1 is under disk pressure, or when a newly discovered
            # release did not fit at all (urgent), with an idle swarm and
            # only while the SSD bytes are verifiably still present.
            try:
                low_free = int(getattr(cfg, "low_watermark_free_bytes", 0) or 0)
            except (TypeError, ValueError):
                low_free = 0
            pressured = bool(low_free and free_bytes is not None and free_bytes < low_free)
            if urgent is None:
                try:
                    urgent = bool(getattr(self, "_cleanup_urgent", False))
                except Exception:
                    urgent = False
            if not (pressured or urgent):
                _keep("not pressured")
                return None
            if not self._cleanup_idle_confirmed(ts, group, now_utc, cfg):
                _keep("grace")
                return None
            if not await self._cleanup_ssd_bytes_present(ts):
                _keep("verification")
                return None
        else:
            _keep("state")
            return None
        _anchor = completed or ts.created_at
        sort_key = _anchor.timestamp() if _anchor else now_utc.timestamp()
        # Grouping guarantees a known row total (unknown sizes never
        # group), so this is always the row's own byte count.
        size = float(ts.total_bytes or 0)
        if rescued:
            self._cleanup_note_pressure_override(ts, rescued, len(group), free_bytes)
        if emergency_rescued and emergency is not None:
            self._cleanup_note_emergency_rescue(ts, free_bytes, emergency, age_s, grace_s)
        return (sort_key, size, ts, group)

    def _cleanup_idle_confirmed(
        self,
        ts: TorrentState,
        group: list[Torrent],
        now_utc: dt.datetime,
        cfg: object,
    ) -> bool:
        """Fast lane: swarm quiet now (checked by caller) AND quiet long enough.

        Primary evidence is a previously recorded activity stamp older than
        the confirm window. Fallback for rows that were never observed
        active (e.g. pre-existing idle backlog): every member's client
        `added_on` predates the window — a torrent cannot have raced before
        it existed, so old + quiet now means finished. Unknown `added_on`
        (<=0) fails closed and waits out the grace period instead.
        """
        try:
            window_s = float(getattr(cfg, "idle_confirm_minutes", 45) or 0) * 60.0
        except (TypeError, ValueError):
            window_s = 45.0 * 60.0
        if window_s <= 0:
            return True
        last = ts.vps1_last_activity_at
        if last is not None:
            try:
                return (now_utc - last).total_seconds() >= window_s
            except Exception:
                return False
        if not group:
            return False
        try:
            # Cross-host clock skew guard: VPS1 added_on is compared against
            # VPS2 now. Future timestamps (VPS1 ahead) and near-epoch values
            # (bogus/unknown, e.g. 1) fail closed instead of fast-laning.
            now_ts = now_utc.timestamp()
            youngest_added = 0
            for m in group:
                added = int(m.added_on or 0)
                if added <= 1_000_000_000:
                    return False
                if added > now_ts + 300:
                    return False
                youngest_added = max(youngest_added, added)
            added_dt = dt.datetime.fromtimestamp(youngest_added, tz=dt.UTC)
            return (now_utc - added_dt).total_seconds() >= window_s
        except Exception:
            return False

    async def _cleanup_fuse_healthy(self, ts: TorrentState, dry_run: bool) -> bool:
        """Re-verify the row really seeds from fuse (entries AND bytes).

        Single batched entry lookup by hashes, plus a file-presence check at
        the fuse target whenever the torrent bytes are available. Entries
        alone are not proof (a skip_check entry reports complete with zero
        bytes). Any doubt fails closed: keep VPS1.

        A row whose bytes verify live is stamped fuse-verified (narrow
        update, no whole-row overwrite). Rows without bytes get one
        blob re-export attempt per janitor run — a healed blob verifies
        normally next time instead of pinning VPS1 forever.

        Dry-run performs zero state mutations: the stamp and the healed
        blob apply in memory only, so rehearsal output matches a live run
        without persisting anything.
        """
        hashes = {
            h.lower()
            for h in (
                ts.dest_infohash,
                ts.cross_seed_infohash,
                *ts.injected_private_hashes.split(","),
            )
            if h
        }
        if not hashes:
            log.warning(
                "cleanup: %s has no recorded fuse hashes; keeping VPS1", ts.source_name[:60]
            )
            return False
        try:
            present: list = []
            for _chunk in chunked(sorted(hashes)):
                present.extend(
                    await rpc(
                        self.dest_client.list_torrents(hashes=_chunk),
                        getattr(self, "cfg", None),
                        "dest cleanup list_torrents",
                    )
                    or []
                )
        except Exception as e:  # noqa: BLE001
            log.warning("cleanup: dest lookup failed for %s: %s", ts.source_infohash[:10], e)
            return False
        # An entry carrying the hash while seeding from the SSD is not a
        # fuse seed — the byte check below would pass on bytes that are
        # about to move. Shared placement rule with the DONE-row verifier.
        _have, _missing_entries = self._fuse_entry_split(present, hashes)
        missing = sorted(_missing_entries)
        if missing:
            log.warning(
                "cleanup: %d fuse entr%s missing for %s (%s); keeping VPS1",
                len(missing),
                "y" if len(missing) == 1 else "ies",
                ts.source_name[:60],
                self._cleanup_preview_list([h[:10] for h in missing], limit=3, width=10),
            )
            return False
        blob = ts._blob or ts.cross_seed_blob
        if not blob and getattr(self, "store", None) is not None:
            try:
                blob = await asyncio.to_thread(self.store.get_blob, ts.source_infohash)
            except Exception:
                blob = None
        if not blob:
            # No bytes to verify against: entries existing is NOT enough
            # (skip_check ghosts). Try healing the blob from the live
            # dest entry; until that succeeds the VPS1 copy stays.
            if await self._cleanup_heal_blob(ts, hashes, dry_run):
                if dry_run:
                    blob = ts.cross_seed_blob or ts._blob
                else:
                    try:
                        blob = await asyncio.to_thread(self.store.get_blob, ts.source_infohash)
                    except Exception:
                        blob = None
        expected = self._expected_fuse_files(blob)
        if not expected:
            log.warning(
                "cleanup: no verifiable fuse bytes for %s; keeping VPS1", ts.source_name[:60]
            )
            return False
        try:
            target = self._verify_mount_for(ts, blob, self._target_mount_for(ts))
        except Exception:  # noqa: BLE001
            return False
        try:
            from .fuse_gate import verify_blob_on_mount as _verify

            check = await _verify(target, blob, expected=expected)
        except Exception:  # noqa: BLE001
            return False
        if check.missing:
            log.warning(
                "cleanup: %d fuse file(s) missing for %s at %s (e.g. %s); keeping VPS1",
                len(check.missing),
                ts.source_name[:60],
                target,
                self._cleanup_preview_list(check.missing),
            )
            return False
        if not ts.fuse_verified and getattr(self, "store", None) is not None:
            try:
                if not dry_run:
                    await asyncio.to_thread(self.store.set_fuse_verified, ts.source_infohash, True)
                ts.fuse_verified = 1
            except Exception as e:  # noqa: BLE001
                # Only the memo is lost: the verdict itself stands, and the
                # next run re-verifies the bytes before deleting.
                log.warning(
                    "cleanup: could not stamp fuse_verified for %s: %s",
                    ts.source_infohash[:10],
                    e,
                )
        return True

    async def _cleanup_heal_blob(
        self,
        ts: TorrentState,
        hashes: set[str],
        dry_run: bool,
    ) -> bool:
        """Re-export .torrent bytes for a blobless row; True when healed.

        One best-effort export per janitor run from the live dest entry
        (persisted via narrow update — in-memory only under dry-run).
        Failures keep the old blobless behavior — the VPS1 copy stays
        until bytes are verifiable.

        The export is RPC-bounded (a wedged WebUI must not stall the whole
        janitor), and the row's own hashes are tried before injected
        cross-seeds: healing from a private copy would quietly change the
        blob future re-adds route by.
        """
        try:
            export = getattr(self.dest_client, "export_torrent", None)
            if not callable(export):
                return False
            order: list[str] = []
            for h in (ts.dest_infohash, ts.cross_seed_infohash):
                norm = (h or "").lower()
                if norm and norm in hashes and norm not in order:
                    order.append(norm)
            order.extend(sorted(h for h in hashes if h not in order))
            for h in order:
                try:
                    blob = await rpc(
                        export(h),
                        getattr(self, "cfg", None),
                        "dest cleanup export_torrent",
                    )
                except Exception:
                    continue
                if isinstance(blob, (bytes, bytearray)) and blob:
                    try:
                        if not dry_run:
                            await asyncio.to_thread(
                                self.store.set_blob, ts.source_infohash, bytes(blob)
                            )
                    except Exception as e:  # noqa: BLE001
                        # Not persisted means the next run repeats the export;
                        # the VPS1 copy stays until bytes are verifiable.
                        log.warning(
                            "cleanup: could not persist healed blob for %s: %s",
                            ts.source_infohash[:10],
                            e,
                        )
                        return False
                    ts.cross_seed_blob = bytes(blob)
                    log.info(
                        "cleanup: healed missing blob for %s from %s", ts.source_name[:60], h[:10]
                    )
                    return True
            return False
        except Exception:
            return False

    async def _cleanup_ssd_bytes_present(self, ts: TorrentState) -> bool:
        """Confirm the MOVING row's bytes are still on the VPS2 SSD.

        Guards the early pressure path: if the SSD bytes vanished (manual
        wipe, failed worker cleanup), deleting VPS1 would leave no copy
        anywhere mid-pipeline.
        """
        h = (ts.dest_infohash or ts.source_infohash or "").lower()
        if not h:
            return False
        try:
            files = await rpc(
                self.dest_client.get_torrent_files(h),
                getattr(self, "cfg", None),
                "source cleanup list files",
            )
        except Exception as e:  # noqa: BLE001
            log.warning("cleanup: cannot list SSD files for %s: %s", ts.source_infohash[:10], e)
            return False
        expected = [
            (f.name, f.size_bytes, getattr(f, "progress", None))
            for f in (files or [])
            if getattr(f, "name", "")
        ]
        if not expected:
            return False
        base = Path(ts.save_path) if ts.save_path else Path(self.rt.storage.save_path)
        # SSD-root assert: a stale save_path pointing at the fuse mount
        # (or anywhere outside the SSD trees) must fail closed — proving
        # bytes there is not proving them on SSD. Resolves, joins, stats
        # and sizes all run in one worker thread: per-file exists/stat on
        # the loop stalls the janitor on a slow disk.
        try:
            _cfg = getattr(self, "cfg", None)
            _raw_roots = [
                getattr(getattr(_cfg, "ssd", None), "path", None),
                getattr(getattr(_cfg, "dest", None), "save_path", None),
            ]
        except Exception:
            _raw_roots = []

        def _verify_ssd() -> bool:
            try:
                _roots: list[Path] = []
                for _raw in _raw_roots:
                    if not isinstance(_raw, (str, Path)) or not str(_raw):
                        continue
                    try:
                        _roots.append(Path(_raw).resolve())
                    except OSError:
                        continue
                try:
                    _base_real = base.resolve()
                except OSError:
                    return False
                if not _roots or not any(
                    _base_real == _r or _base_real.is_relative_to(_r) for _r in _roots
                ):
                    return False
            except OSError:
                return False
            for name, want, progress in expected:
                try:
                    p = _safe_ssd_join(base, name)
                    if p is None:
                        return False
                    if not p.exists():
                        return False
                    # Size alone is not proof: qB pre-allocates deselected files
                    # at full size. Require client-verified progress when known —
                    # and fail closed when unknown: a full-size preallocated
                    # partial must never pass as "complete".
                    if want and (not isinstance(progress, (int, float)) or progress < 0.999):
                        return False
                    if want and p.stat().st_size < want:
                        return False
                except OSError:
                    return False
            return True

        try:
            ok = await asyncio.to_thread(_verify_ssd)
        except Exception:
            return False
        if not ok:
            log.warning(
                "cleanup: SSD bytes unverifiable for %s at %s; keeping VPS1",
                ts.source_infohash[:10],
                base,
            )
        return ok

    async def _cleanup_fuse_seeded_hashes(self, hashes: set[str]) -> set[str] | None:
        """Hashes with a live fuse seed on VPS2 (non-SSD placement).

        The cleanup verifier's evidence: a dest entry carrying the *same*
        hash and serving from the mount (an SSD-placed entry is a download,
        not a seed). Batched and RPC-bounded. Returns None when the lookup
        itself failed — callers must treat that as "unproven", preserve the
        .torrent bytes and never assume the bookkeeping is right.
        """
        if not hashes:
            return set()
        try:
            entries: list = []
            for _chunk in chunked(sorted(hashes)):
                entries.extend(
                    await rpc(
                        self.dest_client.list_torrents(hashes=_chunk),
                        getattr(self, "cfg", None),
                        "dest cleanup racing lookup",
                    )
                    or []
                )
            seeded, _missing = self._fuse_entry_split(entries, set(hashes))
            return set(seeded)
        except Exception as e:  # noqa: BLE001
            log.warning(
                "cleanup: cannot verify fuse seeds for %d racing torrent(s): %s",
                len(hashes),
                e,
            )
            return None

    async def _cleanup_preserve_racing_blobs(
        self, ts: TorrentState, group: list[Torrent], dry_run: bool
    ) -> bool:
        """Make every racing hash in the group survive the VPS1 delete.

        Per member, the same hash must be *provably* seeding from the VPS2
        fuse mount. `injected_private_hashes` is only a hint: a stale or
        optimistic stamp must not let a hash vanish before it actually
        serves from VPS2, so the live dest entries are the evidence. A hash
        without a verified fuse seed — a MOVING row cleaned early, a
        never-injected cross-seed, or a lost injection — first gets its
        .torrent bytes persisted (row blob -> DB -> live VPS1 fetch) so the
        re-inject loop and the late-seed repair can still rebuild the seed.

        Returns True when every hash is either fuse-seeded or durably
        preserved. Any hash that cannot be preserved returns False:
        fail-closed, keep VPS1 this run. Dry-run verifies and fetches to
        prove readability but performs zero writes.
        """
        try:
            injected = {h.lower() for h in ts.injected_private_hashes.split(",") if h}
        except Exception:  # noqa: BLE001
            injected = set()
        store = getattr(self, "store", None)
        member_hashes = {str(getattr(m, "infohash", "") or "").lower() for m in (group or [])}
        member_hashes.discard("")
        seeded = await self._cleanup_fuse_seeded_hashes(member_hashes)
        unverified = seeded is None
        seeded = seeded or set()
        if unverified:
            log.info(
                "cleanup: could not verify fuse seeds for %s; preserving every "
                "racing .torrent before deleting VPS1",
                (ts.source_name or "")[:60],
            )
        for m in group or []:
            h = str(getattr(m, "infohash", "") or "").lower()
            if h and h in seeded:
                continue
            if h and h in injected and not unverified:
                log.warning(
                    "cleanup: %s is recorded as injected but does not seed from "
                    "fuse; preserving its .torrent before deleting VPS1",
                    h[:10],
                )
            blob: bytes | None = None
            # The row's cross_seed_blob is the DOWNLOAD torrent for private
            # releases, not the racing torrent. Never use it here: caching it
            # under the racing hash made every later re-inject add the wrong
            # torrent (the public cross-seed copy) and the racing seed never appeared.
            if h:
                try:
                    blob = await self._load_racing_torrent_bytes(h)
                except Exception as e:  # noqa: BLE001
                    log.warning(
                        "cleanup: could not fetch .torrent bytes for racing torrent %s: %s",
                        h[:10],
                        e,
                    )
                    blob = None
            if not blob:
                log.warning(
                    "cleanup: keeping %s — racing torrent %s is not injected "
                    "and its .torrent bytes cannot be preserved",
                    (ts.source_name or "")[:60],
                    h[:10] or "?",
                )
                return False
            if not dry_run:
                try:
                    saved = await asyncio.to_thread(store.set_racing_blob, h, bytes(blob))
                except Exception as e:  # noqa: BLE001
                    log.warning(
                        "cleanup: could not preserve .torrent bytes for racing torrent %s: %s; "
                        "keeping VPS1",
                        h[:10],
                        e,
                    )
                    return False
                if saved is False:
                    # The store refused the write (row gone/tombstoned): the
                    # bytes are not on disk, so the VPS1 copy must stay.
                    log.warning(
                        "cleanup: .torrent bytes for racing torrent %s were not persisted; "
                        "keeping VPS1",
                        h[:10],
                    )
                    return False
        return True

    async def _delete_source_group(
        self, ts: TorrentState, group: list[Torrent], cfg: object, dry_run: bool
    ) -> bool:
        """Atomically delete one content group from the VPS1 client.

        Members sharing a save_path hold the same on-disk files: entries for
        all of them are removed while files are deleted exactly once (first
        member). Distinct save_paths are handled as independent partitions.
        Any failure aborts that partition for retry next run.
        """
        try:
            delete_files = bool(getattr(cfg, "delete_files", True))
        except (TypeError, ValueError):
            delete_files = True
        # Partitioned by (save_path, size, name): same-directory same-size
        # members are cross-seeds sharing files (deleted exactly once via
        # the first member); different sizes are different files even under
        # one directory. Unknown-size (0) rows NEVER share a partition:
        # lumping distinct releases by (path, 0) lets one delete_files=True
        # wipe another release's data — each 0-size row is its own
        # partition, matched conservatively (first deletes files, and a
        # 0-size partition holds exactly one member so nothing else rides
        # along).
        parts: dict[tuple[str, int, str], list[Torrent]] = {}
        for m in group:
            try:
                size_key = int(m.size_bytes or 0)
            except (TypeError, ValueError):
                size_key = 0
            if size_key > 0:
                parts.setdefault((m.save_path or "", size_key, ""), []).append(m)
            else:
                parts.setdefault((m.save_path or "", 0, (m.infohash or "").lower()), []).append(m)
        ok = True
        for (save_path, _size_key, _name_key), members in parts.items():
            hashes = [m.infohash for m in members]
            # Members sharing a save_path hold the same on-disk files
            # (deleted exactly once via the first member), so the freed
            # size is one copy, not the sum over cross-seeds.
            total_mb = (max((m.size_bytes or 0) for m in members) if members else 0) // (
                1024 * 1024
            )
            if dry_run:
                log.info(
                    "cleanup dry-run: would delete %d VPS1 torrent(s) for %s "
                    "(~%d MB, save_path=%s, delete_files=%s)",
                    len(hashes),
                    ts.source_name[:60],
                    total_mb,
                    save_path or "?",
                    delete_files,
                )
                continue
            log.warning(
                "cleanup: deleting %d VPS1 torrent(s) for %s (~%d MB, save_path=%s)",
                len(hashes),
                ts.source_name[:60],
                total_mb,
                save_path or "?",
            )
            first, rest = hashes[0], hashes[1:]
            try:
                await self.source_client.delete(first, delete_files=delete_files)
            except Exception as e:  # noqa: BLE001
                log.warning("cleanup: delete %s failed: %s", first[:10], e)
                ok = False
                continue
            for h in rest:
                try:
                    await self.source_client.delete(h, delete_files=False)
                except Exception as e:  # noqa: BLE001
                    log.warning("cleanup: delete entry %s failed: %s", h[:10], e)
                    ok = False
        return ok


__all__ = ["CleanupMixin"]
