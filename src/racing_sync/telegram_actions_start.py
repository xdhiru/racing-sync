"""Telegram start actions (split from telegram_actions.py).

`StartActionsMixin` runs the start-side verbs — fetch now, prefer,
skip/unskip, resume, unignore — plus the group-token ladder and the
`/act` sheet they share; moved verbatim, everything resolves through
`self` (instance state and sibling mixins).
"""

import asyncio
import datetime as dt
import logging
from typing import Any

from .state import State
from .telegram_buttons import ButtonMaker
from .telegram_protocol import (
    CancelScope,
    _action_error_text,
    _resume_command,
    _scrubbed,
)
from .telegram_views import (
    _group_active_items,
    _live_group_by_gid,
    _member_button_labels,
    _member_hash,
)

# Keep the historic logger name so log output is unchanged by the split.
log = logging.getLogger("racing_sync.telegram_bot")


class StartActionsMixin:
    async def _start_single_command(self, kind: str, full_hash: str, message: Any) -> None:
        """Typed full-hash command: keepq for cancel, direct otherwise.

        The "now" kind is state-dependent: WAITING rows use their starting
        bytes, grace-held rows start at once.
        """
        if kind == "injectfuse":
            await self._start_injectfuse_single(full_hash, message)
            return
        try:
            row = await asyncio.to_thread(self._store.get, full_hash)
        except Exception as e:  # noqa: BLE001
            # Same rule as the snapshot cancel: a failed read is not a
            # gone row — say so instead of stranding a live torrent.
            try:
                await self._reply(_action_error_text(e)[:300], reply_to=message)
            except Exception:
                pass
            return
        if row is None:
            try:
                await self._reply("Already gone from tracking", reply_to=message)
            except Exception:
                pass
            return
        title = str(getattr(row, "source_name", "") or full_hash[:10])[:60]
        if kind == "files":
            await self._files_for_row(row, message)
            return
        if kind == "forcestart":
            # Full-hash fast path (group numbers go through
            # _start_group_command): same arming, one row.
            _hh = (full_hash or "").strip().lower()
            try:
                _coord = getattr(self, "_coord", None)
                if _coord is None:
                    raise LookupError("bot not attached")
                _forced = getattr(_coord, "_forced", None)
                if not isinstance(_forced, set):
                    _forced = set()
                    try:
                        _coord._forced = _forced
                    except Exception:
                        pass
                _forced.add(_hh)
                from .task import handles_for

                _woke = 0
                for _hd in handles_for(self, _hh):
                    try:
                        _hd.force = True
                        _hd.queued = False
                        _hd.event.set()
                        _woke += 1
                    except Exception:
                        continue
                result = (
                    f"Force-start armed for {title}"
                    f"{f' ({_woke} waiter(s) woken)' if _woke else ''} "
                    f"— bypasses slot caps until it leaves the lane."
                )
            except (LookupError, ValueError) as e:
                result = _scrubbed(e)[:300]
            except Exception as e:  # noqa: BLE001
                result = _action_error_text(e)
            try:
                await self._reply(result[:300], reply_to=message)
            except Exception:
                pass
            try:
                self._last_active_cache = None
                await self._refresh_active_message()
            except Exception:
                pass
            return
        if kind in ("skip", "unskip"):
            # Group-scoped even from one hash: expand to the live group
            # first (single hash as the fallback when it is not grouped
            # anymore). See _group_hashes_for for why the hold covers
            # the release, not the tracker copy.
            try:
                _gh = await asyncio.to_thread(self._group_hashes_for, full_hash)
            except Exception:
                _gh = None
            try:
                result = await self._skip_torrents(_gh or [full_hash], hold=(kind == "skip"))
            except Exception as e:  # noqa: BLE001
                result = _action_error_text(e)
            try:
                await self._reply(result[:300], reply_to=message)
            except Exception:
                pass
            try:
                self._last_active_cache = None
                await self._refresh_active_message()
            except Exception:
                pass
            return
        if kind == "cancel":
            _text, _rows = CancelScope("single", full_hash).remember_sheet(f"Forget {title}?")
            try:
                await self._send_sheet(_text, _rows, reply_to=message)
            except Exception:
                pass
            return
        try:
            if kind == "now":
                result = await self._fetch_torrent(full_hash)
            else:
                result = await self._prefer_torrent(full_hash)
        except Exception as e:  # noqa: BLE001
            result = _action_error_text(e)
        try:
            await self._reply(result[:300], reply_to=message)
        except Exception:
            pass
        try:
            self._last_active_cache = None
            await self._refresh_active_message()
        except Exception:
            pass

    def _group_hashes_for(self, full_hash: str) -> list[str] | None:
        """Live group member hashes containing `full_hash`, else None.

        Group-scoped commands (/hold_, /unhold_) act on the whole
        release even from a single hash: every copy shares one hold
        because the hold covers the content (fuse-seeding), not the
        tracker — same all-or-nothing shape as injectfuse (cancel /
        fetch / prefer stay per-member: destructive or surgical).
        Fail-open None: the caller falls back to the single hash.
        """
        try:
            norm = (full_hash or "").strip().lower()
            if not norm:
                return None
            rows = self._store.list_active_inflight()
            for _key, _members in _group_active_items([(_r, None) for _r in rows or []]):
                _hs: list[str] = []
                _hit = False
                for _t, _p in _members or []:
                    try:
                        _h = _member_hash(_t)
                    except Exception:
                        continue
                    if not _h:
                        continue
                    _hs.append(_h)
                    if _h == norm:
                        _hit = True
                if _hit and _hs:
                    return _hs
            return None
        except Exception:
            return None

    def _positional_groups(self, rows: list) -> list:
        """Display groups, with the active status filter applied.

        Numbers address what the operator sees; the gid/hash/prefix
        fallbacks resolve against the full row list.
        """
        try:
            return _group_active_items([(_r, None) for _r in self._positional_active_rows(rows)])
        except Exception:
            return []

    def _group_by_number(self, groups: list, token: str) -> list | None:
        """Members addressed by a 1-based positional number, else None."""
        try:
            if not token.isdigit():
                return None
            _n = int(token)
        except (TypeError, ValueError):
            return None
        if not 1 <= _n <= len(groups or []):
            return None
        try:
            _gk, _mem = groups[_n - 1]
        except Exception:
            return None
        return [t for (t, _) in _mem]

    def _resolve_group_token(self, rows: list, token: str) -> tuple[list, int, bool]:
        """Members for a positional number or a group id.

        One ladder for the group verbs: a number addresses the displayed
        (filtered) list, a group id (older messages) the full row list.
        Returns (members, live_group_count, matched_by_number); empty
        members means neither matched, so callers fall back to a full
        hash / legacy prefix.
        """
        groups = self._positional_groups(rows)
        members = self._group_by_number(groups, token)
        by_number = members is not None
        if members is None:
            try:
                _by_gid = _live_group_by_gid(list(rows or []), token)
            except Exception:
                _by_gid = None
            members = list(_by_gid[1]) if _by_gid is not None else []
        return members, len(groups), by_number

    async def _act_command_entry(self, token: str, message: Any) -> None:
        """Reply with the action sheet for a content group.

        Single displayed entry point (`/act_<n>`): resolves like the
        other group commands (number, group id, hash, prefix) and shows
        only the currently eligible actions as hash-direct buttons.
        """
        try:
            store = getattr(self, "_store", None)
            if store is None:
                try:
                    await self._reply("Action failed: bot not attached.", reply_to=message)
                except Exception:
                    pass
                return
            norm = (token or "").strip().lower()
            try:
                rows = await asyncio.to_thread(store.list_active_inflight)
            except Exception as e:  # noqa: BLE001
                try:
                    await self._reply(_action_error_text(e), reply_to=message)
                except Exception:
                    pass
                return
            members, _live_n, _positional = self._resolve_group_token(rows, norm)
            # Positional numbers rot when the list changes between render
            # and tap: warn when the live group count differs from the
            # last rendered generation (gid/hash/prefix paths are stable
            # and skip this). The sheet still names the resolved title so
            # the operator confirms the file, not the number.
            _gen_warn = ""
            if _positional:
                try:
                    _rendered = int(getattr(self, "_last_rendered_groups", 0) or 0)
                except Exception:
                    _rendered = 0
                if _rendered and _live_n and _rendered != _live_n:
                    _gen_warn = (
                        f"⚠️ List changed since render "
                        f"({_rendered}→{_live_n} titles) — confirm this "
                        f"is the file you meant.\n"
                    )
            if not members and norm:
                try:
                    _by_gid = _live_group_by_gid(list(rows or []), norm)
                except Exception:
                    _by_gid = None
                if _by_gid is not None:
                    members = list(_by_gid[1])
            if not members and norm:
                try:
                    target = await asyncio.to_thread(self._resolve_cancel_target, norm, cmd="act")
                    _gh = await asyncio.to_thread(
                        self._group_hashes_for, (target.source_infohash or "").lower()
                    )
                    if _gh:
                        _all = {(_r.source_infohash or "").lower(): _r for _r in rows or []}
                        members = [_all[h] for h in _gh if h in _all]
                    else:
                        members = [target]
                except LookupError as e:
                    try:
                        await self._reply(_scrubbed(e)[:300], reply_to=message)
                    except Exception:
                        pass
                    return
                except Exception as e:  # noqa: BLE001
                    try:
                        await self._reply(_action_error_text(e), reply_to=message)
                    except Exception:
                        pass
                    return
            if not members:
                try:
                    await self._reply("No live group — refresh the list.", reply_to=message)
                except Exception:
                    pass
                return
            title = self._group_title(members)
            try:
                notes = self._live_notes_for(members)
            except Exception:
                notes = {}
            try:
                _text, _rows = self._sheet_text_and_rows(
                    members, title, getattr(members[0], "total_bytes", 0) if members else 0, notes
                )
                if _gen_warn:
                    _text = _gen_warn + _text
            except Exception as e:  # noqa: BLE001
                try:
                    await self._reply(_action_error_text(e), reply_to=message)
                except Exception:
                    pass
                return
            try:
                await self._send_sheet(_text, _rows, reply_to=message)
            except Exception:
                pass
        except Exception as e:  # noqa: BLE001
            log.debug("act command failed: %s", e)

    async def _start_group_command(self, kind: str, token: str, message: Any) -> None:
        """Typed command: positional number, gid, or legacy hash prefix.

        Positional numbers (what the list shows) and group ids resolve
        against the LIVE list right now; member buttons below carry full
        hashes re-resolved at tap time, so renumbering mid-flow cannot
        misroute. Full hashes and legacy prefixes act on one row.
        """
        token = (token or "").strip().lower()
        if len(token) == 40 and all(c in "0123456789abcdef" for c in token):
            await self._start_single_command(kind, token, message)
            return
        try:
            rows = await asyncio.to_thread(self._store.list_active_inflight)
        except Exception as e:  # noqa: BLE001
            try:
                await self._reply(_action_error_text(e), reply_to=message)
            except Exception:
                pass
            return
        members, _live_n, _by_number = self._resolve_group_token(rows, token)
        if not members:
            try:
                target = await asyncio.to_thread(self._resolve_cancel_target, token, cmd=kind)
                full_hash = target.source_infohash
            except LookupError as e:
                try:
                    await self._reply(_scrubbed(e)[:300], reply_to=message)
                except Exception:
                    pass
                return
            except Exception as e:  # noqa: BLE001
                try:
                    await self._reply(_action_error_text(e), reply_to=message)
                except Exception:
                    pass
                return
            await self._start_single_command(kind, full_hash, message)
            return
        title = self._group_title(members)
        if kind == "files":
            if not members:
                try:
                    await self._reply("Already gone from tracking", reply_to=message)
                except Exception:
                    pass
            else:
                await self._files_for_row(members[0], message)
            return
        if kind == "cancel":
            _labels = _member_button_labels(members)
            if len(_labels) <= 1:
                _hashes = [h for (h, _) in _labels]
                if not _hashes:
                    try:
                        await self._reply("Already gone from tracking", reply_to=message)
                    except Exception:
                        pass
                    return
                try:
                    _text, _rows = CancelScope("single", _hashes[0]).remember_sheet(
                        f"Forget {title}?"
                    )
                    await self._send_sheet(_text, _rows, reply_to=message)
                except Exception:
                    pass
                return
            _maker = ButtonMaker()
            for _h, _lbl in _labels:
                _maker.add(f"✖ Cancel {_lbl}…", f"cancel:{_h}").newline()
            _maker.add(
                f"✖ Cancel all {len(_labels)} copies…", f"cancel:all:{_labels[0][0]}"
            ).newline()
            _maker.add("Close", "abort")
            _rows = _maker.rows()
            try:
                await self._send_sheet(
                    f"Cancel {title} ({len(_labels)} copies): pick below (tracker names).",
                    _rows,
                    reply_to=message,
                )
            except Exception:
                pass
            return
        # injectfuse: whole group, all-or-nothing. The question names
        # the LIVE VPS1 group (torrents added after the row was tracked
        # belong to it); the Yes button below re-resolves live too, so a
        # stale list can never misinject. Yes auto-lifts a prior cancel
        # and stops any SSD download first.
        if kind == "injectfuse":
            _lead_hashes = []
            for _m in members or []:
                try:
                    _mh = _member_hash(_m)
                except Exception:
                    _mh = ""
                if _mh:
                    _lead_hashes.append(_mh)
            if not _lead_hashes:
                try:
                    await self._reply("Already gone from tracking", reply_to=message)
                except Exception:
                    pass
                return
            try:
                coord = getattr(self, "_coord", None)
                if coord is None:
                    raise LookupError("bot not attached")
                resolved = await coord.injectfuse_resolve(_lead_hashes[0])
            except (LookupError, ValueError) as e:
                # VPS1 no longer lists the group (cleaned/pruned): fall
                # back to the tracked members — the remote bytes may
                # still be injectable.
                try:
                    _tracked = []
                    for _m in members or []:
                        try:
                            if _member_hash(_m):
                                _tracked.append(_m)
                        except Exception:
                            continue
                    if not _tracked:
                        raise
                    try:
                        _tsz = getattr(_tracked[0], "total_bytes", 0)
                    except Exception:
                        _tsz = 0
                    resolved = {"members": _tracked, "title": title, "size": _tsz}
                except Exception:
                    try:
                        await self._reply(_scrubbed(e)[:300], reply_to=message)
                    except Exception:
                        pass
                    return
            except Exception as e:  # noqa: BLE001
                try:
                    await self._reply(_action_error_text(e), reply_to=message)
                except Exception:
                    pass
                return
            try:
                _resolved_for_ask = dict(resolved)
            except Exception:
                _resolved_for_ask = resolved
            await self._inject_ask_send(_resolved_for_ask, message)
            return
        # skip/unskip: whole group, all-or-nothing — no member pick
        # (a hold covers the release, every copy of it). Even one
        # member goes direct with the group name on screen.
        if kind in ("skip", "unskip"):
            _hashes = []
            for _m in members or []:
                try:
                    _mh = _member_hash(_m)
                except Exception:
                    _mh = ""
                if _mh:
                    _hashes.append(_mh)
            if not _hashes:
                try:
                    await self._reply("Already gone from tracking", reply_to=message)
                except Exception:
                    pass
                return
            try:
                result = await self._skip_torrents(_hashes, hold=(kind == "skip"))
            except Exception as e:  # noqa: BLE001
                result = _action_error_text(e)
            try:
                await self._reply(result[:300], reply_to=message)
            except Exception:
                pass
            try:
                await self._refresh_active_message()
            except Exception:
                pass
            return
        # forcestart: operator override — arm the lane-cap bypass for the
        # whole group (in-memory force + wake parked waiters). Queued and
        # moving rows start past full lanes; force disarms on leaving the
        # lane and clears on restart (never wedged). No state change here:
        # the woken/spawned worker still honors every other gate.
        if kind == "forcestart":
            _hashes = []
            for _m in members or []:
                try:
                    _mh = _member_hash(_m)
                except Exception:
                    _mh = ""
                if _mh:
                    _hashes.append(_mh)
            if not _hashes:
                try:
                    await self._reply("Already gone from tracking", reply_to=message)
                except Exception:
                    pass
                return
            _armed = 0
            try:
                _coord = getattr(self, "_coord", None)
                if _coord is None:
                    raise LookupError("bot not attached")
                _forced = getattr(_coord, "_forced", None)
                if not isinstance(_forced, set):
                    _forced = set()
                    try:
                        _coord._forced = _forced
                    except Exception:
                        pass
                from .task import handles_for

                for _hh in _hashes:
                    try:
                        _forced.add(_hh)
                        if len(_forced) > 5000:
                            break
                    except Exception:
                        pass
                    try:
                        for _hd in handles_for(self, _hh):
                            try:
                                _hd.force = True
                                _hd.queued = False
                                _hd.event.set()
                                _armed += 1
                            except Exception:
                                continue
                    except Exception:
                        pass
            except (LookupError, ValueError) as e:
                try:
                    await self._reply(_scrubbed(e)[:300], reply_to=message)
                except Exception:
                    pass
                return
            except Exception as e:  # noqa: BLE001
                try:
                    await self._reply(_action_error_text(e), reply_to=message)
                except Exception:
                    pass
                return
            try:
                await self._reply(
                    f"Force-start armed for {title} ({len(_hashes)} "
                    f"cop{'y' if len(_hashes) == 1 else 'ies'}"
                    f"{f', {_armed} waiter(s) woken' if _armed else ''}) — "
                    f"bypasses slot caps until it leaves the lane.",
                    reply_to=message,
                )
            except Exception:
                pass
            try:
                self._last_active_cache = None
                await self._refresh_active_message()
            except Exception:
                pass
            return
        # now: eligible members choose their own copy via hash buttons.
        # A lone eligible copy starts at once; several get one button
        # each. Empty set replies inline. No snapshot: each button
        # carries its copy's hash and execution re-checks eligibility.
        _notes = self._live_notes_for(members)
        _eligible = set(self._now_eligible_hashes(members, _notes))
        _labels = [(h, lbl) for (h, lbl) in _member_button_labels(members) if h in _eligible]
        if not _labels:
            try:
                await self._reply(
                    f"Nothing to {kind} right now (states changed).", reply_to=message
                )
            except Exception:
                pass
            return
        if len(_labels) == 1:
            try:
                result = await self._fetch_torrent(_labels[0][0])
            except Exception as e:  # noqa: BLE001
                result = _action_error_text(e)
            try:
                await self._reply(result[:300], reply_to=message)
            except Exception:
                pass
            try:
                self._last_active_cache = None
                await self._refresh_active_message()
            except Exception:
                pass
            return
        _maker = ButtonMaker()
        for _h, _lbl in _labels:
            _maker.add(f"▶ Start {_lbl} now", f"go:{_h}").newline()
        _maker.add("Close", "abort")
        _rows = _maker.rows()
        try:
            await self._send_sheet(
                f"Start {title}: pick a copy below (tracker names).", _rows, reply_to=message
            )
        except Exception:
            pass

    async def _fetch_torrent(self, infohash: str) -> str:
        """Flag a waiting row to use its starting torrent now.

        WAITING_INDEXER rows use their starting bytes instead of
        waiting out Prowlarr (force_direct + due timer): the
        VPS1 original when reachable, else a supplied .torrent
        attached via /add. NEW
        grace-held rows get their starting bytes instead (force_direct
        only — no state change; the NEW flow honors the flag next tick
        and a worker is woken for this tick): watch drops use the starting
        .torrent, racing rows use the VPS1 original.
        """
        try:
            from .api import _hold_ops_lock
        except Exception:
            _hold_ops_lock = None  # type: ignore[assignment]
        coord = getattr(self, "_coord", None)
        store = getattr(self, "_store", None)
        if coord is None or store is None:
            return "Fetch failed: bot not attached"

        acted: list[str] = []

        def _flag() -> str:
            row = store.get(infohash)
            if row is None:
                raise LookupError("no longer tracked (done/cancelled?)")
            if getattr(row, "skipped", 0):
                return (
                    f"{(row.source_name or infohash[:10])[:50]} is skipped — "
                    f"{_resume_command(infohash)} first"
                )
            if row.state == State.WAITING_INDEXER:
                # Fresh retry window for the direct phase, same as the automatic
                # timeout fallback: an explicit fetch buys full direct retries,
                # not just the remainder of the spent prowlarr window. The row
                # stays WAITING_INDEXER with its timer due now, so the next
                # scheduler wakeup picks it up without an intermediate state.
                row.force_direct = 1
                row.indexer_first_queried_at = dt.datetime.now(dt.UTC)
                row.indexer_attempts = 0
                row.indexer_next_retry_at = dt.datetime.now(dt.UTC)
                try:
                    store.upsert(row)
                except Exception as e:  # noqa: BLE001
                    return f"Fetch failed: {_scrubbed(e)}"
                return (
                    f"Fetching original for {(row.source_name or infohash[:10])[:50]} "
                    f"(bypassing Prowlarr; VPS1 copy, else supplied .torrent)"
                )
            # NEW grace-held rows: "fetch" = use the dropped .torrent now
            # (skip Prowlarr search and grace hold). No state change —
            # NEW rows are worked every tick — and election/in-flight
            # deferrals still apply, since those guard live downloads,
            # not indexer waits.
            if row.state == State.NEW and self._fetch_new_eligible(row):
                try:
                    _upd = getattr(store, "update_columns", None)
                    if callable(_upd):
                        _applied = bool(
                            _upd(infohash, {"force_direct": 1}, expected_state=State.NEW)
                        )
                    else:
                        row.force_direct = 1
                        store.upsert(row)
                        _applied = True
                except Exception as e:  # noqa: BLE001
                    return f"Fetch failed: {_scrubbed(e)}"
                if not _applied:
                    return (
                        f"{(row.source_name or infohash[:10])[:50]} moved state — nothing to fetch"
                    )
                acted.append(infohash)
                try:
                    _is_watch = (
                        bool(coord._is_watch_row(row))
                        if callable(getattr(coord, "_is_watch_row", None))
                        else False
                    )
                except Exception:
                    _is_watch = False
                if _is_watch:
                    return (
                        f"Using dropped .torrent directly for "
                        f"{(row.source_name or infohash[:10])[:50]} "
                        f"(skipping Prowlarr search and grace hold)"
                    )
                return (
                    f"Using VPS1 original directly for "
                    f"{(row.source_name or infohash[:10])[:50]} "
                    f"(skipping Prowlarr search and grace hold)"
                )
            try:
                _label = row.state.value
            except Exception:
                _label = "unknown"
            return f"{(row.source_name or infohash[:10])[:50]} is {_label} — nothing to fetch"

        try:
            if _hold_ops_lock is not None:
                async with _hold_ops_lock(coord):
                    outcome = await asyncio.to_thread(_flag)
            else:
                outcome = await asyncio.to_thread(_flag)
        except LookupError as e:
            return f"Fetch failed: {_scrubbed(e)}"
        except ValueError as e:
            # Illegal transition (row left WAITING_INDEXER concurrently).
            return f"Fetch failed: {_scrubbed(e)}"
        except Exception as e:  # noqa: BLE001
            return f"Fetch failed: {_scrubbed(e)}"
        # NEW watch rows stay NEW: wake one worker now so the flag takes
        # effect this tick (the scheduler is the backstop).
        try:
            if acted:
                _wrow = store.get(acted[0])
                if _wrow is not None:
                    _spawn = getattr(coord, "_spawn_worker", None)
                    if callable(_spawn):
                        _spawn(_wrow)
        except Exception:
            log.debug("fetch wake failed; next tick picks it up")
        return outcome

    async def _prefer_torrent(self, short: str) -> str:
        """Start a grace-held watch row's SSD download now (operator override).

        The coordinator validates (NEW watch row, rank-2, in grace, no
        owner), records a one-shot exemption and returns the row to wake;
        the worker is spawned here (running loop) with next-tick pickup
        as the backstop. Never raises: all outcomes arrive as reply text.
        """
        try:
            from .api import _hold_ops_lock
        except Exception:
            _hold_ops_lock = None  # type: ignore[assignment]
        coord = getattr(self, "_coord", None)
        if coord is None:
            return "Prefer failed: bot not attached"
        prefer = getattr(coord, "prefer_grace_row", None)
        if not callable(prefer):
            return "Prefer failed: coordinator too old"
        try:
            if _hold_ops_lock is not None:
                async with _hold_ops_lock(coord):
                    row, outcome = await asyncio.to_thread(prefer, short)
            else:
                row, outcome = await asyncio.to_thread(prefer, short)
        except LookupError as e:
            return f"Prefer failed: {_scrubbed(e)}"
        except Exception as e:  # noqa: BLE001
            return f"Prefer failed: {_scrubbed(e)}"
        if row is not None:
            try:
                spawn = getattr(coord, "_spawn_worker", None)
                if callable(spawn):
                    spawn(row)
            except Exception as e:  # noqa: BLE001
                log.debug("prefer wake failed (%s); next tick picks it up", e)
        return outcome

    async def _skip_torrents(self, hashes: list[str], *, hold: bool) -> str:
        """Hold (`hold=True`, /hold_) or resume (/unhold_) tracked releases.

        Whole-hash list (a group acts on every copy): the `skipped` flag
        keeps each row's state but gives it no workers — no Prowlarr
        queries, no downloads, no moves — until resumed. Resumed rows
        get a worker woken now (next tick is the backstop) so the
        normal flow (fuse check, VPS1/VPS2 checks, Prowlarr cross-seed
        search) restarts immediately. Never raises: all outcomes arrive
        as reply text.
        """
        verb = "skip" if hold else "unskip"
        try:
            from .api import _hold_ops_lock
        except Exception:
            _hold_ops_lock = None  # type: ignore[assignment]
        coord = getattr(self, "_coord", None)
        store = getattr(self, "_store", None)
        if coord is None or store is None:
            return f"{verb.capitalize()} failed: bot not attached"
        wanted = []
        try:
            for h in hashes or []:
                _h = (h or "").strip().lower()
                if len(_h) == 40 and all(c in "0123456789abcdef" for c in _h):
                    wanted.append(_h)
        except Exception:
            pass
        if not wanted:
            return f"{verb.capitalize()} failed: nothing tracked to act on"

        acted: list[str] = []
        gone = 0

        def _flag() -> None:
            nonlocal gone
            for h in wanted:
                try:
                    row = store.get(h, include_blob=False)
                except Exception:
                    row = None
                if row is None:
                    gone += 1
                    continue
                # Settled rows have no pipeline to hold or resume: skipping
                # one changes nothing, and "resuming" a DONE row spawned a
                # worker that had no state to act on while reporting success.
                try:
                    _st = getattr(getattr(row, "state", None), "value", "")
                except Exception:
                    _st = ""
                if _st in ("done", "failed"):
                    continue
                try:
                    _upd = getattr(store, "update_columns", None)
                    if callable(_upd):
                        _applied = bool(_upd(h, {"skipped": 1 if hold else 0}))
                    else:
                        row.skipped = 1 if hold else 0
                        store.upsert(row)
                        _applied = True
                except Exception:
                    continue
                if _applied:
                    acted.append(h)

        try:
            if _hold_ops_lock is not None:
                async with _hold_ops_lock(coord):
                    await asyncio.to_thread(_flag)
            else:
                await asyncio.to_thread(_flag)
        except Exception as e:  # noqa: BLE001
            return f"{verb.capitalize()} failed: {_scrubbed(e)}"
        if not acted:
            return "Already gone from tracking" if gone else f"Nothing to {verb} (states changed)"
        try:
            _names = []
            for h in acted:
                try:
                    _r = store.get(h, include_blob=False)
                    _names.append(str(getattr(_r, "source_name", "") or h[:10])[:50])
                except Exception:
                    _names.append(h[:10])
        except Exception:
            _names = [h[:10] for h in acted]
        _title = _names[0] if len(_names) == 1 else f"{len(acted)} copies"
        if not hold:
            try:
                _spawn = getattr(coord, "_spawn_worker", None)
                if callable(_spawn):
                    for h in acted:
                        try:
                            _wrow = store.get(h, include_blob=False)
                            if _wrow is not None:
                                _spawn(_wrow)
                        except Exception:
                            continue
            except Exception:
                log.debug("unskip wake failed; next tick picks it up")
            return (
                f"Resumed {_title} — fuse/VPS checks and Prowlarr "
                f"cross-seed search run again next tick"
            )
        return (
            f"Skipped {_title} — held with state kept, no workers until {_resume_command(acted[0])}"
        )

    def _unignore_core(self, target: str) -> tuple[str | None, str | None]:
        """Lift one ignore entry (+ tombstone); returns (name, error).

        Sync core behind _unignore_torrent and the /resume_ composer:
        (name, None) on success, (None, message) when there is nothing
        to lift or it fails. Full 40-char hash only (see _unignore_torrent).
        """
        try:
            norm = (target or "").strip().lower()
        except Exception:
            norm = ""
        if len(norm) != 40 or any(c not in "0123456789abcdef" for c in norm):
            return None, ("Send /resume_<full 40-char infohash> (copy it from the detail card).")
        store = getattr(self, "_store", None)
        if store is None:
            return None, "Action failed: bot not attached."
        try:
            entry = store.find_ignored(norm)
        except LookupError as e:
            return None, str(e)[:300]
        except Exception as e:  # noqa: BLE001
            return None, _action_error_text(e)
        try:
            h = (entry.get("source_infohash") or "").lower()
            name = str(entry.get("source_name") or h[:10])[:60]
        except Exception:
            h, name = norm, norm[:10]
        try:
            store.unignore_torrent(h)
            store.clear_tombstone(h)
        except Exception as e:  # noqa: BLE001
            return None, _action_error_text(e)
        return name, None

    async def _unignore_torrent(self, target: str, message: Any) -> None:
        """Drop one cancelled release from the ignore list (+ tombstone).

        Legacy alias: canonical verb is now `/resume_`, which also
        resumes held rows. This path keeps the exact historical replies.
        Full 40-char hash only: ignored entries are invisible in the
        task list, so prefixes cannot be disambiguated there. Mirrors
        the `unignore` CLI (which refuses while the daemon runs, so
        this is the live path). After this, re-send the .torrent via
        /add or re-drop it. Never raises: all outcomes arrive as replies.
        """
        try:
            name, err = await asyncio.to_thread(self._unignore_core, target)
        except Exception as e:  # noqa: BLE001
            name, err = None, _action_error_text(e)
        if err is not None and name is None:
            try:
                await self._reply(err[:300], reply_to=message)
            except Exception:
                pass
            return
        try:
            await self._reply(
                f"Unignored {name} — re-send its .torrent via /add "
                f"(or re-drop it) to track it again.",
                reply_to=message,
            )
        except Exception:
            pass

    async def _resume_command_entry(self, token: str, message: Any) -> None:
        """Unified resume: unhold tracked rows + lift ignored hashes.

        Replaces `/unhold_` + `/unignore_` with one verb branching on
        current state (digits address a live group like the other group
        commands; a full hash reaches tracked rows AND invisible ignore
        entries; shorter prefixes resolve tracked rows only — ignored
        entries still need the full hash, as with `/unignore_` before).
        Never raises: all outcomes arrive as replies.
        """
        try:
            store = getattr(self, "_store", None)
            if store is None:
                try:
                    await self._reply("Action failed: bot not attached.", reply_to=message)
                except Exception:
                    pass
                return
            norm = (token or "").strip().lower()
            hashes: list[str] = []
            if norm.isdigit():
                try:
                    rows = await asyncio.to_thread(store.list_active_inflight)
                except Exception as e:  # noqa: BLE001
                    try:
                        await self._reply(_action_error_text(e), reply_to=message)
                    except Exception:
                        pass
                    return
                _members = self._group_by_number(self._positional_groups(rows), norm) or []
                for _t in _members:
                    try:
                        _h = _member_hash(_t)
                    except Exception:
                        continue
                    if _h:
                        hashes.append(_h)
                if not hashes:
                    try:
                        await self._reply(
                            f"No live group #{norm} — refresh the list.", reply_to=message
                        )
                    except Exception:
                        pass
                    return
            elif len(norm) == 40 and all(c in "0123456789abcdef" for c in norm):
                # Single hash: the shared core unholds tracked rows and
                # lifts ignored hashes (each half no-ops cleanly absent).
                hashes = [norm]
            else:
                # Group id (older messages) or legacy torrent prefix for
                # tracked rows. A hex token matching nothing tracked keeps
                # the old /unignore_ hint (ignored entries need the full
                # hash); anything else keeps the resolver text.
                _by_gid = None
                try:
                    _rows_g = await asyncio.to_thread(store.list_active_inflight)
                    _by_gid = _live_group_by_gid(list(_rows_g or []), norm)
                except Exception:
                    _by_gid = None
                if _by_gid is not None:
                    _gk, _mem = _by_gid
                    for _t in list(_mem or []):
                        try:
                            _h = _member_hash(_t)
                        except Exception:
                            continue
                        if _h:
                            hashes.append(_h)
                if not hashes:
                    try:
                        target = await asyncio.to_thread(
                            self._resolve_cancel_target, norm, cmd="resume"
                        )
                        hashes = [(target.source_infohash or "").lower()]
                    except LookupError as e:
                        try:
                            _hexish = bool(norm) and all(c in "0123456789abcdef" for c in norm)
                        except Exception:
                            _hexish = False
                        await self._reply(
                            ("Send /resume_<full 40-char infohash> (copy it from the detail card).")
                            if _hexish
                            else str(e)[:300],
                            reply_to=message,
                        )
                        return
                    except Exception as e:  # noqa: BLE001
                        try:
                            await self._reply(_action_error_text(e), reply_to=message)
                        except Exception:
                            pass
                        return
                if not hashes:
                    try:
                        await self._reply(
                            f"No live group #{norm} — refresh the list.", reply_to=message
                        )
                    except Exception:
                        pass
                    return
            try:
                reply = await self._resume_hashes_reply(hashes)
            except Exception as e:  # noqa: BLE001
                reply = f"Resume failed: {e}"
            try:
                await self._reply(reply[:300], reply_to=message)
            except Exception:
                pass
            try:
                self._last_active_cache = None
                await self._refresh_active_message()
            except Exception:
                pass
        except Exception as e:  # noqa: BLE001
            log.debug("resume command failed: %s", e)
