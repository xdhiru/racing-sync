"""Telegram cancel execution (split from telegram_bot.py).

`ActionsMixin` carries what the bot *does* when told — cancel
execution first, command entries and torrent executors to follow —
moved verbatim; everything resolves through `self` (instance state
and sibling mixins) and the `telegram_protocol` / `telegram_views`
helpers.
"""

import asyncio
import datetime as dt
import logging
from typing import Any

from .state import State
from .telegram_buttons import ButtonMaker
from .telegram_protocol import (
    _MATCH_MAX_BYTES,
    _MATCH_MIN_CHARS,
    CancelScope,
    _action_error_text,
    _match_token,
    _resume_command,
    _scrubbed,
)
from .telegram_views import (
    _batch_display,
    _group_active_items,
    _live_group_by_gid,
    _member_button_labels,
    _member_hash,
    _safe_display_name,
    _size_compact,
    _tracker_domain,
)

# Keep the historic logger name so log output is unchanged by the split.
log = logging.getLogger("racing_sync.telegram_bot")


class ActionsMixin:
    def _positional_active_rows(self, rows: list) -> list:
        """Rows the active list is currently numbering (status filter applied).

        Positional commands (`/act_N`, `/now_N`, `/cancel_N`, `/resume_N`)
        quote what the operator sees; the renderer numbers only the rows
        passing `_status_filter` (`telegram_refresh.py`), so resolving a
        number against the raw store list would misroute #1 to a
        filtered-out row. Identity tokens (group id, full hash, legacy
        prefix) are stable across filters and must keep the full list.
        Fails open to the unfiltered rows — never raises.
        """
        try:
            full = list(rows or [])
        except Exception:
            return []
        try:
            flt = getattr(self, "_status_filter", None) or "all"
        except Exception:
            flt = "all"
        if not flt or str(flt).lower() == "all":
            return full
        try:
            from .telegram_protocol import match_status_filter

            return [
                _r
                for _r in full
                if match_status_filter(getattr(getattr(_r, "state", None), "value", ""), flt)
            ]
        except Exception:
            return full

    async def _execute_cancel_one(
        self, infohash: str, *, delete_files: bool, remember: bool = True
    ) -> str:
        """Forget one release; files kept iff not `delete_files`.

        `remember=False` skips the ignore entry and hard-deletes the row
        (never-seen semantics): workers are stopped first so no stale
        upsert resurrects it, and a later re-drop re-ingests from scratch.
        """
        _verb = "Cancelled" if delete_files else "Kept files for"
        _done = (
            ("(removed + ignored)" if remember else "(removed, can be re-added)")
            if delete_files
            else (
                "untracked + ignored, data left in place"
                if remember
                else "untracked, data left in place (can be re-added)"
            )
        )
        try:
            from .api import _hold_ops_lock
        except Exception:
            _hold_ops_lock = None  # type: ignore[assignment]
        try:
            from .forget import forget_torrent
        except Exception as e:  # noqa: BLE001
            return _action_error_text(e)
        coord = getattr(self, "_coord", None)
        store = getattr(self, "_store", None)
        if coord is None or store is None:
            return "Action failed: bot not attached"
        dest = getattr(coord, "dest_client", None)
        cfg = getattr(coord, "cfg", None)
        if dest is None or cfg is None:
            return "Action failed: coordinator not ready"
        # Snapshot the detail card before forget deletes the row (the
        # telegram_message_id lives on the row): on success the card is
        # edited to CANCELLED so it doesn't freeze at its last live state.
        detail_msg_id: int | None = None
        try:
            cached = getattr(self, "_detail_cache", None)
            if isinstance(cached, dict):
                cached_id = cached.get(infohash)
                if isinstance(cached_id, int) and cached_id > 0:
                    detail_msg_id = cached_id
            if detail_msg_id is None:
                detail_msg_id = await asyncio.to_thread(store.get_telegram_message_id, infohash)
        except Exception:
            detail_msg_id = None
        _release_key = infohash
        _keep_reservation = False
        try:
            _stop = getattr(coord, "stop_workers_for", None)
            # Worker shutdown rides inside forget_torrent (primary + every
            # pair, failures recorded in result errors) — never silent.
            _stop_cb = _stop if callable(_stop) else None
            _forget_kwargs = dict(
                cfg=cfg,
                dest=dest,
                store=store,
                target=infohash,
                apply=True,
                delete_files=delete_files,
                ignore=remember,
                hard=not remember,
                stop_workers=_stop_cb,
            )
            if _hold_ops_lock is not None:
                async with _hold_ops_lock(coord):
                    result = await forget_torrent(**_forget_kwargs)
            else:
                result = await forget_torrent(**_forget_kwargs)
            _release_key = result.get("source_infohash") or infohash
            # A forget that kept the row (local bytes could not be removed)
            # still describes real SSD bytes: its reservation must stay.
            _keep_reservation = bool(result.get("row_kept"))
        except LookupError:
            return "Already gone from tracking"
        except Exception as e:  # noqa: BLE001
            return _action_error_text(e)
        finally:
            # Never let the reservation outlive the attempt: a forget that
            # raised strands its SSD budget until restart otherwise.
            if not _keep_reservation:
                try:
                    await coord._ssd_release(_release_key)
                except Exception:
                    pass
        name = str(result.get("source_name") or infohash[:10])[:50]
        # The row (and its telegram_message_id) is gone: mark its detail
        # card cancelled (best-effort) and drop the cached id so a future
        # re-discovery of the same hash starts a fresh card instead of
        # editing this one.
        try:
            cached = getattr(self, "_detail_cache", None)
            if isinstance(cached, dict):
                cached.pop(infohash, None)
            sent_map = getattr(self, "_detail_sent_state", None)
            if isinstance(sent_map, dict):
                sent_map.pop(infohash, None)
        except Exception:
            pass
        if detail_msg_id:
            await self._mark_detail_cancelled(
                detail_msg_id,
                name,
                infohash,
                remember=remember,
                delete_files=delete_files,
            )
        try:
            for pair in result.get("paired_cancelled") or []:
                try:
                    await coord._ssd_release(pair.get("source_infohash") or "")
                except Exception:
                    pass
        except Exception:
            pass
        pairs = result.get("paired_cancelled") or []
        pair_note = ""
        if pairs:
            pair_note = " + {} waiting pair{}: {}".format(
                len(pairs),
                "" if len(pairs) == 1 else "s",
                ", ".join(
                    str(p.get("source_name") or p.get("source_infohash", "")[:10])[:40]
                    for p in pairs[:3]
                ),
            )
            if len(pairs) > 3:
                pair_note += f" (+{len(pairs) - 3} more)"
        errs = result.get("errors") or []
        if errs:
            return f"{_verb} {name}{pair_note} with {len(errs)} error(s); check logs"
        return f"{_verb} {name}{pair_note} {_done}"

    async def _execute_snapshot_cancel(
        self, hashes: list[str], title: str, *, delete_files: bool, remember: bool = True
    ) -> str:
        """Forget resolved hashes (liveness re-checked each).

        `remember=False` forgets without the ignore entry and hard-deletes
        the rows (never-seen semantics): a later re-drop re-ingests from
        scratch. Workers are stopped first so no stale upsert resurrects.
        A duplicate tap while one execution is still running gets "working
        on it" instead of a second full forget (which would end in "gone").
        Holds are per-hash (not one scope-string for the whole set), so an
        overlapping tap on any shared copy conflicts instead of
        double-forgetting it.
        """
        _flag = f"{'1' if remember else '0'}:{'del' if delete_files else 'keep'}"
        _hashes = sorted({(h or "").lower() for h in hashes or [] if (h or "").strip()})
        _claimed: list[str] = []
        try:
            for _h in _hashes:
                _k = f"keep:{_h}:{_flag}"
                if not self._claim_action(_k):
                    for _c in _claimed:
                        self._release_action(_c)
                    return "Already working on it — one tap is enough."
                _claimed.append(_k)
        except Exception:
            for _c in _claimed:
                try:
                    self._release_action(_c)
                except Exception:
                    pass
            return "Already working on it — one tap is enough."
        try:
            live = []
            for h in hashes or []:
                try:
                    row = await asyncio.to_thread(self._store.get, h)
                except Exception as e:  # noqa: BLE001
                    # A failed read is not a gone row: reporting "gone"
                    # here strands a live torrent with no way forward.
                    log.warning("cancel liveness read failed for %s: %s", (h or "")[:10], e)
                    return _action_error_text(e)
                if row is not None:
                    live.append(h)
            if not live:
                return "Already gone from tracking"
            outs = []
            for _i, h in enumerate(live):
                # Stagger bulk cancels (2s between executions): a lane
                # wipe otherwise stampedes the clients + DB at once.
                if _i > 0:
                    try:
                        await asyncio.sleep(2.0)
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        pass
                try:
                    outs.append(
                        await self._execute_cancel_one(
                            h, delete_files=delete_files, remember=remember
                        )
                    )
                except Exception as e:  # noqa: BLE001
                    outs.append(_action_error_text(e))
            if len(outs) == 1:
                return outs[0]
            ok = sum(1 for o in outs if o.startswith(("Cancelled", "Kept files")))
            verb = "Cancelled" if delete_files else "Kept files for"
            suffix = "" if delete_files else " (data left in place)"
            return f"{verb} {title} ({ok}/{len(outs)} copies){suffix}"
        except Exception as e:  # noqa: BLE001
            return _action_error_text(e)
        finally:
            for _c in _claimed:
                try:
                    self._release_action(_c)
                except Exception:
                    pass

    def _live_notes_for(self, members: list) -> dict[str, str]:
        """Wait-note map for prefer eligibility (best-effort, str-only)."""
        notes: dict[str, str] = {}
        try:
            fn = getattr(getattr(self, "_coord", None), "_watch_wait_note", None)
            if not callable(fn):
                return notes
            for m in members or []:
                try:
                    if m.state not in (State.NEW, State.WAITING_DISK):
                        continue
                    n = fn(m)
                    if isinstance(n, str) and n:
                        notes[(m.source_infohash or "")] = n
                except Exception:
                    continue
        except Exception:
            pass
        return notes

    async def _files_for_row(self, row: Any, message: Any) -> None:
        """Read-only file listing for one tracked row (group leader).

        Dest-client files with sizes, progress, and skip flags, plus
        the row's batch position. Pure readout — file priorities are
        owned by the batch flow and never touched here.
        """
        dest = getattr(getattr(self, "_coord", None), "dest_client", None)
        if dest is None:
            try:
                await self._reply("Action failed: bot not attached.", reply_to=message)
            except Exception:
                pass
            return
        try:
            h = (
                (getattr(row, "dest_infohash", "") or "")
                or (getattr(row, "source_infohash", "") or "")
            ).lower()
        except Exception:
            h = ""
        if not h:
            try:
                await self._reply("No client entry for this row.", reply_to=message)
            except Exception:
                pass
            return
        try:
            files = await dest.get_torrent_files(h)
        except Exception as e:  # noqa: BLE001
            try:
                await self._reply(_action_error_text(e), reply_to=message)
            except Exception:
                pass
            return
        if not files:
            try:
                await self._reply("No files listed yet (nothing downloaded?).", reply_to=message)
            except Exception:
                pass
            return
        try:
            title = self._group_title([row])
        except Exception:
            title = "?"
        try:
            _bd = _batch_display(row)
            _spec = f" · {_bd}" if _bd else ""
        except Exception:
            _spec = ""
        lines = [f"📁 {title}{_spec} ({len(files)} files):"]
        for f in (files or [])[:25]:
            try:
                _name = str(getattr(f, "name", "") or "?")[:70]
            except Exception:
                _name = "?"
            try:
                _size = _size_compact(getattr(f, "size_bytes", 0))
            except Exception:
                _size = "?"
            try:
                _skipped = int(getattr(f, "priority", 1)) == 0
            except (TypeError, ValueError):
                _skipped = False
            if _skipped:
                lines.append(f"• {_name} ({_size}) — skipped")
                continue
            try:
                _pct = float(getattr(f, "progress", 0) or 0)
                _pct_s = f"{_pct * 100:.0f}%"
            except (TypeError, ValueError):
                _pct_s = "?"
            lines.append(f"• {_name} ({_size}, {_pct_s})")
        if len(files) > 25:
            lines.append(f"+{len(files) - 25} more")
        try:
            await self._reply("\n".join(lines)[:1500], reply_to=message)
        except Exception:
            pass

    async def _start_single_command(self, kind: str, full_hash: str, message: Any) -> None:
        """Typed full-hash command: keepq for cancel, direct otherwise.

        fetch/prefer merged into "now": WAITING rows and grace-held rows
        both start at once via the starting-bytes path.
        """
        if kind in ("fetch", "prefer"):
            kind = "now"
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
            if kind in ("fetch", "now"):
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

    @staticmethod
    def _injectfuse_member_label(t: Any) -> str:
        """Short label for a VPS1 group member (tracker domain or hash)."""
        try:
            for u in list(getattr(t, "trackers", None) or []):
                try:
                    d = _tracker_domain(u)
                except Exception:
                    d = ""
                if d:
                    return str(d)[:30]
        except Exception:
            pass
        try:
            return (getattr(t, "infohash", "") or "")[:10] or "?"
        except Exception:
            return "?"

    @staticmethod
    def _injectfuse_snap(members: list) -> list[tuple[str, str]]:
        """Frozen [(hash, label)] snapshot for an injectfuse question."""
        snap: list[tuple[str, str]] = []
        seen: dict[str, int] = {}
        for t in members or []:
            try:
                h = (getattr(t, "infohash", "") or "").lower()
            except Exception:
                continue
            if not h:
                continue
            base = ActionsMixin._injectfuse_member_label(t)
            n = seen.get(base, 0)
            seen[base] = n + 1
            snap.append((h, base if n == 0 else f"{base} {h[:6]}"))
        return snap

    async def _inject_ask_send(self, resolved: dict, message: Any) -> None:
        """Ask the fuse-inject Yes/No question with hash-scoped buttons.

        Shared by the single, group, and tap-again ask paths: `resolved`
        is a coordinator.injectfuse_resolve() dict (members/title/size).
        The Yes button carries the resolved leader hash and re-resolves
        live at tap time, so no snapshot needs to survive between taps.
        """
        try:
            members = list(resolved.get("members") or [])
            _lead = ""
            try:
                _m0 = members[0] if members else None
                _lead = (getattr(_m0, "infohash", "") or "") or (
                    getattr(_m0, "source_infohash", "") or ""
                )
                _lead = _lead.lower() if _lead else ""
            except Exception:
                _lead = ""
            if not _lead:
                try:
                    await self._reply("Nothing to inject (group changed).", reply_to=message)
                except Exception:
                    pass
                return
            title = str(resolved.get("title") or _lead[:10])[:60]
            try:
                _n = len(members)
            except Exception:
                _n = 0
            extra = ""
            try:
                extra = str(resolved.get("extra_note") or "")
            except Exception:
                extra = ""
            if not extra:
                try:
                    if resolved.get("ignored"):
                        extra = " Prior cancel will be lifted."
                except Exception:
                    pass
            _text = (
                f"Inject {title} ({_n} cop{'y' if _n == 1 else 'ies'}) "
                "to fuse seeding? Files must already be at the "
                f"remote.{extra} Choose below."
            )
            _rows = (
                ButtonMaker()
                .add("✔ Yes, inject", f"inject:{_lead}:yes")
                .newline()
                .add("✖ No", f"inject:{_lead}:no")
                .newline()
                .add("Close", "abort")
                .rows()
            )
            await self._send_sheet(_text, _rows, reply_to=message)
        except Exception as e:  # noqa: BLE001
            log.debug("inject ask failed: %s", e)

    async def _start_injectfuse_single(self, full_hash: str, message: Any) -> None:
        """`/injectfuse <hash>`: tracked or untracked VPS1 torrent.

        Resolves the live VPS1 group (category-agnostic, so torrents the
        poller never ingested work too) and arms the Yes/No question.
        Nothing changes until Yes — and Yes verifies fuse readiness
        first, so a premature tap only yields an informative message.
        Yes auto-lifts a prior /cancel and stops any SSD download first,
        so there is no unignore/cancel round-trip.
        """
        coord = getattr(self, "_coord", None)
        if coord is None:
            try:
                await self._reply("Action failed: bot not attached.", reply_to=message)
            except Exception:
                pass
            return
        try:
            resolved = await coord.injectfuse_resolve(full_hash)
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
        row = resolved.get("row")
        try:
            state = getattr(row, "state", None) if row is not None else None
        except Exception:
            state = None
        title = str(resolved.get("title") or full_hash[:10])[:60]
        if state in (State.DONE, State.RE_ADDING):
            try:
                await self._reply(
                    f"already {state.value}: {title} (fuse gate runs next tick)", reply_to=message
                )
            except Exception:
                pass
            return
        members = list(resolved.get("members") or [])
        snap = self._injectfuse_snap(members)
        if not snap:
            try:
                await self._reply("Nothing to inject (group changed).", reply_to=message)
            except Exception:
                pass
            return
        extra = ""
        try:
            if resolved.get("ignored"):
                extra = " Prior cancel will be lifted."
            elif state in (State.QUEUED, State.DOWNLOADING, State.MOVING):
                extra = f" SSD {state.value} will be stopped first."
        except Exception:
            pass
        try:
            _resolved_for_ask = dict(resolved)
        except Exception:
            _resolved_for_ask = resolved
        try:
            _resolved_for_ask["extra_note"] = extra
        except Exception:
            pass
        await self._inject_ask_send(_resolved_for_ask, message)
        try:
            if members:
                return _safe_display_name(str(getattr(members[0], "source_name", "") or "")[:60])
        except Exception:
            pass
        return "?"

    def _group_hashes_for(self, full_hash: str) -> list[str] | None:
        """Live group member hashes containing `full_hash`, else None.

        Group-scoped commands (/skip_, /unskip_) act on the whole
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
            try:
                groups = _group_active_items(
                    [(_r, None) for _r in self._positional_active_rows(rows)]
                )
            except Exception:
                groups = []
            members: list = []
            _positional = False
            if norm.isdigit():
                try:
                    _n = int(norm)
                except (TypeError, ValueError):
                    _n = 0
                if 1 <= _n <= len(groups):
                    _gk, _mem = groups[_n - 1]
                    members = [t for (t, _) in _mem]
                    _positional = True
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
                try:
                    _live_n = len(groups)
                except Exception:
                    _live_n = 0
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

    def _live_match(self, query: str) -> list[tuple[str, list[str]]]:
        """Live (title, member-hashes) groups whose title contains `query`.

        Case-insensitive substring on the lead row's full name (the same
        name the list heading shows). Re-derived from the current store
        on every call, so the Q1 sheet, the Q2 sheet, and execution each
        see the live set — same stateless shape as `all:`-scoped buttons.
        """
        try:
            q = (query or "").strip().casefold()
            if not q:
                return []
            store = getattr(self, "_store", None)
            if store is None:
                return []
            _all = getattr(store, "list_active_inflight", None)
            rows = _all() if callable(_all) else []
            out: list[tuple[str, list[str]]] = []
            for _key, _members in _group_active_items([(_r, None) for _r in rows or []]):
                try:
                    _rows = [_t for (_t, _) in _members or []]
                    if not _rows:
                        continue
                    _name = str(getattr(_rows[0], "source_name", "") or "")
                    if q not in _name.casefold():
                        continue
                    _hashes = [_h for _h in (_member_hash(_t) for _t in _rows) if _h]
                    if _hashes:
                        out.append((self._group_title(_rows), _hashes))
                except Exception:
                    continue
            return out
        except Exception:
            return []

    async def _live_stalled(self) -> list[tuple[str, list[str]]]:
        """Live (title, member-hashes) groups with a stalled download.

        Stalled = DOWNLOADING row whose dest-client entry reports 0
        seeders and incomplete progress. Rows with no dest entry get no
        verdict (entry gone/transient) and are never assumed stalled.
        Groups qualify on any stalled member; execution covers the whole
        group (same release — the sheet names the counts). Raises on
        infrastructure failure (callers report Action failed, never
        "none found").
        """
        try:
            from .forget import _row_hashes
        except Exception as e:  # noqa: BLE001
            raise RuntimeError(f"stalled check unavailable: {e}") from e
        store = getattr(self, "_store", None)
        coord = getattr(self, "_coord", None)
        dest = getattr(coord, "dest_client", None) if coord else None
        if store is None or dest is None:
            raise RuntimeError("stalled check unavailable: bot not attached")
        rows = [
            r
            for r in (store.list_active_inflight() or [])
            if getattr(r, "state", None) == State.DOWNLOADING
        ]
        if not rows:
            return []
        known: dict[str, object] = {}
        for r in rows:
            try:
                for k in _row_hashes(r) or ():
                    known.setdefault((k or "").lower(), r)
            except Exception:
                continue
        if not known:
            return []
        entries = await dest.list_torrents(hashes=sorted(known))
        by_hash = {}
        for t in entries or []:
            try:
                _h = (getattr(t, "hash", "") or "").lower()
            except Exception:
                continue
            if _h:
                by_hash[_h] = t
        stalled: list = []
        for r in rows:
            try:
                _hs = [k for k in _row_hashes(r) or () if k in by_hash]
                if not _hs:
                    continue
                t = by_hash[_hs[0]]
                try:
                    _seeds = int(getattr(t, "num_seeds", 0) or 0)
                except (TypeError, ValueError):
                    _seeds = 0
                try:
                    _done = (
                        bool(t.is_complete())
                        if callable(getattr(t, "is_complete", None))
                        else float(getattr(t, "progress", 1) or 1) >= 0.999
                    )
                except (TypeError, ValueError):
                    continue
                if _seeds == 0 and not _done:
                    stalled.append(r)
            except Exception:
                continue
        out: list[tuple[str, list[str]]] = []
        for _key, _members in _group_active_items([(_r, None) for _r in stalled]):
            try:
                _rows = [_t for (_t, _) in _members or []]
                if not _rows:
                    continue
                _hashes = [_h for _h in (_member_hash(_t) for _t in _rows) if _h]
                if _hashes:
                    out.append((self._group_title(_rows), _hashes))
            except Exception:
                continue
        return out

    async def _cancel_match_command_entry(self, query: str, message: Any) -> None:
        """`/cancel_match <text>`: one remember + one keep/delete for all.

        No match → explanatory reply. A single live copy skips straight
        to its normal forget sheet; otherwise one sheet names every
        matched group and the two remember buttons carry the whole set
        (`forget:match:<tok>:<0|1>`), re-resolved live at each tap.
        """
        q = (query or "").strip()
        if len(q) < _MATCH_MIN_CHARS:
            try:
                await self._reply(
                    "Send /cancel_match <text> (at least "
                    f"{_MATCH_MIN_CHARS} characters) — cancels every "
                    "active group whose title contains <text>.",
                    reply_to=message,
                )
            except Exception:
                pass
            return
        tok = _match_token(q)
        if not tok:
            try:
                await self._reply(
                    "That text is too long for buttons — shorten it to "
                    f"~{_MATCH_MAX_BYTES} characters.",
                    reply_to=message,
                )
            except Exception:
                pass
            return
        try:
            matches = await asyncio.to_thread(self._live_match, q)
        except Exception as e:  # noqa: BLE001
            try:
                await self._reply(_action_error_text(e), reply_to=message)
            except Exception:
                pass
            return
        if not matches:
            try:
                await self._reply(
                    f"No active groups match '{_safe_display_name(q[:60])}'", reply_to=message
                )
            except Exception:
                pass
            return
        if len(matches) == 1 and len(matches[0][1]) == 1:
            await self._start_single_command("cancel", matches[0][1][0], message)
            return
        _total = sum(len(_hs) for (_, _hs) in matches)
        _shown = [f"• {_t} ({len(_hs)})" for (_t, _hs) in matches[:8]]
        if len(matches) > 8:
            _shown.append(f"• …and {len(matches) - 8} more")
        _scope = CancelScope("match", tok)
        _text, _rows = _scope.remember_sheet(
            f"{len(matches)} groups "
            f"({_total} copies) match "
            f"'{_safe_display_name(q[:60])}':\n" + "\n".join(_shown) + "\nForget them all?"
        )
        try:
            await self._send_sheet(_text, _rows, reply_to=message)
        except Exception:
            pass

    async def _cancel_stalled_command_entry(self, message: Any) -> None:
        """`/cancel_stalled`: one remember + one keep/delete for all stalled.

        Stalled = DOWNLOADING with 0 seeders and incomplete progress
        (dest-client verdict at tap time). Same flow shape as
        `/cancel_match`: single live copy skips to its normal forget
        sheet, otherwise one sheet names every stalled group and the
        remember buttons carry the `stalled` scope,
        re-resolved live at each tap.
        """
        try:
            groups = await self._live_stalled()
        except Exception as e:  # noqa: BLE001
            try:
                await self._reply(_action_error_text(e), reply_to=message)
            except Exception:
                pass
            return
        if not groups:
            try:
                await self._reply(
                    "No stalled downloads right now — every downloading copy has seeders.",
                    reply_to=message,
                )
            except Exception:
                pass
            return
        if len(groups) == 1 and len(groups[0][1]) == 1:
            await self._start_single_command("cancel", groups[0][1][0], message)
            return
        _total = sum(len(_hs) for (_, _hs) in groups)
        _shown = [f"• {_t} ({len(_hs)})" for (_t, _hs) in groups[:8]]
        if len(groups) > 8:
            _shown.append(f"• …and {len(groups) - 8} more")
        _scope = CancelScope("stalled", "")
        _text, _rows = _scope.remember_sheet(
            f"{len(groups)} stalled downloads "
            f"({_total} copies, no seeders):\n" + "\n".join(_shown) + "\nForget them all?"
        )
        try:
            await self._send_sheet(_text, _rows, reply_to=message)
        except Exception:
            pass

    async def _cancel_status_command_entry(self, message: Any) -> None:
        """`/cancel_status`: pick a lane, then the usual remember sheet.

        One button per non-empty lane (DL/MV/RD/QD + live counts);
        tapping one opens the same remember -> keep/delete sheets as
        every other scope (`cancel:status:<key>`, re-resolved live at
        each tap). Empty lanes are omitted; an empty list replies so.
        """
        try:
            from .telegram_protocol import _STATUS_FILTER_BUTTONS, match_status_filter

            _get_all = getattr(self._store, "list_active_inflight", None)
            rows = await asyncio.to_thread(_get_all) if callable(_get_all) else []
        except Exception as e:  # noqa: BLE001
            try:
                await self._reply(_action_error_text(e), reply_to=message)
            except Exception:
                pass
            return
        _counts: dict[str, int] = {}
        try:
            for _r in rows or []:
                try:
                    _sv = getattr(getattr(_r, "state", None), "value", "")
                except Exception:
                    continue
                for _key, _ in _STATUS_FILTER_BUTTONS:
                    if _key in ("all", "ov"):
                        continue
                    try:
                        if match_status_filter(_sv, _key):
                            _counts[_key] = _counts.get(_key, 0) + 1
                            break
                    except Exception:
                        continue
        except Exception:
            pass
        if not _counts:
            try:
                await self._reply("Nothing live to cancel.", reply_to=message)
            except Exception:
                pass
            return
        if len(_counts) == 1 and list(_counts.values())[0] == 1:
            _only = next(iter(_counts))
            try:
                _solo = await asyncio.to_thread(self._store.list_active_inflight)
            except Exception:
                _solo = []
            _hsolo = []
            try:
                for _r in _solo or []:
                    try:
                        if match_status_filter(
                            getattr(getattr(_r, "state", None), "value", ""), _only
                        ):
                            _rh = (getattr(_r, "source_infohash", "") or "").lower()
                            if _rh:
                                _hsolo.append(_rh)
                    except Exception:
                        continue
            except Exception:
                pass
            if len(_hsolo) == 1:
                await self._start_single_command("cancel", _hsolo[0], message)
                return
        _maker = ButtonMaker()
        for _key, _label in _STATUS_FILTER_BUTTONS:
            if _key in ("all", "ov"):
                continue
            try:
                _n = int(_counts.get(_key, 0) or 0)
            except (TypeError, ValueError):
                _n = 0
            if _n <= 0:
                continue
            try:
                _maker.add(f"X {_label} ({_n})", f"cancel:status:{_key}")
            except Exception:
                continue
        try:
            _maker.add("Close", "abort")
        except Exception:
            pass
        try:
            _rows = _maker.newline().rows()
        except Exception:
            return
        try:
            await self._send_sheet("Cancel by status - pick a lane:", _rows, reply_to=message)
        except Exception:
            pass

    async def _start_group_command(self, kind: str, token: str, message: Any) -> None:
        """Typed command: positional number, gid, or legacy hash prefix.

        Positional numbers (what the list shows) and group ids resolve
        against the LIVE list right now; member buttons below carry full
        hashes re-resolved at tap time, so renumbering mid-flow cannot
        misroute. Full hashes and legacy prefixes act on one row.
        """
        token = (token or "").strip().lower()
        # Legacy aliases: fetch/prefer merged into the state-dependent
        # "now" verb (WAITING rows use original bytes, grace-held rows
        # start at once).
        if kind in ("fetch", "prefer"):
            kind = "now"
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
        groups: list = []
        if token.isdigit():
            # Numbers address the displayed (status-filtered) list; the
            # gid/hash/prefix fallbacks below stay on the full rows.
            try:
                groups = _group_active_items(
                    [(_r, None) for _r in self._positional_active_rows(rows)]
                )
            except Exception:
                groups = []
        found: tuple | None = None
        if token.isdigit():
            try:
                _n = int(token)
            except (TypeError, ValueError):
                _n = 0
            if 1 <= _n <= len(groups):
                _gk, _mem = groups[_n - 1]
                found = (_gk, [t for (t, _) in _mem])
        if found is None:
            # Group id (older messages) or legacy torrent prefix.
            _by_gid = _live_group_by_gid(list(rows or []), token)
            if _by_gid is not None:
                _gk, _mem = _by_gid
                found = (_gk, list(_mem))
        if found is None:
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
        _gkey, members = found
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
        """Hold (`hold=True`, /skip_) or resume (/unskip_) tracked releases.

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

        Replaces `/unskip_` + `/unignore_` with one verb branching on
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
                try:
                    groups = _group_active_items(
                        [(_r, None) for _r in self._positional_active_rows(rows)]
                    )
                except Exception:
                    groups = []
                try:
                    _n = int(norm)
                except (TypeError, ValueError):
                    _n = 0
                if 1 <= _n <= len(groups):
                    _gk, _mem = groups[_n - 1]
                    for _t in [t for (t, _) in _mem]:
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
