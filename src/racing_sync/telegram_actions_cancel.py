"""Telegram cancel actions (split from telegram_actions.py).

`CancelActionsMixin` executes forgets — one row, a resolved snapshot,
a title match, the stalled set, a lane — and lists the files of one
tracked row; moved verbatim, everything resolves through `self`
(instance state and sibling mixins).
"""

import asyncio
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
)
from .telegram_views import (
    _batch_display,
    _group_active_items,
    _member_hash,
    _safe_display_name,
    _size_compact,
)

# Keep the historic logger name so log output is unchanged by the split.
log = logging.getLogger("racing_sync.telegram_bot")


class CancelActionsMixin:
    def _positional_active_rows(self, rows: list) -> list:
        """Rows the active list is currently numbering (status filter applied).

        Positional commands (`/act_N`, `/start_N`, `/cancel_N`, `/resume_N`)
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
                except Exception as e:  # noqa: BLE001
                    # A stranded reservation shrinks the SSD budget until the
                    # next restart: name the row instead of vanishing.
                    log.warning(
                        "cancel: SSD ledger release failed for %s "
                        "(reservation stays until restart): %s",
                        (_release_key or "")[:10],
                        e,
                    )
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
                _pair_h = pair.get("source_infohash") or ""
                try:
                    await coord._ssd_release(_pair_h)
                except Exception as e:  # noqa: BLE001
                    log.warning(
                        "cancel: SSD ledger release failed for paired %s "
                        "(reservation stays until restart): %s",
                        (_pair_h or "")[:10],
                        e,
                    )
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
                # Shutdown must not leave a wipe half-applied with no word:
                # finish the copy in flight, then report what was done.
                try:
                    if getattr(self, "_stopped", False):
                        break
                except Exception:
                    pass
                # The run can outlive the hold TTL: keep every claim fresh so
                # a concurrent tap still sees this scope as in flight.
                try:
                    _refresh = getattr(self, "_refresh_actions", None)
                    if callable(_refresh):
                        _refresh(_claimed)
                except Exception:
                    pass
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
            if len(outs) == 1 and len(live) == 1:
                return outs[0]
            ok = sum(1 for o in outs if o.startswith(("Cancelled", "Kept files")))
            verb = "Cancelled" if delete_files else "Kept files for"
            suffix = "" if delete_files else " (data left in place)"
            if len(outs) < len(live):
                # Stopped mid-wipe: name the shortfall so the operator knows
                # to re-run the cancel for the rest.
                return (
                    f"{verb} {len(outs)}/{len(live)} copies of {title} before "
                    f"shutdown{suffix}; re-run the cancel for the rest"
                )
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
        """`/cancel_all <text>`: one remember + one keep/delete for all.

        No match → explanatory reply. A single live copy skips straight
        to its normal forget sheet; otherwise one sheet names every
        matched group and the two remember buttons carry the whole set
        (`forget:match:<tok>:<0|1>`), re-resolved live at each tap.
        """
        q = (query or "").strip()
        if len(q) < _MATCH_MIN_CHARS:
            try:
                await self._reply(
                    "Send /cancel_all <text> (at least "
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
        `/cancel_all`: single live copy skips to its normal forget
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
        """`/cancel_lane`: pick a lane, then the usual remember sheet.

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
