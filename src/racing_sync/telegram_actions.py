"""Telegram cancel execution (split from telegram_bot.py).

`ActionsMixin` carries what the bot *does* when told — cancel
execution first, command entries and torrent executors to follow —
moved verbatim; everything resolves through `self` (instance state
and sibling mixins) and the `telegram_protocol` / `telegram_views`
helpers.
"""

import asyncio
import logging

from .state import State

# Keep the historic logger name so log output is unchanged by the split.
log = logging.getLogger("racing_sync.telegram_bot")


class ActionsMixin:
    async def _execute_cancel_one(self, infohash: str, *, delete_files: bool,
                                    remember: bool = True) -> str:
        """Forget one release; files kept iff not `delete_files`.

        `remember=False` skips the ignore entry and hard-deletes the row
        (never-seen semantics): workers are stopped first so no stale
        upsert resurrects it, and a later re-drop re-ingests from scratch.
        """
        _verb = "Cancelled" if delete_files else "Kept files for"
        _done = ("(removed + ignored)" if remember
                 else "(removed, can be re-added)") if delete_files else (
            "untracked + ignored, data left in place" if remember else
            "untracked, data left in place (can be re-added)")
        try:
            from .api import _hold_ops_lock
        except Exception:
            _hold_ops_lock = None  # type: ignore[assignment]
        try:
            from .forget import forget_torrent
        except Exception as e:  # noqa: BLE001
            return f"Action failed: {e}"
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
                detail_msg_id = await asyncio.to_thread(
                    store.get_telegram_message_id, infohash)
        except Exception:
            detail_msg_id = None
        try:
            _stop = getattr(coord, "stop_workers_for", None)
            if callable(_stop):
                try:
                    await _stop(infohash)
                except Exception:
                    pass
            if _hold_ops_lock is not None:
                async with _hold_ops_lock(coord):
                    result = await forget_torrent(
                        cfg, dest=dest, store=store, target=infohash,
                        apply=True, delete_files=delete_files,
                        ignore=remember, hard=not remember,
                    )
                    try:
                        await coord._ssd_release(
                            result.get("source_infohash") or infohash)
                    except Exception:
                        pass
            else:
                result = await forget_torrent(
                    cfg, dest=dest, store=store, target=infohash,
                    apply=True, delete_files=delete_files,
                    ignore=remember, hard=not remember,
                )
                try:
                    await coord._ssd_release(
                        result.get("source_infohash") or infohash)
                except Exception:
                    pass
        except LookupError:
            return "Already gone from tracking"
        except Exception as e:  # noqa: BLE001
            return f"Action failed: {e}"
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
            await self._mark_detail_cancelled(detail_msg_id, name, infohash)
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
                len(pairs), "" if len(pairs) == 1 else "s",
                ", ".join(str(p.get("source_name") or p.get("source_infohash", "")[:10])[:40]
                          for p in pairs[:3]),
            )
            if len(pairs) > 3:
                pair_note += f" (+{len(pairs) - 3} more)"
        errs = result.get("errors") or []
        if errs:
            return f"{_verb} {name}{pair_note} with {len(errs)} error(s); check logs"
        return f"{_verb} {name}{pair_note} {_done}"

    async def _execute_snapshot_cancel(self, hashes: list[str], title: str,
                                         *, delete_files: bool,
                                         remember: bool = True) -> str:
        """Forget resolved hashes (liveness re-checked each).

        `remember=False` forgets without the ignore entry and hard-deletes
        the rows (never-seen semantics): a later re-drop re-ingests from
        scratch. Workers are stopped first so no stale upsert resurrects.
        A duplicate tap while one execution is still running gets "working
        on it" instead of a second full forget (which would end in "gone").
        """
        _key = (f"keep:{','.join(sorted((h or '').lower() for h in hashes or []))}"
                f":{'1' if remember else '0'}:{'del' if delete_files else 'keep'}")
        if not self._claim_action(_key):
            return "Already working on it — one tap is enough."
        try:
            live = []
            for h in hashes or []:
                try:
                    row = await asyncio.to_thread(self._store.get, h)
                except Exception as e:  # noqa: BLE001
                    # A failed read is not a gone row: reporting "gone"
                    # here strands a live torrent with no way forward.
                    log.warning("cancel liveness read failed for %s: %s",
                                (h or "")[:10], e)
                    return f"Action failed: {e}"
                if row is not None:
                    live.append(h)
            if not live:
                return "Already gone from tracking"
            outs = []
            for h in live:
                try:
                    outs.append(await self._execute_cancel_one(
                        h, delete_files=delete_files, remember=remember))
                except Exception as e:  # noqa: BLE001
                    outs.append(f"Action failed: {e}")
            if len(outs) == 1:
                return outs[0]
            ok = sum(1 for o in outs
                     if o.startswith(("Cancelled", "Kept files")))
            verb = "Cancelled" if delete_files else "Kept files for"
            suffix = "" if delete_files else " (data left in place)"
            return f"{verb} {title} ({ok}/{len(outs)} copies){suffix}"
        except Exception as e:  # noqa: BLE001
            return f"Action failed: {e}"
        finally:
            self._release_action(_key)

    def _live_notes_for(self, members: list) -> dict[str, str]:
        """Wait-note map for prefer eligibility (best-effort, str-only)."""
        notes: dict[str, str] = {}
        try:
            fn = getattr(getattr(self, "_coord", None),
                         "_watch_wait_note", None)
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
