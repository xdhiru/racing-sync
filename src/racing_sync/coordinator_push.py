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
  VPS2 would; that is the whole point of the extraction.
* **clean refusals.** `start_source_push` returns (started, one-line reason)
  instead of raising, because its callers (a Telegram command, an /act
  button, the indexer-timeout fallback) must all be able to fall back to
  today's behaviour.
* **no live checkpoint outlives a failed attempt.** Every branch after
  `claim_push` settles the record (abandoned/failed/done), so a broken push
  can never leave the pipeline suppressing work for nothing.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import logging
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
    remote_progress,
    sanitize_push_files,
    start_copy,
)

# Keep the historic logger name so log output is unchanged by the split.
log = logging.getLogger("racing_sync.coordinator")

#: Park reason while a push owns the content (see `_push_park_row`). The
#: indexer park path recognises it only through `push_is_live`, so the row
#: re-arms on the push check cadence instead of hitting the max-age FAILED.
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

#: Fallback for a corrupt/absent cadence value (seconds).
_DEFAULT_CHECK_S = 120.0
_DEFAULT_DEADLINE_S = 21600


def _one_line(text: object) -> str:
    flat = " ".join(str(text or "").split())
    return flat[:300]


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

    def _push_check_interval(self) -> float:
        try:
            return max(60.0, float(self.rt.push.check_interval_seconds))
        except Exception:
            return _DEFAULT_CHECK_S

    def _push_deadline(self) -> dt.timedelta:
        try:
            return dt.timedelta(seconds=max(600, int(self.rt.push.deadline_seconds)))
        except Exception:
            return dt.timedelta(seconds=_DEFAULT_DEADLINE_S)

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
        """The SSH dial the command travels on (`self.sftp`), or None.

        `rt.push.ssh_dial` re-checks the config side (an enabled section is
        validated against an enabled `deluge_sftp`); the object with
        `run_shell` / `write_remote_file` / `launch_detached` is the pooled
        exporter the coordinator already holds.
        """
        try:
            if self.rt.push.ssh_dial is None:
                return None
        except Exception:
            return None
        dial = getattr(self, "sftp", None)
        if dial is None or not callable(getattr(dial, "run_shell", None)):
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
            return False, "push refused: no SSH dial to VPS1 ([client.deluge_sftp] enabled)"

        request = PushRequest(
            infohash=h,
            group_key=self._push_group_key(ts),
            content_name=display,
            src_dir=src_dir,
            target_remote=target_remote,
            files=tuple(files),
        )
        deadline_at = dt.datetime.now(dt.UTC) + self._push_deadline()
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

        started = await start_copy(
            request,
            ssh=ssh,
            rclone_bin=self._push_rclone_bin(),
            transfers=self._push_transfers(),
            bwlimit=self._push_bwlimit(),
        )
        if not started.ok:
            await self._push_settle(store, h, PushState.FAILED, started.reason)
            return False, started.reason

        # The copy is detached and running: stamp the scratch paths + counters
        # the transport generated, then hand the row over.
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
            )
        except Exception as e:  # noqa: BLE001
            log.warning("push: could not stamp the checkpoint for %s: %s", h[:10], e)
        self._remember_wanted(h, files)
        # Hand the row to the push (parked, on the check cadence). If the row
        # cannot be parked cleanly the copy still runs on VPS1, but nothing
        # may be suppressed for it: settle the checkpoint as abandoned so the
        # pipeline picks up exactly where it was today.
        parked, park_why = await self._push_park_row(ts)
        if not parked:
            await self._push_settle(store, h, PushState.ABANDONED, park_why)
            return False, park_why
        log.info(
            "push from VPS1 started for %s (%d file(s) -> %s; %s)",
            display,
            len(files),
            target_remote,
            reason or "manual",
        )
        return True, f"pushing {len(files)} file(s) from VPS1 to {target_remote}"

    # ---- step helpers --------------------------------------------------- #

    def _push_group_key(self, ts: TorrentState) -> str:
        try:
            name, size = content_key(
                getattr(ts, "source_name", "") or "", int(getattr(ts, "total_bytes", 0) or 0)
            )
            return f"{name}|{size}"
        except Exception:
            return ""

    def _push_transfers(self) -> int:
        try:
            return int(self.rt.push.transfers)
        except Exception:
            return 4

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
        """
        names = [f.name for f in files]
        kind = str(getattr(ts, "classification_kind", "") or "")
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
        try:
            cache = getattr(self, "_push_wanted", None)
            if isinstance(cache, dict):
                cache.pop((infohash or "").lower(), None)
        except Exception:
            pass

    # ---- per-poll sweep ------------------------------------------------- #

    async def push_sweep(self) -> None:
        """Advance every live push checkpoint; hand finished rows to admission.

        Runs once per poll next to the manual-move sweep and does nothing at
        all while the switch is off. Progress is one metadata-only
        ``rclone lsf -R --files-only`` per push against the seed remote —
        never the fuse mount, never VPS2's SSD, never a content read — so at
        most one remote listing per push per `check_interval_seconds`. When
        the remote holds every expected file the push goes `verifying` and
        the EXISTING evidence path (`_fuse_content_evidence` ->
        `_admit_fuse_existing` -> the all-remote shortcut) admits the row
        straight away instead of waiting for the next sweep tick. Past the
        deadline the checkpoint is `failed`, which switches the suppression
        off and leaves the row exactly where today's behaviour picks up.
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
        interval = self._push_check_interval()
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
        present, total = await remote_progress(getattr(self, "cfg", None), target, files)
        try:
            await offload(
                store.note_push_progress,
                h,
                files_present=int(present or 0),
                files_total=int(total or len(files)),
            )
        except Exception as e:  # noqa: BLE001
            log.debug("push: could not note progress for %s: %s", h[:10], e)
        log.debug(
            "push: %s has %d/%d file(s) on %s", h[:10], int(present or 0), int(total or 0), target
        )
        if total <= 0 or present < total:
            return
        await self._push_handoff(store, h, ts, present, total)

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

    # ---- suppression helpers ------------------------------------------- #

    def _park_behind_push(self, ts: TorrentState) -> None:
        """Park a row on the push cadence instead of admitting it to SSD.

        Rides the indexer park, whose max-age branch re-arms on
        `check_interval_seconds` while a push is live, and keeps
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
