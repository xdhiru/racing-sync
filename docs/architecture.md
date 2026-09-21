# Architecture

## Layout

```
src/racing_sync/
  __main__.py         CLI (run [--reset/--full] / forget [--ignore] / unignore / check-config)
  config.py           Pydantic schema, cross-validates everything
  logging_setup.py    Rotating files + JSONL + ring buffer + optional HTTP sink
  state.py            SQLite state machine (State, ALLOWED, StateStore)
  classifier.py       movie / episode / season
  batcher.py          SSD-aware episode batching
  rclone_ops.py       rclone subprocess wrapper
  sftp_source.py      paramiko-based .torrent export (pooled)
  prowlarr.py         Prowlarr client (indexers, search, download)
  watchdir.py         Watch-dir scanner with bencoded torrent parser
  recovery.py         Startup reconciler
  forget.py           Abandon-torrent off-switch (row + entries + SSD data + blob cache + optional ignore)
  coordinator.py      Main async loop + per-torrent workers (tick, dispatch)
  coordinator_ssd.py  SSD batch caps + global reservation ledger
  coordinator_picker.py  Cross-seed SSD-source picker (req #1/#2)
  coordinator_cleanup.py VPS1 cleanup janitor
  coordinator_paths.py   Untrusted torrent-relative path guard
  coordinator_errors.py  Retryable WebUI / batch-move error contract
  coordinator_content.py  Stateless helpers (normalize, grace, notify filter)
  telegram_bot.py     Per-torrent detail cards + active-tasks list
  api.py              Optional FastAPI control plane
  clients/
    abstract.py       TorrentClient ABC + dataclasses
    http_base.py      aiohttp + nginx auth helper
    qbittorrent.py    qBittorrent WebUI v2
    deluge.py         Deluge JSON-RPC
```

## State machine

```
NEW ──┬─> QUERYING ──> WAITING_INDEXER ──> WAITING_DISK ──> QUEUED ──> DOWNLOADING ──> MOVING ──> RE_ADDING ──> DONE
      │       │                │                  │    │          │            │             │                  │ ^
      │       │                │                  │    │          │            │             │                  │ │
      └───────┴────────────────┴──────────────────┴────┴──────────┴────────────┴──> (any) ──> FAILED ──────────┘ │
                                                                                  │  │                          │
                                                                          QUEUED/NEW retry  │  lost fuse ───────┘
                                                                                            │  (RE_ADDING, ≤5 rapid
                                                                                            │   flaps, else FAILED)
                                                                             false DONE ────────────────────────┘
                                                                                  (self-heal via MOVING)
```

`DONE` is terminal-ish but not final: a lost fuse entry demotes
`DONE → RE_ADDING`, a falsely adopted `DONE` (bytes never moved) demotes
`DONE → MOVING`. Rapid `DONE` demotions are counted (`readd_cycles`);
past 5 in 24h the row fails for operator attention instead of flapping
forever. `FAILED → QUEUED/NEW` allows manual and auto retry.

Pre-SSD states (`NEW`/`QUERYING`/`WAITING_INDEXER`/`WAITING_DISK`) may
fast-track straight to `DONE` on manual fuse adoption: the operator moved
the files to the remote and added the same infohash on VPS2 pointing at a
fuse mount (any category) with verified bytes, so no SSD download or rclone
move is needed. The check is category-agnostic (hash lookup, not the
`racing`-category filter recovery relies on) and fail-closed on bytes
(skip_check ghosts never mark `DONE`).

The state lives in `state.db` (SQLite, WAL journal). Transitions are
written through `StateStore.transition`, which restores the in-memory row
if the `upsert` fails (e.g. full disk) so memory never disagrees with the
DB. Failures are logged to the app log and to the `run_log` table (pruned
to 5000 rows). Two extra persisted columns keep long-run behavior stable
across restarts: `batch_cap_bytes` (frozen batch boundaries) and
`readd_cycles` (flap counter).

## Per-torrent workflow

1. **Discover** — VPS1 racing client lists torrents matching configured category (or all if category="").
   Insert/update row in `state.db` at `state=NEW`.

2. **Decide SSD source** (`pick_ssd_source_for_racing`):

   Manual fuse fast-track runs first: if the same infohash already seeds
   from a fuse mount on VPS2 (any category) with verified bytes, the row
   goes straight to `DONE` with no Prowlarr query or SSD work. Workers check
   this at the top of `_do_new` / `_do_waiting_indexer`, and each tick runs
   one batched hash lookup (`_sweep_manual_fuse_adoptions`) so parked
   `WAITING_INDEXER` rows are picked up within one poll interval instead of
   waiting out their 30-minute retry timer. Rows with live workers are
   skipped by the sweep — their own worker check adopts without racing it.
    - Multiple racing-client torrents for the same content?
     Prefer public. Try:
       - `cross_seed.refetch_public_via_prowlarr` → prowlarr → download-target indexers (priority order)
       - SFTP fallback (Deluge) or qB `export_torrent` (qB)
    - Only private? Map each racing torrent's announce URL to a prowlarr indexer
      via `[prowlarr.tracker_map]` (Beta, Alpha, Gamma, …). Search.
      No tracker_map hit → query the `[[prowlarr.download_indexers]]`
      download-target indexers in priority order; first exact release wins.
      Park in `WAITING_INDEXER` + retry per `prowlarr_retry_*` if no hit yet.
      Past `prowlarr_max_age_seconds` the row fails — unless
      `cross_seed.fallback_to_racing_torrent_on_prowlarr_timeout` is set,
      in which case it uses the racing torrent's own bytes instead (with a
      fresh retry window for the direct attempts, then FAILED if VPS1 stays
      unreachable), or the operator sends Telegram `/fetch_<hash>` while it
      is still waiting (sticky per-row `force_direct` bypasses Prowlarr on
      every later pick, also with a fresh window). Transient SFTP drops
      park for the next interval; only a persistently unreachable VPS1 fails
      the row. `--reset` wipes the flag with the DB — the torrent is
      re-discovered from VPS1 and retries from zero.
    - No luck anywhere → SFTP-export the racing torrent.

3. **Add to VPS2**:
    - `save_path = dest.save_path` (local SSD)
    - `paused = True`, `skip_check = False` (req #3: we *want* the hash check on SSD)
    - `category = "racing"`
    - The QUEUED→DOWNLOADING edge re-checks `max_active_downloads`
      (atomic admit via `_download_admissions`, extras stay QUEUED) so
      fresh-start bursts can't sail past the tick's snapshot-only gate;
      already-DOWNLOADING rows are grandfathered and drain naturally.

4. **Classify** (`classifier.classify`):
    - Individual episode matching episode regex -> `episode`
    - Single file or multi-file bundle without episode tags -> `movie`
    - Multi-episode pack (>= 90% episodes) -> `season`
    - Multiple episodes with many extras -> `mixed`
    - A single *file* larger than `ssd.skip_movie_larger_than_bytes` fails
      the row; total torrent size never disqualifies batched content.

5. **Batch & download** (global SSD budget + isolated batches):
    - Admission reserves `min(total, max_inflight_bytes)` from the *global*
      ledger before `QUEUED`; over-budget rows park in `WAITING_DISK` and
      re-check quietly (≤1/minute). After classification the reservation
      refines to the real footprint (max batch for seasons/games, total for
      singles); singles that no longer fit roll back to `WAITING_DISK`.
    - For `season`/`mixed`: `make_batches(episodes, cap=frozen_cap)`; for
      multi-file non-episodic content: name-sorted file groups. The cap is
      frozen per row and persisted, so restarts keep identical boundaries.
    - Set file priorities: priority 1 for batch N, 0 for everything else.
      Resume; poll until the batch's files are client-complete *and* present
      on SSD at full size (a desynced piece map alone never counts).
      All batches already remote (remaining footprint zero) skips download
      + move entirely: QUEUED goes straight to fuse-gated RE_ADDING instead
      of waiting on a paused, fully-deselected entry that can never complete.
    - After a verified move (rclone exit 0 *plus* nothing left on disk),
      the torrent entry is deleted **with files** and re-added fresh for
      batch N+1, so shared piece-boundary partials can't leak across
      batches. Only complete files ever reach the remote.

6. **Move to remote** (`rclone_ops.move_local_to_remote`, ceiling
    `[rclone].move_timeout_seconds`, 6h default):

    A hung remote (flood-wait pileup, stalled uplink) raises
    `RcloneTimeoutError` after the ceiling; the child is terminated and
    the row PARKS (source bytes are intact — rclone only removes them
    after verified transfer), never fails. Repeated MOVING parks escalate
    to `ERROR "MOVING stalled …"` with the gate reason in `last_error`.
    - `rclone move <local> <remote> -- <extra_move_flags>` with per-file
      `--files-from-raw` lists (literal paths, no globs) preserving the
      torrent-relative tree. `extra_move_flags` / `batch_move_extra_flags`
      live under `[rclone]` and reject config/credential-hijack flags.
    - A move only counts when rclone exits 0 *and* the listed files are gone
      from SSD (symlinks/unreadables count as leftovers); otherwise the
      batch stays put for retry — never wipe unmoved data.
    - Movies + full season roots → `rclone.remote.default`
    - Individual episodes → `rclone.remote.unsorted`
    - The classification pinned at queue time routes the move, so a file
      list that changed mid-flight (tracker sidecar added) can't reroute it.

7. **Re-inject** (`coordinator._do_re_add`):
    - Fail closed on the fuse gate first: the cross-seed's files must be
      stat-able at the blob-derived target mount before anything is injected
      with `skip_check=True`. Missing blob parks; undecodable test blobs
      warn through.
    - Delete the SSD torrent entry (`delete_files=False`; bytes already moved).
    - For each racing-client torrent for the content (+ the cross-seed):
      `add_torrent(save_path=fuse.mount, skip_check=True, paused=False)`.
    - Every fresh add is **verified visible at the target** (4×2s). The fuse
      index lags while rclone is busy, so accepted-but-invisible parks and
      retries — never `DONE`, never destructive. Transient rejections park;
      hard rejections skip just that torrent.
    - Duplicate entries pointing elsewhere are replaced; already-correct
      entries are kept as-is.
    - Public torrents land paused when
      `cross_seed.pause_public_torrents_on_fuse` is set (default true):
      fresh adds go in paused and already-seeding fuse entries are paused
      in place, so operators that don't seed publics still keep the entry.
      Detection is best-effort from the blob announce URL and fails open
      toward seeding, so privates always seed. Set false to seed publics.

8. **Mark DONE** (recording the fuse mount as `save_path`).

## Recovery (req #4)

On startup `reconcile()` audits active states against destination client
torrents and the local/remote filesystem:

- `state=downloading` + missing on destination -> re-add torrent to continue download.
- `state=moving` + missing on SSD -> verify if data arrived on remote; transition to `RE_ADDING` if complete.
- `state=done` + missing on fuse -> re-add to fuse mount via `RE_ADDING` (data already on remote).
- `state=done` + present but byte-missing (skip_check ghost) with bytes on
  SSD -> demote to `MOVING` for a real move; without SSD bytes it keeps
  trust (mount may be warming) and the fuse gate parks re-adds.
- Torrents on destination client not tracked in `state.db` are adopted
  (`DONE` on fuse with verified bytes, `MOVING` when SSD-complete,
  `DOWNLOADING` for partials) only under known SSD/fuse roots — foreign
  placements stay `unknowns`. Name matching requires the same normalized
  release, so repacks get their own rows instead of merging.
- Torrents on destination client not tracked in `state.db` are audited and logged as orphans.
- Startup recovery only lists `category="racing"`, so manual adds without a
  category are invisible to it by design — the per-tick manual fuse sweep
  (hash lookup, any category) covers those after startup.

The state DB is the source of truth; VPS2 + filesystem are reality. The
reconciler bridges them. After recovery the SSD ledger rebuilds from
`QUEUED/DOWNLOADING/MOVING` rows, so an abrupt stop resumes with a correct
budget instead of double-spending freed space.

## SSD budget

`ssd.max_inflight_bytes` is a **global** budget shared by every concurrent
download — not a per-torrent cap:

- Admission reserves `min(total, max_inflight_bytes)`; over budget parks in
  `WAITING_DISK` (quiet, ≤1 re-check/minute; download-slot checked too).
  Content already fully verified on fuse skips reservation entirely (no
  budget queued for bytes that will never download).
- Post-classify refinement: max *remaining* batch (fuse-present bytes
  excluded — a 32 GB season with 20 GB already moved holds ~12 GB, not
  32 GB), full total for singles; singles that no longer fit roll back
  (entry deleted, row parked). Each completed batch re-tightens to the
  batches still outstanding, and WAITING_DISK retries of cursor-having
  rows reserve the remainder, not the total.
- Reservations release on `WAITING_DISK` / `RE_ADDING` / `DONE` / `FAILED` /
  `forget`, stale rows are pruned, and abrupt stops rebuild from the DB.

`ssd_max_inflight_bytes(cfg)` (live free-space headroom) still sizes each
row's *batch* cap; the ledger caps their *sum*.

## Fuse verification & lag

The fuse (rclone) index updates asynchronously — it can lag while the
remote is busy with another move. The pipeline treats "accepted but not
yet visible" as *not yet*, not *failed*: every injection path verifies the
entry at its target mount and parks on unconfirmed results. A dead mount
short-circuits on one mount stat instead of one failing stat per file per
row per tick. Late cross-seeds for `DONE` rows are checked per tick but
memoized 30 minutes when healthy (new arrivals wait at most one window).

## Logging

- `racing-sync.log` — human, rotated daily. Handlers disable themselves on
  `ENOSPC` instead of traceback-storming a full disk; startup warns when
  the log dir shares a filesystem with SSD/state data.
- `racing-sync.jsonl` — structured, secrets scrubbed recursively, for grep/jq/vector.
- `RingBufferHandler` — last 300 events in memory.
- `[logging_sink]` — optional HTTPS POST to a central collector.

## Telegram

One message per torrent (detail card, edited in place as the state
advances) plus one active-tasks list message, refreshed every
`status_update_interval` seconds with pagination buttons. Each active
task renders a `Cancel: /cancel_<short-hash>` line (10-char prefix in
backticks so mobile offers tap-to-copy; full hashes accepted too).
`WAITING_INDEXER` rows additionally render
`Fetch original: /fetch_<short-hash>`, and grace-held NEW watch rows
render `Prefer this copy now: /prefer_<short-hash>`. The updates poller also watches
chat messages: a `/cancel_<...>` line from the configured chat/user
resolves the prefix against tracked rows and runs forget+ignore
immediately (row, dest entries, SSD data, blob cache) with no
confirmation, then replies with the outcome and frees the SSD
reservation. Cancelling an SSD owner also forgets the watch rows
currently deferred on it (same election winner), reported in the reply;
cancelling a waiter leaves the rest alone. A `/fetch_<...>` line flags
a waiting row (`force_direct`)
and wakes it (WAITING_INDEXER → QUERYING) so the racing torrent's own
bytes feed the SSD download at once; non-waiting targets get an
explanatory reply. A `/prefer_<...>` line exempts a grace-held row
(watch drop or racing row, one-shot) and wakes it so its SSD download
starts at once; later same-content rows defer to it via the existing election, no follower
update needed. Unknown/ambiguous prefixes get an explanatory
reply. Command and callback handling share a 0.5s per-chat debounce
(double-sent commands resolve+act once). Cancelled releases live
in `ignored_torrents` (in state.db, so `--reset` clears them) and are
skipped at discovery, recovery adoption, re-injection and late-seed
time. Flood control (including plain-text "Flood control exceeded" errors,
not just `RetryAfter`) sleeps out the requested window and re-queues the
update instead of dropping it; every successful detail send records its
state, and each active-tasks refresh re-queues cards that drifted, so no
card freezes at a dead state forever.

## FastAPI control plane

`GET /api/state` (paginated) · `GET /api/active` (bounded) ·
`GET /api/logs` · `GET /api/ssd` · `POST /api/recover` ·
`POST /api/retry/{hash}` · `POST /api/forget/{hash}?ignore=true`
(always applies; `ignore` records the cancellation) ·
`POST /api/scan-watch`. Useful when the Telegram bot isn't enough. Auth via
nginx-injected `X-Authenticated-User` header or a static token.

## Cross-seed tracker map

`[prowlarr.tracker_map]` is a mapping where keys are announce-URL substrings
and values are Prowlarr indexer names. The first matching substring wins
(evaluated in insertion order).

Keys match case-insensitively against the announce URLs of racing torrents.
This is what `prowlarr.resolve_indexer_for_announce(url)` uses internally.

## Operational notes

- Always run the destination qBittorrent as a separate user; the SSD save_path
  must be writable by that user.
- Keep `general.log_dir` on a **different filesystem** from SSD/state data:
  a full SSD otherwise takes down logging and `state.db` with it (startup
  warns when they share a device).
- The fuse mount (`/mnt/remote/...`) should be **read-only** to qBittorrent
  if possible, but qB doesn't care: it only reads from `save_path` after the
  files are there.
- Isolated batches re-download shared boundary pieces per batch: budget a
  few extra GB of swarm traffic per season for the guarantee that only
  complete files reach the remote.
- Extra rclone tuning belongs under `[rclone]` (`extra_move_flags`,
  `batch_move_extra_flags`); keys under `[rclone.fuse]` are ignored (with a
  warning). Config flags `--config`/`--password-command`/`--ask-password`
  are rejected outright.
- HTTP sessions are IPv4-only by default (`use_ipv6 = false` on
  `[source]`/`[dest]`/`[prowlarr`) — tracker allowlists and seedbox egress
  are overwhelmingly v4. Enable only on v6-capable setups.
- The state DB is append-only-safe; it can be inspected with the `sqlite3` CLI:
  `sqlite3 /var/lib/racing-sync/state.db "select state, count(*) from torrent_state group by state"`.