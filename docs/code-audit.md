# Racing Sync — Code Audit

- **Date:** 2026-10-25
- **Commit audited:** `ba8f827` (`dev`)
- **Scope:** all 41 modules under `src/racing_sync/` (~40.5k LOC) plus config example and docs
- **Method:** full-file static review, cross-checked against call sites. Every finding below was re-read in the
  source before inclusion; findings that depend on runtime/platform behavior are marked **Likely**.
- **Not focused on:** throughput/performance. Recommendations target stability, data safety, fewer code paths,
  and better failure behavior for a single-operator daemon.

### Confidence labels

| Label | Meaning |
|---|---|
| **Verified** | The code path is deterministic given the stated trigger. |
| **Likely** | The code clearly permits it; trigger depends on client/platform/timing behavior. |

### Severity

| Severity | Meaning |
|---|---|
| **High** | Data loss/corruption, stuck pipeline, upgrade blocker, wrong destructive action, secret leak. |
| **Medium** | Edge-case failure, silent degradation, recurring manual intervention. |
| **Low** | Robustness/simplification/UX polish; no data loss. |

### Environment note

The full suite runs green: `python -m pytest -p no:vsengine -q` → **1294 passed** in ~95 s (Python 3.13,
commit `ba8f827`). The `-p no:vsengine` flag is required only because this machine has the unrelated
VapourSynth `vsengine` package installed globally, whose `pytest11` auto-plugin raises during session start;
the project itself has no `vsengine` dependency or reference. A bare `pytest` aborting at collection is
therefore an environment artifact, not a project defect.

Implication for this audit: the findings below are gaps *not covered* by the existing tests — each is a
code path the suite either mocks around or never triggers. No finding was refuted by a passing test. Note
also that `asyncio_mode` is reported as an unknown option (the user-site `pytest-asyncio` is not the one
loaded), yet async tests still run via anyio; this is a latent environment inconsistency worth pinning.
`ruff check src` reports **199 findings** (17 blocking path ops in async functions, 11 unused imports,
9 raise-without-from, 5 closure-in-loop warnings, plus style). Line numbers are from `ba8f827`.

---

## Summary

| Area | High | Medium | Low |
|---|---:|---:|---:|
| A. Destructive paths & data safety | 3 | 3 | 2 |
| B. State, persistence & recovery | 2 | 4 | 2 |
| C. Move / offload pipeline | 2 | 4 | 3 |
| D. Admission, SSD budget & batching | 0 | 7 | 5 |
| E. Fuse, re-add & cleanup | 0 | 9 | 5 |
| F. Watchdog & loop lifecycle | 1 | 1 | 3 |
| G. Clients, config, API, logging & CLI | 1 | 5 | 11 |
| H. Telegram control plane | 1 | 8 | 10 |
| **Total** | **10** | **41** | **41** |

The highest-value work is concentrated in four themes:

1. **Every destructive step needs one invariant:** re-read state+version, prove the path is local SSD, then delete.
   Several paths skip at least one of the three.
2. **`upsert()` writes the whole row with no state/version predicate.** Any stale snapshot can erase a concurrent
   transition or the MOVING checkpoint. This single primitive undermines many of the state-machine guarantees.
3. **Unknown is being treated as absent** in several places (mount not mounted, stat failed, file list empty,
   entry not visible). For this pipeline, unknown must park, never delete/fail.
4. **The "same rule" utilities exist but are not used consistently** (content key, fuse gate, retry policy,
   infohash parsing, path ownership). Consolidating them removes whole classes of drift.

---

## A. Destructive paths & data safety

### A1 — A transient `stat()` error deletes a fully downloaded file
**High · Verified** — `src/racing_sync/coordinator_move.py:1004-1042`, deletion at `:1086-1113`

If `file_path.stat()` raises `OSError` (EACCES/ESTALE/AV lock/fuse blip), the complete file is assigned
`_on_disk = -1`, falls through both size checks, and lands in `incomplete_files`. Step 3 then unlinks every
member of `incomplete_files`. The subsequent folder wipe clears the source and the row proceeds to
`RE_ADDING`, where the fuse gate parks/fails because the bytes exist nowhere.

**Fix:** any `stat()` `OSError` → `uncertain_files` (park the move, delete nothing), regardless of client progress.
Same for `:1039-1042`: a failed stat on a sub-99.9% file is silently `continue`d and later wiped unverified.

### A2 — Budget-failure park deletes files in the multi-file movie branch
**High · Verified** — `src/racing_sync/coordinator_admit.py:3502-3515` (compare `:3457-3463`, `:3595-3601`)

The season branch deletes the just-added entry with `delete_files=False`; the movie/file-group branch uses
`delete_files=True`. The neighboring comment at `:3592` explicitly says the entry is kept and that a
metadata-driven wipe could take shared-directory sibling data. This branch does exactly that wipe.

**Failure:** a shared `save_path` (or a pre-created directory) contains files belonging to another torrent; a
budget re-check failure deletes them. This is the one place where an admission park is destructive.

**Fix:** use `delete_files=False` here. Better: one `_park_budget(ts)` helper used by every branch, with a single
delete policy.

### A3 — `injectfuse` preempt deletes files at unproven paths
**Medium · Verified** — `src/racing_sync/coordinator_inject.py:398-414`

`delete_files = not on_fuse`; `on_fuse` is false for every save_path that is not a configured fuse mount —
including foreign disks, network shares, or an empty/unknown path. The preempt therefore deletes files at
locations the app does not own.

**Fix:** delete files only when the path is *proven* local SSD (`_save_path_on_ssd`, already used by
`coordinator_readd.py`); otherwise remove the entry file-less. This also matches the "refuse foreign paths"
hardening already present elsewhere.

### A4 — Folder wipe and temp purge can follow symlinked directories
**Medium · Likely** — `src/racing_sync/coordinator_move.py:2158-2206`, `:1118-1123`

`folder.rglob("*")` and `folder.glob("**/*.!qB")` traverse into symlinked directories on several Python/pathlib
versions. Entries inside such a directory are lexically under `src_dir`, pass `relative_to`, and are unlinked —
outside the SSD tree. The code already avoids this when building the rclone list (`:1167-1184`).

**Fix:** walk with `os.walk(followlinks=False)` or skip any path with a symlinked directory component; verify on
the target Python version. Also skip symlinked ancestors in the post-check loop.

### A5 — Forget tombstones the row even when the SSD delete failed
**Medium · Verified** — `src/racing_sync/forget.py:424-448`, tombstone/hard-delete at `:456-476`

Dest-entry deletion failures keep the row (good). Local path removal failures are only appended to
`result["errors"]`; the row is then tombstoned or hard-deleted anyway. The bytes become unreferenced: no row,
no retry, no janitor. They fill the SSD until `WAITING_DISK` stalls everything.

**Fix:** if any `local_paths` removal failed, keep the row (return with errors) so `forget --apply` can be
re-run; only drop/tombstone when both dest entries and local bytes are confirmed gone.

### A6 — Waiter cascade runs even when the owner forget failed
**Medium · Verified** — `src/racing_sync/forget.py:535-580`

The paired-waiter loop ignores the primary result. If the owner row was kept (dest delete failed), its waiters
are still tombstoned. The operator sees a partially applied cancel and the waiters stay suppressed for 24h.

**Fix:** return a `row_kept`/`dest_ok` flag from `_forget_one`; skip the cascade when the primary was kept.

### A7 — `overlaps_fuse` fails open on an unstat-able mount
**Low · Verified** — `src/racing_sync/safety.py:200-213`

The docstring says a missing mount is treated as overlapping; the code does `if not f.exists(): continue`.
`Path.exists()` is False both for "absent" and for a failed stat (wedged fuse), so reset/wipe guards can
conclude there is no overlap exactly when the mount is unhealthy.

**Fix:** compare resolved paths without requiring `exists()`, and treat stat failure as overlap.

### A8 — Windows case-sensitivity in fuse save-path checks
**Low · Likely** — `src/racing_sync/forget.py:259-284`

`_is_fuse_save_path` compares raw strings. `coordinator_content.fold_path_case` exists because NTFS is
case-insensitive and is used by `_save_path_on_ssd`. On a Windows host, `G:/Fuse` vs `g:/fuse` classifies a fuse
entry as local SSD and a `delete_files=True` forget would delete remote data through the mount.

**Fix:** fold both sides before prefix comparison; use the same helper for every path-ownership check.

---

## B. State, persistence & recovery

### B1 — Indexes are created before the migrations that add their columns
**High · Verified** — `src/racing_sync/state.py:343-353`, `:428-429`, migrations `:430-461`

`SCHEMA_INDEXES` includes `ix_public_export_retry` (`public_export_next_retry_at`) and `ix_active_updated`
(`deleted_at`), both listed as "columns added after 1.0.0" migrations. `executescript(SCHEMA_INDEXES)` runs
**before** the `ALTER TABLE` loop. Any pre-existing DB without those columns raises
`OperationalError: no such column` and the daemon cannot start. Fresh DBs are fine, which is why this is easy to
miss in testing.

**Fix:** run the migration loop before index creation (or move those two indexes after it). Longer term, track a
`schema_version` in `meta` and derive the no-blob column list from `PRAGMA table_info` so the hand-maintained
lists cannot drift.

### B2 — `upsert()` is an unguarded whole-row write
**High · Verified code, likely trigger** — `src/racing_sync/state.py:667-726`; callers include
`recovery.py:577,694,856,918,951,991`, `coordinator_admit.py:870,1177,1633,2152,2207,2235,2296,2308,2394,2402`,
`coordinator_move.py:162,164,840,1989`, `coordinator_readd.py:685,698,775,793,835,910,921`,
`coordinator_watchdog.py:121,306`

Only `transition()` is state/version-guarded. `upsert()` writes every semantic column (`state`, `move_stage`,
`move_plan`, `batch_index`, timers) from a possibly stale in-memory snapshot. The code itself warns about this at
`coordinator_move.py:142-145`.

Concrete failure (API `/recover` while the pipeline runs): recovery snapshots rows, awaits per-row client calls,
then `store.upsert(ts)` writes the old state back — erasing a concurrent `MOVING → RE_ADDING` transition or a
`move_stage="entry_deleted"` checkpoint. A worker's own park write can similarly undo an operator retry or
`/injectfuse`.

**Fix:** add `expected_state` / `expected_version` to `upsert` (raise `AbandonedError` on mismatch), or convert
all mid-flight call sites to `update_columns(..., expected_state=...)` + `set_blob`. Keep whole-row upsert for
creation/discovery only. The API report should count skipped rows.

### B3 — A failed `COMMIT` leaves a transaction open and wedges all writes
**Medium · Verified** — `src/racing_sync/state.py:542-553`

`COMMIT` is in the `else` clause outside the `try` that rolls back. If COMMIT fails (ENOSPC, SQLITE_BUSY after
the busy timeout), the connection remains inside a transaction; the next `BEGIN IMMEDIATE` fails with "cannot
start a transaction within a transaction", and every write fails until restart. A `BaseException` from the
`yield` (KeyboardInterrupt) also skips rollback because only `Exception` is caught.

**Fix:** wrap COMMIT in `try/except: ROLLBACK; raise`; in `finally`, roll back when `conn.in_transaction` is true.
The store's lock already serializes threads, so this is the only remaining transaction-integrity gap.

### B4 — Recovery adopts using a stale row snapshot, creating duplicate rows
**Medium · Verified code, likely trigger** — `src/racing_sync/recovery.py:660`, `:799-821`, `:918`, `:951`

`db_hashes` is read once, then the verify loop can run for minutes. A row admitted by the coordinator during the
pass is invisible; its destination entry is adopted as a second row keyed by the dest hash. Two workers then
drive the same torrent with divergent batch cursors/priorities.

**Fix:** immediately before adoption, re-read the candidate hashes (`get_many`) and skip if any row now claims
the content (hash or content key).

### B5 — Non-semantic writes bump `version` and cause spurious abandonment
**Medium · Likely** — `src/racing_sync/state.py:697-700`; UI writes e.g. `set_telegram_message_id`

Every `upsert` increments `version`, including message-id bookkeeping. A worker holding a different in-memory
object then fails its `transition()` version guard with `AbandonedError`, even though no semantic state changed.
This shows up as random re-dispatches and "row written concurrently" logs.

**Fix:** keep `version` for semantic columns only; write display/message columns through narrow helpers that do
not bump it (or have `transition` ignore version-only drift).

### B6 — Frozen batch cap is never persisted
**Medium · Verified** — `src/racing_sync/coordinator_ssd.py:65-112`, `src/racing_sync/state.py:1768`

`_frozen_batch_cap` reads `ts.batch_cap_bytes`, but nothing ever assigns it (grep: only the row decoder sets it
to 0). Architecture claims batch boundaries are frozen per row and persisted, which is false. If
`max_inflight_bytes` or free space changes between restarts, `make_batches` produces different boundaries while
`batch_index` survives — moved batches can be re-downloaded or un-downloaded ones skipped.

**Fix:** persist the cap on first `QUEUED` resolution (version-safe narrow update), and fall back to re-deriving
the cursor by content when the stored plan is absent.

### B7 — `StateStore.delete()` destroys an existing tombstone
**Low · Verified** — `src/racing_sync/state.py:965-979`, `:1015-1034`

`tombstone()` returns False for an already-tombstoned row (it refreshes the stamp), so `delete()` falls through
to an unqualified `DELETE`, removing the tombstone and reopening the resurrection window. No production caller
(tests only).

**Fix:** delete the method, or `if tombstone(...) or _is_tombstoned(...): return`.

### B8 — WAL `synchronous=NORMAL` can lose the last commit on power loss
**Low · Verified (by design)** — `src/racing_sync/state.py:405-421`

In WAL/NORMAL, commits are not fsynced until checkpoint. A power cut can roll back the most recent
transactions — including the MOVING checkpoint that exists specifically to make replay safe. The code only
forces `FULL` when WAL is unavailable.

**Fix:** document the tradeoff explicitly, or use `synchronous=FULL` for the MOVING checkpoint writes.

### B9 — `reconcile()` is documented as startup background work but is now API-only
**Low · Docs drift** — `docs/architecture.md:282-288`, `src/racing_sync/coordinator.py:545-562`

The docs describe `reconcile_verify()` as a startup background task; the coordinator no longer starts it.
This matters because most B2/B4 races are only reachable through `POST /api/recover` while running. Update the
docs and state the concurrency contract (recover is safe to call live only after B2 is fixed).

---

## C. Move / offload pipeline

### C1 — Remote verification is non-recursive, so a replayed move reports "data loss"
**High · Verified** — `src/racing_sync/rclone_ops.py:951-973` (caller `:864-875`)

`_remote_has_bytes` runs `rclone lsf --files-only <remote>` and intersects the result with the move's relative
paths. `lsf` without `-R` lists only top-level entries. For any nested file (`Show/Season/ep.mkv`,
`Top/file.mkv`) the intersection is empty. If the move succeeded but the checkpoint/cursor was not persisted
(process killed after rclone exit), the rerun sees an empty source and raises
`RcloneError("source empty and remote holds no bytes — data loss?")`; the row fails with its bytes on the
remote.

**Fix:** use `lsf -R --files-only` (or walk the source tree directory-by-directory) and compare the same
relative paths. When the source is empty and the remote check is inconclusive, park for operator confirmation
instead of hard-failing.

### C2 — The mixed-torrent move branch uploads unverified files
**Medium · Likely** — `src/racing_sync/coordinator_move.py:1843-1878`

The strict branch moves `completed_files` (client-complete **and** full size on disk). The mixed branch builds
`names` from `batch.file_names()`, which includes files step 3 left in place when a hash check was detected
(`_post_checking`). Those can be partial and still get transferred, violating "only complete files reach the
remote".

**Fix:** build the list from `completed_files` (minus `_fuse_skipped`) in this branch too; park if any selected
file is unverified.

### C3 — A ≥99.9% download can wait forever for the completion flush
**Medium · Verified** — `src/racing_sync/coordinator_download.py:696-738`

Stall detection is skipped at `prog >= 0.999`, and the single-flow branch waits for the client to leave
`downloading` with no deadline ("bounded only by the flush landing"). A stuck final block or unfinalized file
pins a worker and a download slot until the 24h janitor.

**Fix:** apply a hard per-row completion deadline (or stall timeout on "no byte change") regardless of progress;
park with a clear reason.

### C4 — Expected-file drift fails the row instead of re-planning
**Medium · Verified** — `src/racing_sync/coordinator_download.py:635-652`

If a file in the frozen batch's expected set is missing from the client's file list (metadata re-resolve,
deselect, rename), the code raises `RuntimeError` and the row fails. Batch-count drift is explicitly healed at
`:249-258`; this is the same class of event.

**Fix:** reload the file list, rebuild batches when byte totals still match, else park for a re-plan. Only a
persistent mismatch should fail.

### C5 — New moves start after shutdown was requested
**Low · Verified** — `src/racing_sync/coordinator_move.py:2237-2288`

The lane queue loop breaks on `_stop`, but execution continues into `async with _sem` and launches rclone
anyway. Shutdown then waits on a transfer that began after stop.

**Fix:** return a `CancelledError`/busy sentinel if `_stop` is set after the queue wait, before acquiring the
semaphore.

### C6 — Legacy checkpoint-less MOVING rows park forever
**Low · Verified** — `src/racing_sync/coordinator_move.py:864-883`

A MOVING row with no `move_stage` and no destination entry takes the "no entry" park path, escalating to ERROR
after 5 parks but never FAILED and never notifying (beyond logs). Pre-checkpoint rows can strand indefinitely.

**Fix:** on the Nth park, transition to FAILED with a Telegram notification; or synthesize a `moved` checkpoint
when the destination entry proves the bytes are remote.

### C7 — Move classification pin upserts the whole row
**Low · Verified** — `src/racing_sync/coordinator_move.py:835-842`

This is an instance of B2: a narrow "pin the classification" write is done with a whole-row upsert after awaits.

**Fix:** `update_columns(classification_kind=...)` with an expected state/version.

### C8 — `_TESTING` shortcuts bypass the production completion path
**Low · Verified** — `src/racing_sync/coordinator_download.py:497-505`

Test-only branches keep CI fast but mean the real wait/completion code is less exercised. Prefer injecting fake
clients/timers instead.

---

## D. Admission, SSD budget & batching

### D1 — Physical free-space check is outside the ledger lock
**Medium · Verified** — `src/racing_sync/coordinator_ssd.py:404-444`

The ledger cap is re-checked under the lock, but `ssd_has_room()` (the physical stat) runs before acquiring it.
Two admissions against 10 GB free with a 40 GB ledger cap can both pass and reserve 12 GB. The grow path
(`:506-524`) has the same shape.

**Fix:** re-check `free - reserved_total >= amount` under the lock (or reserve first and roll back if the post-
lock stat fails).

### D2 — Prune can drop a fresh promotion reservation
**Medium · Verified** — `src/racing_sync/coordinator_ssd.py:265-359`; promotion at
`coordinator_admit.py:1443`, `:1510`, `:1531`

`_ssd_prune_stale` pops any hash whose DB row is not QUEUED/DOWNLOADING/MOVING. The WAITING_DISK promotion path
reserves, then awaits `_live_download_count`, then transitions. During that window the row is still WAITING_DISK,
so a concurrent admission prunes the new reservation; the row reaches QUEUED with no ledger entry (undercount).

**Fix:** store a reservation timestamp and skip entries younger than ~60s in prune, or transition before the
await and roll back on failure.

### D3 — Crash rebuild over-reserves
**Medium · Verified** — `src/racing_sync/coordinator_ssd.py:575-612`

Each in-flight multi-batch row reserves `min(total, cap)`, and the aggregate is never clamped to the global cap.
After a crash with N season rows the ledger is N×cap and blocks all admissions until each row refines.

**Fix:** clamp the rebuilt aggregate to the cap (oldest/highest-priority rows win), or multiply-derive the
remaining batch footprint from `move_plan`/`batch_index`.

### D4 — Multi-file episode rows can never fit and loop forever
**Medium · Verified** — `src/racing_sync/coordinator_admit.py:3675-3690`, `:3578-3602`

`_resolve_batches` returns `[]` for kind `episode`, so `_real_footprint` falls back to `sum(files)`. If extras
push that over the SSD cap while each file fits, `_ssd_adjust` fails, the row parks in WAITING_DISK, retries
~60s later, re-adds, re-hashes, and fails identically forever.

**Fix:** batch multi-file episode rows with `make_file_batches` (or fail with an explicit "single episode with
extras exceeds cap" message if batching is intentionally unsupported).

### D5 — Empty file list is accepted as "full-torrent flow"
**Medium · Verified** — `src/racing_sync/coordinator_admit.py:3364-3371`, `:3578-3582`

`retryable` can legitimately return `[]` transiently. The code classifies it as movie with no files, reserves
`ts.total_bytes`, and resumes without priorities/batches, potentially flooding the SSD.

**Fix:** treat an empty list like the `_prepare_next_batch(strict)` path: park and retry; only accept empty after
a persistent streak with an explicit reason.

### D6 — Same-content dedup only sees already-QUEUED rows
**Medium · Likely** — `src/racing_sync/coordinator_admit.py:873-934`, callers `:685`, `:1243`, `:2763`, `:2883`

`_inflight_same_content` considers only QUEUED/DOWNLOADING/MOVING/RE_ADDING, and the snapshot is cached 30s. Two
same-content rows in NEW/WAITING_INDEXER can both admit in one tick before either is QUEUED, producing duplicate
SSD downloads into the same relative `save_path`.

**Fix:** elect deterministically across *all* non-terminal rows (state rank, `created_ts`, infohash), defer the
losers, and invalidate the snapshot on every transition.

### D7 — Sacrificial-indexer loop stops at the first failure
**Low · Verified** — `src/racing_sync/coordinator_admit.py:3056-3068`

A failed download-indexer attempt `break`s instead of trying the remaining indexers, then falls back to the
dropped file. That gives up on viable sources.

**Fix:** `continue` while alternatives remain; only fall back after all fail.

### D8 — Sticky/unbounded retry flags
**Low · Verified** — `src/racing_sync/coordinator_admit.py:132-136`, `:839`

`force_direct` is never cleared on success/FAILED retry, so later attempts never re-query Prowlarr.
`public_export_attempts` grows without reset.

**Fix:** clear `force_direct` on DONE/FAILED; bound/reset attempt counters per retry window.

### D9 — Name-only fallback can bind the wrong infohash
**Low/Medium · Verified** — `src/racing_sync/coordinator_admit.py:3648-3654`

The normalized-name fallback may adopt a different torrent with the same name. A later
`_fail_downloading_row` deletes that hash with `delete_files=True`.

**Fix:** require exact name + size agreement, or persist the expected hash at add time and only adopt on a hash
match.

### D10 — Unknown/zero size reserves the full cap
**Low · Verified** — `src/racing_sync/coordinator_ssd.py` (`_ssd_estimate_for_new`)

`total <= 0` reserves `cap`, so several size-unknown rows exhaust the ledger until prune.

**Fix:** reserve a small floor, or defer reservation until the size is known.

### D11 — Public election ignores size
**Low · Verified** — `src/racing_sync/coordinator_picker.py:72-81`, `:114-159`

Candidates sort by size/name/hash with no equality guard; the chosen blob is returned unverified. Because
`normalize_content_name` strips trailing `[...]`, a same-name repack/Director's cut of different size can win.

**Fix:** require size within a tolerance and verify the blob like `_verified_cross_seed_blob`.

### D12 — `mixed` classification routes to `unsorted`
**Low/Medium · Verified** — `src/racing_sync/classifier.py:241-250`,
`src/racing_sync/coordinator_admit.py:3740-3744`

A 22-episode pack with 3 extras (88% episodes) classifies `mixed`, and routing defaults `mixed` to
`remote.unsorted`, because only `movie`/`season` go to `remote.default`. Season packs with a few extras land in
the wrong remote directory.

**Fix:** route `mixed`-with-episodes like a season (or lower the 90% threshold); keep routing as a single
explicit mapping table.

### D13 — Retry delay list is off by one
**Low · Verified** — `src/racing_sync/retry.py:83-94`

`attempts=3` with delays `(1,2,5)` executes three tries using only 1s and 2s; the 5s delay is unreachable. Call-
site comments promise 1/2/5s.

**Fix:** define `attempts` as retries (`range(attempts + 1)`) or use one delay per attempt and update comments.

### D14 — Retry policy retries fatal `OSError`s
**Low · Verified** — `src/racing_sync/retry.py:36`

All `OSError` retries, including ENOSPC/EACCES/ENOENT, while `coordinator_errors.is_retryable_client_error`
deliberately excludes fatal errnos. Two policies disagree.

**Fix:** share one retry predicate.

### D15 — Unknown-size rows can sit in `WAITING_DISK` without a real reservation
**Low · Verified** — `src/racing_sync/coordinator_ssd.py` rebuild/estimate paths

Rows restored from an older DB with `total_bytes=0` reserve `cap` or 0 depending on the path; their footprint
is then unknown to the ledger until classification. See D10.

---

## E. Fuse, re-add & cleanup

### E1 — An unmounted mountpoint is indistinguishable from missing content
**Medium/High · Verified** — `src/racing_sync/fuse_gate.py:85-89`

`fuse_stat_cached(mount)` stats the directory; there is no `os.path.ismount`. If rclone died or the daemon
started before the mount, the empty mountpoint stats fine and every expected file is "missing". Callers then
record a 30-minute content backoff, may demote DONE→MOVING, and the sustained-missing healer can re-download
files that are present remotely.

**Fix:** after the stat, require `os.path.ismount(mount)`; return `<mount unavailable>` otherwise. Centralize
this in the fuse gate.

### E2 — Lost injected entries are detected but never repaired
**Medium · Verified** — `src/racing_sync/coordinator_fuse.py:205-212`, `:264-268`, `:572-574`;
`coordinator_cleanup.py:585-613`

`known_hashes` includes `injected_private_hashes`, so the new-torrent loop and source repair skip them. A lost
injected private entry is not re-added anywhere: `_verified_fuse_mount_for_done_row` can re-add missing recorded
entries, but it only runs when `ts.save_path` is **not** on fuse (`:183`). The janitor notices the missing entry
and keeps VPS1 forever, but does not fix the seed.

**Fix:** add a quiet-tick verifier for all recorded hashes (entry placement + expected files), independent of
save_path. Treat "recorded" as "must be present", not "already handled".

### E3 — Subfolder-adopted DONE rows are verified at the wrong root
**Medium · Verified** — `src/racing_sync/coordinator_readd.py:556-641` (adoption at `save_path`),
`src/racing_sync/coordinator_fuse.py:1141-1151`, `src/racing_sync/coordinator_cleanup.py:637-655`

Adoption stores the entry's own `save_path` (e.g. `mount/SUB`) and verifies there. Later, cleanup and original-
entry repair derive the target with `_target_mount_for_blob`, which returns the configured `mount`/`mount_unsorted`
root. The bytes at `mount/SUB` are then reported missing, so VPS1 is never cleaned and repair defers forever.

**Fix:** when `ts.save_path` is on a configured fuse mount, verify there first and use the blob-derived path only
as a fallback.

### E4 — Replace path skips the post-add byte/completion check
**Low/Medium · Verified** — `src/racing_sync/coordinator_fuse.py:1385-1414`

The fresh-add path and duplicate-on-fuse path call `_fuse_entry_bytes_ok`; the replace path only checks that the
new entry's save_path points at the target and that files are selected. A gate→add race can mark an entry
injected while bytes vanished; paused/incomplete duplicates are also accepted.

**Fix:** run the same byte check (and a completion/resume check for duplicates) before returning success. One
`_confirm_visible()` helper for all three paths.

### E5 — Transient uncertainty is charged as a 30-minute content miss
**Medium · Verified** — `src/racing_sync/coordinator_fuse.py:959-960`, `:385-390`, `:646-653`

`_confirmed_fuse_missing` returns the cached miss when the fresh probe is indeterminate, and
`_NOT_VISIBLE_DETAIL` (explicitly transient in re-add) is written to `_failed_late_cross_seeds` for 30 minutes.

**Fix:** return `None` on an indeterminate fresh probe and route it to the short fuse-outage backoff; treat
not-visible/already-added as outage, not content miss.

### E6 — The healer's SSD-absent check ignores size
**Medium · Verified** — `src/racing_sync/coordinator_readd.py:277-291`

`_ssd_has_any` uses `exists()` only. A truncated or zero-byte same-name SSD leftover suppresses healing forever,
so the row hits the 24h FAIL even though the full file exists remotely.

**Fix:** stat and compare against `want_map`; a stat error means unverifiable (do not heal).

### E7 — Cross-seed re-add does not record the hash it injected
**Medium · Verified** — `src/racing_sync/coordinator_readd.py:925-972`

After `_ensure_fuse_entry(target_hash)` succeeds, the function neither calls `_record_injected_hash` nor updates
`cross_seed_infohash`/`dest_infohash`. Consequences: the next cycle pays a duplicate add/verify; a stale
`dest_infohash` makes `_ensure_original_fuse_entry` re-export it every 5 minutes; the janitor's all-hashes gate
can keep VPS1 unnecessarily.

**Fix:** on success record the hash and repoint/clear `dest_infohash` in one narrow, version-guarded write.

### E8 — Supplement memo suppresses retries after a failed injection
**Medium · Verified** — `src/racing_sync/coordinator_readd.py:1571-1578`

The 6h memo is set when `fetched or added`, even if every `_ensure_fuse_entry` returned not-ok and `added == 0`.
One registration-lag event means no supplement retry for 6 hours.

**Fix:** long memo only when `added > 0`; otherwise use `_SUPPLEMENT_FAIL_S`.

### E9 — Cleanup deletes from a stale verdict snapshot
**Medium · Verified** — `src/racing_sync/coordinator_cleanup.py:244-305`, `:783-850`

Candidates are computed from rows read at the start; deletions run later with a stagger and never re-read state
or version. A row that transitioned to RE_ADDING/MOVING (operator re-add, recovery demote) after the verdict can
have its VPS1 files deleted while a worker is re-injecting.

**Fix:** immediately before each delete, re-read the row and skip unless it is still DONE/MOVING with the same
version and the fuse health still verifies (or at least the state/version).

### E10 — Rows without `completed_at` bypass the grace period
**Medium · Verified** — `src/racing_sync/coordinator_cleanup.py:493-499`, `recovery.py:903-911`

`completed_at is None` yields `age_s = 0`; the row falls through to the 45-minute idle-confirm path instead of the
2–72h grace. Recovery adopts fuse seeds as DONE without setting `completed_at`, so a newly adopted row with an
old VPS1 `added_on` can be deleted after 45 minutes.

**Fix:** fall back to `updated_at`/`created_at`, or treat missing `completed_at` as ineligible for cleanup.

### E11 — Blob healing is unbounded and can overwrite the canonical blob
**Medium · Verified** — `src/racing_sync/coordinator_cleanup.py:660-690`

`await export(h)` bypasses `rpc()`, so a wedged destination client stalls the entire janitor. It also picks
`sorted(hashes)[0]`, which may be an injected private hash, and overwrites `cross_seed_blob`, changing the target
blob for future re-adds.

**Fix:** wrap in `rpc`; heal only from the canonical dest/cross-seed hash, and never overwrite a non-empty blob
with a different infohash's bytes.

### E12 — Fuse health is proven by hash presence, not entry placement
**Low/Medium · Verified** — `src/racing_sync/coordinator_fuse.py:739-742`,
`coordinator_cleanup.py:596-613`

Any destination entry carrying a recorded hash is accepted as alive, regardless of where it seeds from. A hash
present as an SSD-backed entry while the fuse target also holds bytes passes the entry gate; only the blob-
derived target is byte-checked.

**Fix:** require each present entry's `save_path` to be on a configured fuse mount (reuse
`_save_path_is_on_fuse`), then verify bytes.

### E13 — Demotion swallows `AbandonedError`
**Low · Verified** — `src/racing_sync/coordinator_fuse.py:817-833`

`AbandonedError` is a `RuntimeError`, so it lands in the generic handler, logs "demote failed", and the caller
proceeds as if handled. A tombstoned row stays DONE until the next tick.

**Fix:** re-raise `AbandonedError` (or return a success flag).

### E14 — Fuse outage map is effectively unpruned
**Low · Likely** — `src/racing_sync/coordinator_fuse.py:502-526`

The sweep returns before pruning unless the failure map exceeds 2000 entries; outage keys accumulate per hash for
the daemon's lifetime.

**Fix:** prune expired keys before the early return.

---

## F. Watchdog & loop lifecycle

### F1 — Watchdog resurrects a hard-deleted row as FAILED
**High · Verified code, race trigger** — `src/racing_sync/coordinator_watchdog.py:110-123`

`transition()` raises `AbandonedError` when the row was forgotten or written concurrently. The fallback then
force-assigns FAILED and calls `store.upsert(ts)`. For a tombstoned row, upsert refuses (safe); for a
`forget(ignore=False)` hard-delete, upsert **re-inserts** a zombie FAILED row and notifies Telegram. Its
reservation/entry are gone, so the operator sees a phantom.

**Fix:** `except AbandonedError: raise` (log and unwind); keep the force-write only for `ValueError`, and
re-check existence before writing.

### F2 — Worker task exceptions are never retrieved
**Low · Verified** — `src/racing_sync/coordinator.py:1278-1297`

The done callback checks `cancelled()`/`done()` but never calls `task.exception()`. `AbandonedError` from
handlers therefore produces "Task exception was never retrieved" noise at GC and hides real crashes.

**Fix:** call `task.exception()` once inside a try/except and log at debug.

### F3 — `WAITING_DISK` is re-dispatched in the same tick
**Low · Verified** — `src/racing_sync/coordinator.py:2602-2613`, park timers `:2459-2473`

The entry-state guard covers WAITING_INDEXER but not WAITING_DISK, so a worker that just parked with quiet-wait
timers can run `_wait_disk_then_queue` immediately and defeat the 60s quiet period.

**Fix:** extend the guard to WAITING_DISK.

### F4 — Waiting map grows without an upper bound
**Low · Verified** — `src/racing_sync/coordinator_admit.py` (`_waiting_disk_next_check`, `_grace_exempt`,
`_last_grace_search`, `_deferred_on`)

Prune removes stale rows, but per-hash entries from forgotten/renamed rows can persist between prune cycles and
are capped only indirectly. `_deferred_on` is capped; the others rely on transition pops.

**Fix:** prune all maps in one place and cap each (like `_supplement_ok_at`).

### F5 — `/tick` can run concurrently with the scheduler tick
**Low · Likely** — `src/racing_sync/telegram_protocol.py:282`, `src/racing_sync/coordinator.py` tick loop

A manual tick awaits a full pass inline; there is no explicit single-flight guard visible at the command entry.
Concurrent passes share the admission/ledger maps and can double-dispatch workers (worker registry usually
absorbs this, but not all paths check).

**Fix:** make tick single-flight (`asyncio.Lock` or skip if already running) and reply "already ticking".

---

## G. Clients, config, API, logging & CLI

### G1 — Prowlarr download follows redirects without re-validation
**High (security) · Verified code, likely trigger** — `src/racing_sync/prowlarr.py:453-528`

`_is_fetchable_http_url` validates only the initial URL. `session.get()` follows redirects by default. A
Prowlarr `/download` entry (or compromised indexer/proxy) can 302 to a link-local/attacker host; aiohttp strips
`Authorization`/`Cookie` on host change but **not** the custom `X-Api-Key`. Result: key exfiltration and SSRF
past the URL gate.

**Fix:** `allow_redirects=False`; validate each `Location`; recompute same-origin headers per hop; cap hops.

### G2 — Config `category` comma list is documented but not implemented
**Medium · Likely** — `src/racing_sync/config.py:173-187`, `config.example.toml:76-84`,
`clients/qbittorrent.py:195-197`, `coordinator.py:746`

The docs promise `category = "auto,manual"`; the value is sent verbatim as a single category/label, which matches
nothing. The daemon silently syncs zero torrents.

**Fix:** split on commas and query each (union), or reject commas in config and remove the claim. Add a
`check-config` warning for values containing commas.

### G3 — Watch-dir `glob` is bypassed by the suffix sweep
**Medium · Verified** — `src/racing_sync/watchdir.py:380-411`

`iterdir()` adds every `*.torrent` regardless of the configured `glob`. `glob="private-*.torrent"` still picks
up, processes, and (with delete-after-pickup) deletes `Other.torrent`.

**Fix:** run sweep entries through a case-folded `fnmatch` of the configured glob.

### G4 — v2-only torrents get a fabricated SHA1 infohash
**Medium · Verified** — `src/racing_sync/watchdir.py:184-228`

The parser always hashes the info dict with SHA1. A v2-only `.torrent` is consumed under a hash that qB/SFTP
never use, so it never syncs. Downstream 40-hex gates happily accept it.

**Fix:** parse `meta version`/`piece layers`; emit SHA256 for v2 and carry both (v1 + v2) through the row, or
reject v2-only drops with an explicit log/Telegram message.

### G5 — qB add reports duplicate failures for v2-only torrents
**Medium · Verified logic** — `src/racing_sync/clients/qbittorrent.py:448-511`

`_hashes_from_blobs` yields SHA1 only; qB indexes v2-only torrents by 64-hex. Every candidate misses, and the
`[:40]` fallback is not a real hash. A re-add of an existing v2 torrent reports `accepted=False`.

**Fix:** the shared v1/v2 parser from G4; look up both hashes; drop the fake prefix lookup.

### G6 — Deluge disconnected with zero hosts passes auth
**Medium · Verified** — `src/racing_sync/clients/deluge.py:150-189`

When `web.connected=false` and `web.get_hosts` is empty, `if hosts:` has no `else`, so auth returns success.
Every later RPC fails with "not connected", misclassified as a row error rather than an auth/startup failure.

**Fix:** raise `AuthError` when no host could be connected.

### G7 — Any 404 latches the legacy pause API forever
**Medium · Verified** — `src/racing_sync/clients/qbittorrent.py:588-604`

A 404 from `torrents/stop` flips `_legacy_pause_api=True` permanently, then the `pause` fallback also 404s. One
ambiguous 404 (torrent removed, reverse proxy) disables pause/resume for the process lifetime.

**Fix:** latch legacy only when the fallback call succeeds; treat a second 404 as a no-op/skip.

### G8 — IPv6 Origin/Referer are unbracketed
**Low · Verified** — `src/racing_sync/clients/http_base.py:321-330`

`http://[::1]:8080` becomes `Origin: http://::1:8080`, which is invalid. With `use_ipv6=true`, qB CSRF/Referer
checks can reject every POST.

**Fix:** wrap `parsed.hostname` containing `:` in `[]`.

### G9 — Readiness `ssd_writable` cannot fail on free space
**Low · Verified** — `src/racing_sync/api.py:547-562`

`ssd_free_bytes` returns `0` on `statvfs` failure, and `int(_free) >= 0` is always true. Also `os.path.isdir`
on the fuse mount runs on the event loop.

**Fix:** require `_free > 0` (or a configured minimum), and run the mount check in `asyncio.to_thread`.

### G10 — Broad middleware registration swallow
**Low · Verified** — `src/racing_sync/api.py:416-462`

The body-cap and rate-limit middleware is registered inside `try/except Exception: pass`. One failure silently
disables both.

**Fix:** register unconditionally; contain only the bucket bookkeeping; log failures.

### G11 — Log scrubbing misses common secret keys and allows log injection
**Low/Medium (security) · Verified** — `src/racing_sync/logging_setup.py:34`, `:125-131`

The JSON regex misses `X-Api-Key`, `Cookie`, and `Set-Cookie`; `SENSITIVE_KEY_WORDS` is unused. Control
characters are not stripped, so a torrent name containing `\n` can forge log lines (only watchdir strips it).

**Fix:** strip `[\x00-\x1f\x7f]` in `sanitize_log_text`; extend the key set; delete the unused constant.

### G12 — SFTP failed connect can leave a live transport in a failed pool
**Low · Verified** — `src/racing_sync/sftp_source.py:771-810`

A dial still inside `m.connect()` when another dial fails can finish after the failure-path `close()` loop,
setting `_closed=False`; generation is unchanged, so `_dial_one` leaves the transport live.

**Fix:** bump `_generation` before the failure-path close loop, as `close()` does.

### G13 — `DelugeSFTPConfig.state_dir` is required even when disabled
**Low · Verified** — `src/racing_sync/config.py:138-151`

A leftover stub with `enabled=false` fails config load; `state_dir=""` becomes `Path(".")` (remote home) because
`_empty_path_is_none` covers only the other path fields.

**Fix:** make it optional unless enabled; extend the empty-string normalizer.

### G14 — `DestConfig` does not validate host or `save_path`
**Low · Verified** — `src/racing_sync/config.py:260-271`

A relative `save_path` is forwarded verbatim and resolved against qB's cwd; scheme/userinfo mistakes surface only
when the client starts, unlike `SourceConfig`.

**Fix:** reuse `_ensure_base_url` checks; require `save_path.is_absolute()`.

### G15 — Prowlarr indexer list is fetched only at start
**Low · Verified** — `src/racing_sync/prowlarr.py:217`

Names/disabled flags changed in Prowlarr require a restart; until then `get_download_indexers` raises and rows
park.

**Fix:** refresh on a TTL or when a configured name goes missing.

### G16 — Parallel search timeout discards completed hits
**Low · Verified** — `src/racing_sync/prowlarr.py:655-666`

`wait_for(gather(...))` raises and the handler returns `{}` despite a "using partial hits" log.

**Fix:** collect finished tasks with `return_exceptions=True` before returning.

### G17 — Torrent payload accepted on a single `b"d"` byte
**Low · Verified** — `src/racing_sync/sftp_source.py:388`, `clients/qbittorrent.py:680`

A truncated or HTML-ish blob starting with `d` is treated as a torrent and added.

**Fix:** run the shared strict decoder and require a dict `info`.

### G18 — `watchdir` marks a file seen before successful handoff
**Low · Verified** — `src/racing_sync/watchdir.py:507-510`

With `delete_after_pickup=false`, a pickup the coordinator fails to process is never re-emitted until restart.

**Fix:** add to `_seen` after successful handoff (or clear on failure).

### G19 — SIGTERM exits 130
**Low · Verified** — `src/racing_sync/__main__.py:51-77`

Cancellation always returns 130; systemd/containers mark a clean SIGTERM stop as failed.

**Fix:** remember which signal fired; return 0 for SIGTERM (or 143).

### G20 — `--reset` deletes state before logging is initialized
**Low · Verified** — `src/racing_sync/__main__.py:786-802`

If `setup_logging` then fails, the process exits 2 with `state.db` already gone.

**Fix:** validate/initialize logging before the destructive reset.

### G21 — `offload()` has no deadline despite the module's uniform-bounds claim
**Low · Verified** — `src/racing_sync/io_bounds.py:78-80`

A wedged disk call in a worker thread waits forever; the caller must wrap it. `bounded()` also reclassifies any
inner `TimeoutError` (including `DownloadStalledError`) as its own timeout label.

**Fix:** give `offload` an optional timeout and raise a labeled error; preserve subclass types.

### G22 — `DaemonLock.acquire(blocking=...)` ignores the argument
**Low · Verified** — `src/racing_sync/safety.py:296-324`

The parameter is accepted but unused.

**Fix:** honor it or remove it.

---

## H. Telegram control plane

### H1 — Positional commands resolve against the unfiltered list while the numbering is filtered
**High (wrong destructive action) · Verified** — `src/racing_sync/telegram_refresh.py:375-409`,
`src/racing_sync/telegram_actions.py:685-708`, `:1139-1158`, `:1794-1814`

`render_active` numbers `shown` (status-filtered), but `/act_N`, `/cancel_N`, `/now_N`, `/resume_N` resolve `N`
against `store.list_active_inflight()` unfiltered. With a DL filter and a MOVING row first in the raw list,
`/act_1` opens (or `/cancel_1` cancels) the MOVING torrent. Only `/act_N` warns, and only on a *count* mismatch.

**Fix:** resolve positional tokens against the same filtered rows (`match_status_filter`), or reject positional
commands entirely while a filter is active (buttons already carry hashes).

### H2 — Pagination totals ignore the status filter
**Medium · Verified** — `src/racing_sync/telegram_router.py:507-529`,
`src/racing_sync/telegram_refresh.py:398-420`

`total_pages` counts all rows while the keyboard shows filtered totals. On a filtered 2-page view inside a 3-page
list, "Next" goes to index 2, rendering clamps to 1, and the button is dead; Prev can skip pages.

**Fix:** apply the same filter before counting.

### H3 — A page tap can be lost to a racing refresh
**Medium · Verified** — `src/racing_sync/telegram_refresh.py:414`

The refresh snapshots the page, awaits, then writes `_current_page = cur_page`, overwriting a tap that bumped the
page in between.

**Fix:** write back only if the value is unchanged since the snapshot, or do paging under the refresh lock.

### H4 — Cancelled detail cards misreport the chosen policy
**Medium · Verified** — `src/racing_sync/telegram_refresh.py:54-59`, caller
`src/racing_sync/telegram_actions.py:135`

The card always prints "removed + ignored" and an `/resume_` hint, regardless of `remember`/`delete_files`. With
"Just forget" there is no ignore entry, so `/resume_` does nothing; with "Keep files" the text implies data was
removed.

**Fix:** pass both flags into `_mark_detail_cancelled` and compose the line from them.

### H5 — A failed confirmation-sheet send loses the flow and then deletes the sheet
**Medium · Verified** — `src/racing_sync/telegram_bot.py:1029-1035`,
`src/racing_sync/telegram_router.py:459-468`

`_send_sheet` swallows every error at debug; the router then deletes the tapped sheet unconditionally. A
flood/timeout on the Q1/Q2 send leaves the operator with no sheet, no reply, and no way to continue.

**Fix:** return success from `_send_sheet`; on failure reply "could not post the sheet, retry" and skip the
delete.

### H6 — One debounce bucket silently drops mutating commands
**Medium · Verified** — `src/racing_sync/telegram_router.py:698-705`

The debounce key is `(chat, user)` for all verbs, 2s, no reply on drop. `/skip_<h>` then `/resume_<h>`, or two
`/add`-captioned `.torrent` files, within 2s: the second is discarded (and the dropped `.torrent` with the
sender's passkey is never deleted).

**Fix:** key on `cmd:{name}:{arg}`; answer dropped commands; always remove handled torrent files.

### H7 — Shutdown cancels a bulk cancel mid-execution
**Medium · Verified** — `src/racing_sync/telegram_bot.py:453-460`,
`src/racing_sync/telegram_actions.py:207-237`

`stop()` cancels `_callback_task` while `_execute_snapshot_cancel` is sleeping 2s per copy. Claims are released
in `finally`, but half the scope is applied and no reply is sent.

**Fix:** check a stop flag inside the loop; post `Cancelled k/n` before returning.

### H8 — SSD reservation leaks when forget raises
**Medium · Verified** — `src/racing_sync/telegram_actions.py:86-119`

`_ssd_release` runs only after `forget_torrent` returns; both error paths skip it. Any forget exception (client
error, lock timeout) strands the reservation until restart.

**Fix:** release in `finally`.

### H9 — Destructive scopes and `/tick` block the update loop
**Medium · Verified** — `src/racing_sync/telegram_actions.py:207-229`,
`src/racing_sync/telegram_protocol.py:282`, `:319-320`

A 100-copy lane wipe sleeps 2s per copy inline; `/tick` awaits a full pipeline pass inline; `/stats` runs
`store.all()` on the loop. Polling stalls for the duration, and Telegram may retry/duplicate updates.

**Fix:** run scopes/tick in a task (the in-flight guard already dedupes) and reply on completion; offload
`store.all()`.

### H10 — Sheets are not bound to the operator who opened them
**Low/Medium · Verified** — `src/racing_sync/telegram_router.py:441-468`

Buttons carry only the scope. Any allowed group member can finish another operator's "Delete files" sheet,
contrary to the config comment at `config.py:580-582`.

**Fix:** embed the opener's user id in callback data and verify on tap.

### H11 — False "List changed" warnings under a filter
**Low · Verified** — `src/racing_sync/telegram_actions.py:714-729`

`_last_rendered_groups` is the filtered count; the live comparison is unfiltered.

**Fix:** compare like-for-like (filtered).

### H12 — "Cancel all N copies" can be wider than the displayed group
**Low · Verified** — `src/racing_sync/telegram_bot.py:969-984`, `content_keys.py:74-83`

Display grouping uses normalized name + exact size; `_live_group_hashes` uses `same_content`, which tolerates one
side having size 0. A same-name row with unknown size renders in a different group but enters the destructive
scope.

**Fix:** use the display key (or strict `require_size=True`) for scope resolution.

### H13 — `/resume_<hash>` acts on settled rows
**Low · Verified** — `src/racing_sync/telegram_bot.py:1123`,
`src/racing_sync/telegram_actions.py:1694-1704`

`_skip_torrents(hold=False)` matches any tracked row, including DONE/FAILED, clears `skipped`, and spawns a
worker intending to resume, while reporting "Resumed".

**Fix:** restrict to non-terminal states (or make resume explicitly re-open a terminal row).

### H14 — Detail-edit failures are dropped; truncation is counted wrong
**Low · Verified** — `src/racing_sync/telegram_bot.py:873-881`,
`src/racing_sync/telegram_views.py:68-101`

Non-transient edit errors (`MESSAGE_TOO_LONG`, "message can't be edited") never get a fallback for terminal
rows. `_safe_truncate_markdown` counts code points, not UTF-16 units, so an emoji-heavy message can still exceed
Telegram's 4096-unit limit and freeze the card.

**Fix:** count UTF-16 units (or bytes/2), and re-send a fresh card when edit fails permanently.

### H15 — `_detail_drift_skip` grows without bound
**Low · Verified** — `src/racing_sync/telegram_bot.py:664-682`

Never popped on success; one int per hash that ever tripped the streak.

**Fix:** clear on success/terminal; cap size.

### H16 — `allowlist_open_warning` returns `False` from its `except`
**Low · Verified** — `src/racing_sync/telegram_bot.py:202-204`

The function is typed `str | None` and the comment says "fail closed"; `False` is merely falsy, so behavior is
accidentally right. Return `None` for clarity.

### H17 — Blob loads on hot paths
**Low · Verified** — `src/racing_sync/telegram_protocol.py:615`, `src/racing_sync/telegram_bot.py:739`

`CancelScope.resolve` and `_send_one_detail` call `store.get()` with blobs enabled, pulling multi-MB `.torrent`
blobs on every tap/card; the resolve happens on the event loop.

**Fix:** `include_blob=False`; fetch the blob only where bytes are actually needed.

### H18 — `abort` prefix match is too loose
**Low · Verified** — `src/racing_sync/telegram_router.py:449`

`data.startswith("abort")` also matches `abortx`; parsing then fails, the user gets "Stale button", and the
sheet is not deleted.

**Fix:** match `abort:` exactly.

### H19 — `_stopped` makes the router loop exit normally and respawn every second
**Low · Verified** — `src/racing_sync/telegram_router.py:320`,
`src/racing_sync/coordinator.py:1043-1068`

Setting `_stopped=True` returns from `_loop`; the supervisor sees a normal exit and respawns it repeatedly.

**Fix:** distinguish "stop requested" from "crashed"; do not respawn after stop.

### H20 — `/add` reports success without checking the write
**Low · Verified** — `src/racing_sync/telegram_router.py:945`

The `UpsertResult` is ignored, so a tombstoned write still answers "Queued … downloading".

**Fix:** check `WRITTEN` and report the tombstone.

### H21 — In-flight guard TTL vs. long bulk runs
**Low · Verified** — `src/racing_sync/telegram_router.py:359-367`

TTL is 600s while a large bulk run can exceed it; at the 1000-key cap new holds are refused and `_debounced`
stops recording windows, so a tap storm can re-execute.

**Fix:** refresh claim timestamps during long runs; cap/evict expired keys.

### H22 — Detail-enqueue overflow drain can drop an unrelated update
**Low · Verified** — `src/racing_sync/telegram_bot.py:577-594`

If `put_nowait` hits `QueueFull` during the refill, an unrelated update can be lost.

**Fix:** drain into a local list and retry; never drop silently.

---

## I. Simplification & robustness recommendations

These are not necessarily bugs; they remove code paths that cause the bugs above to recur.

### I1 — One guarded write primitive
Add `expected_state` / `expected_version` to `upsert`, or make every non-creation write go through
`update_columns(..., expected_state=...)`. Delete the scattered whole-row `store.upsert(ts)` calls after awaits
(B2, B5, C7, F1). Add one `_park(ts, reason, next_state, delay)` helper to replace the repeated
"upsert + return" blocks in download/move/readd.

### I2 — One schema/migration path
Run migrations before indexes (B1). Track `schema_version` in `meta`. Derive `_TORRENT_STATE_COLUMNS_NO_BLOB` and
`to_row` from one column tuple or `PRAGMA table_info` so the schema, migrations, and column lists cannot drift.

### I3 — One infohash/torrent parser
`watchdir._bencoded_info_hash`, qB `_hashes_from_blobs`/magnet parsing, Deluge parsing, SFTP export, and Prowlarr
blob checks each re-derive identity. One parser that returns `{v1, v2, name, files, announce}` fixes G4/G5/G17
and makes v2 behavior consistent everywhere.

### I4 — One fuse verifier
`fuse_gate` already has the pieces but `verify_blob_on_mount` is unused. Build one function that returns
`{mount_ok, entry_placement_ok, missing, expected}` using `os.path.ismount`, `ts.save_path` when on fuse, the
blob-derived path otherwise, and per-entry save-path checks. Use it for re-add, late cross-seed, cleanup, and
recovery. Delete the unused export.

### I5 — One content-key rule
Route watch election, racing deferral, watchdog cascade, cleanup grouping, Telegram group scope, and adoption
through `content_keys.same_content` with explicit modes (`require_size`, `allow_unknown`). The current mix of
name-only, name+size, and `same_content` causes D6, E9, and H12.

### I6 — One retry policy
Share the retry predicate between `retry.py` and `coordinator_errors`; exclude fatal errnos; fix the attempt/
delay semantics (D13/D14). Add one `_schedule_retry(ts, kind, delay)` for the duplicated timers in
`coordinator_admit` (`_park_for_indexer_retry`, `_park_for_public_retry`, `_repark_deferred`,
`_do_waiting_indexer`).

### I7 — One path-ownership check, used before every delete
There are three different notions today: `_save_path_on_ssd`, `_save_path_is_on_fuse`, and raw string compares.
A single `classify_save_path(cfg, path) -> {FUSE, SSD, FOREIGN, UNKNOWN}` (case-folded, symlink-aware) should gate
every `delete_files=True` and every wipe (A2/A3/A8, H8).

### I8 — Collapse the MOVING replay paths
`_replay_move_checkpoint` and the inline steps 8-10 in `_do_moving` are near-duplicates. Always materialize a
plan and call the replay routine, so a fresh run and a resumed run execute the same code. Consider making the
transition itself the `wiped` checkpoint and dropping one stored stage.

### I9 — Delete dead code
Confirmed no production callers: `fuse_gate.verify_blob_on_mount`, `StateStore.delete` (tests only),
`telegram_bot._tg_actor_id`, `telegram_protocol._fetch_command/_prefer_command/_unskip_command`,
`telegram_router._fetch_watch_eligible`, `prowlarr.url_quote`, `logging_setup.SENSITIVE_KEY_WORDS`. Also remove
the `pick:` router prefix (no parser shape) unless it is intentionally reserved.

### I10 — Deduplicate Telegram token resolution
The digits/gid/hash/prefix resolution is triplicated (`_start_group_command`, `_act_command_entry`,
`_resume_command_entry`). Extract `_resolve_group_token(rows, token)` that returns members + a stable generation,
and use it for all verbs. This also makes H1 a one-line fix.

### I11 — One per-state deadline
Add a table of maximum dwell times per state (checking, completion flush, file-list wait, fuse visibility,
rclone, cleanup) and enforce it in the worker wrapper. Today the 24h janitor is the only backstop, so a single
wedged condition pins a slot for a day (C3, C4, E3).

### I12 — Consolidate the "park" reason table
The `upsert(... status=...) + return` pattern appears across download/move/readd. A table-driven park helper
makes every park observable in one place, ensures reservations/admissions are released, and prevents another
destructive park (A2).

### I13 — Replace broad `except Exception: pass` on state-changing paths
The audit found many (e.g. `coordinator.py` admission release, `telegram_actions` release, `coordinator_fuse`
demote, `coordinator_cleanup` heal). Each swallowing site hides exactly the failures above. Keep broad catches
for best-effort logging/UI, but on state changes log at warning with the hash and a stable reason.

### I14 — Add `check-config` lints
Warn/fail on: comma-containing categories (G2), non-absolute `dest.save_path` (G14), fuse mount not actually
mounted (E1), watch-dir `glob` with patterns the suffix sweep would bypass (G3), `rclone.fuse` keys under the
wrong section, and a state DB with a schema older than the code.

### I15 — Count/retry hygiene
Cap and prune every in-memory map (`_fuse_outage_until`, `_detail_drift_skip`, `_waiting_*`, `_failed_*`) in one
periodic sweep; cap retry counters (`force_direct`, `public_export_attempts`) at their windows.

### I16 — Make tests exercise production paths
Remove `_TESTING` shortcuts (C8). Add integration tests for: crash after rclone before checkpoint (C1), stat
failure during move (A1), recover-during-move (B2), unmounted fuse (E1), filtered Telegram numbering (H1), and
cancel racing a worker (F1). These are the scenarios most likely to be hit in real use.

---

## Recommended fix order

**Phase 1 — data safety (do first, small diffs):**
A1, A2, A3, C1, B1, G1, G3, G6, G7, H1, H8.

**Phase 2 — state integrity & stuck-pipeline:**
B2 (guarded writes), F1, D1-D6, E1-E3, E6-E10, C3/C4.

**Phase 3 — Telegram correctness:**
H2-H9, H13.

**Phase 4 — simplification & hardening:**
I1-I16, remaining Low findings.

---

## What was checked and found sound

- `transition()` state/version guards, rollback snapshot, and tombstone refusal.
- Path traversal guards in `coordinator_paths._safe_ssd_join` and `batcher` (absolute/`..`/control chars).
- rclone child termination/kill/reap bounds and the `--`/config-flag rejection.
- Download admission atomicity at the QUEUED→DOWNLOADING edge (the admission set is released on the same
  transition; the earlier "slot held across MOVING" concern did not hold up on re-read).
- Per-row isolation in `reconcile_verify` (`_one` catches exceptions; a single row cannot abort the pass).
- Ledger release on terminal/park transitions, and the `_ssd_adjust` shrink/grow contract.
- Batcher episode grouping, cap freezing logic (except that it is never persisted — B6), and the all-remote fast
  path.
- Watch-dir bencode decoder hardening (size caps, recursion limits) apart from the v2 identity issue.
- API token comparison and trusted-proxy handling (apart from readiness/middleware issues above).
