# Refactor plan — capability chain, explicit modules, smaller config

Status: **phase 4 landed (2026-12-01); next: phase 5.** Phases 0–4 are in
`dev`: the config layer, the composition root, the chain engine, and the
`external`/`local`/`local_move`/`seed.existing` providers with single-box
ownership.

## Goal

Make the daemon expressible for every deployment we can reasonably support, without
a config that assumes one of them:

1. **One box** that races, offloads and seeds from a fuse mount is the default.
2. Two boxes (race here, keep there), with or without a fuse mount, with or without
   indexers, with rclone / rsync / a manual copy.
3. Partial pipelines: download-only, move-only, seed-only/adopt, watch/manual only.

Non-negotiable from the audit: destructive steps stay guarded, unknown stays a park,
and old configs keep loading unchanged.

## 1. The capability chain

The pipeline is a chain, not a fixed sequence. Every link except `observe` is
optional, and each link has interchangeable providers. A deployment is a chain.

```
observe → acquire → stage → transfer → seed → (cleanup)
```

| Link | Providers | Notes |
|---|---|---|
| observe | racing client, dest client, watch dir, manual/API/Telegram, existing library | at least one |
| acquire | `local`, `auto`, `cross_seed`, `direct_export`, `external`, `none` | how bytes reach staging |
| stage | SSD admission + batching | only when a *different* client downloads |
| transfer | `rclone`, `local_move`, `none`, `external` (`rsync` later) | how bytes leave staging |
| seed | `path`, `existing`, `none` | repoint the client at the final path |
| cleanup | janitor + a "content secured" proof | proof follows the chain |

- `cross_seed` is the sacrificial-tracker path: find the same release on a
  download-target indexer so the source account's ratio is untouched. On one box the
  link is `local` — the racing client already holds the bytes, so no indexer search
  and no download happen.
- `external` means the operator copies the bytes (their own rsync/cron); the daemon
  waits at the destination, verifies, and continues. It reuses the manual-move wait
  (30 min checks, 12 h ceiling).
- `seed = path` targets a fuse mount **or** a plain directory/NAS; fuse is a
  mountpoint path, not a separate module.

## 2. Shapes (user × chain)

| Shape | observe | acquire | transfer | seed | cleanup |
|---|---|---|---|---|---|
| Single box + cloud fuse (default) | racing | `local` | `rclone` | `path` (mount) | yes |
| Single box, seeds from disk | racing | `local` | `none` | `path` (same dir) | racing entry only |
| Single box, move to HDD/NAS | racing | `local` | `local_move` / `rclone` | `path` | yes |
| Two boxes, ratio-conscious (today) | racing + dest | `cross_seed` | `rclone` | `path` (mount) | yes |
| Two boxes, no indexers | racing + dest | `direct_export` | `rclone` | `path` | yes |
| Two boxes, seed on box 2 | racing + dest | `cross_seed` / `direct_export` | `none` | `path` | after seeding |
| Operator copies by hand | racing + dest | `external` / `none` | `external` | `path` / `existing` | yes |
| Watch-dir / manual only | watch | `local` / `direct_export` | any | any | optional |
| Adopt an existing library | existing | `none` | `none` | `existing` | yes |

## 3. Config

### Resolution rule

Every optional section has `enabled: bool | None = None`:

- `true` → on, its required keys are enforced;
- `false` → off, its keys are **kept** (re-enabling is a flip, not a rewrite);
- absent → on iff the section carries content (legacy files), off otherwise.

Required keys are validated only for enabled links; cross-field rules live in the
module that owns them.

As landed in phase 1: the adapter rewrites new spellings onto today's tables before
validation, so callers see no change. Both spellings of one key may coexist when they
agree and are refused when they differ. `enabled` is tri-state only where the runtime
can honour it today (`[indexers]`, `[watch]`); disabling `acquire`/`transfer`/`seed`
is refused until phase 3 can drain stranded rows. `[acquire]` owns the old
`[cross_seed]` strategy keys (they describe how bytes arrive, not which indexers
exist), and `[seed].unsorted_path` defaults to `<seed.path>/unsorted`.

### Sections

| Old | New |
|---|---|
| `[source]` (+nginx, deluge_sftp) | `[client]` |
| `[dest]` | optional `[dest]` (present/enabled → split) |
| `[ssd]` + `dest.save_path` | `[storage]` |
| `[rclone]` | `[remote]` |
| `[rclone.fuse]` | `[seed]` (fuse keys map to `seed.path`) |
| `[prowlarr]` | `[indexers]` |
| `[cross_seed]` | `[acquire]` (strategy keys only) |
| `[watch_dir]` | `[watch]` |
| `[cleanup] [telegram] [api] [logging_sink]` | same names, explicit `enabled` |

Pipeline links select providers:

```toml
[acquire]
enabled  = true
provider = "local"        # local | auto | cross_seed | direct_export | external | none

[transfer]
enabled  = true
provider = "rclone"       # rclone | local_move | none | external

[seed]
enabled  = true
provider = "path"         # path | existing | none
path     = "/mnt/remote/qbittorrent"
```

`auto` is the legacy acquisition preference (cross_seed → direct export) and only
exists for old configs; new files choose explicitly.

### UX

- `config init` writes a compact starter: core filled, every optional section as a
  one-line `enabled = false` stub.
- `config enable <feature>` appends the documented block (append-only, keeps
  comments); `config disable` flips the boolean and keeps the keys.
- `check-config` prints the resolved chain, each link's provider, disabled links,
  missing keys for enabled links only, deprecated-key warnings, and a pre-flight
  warning when disabling a link would strand rows.
- `config explain <section>` for long-tail options; `config migrate` only for renames.

## 4. Runtime composition

A composition root resolves a module graph and injects capabilities (Protocols) —
no more `hasattr(cfg, …)`.

As landed in phase 2: `runtime.Runtime(cfg)` exposes typed read-through views
(intake, storage, transfer, seed, indexers, acquire, limits, recovery, control,
paths, cleanup); views hold the config and re-read on access, so nothing is
copied and a swapped config is honoured. `composition.compose(cfg)` builds the
concrete client/indexer/blob/watch graph without I/O; `Coordinator.start()`
still owns start order and backoff. `capabilities.py` describes the blob,
watch and indexer-policy surfaces; indexer search stays on the concrete client
until the acquisition provider lands.

| Module | Requires | Provides |
|---|---|---|
| core | — | store, scheduler, state table, locking |
| sources | core | intake (client listing, watch, manual drops) |
| acquisition | sources, indexers/SFTP for its provider | blob provider |
| staging | sources, dest client, storage | download + SSD admission |
| offload | staging, transfer provider | verified move, checkpoint replay |
| seed | dest client, seed provider | re-add, verification, late seeds, healer |
| cleanup | sources + secured-content proof | janitor |
| control plane | core, action registry | Telegram, API |
| recovery | every module's reconcile hook | startup reconciliation |

Cannot be fully separated (kept as modules with explicit hooks): recovery
(force-adopts the whole pipeline), the re-add healer (needs staging + offload),
cleanup (proof depends on the chain), and source identity in single-box mode.

## 5. Shape-aware pipeline

- Tolerated transitions derive from the chain: `NEW → MOVING` when bytes are already
  staged, `DOWNLOADING → DONE` without transfer, `MOVING → DONE` without seed.
- The MOVING checkpoint's terminal stage becomes a callback (today it hard-codes
  `RE_ADDING`).
- Startup drains rows stranded in a now-disabled link to the chain's terminal state
  and releases their SSD reservations; `check-config` warns before the change.
- Ledger rebuild counts only stages that exist.
- Cleanup uses one "content secured" verifier selected by chain (remote-verified,
  path-verified, both), scoped to racing-category ownership.
- `external` links park on the manual-move timer, verify, then continue.
- **Single box**: the source listing excludes entries whose `save_path` is on the
  seed path; cleanup can then only delete the racing entry. One client instance
  serves both roles.

As landed in phase 4: links carry provider names. `external` acquisition
verifies an operator's copy at the seed path and never downloads; `local`
acquisition adopts the racing client's own complete entry (one box, no second
copy); `seed.existing` adopts an already-seeded library without adding an
entry (and disables cleanup, which needs an entry of ours to re-verify).
Single-box topology is derived from the client origins: seeded entries are
excluded from intake and never grouped for deletion. `local_move` moves staged
bytes to a mounted local directory/NAS with copy-verify-rename-then-delete (a
crash re-runs safely; a size conflict, escape or unmounted target fails with
both copies intact), and its destinations must be disjoint from staging.

As landed in phase 3: `chain.Chain` resolves the links from `[pipeline]` (set by
the alias layer from `[transfer]`/`[seed]` switches). Download and move completion
ask `chain.after_download()` / `chain.after_move()`, and the state table tolerates
the two derived terminal edges. Startup drains rows stranded in a disabled link
before the SSD ledger rebuild: verified `RE_ADDING` rows finish, an unmoved
`MOVING` row finishes, a partially moved one fails for the operator. Cleanup is
skipped (with one warning) while no seed path exists, because the janitor's
content-secured verifier follows the seed link.

## 6. Phases

| # | Deliverable | Exit criteria |
|---|---|---|
| 0 ✅ | `config-summary` + golden configs + shape inventory | old configs resolve identically; no behavior change |
| 1 ✅ | Config layer: module models, `enabled` tri-state, renames, legacy adapter, deprecations, exhaustive disposition registry | every old key accepted; one warning per retired key; `AppConfig` facade unchanged for callers |
| 2 ✅ | Composition root + capability Protocols + factories | split/full behaves byte-identically; consumers no longer touch raw sections |
| 3 ✅ | Chain engine: derived transitions, checkpoint resolver, stranded-row drain, ledger, cleanup verifier, providers for today's behavior (`rclone`, `path`, `auto`, `none`) | shape matrix green for split/full/move-only/download-only |
| 4 ✅ | New providers/shapes: `local` acquire, `local_move`, `external`, `seed.existing` | single-box, HDD/NAS, out-of-band and adopt-only shapes green |
| 5 | UX: `config init/enable/disable/explain`, check-config chain report, docs/examples rewritten around the single-box default | a new user can go from zero to running with one screen of config |
| 6 | Retirement: stop documenting duplicate precedence keys; adopt/seed-only; remove dead shims | docs match schema; adapters still accept old files |

Ordering constraints (do not reorder): composition root and the single-client factory
before ownership work; transition table and stranded-row drain before removing any
stage; single-box ownership before enabling cleanup; checkpoint resolver before
splitting offload/seed; module reconcile hooks before removing the seed link.

Deferred: executing `rsync` (own follow-up phase, same safety bar as rclone —
argv lists, no shell, verify-then-delete, deadline); `external` covers "I run rsync
myself" in the meantime.

## 7. Testing

- **Disposition registry**: every current field mapped to keep/rename/derive/deprecate/
  remove; a test fails if a field lacks an entry, so nothing is silently dropped.
- **Golden configs**: `config.example.toml`, the live shape (redacted), one per link
  state, and synthetics (`enabled=false` with real values, legacy tracker map,
  `[dest]` absent with `[seed]` present).
- Assertions: resolved chain + flattened values match today for old files; disabled
  links never activate; one deprecation warning per retired key; `check-config`
  output stable per corpus; export→reload round-trips.
- **Shape × provider matrix**: for each of the nine shapes, assert transitions,
  ledger release, stranded-row drain, cleanup verdict, and an external-copy wait.

## 8. Risks

| Risk | Mitigation |
|---|---|
| Old configs drift | golden tests + adapter before any behavior change |
| New transfer/seed providers mis-delete | verify-then-delete, no shell, deadline, path ownership |
| Rows stranded when a link is disabled | drain at startup + `check-config` warning |
| Recovery force-adopts the wrong shape | per-module reconcile hooks; adoption gated on enabled links |
| Cleanup deletes the wrong entry on one box | save_path exclusion + ownership-scoped delete tests |
| Config rename churn | renames land with the adapter and deprecation warnings in one phase |

## 9. Non-goals

Plugin API for third parties, hot config reload, multi-tenant operation, a web config
editor, and throughput tuning (still separate from this refactor).

## 10. Decision log

- Default deployment: **single box**; two boxes is a variant.
- Config: explicit `enabled` switches (not profiles, not include fragments), keys
  kept when disabled.
- Growth: flipping switches; `config enable/disable` helper.
- Single-box ownership: exclude by `save_path` on the seed path.
- Compatibility: every old config keeps loading.
- `seed` targets a path (fuse mount or plain dir), fuse is not a module.
- First-class now: `none`, `local_move`, `external`, `seed.path/existing`;
  `rsync` execution deferred.
- Phase 1 (landed): alias layer rewrites TOML new→internal before validation;
  the facade is untouched. Old and new spellings may coexist when they agree.
  `[cross_seed]` maps to `[acquire]` (strategy, not indexer config).
  `[seed].unsorted_path` defaults to `<path>/unsorted`. Disabling a link is
  refused until phase 3 can drain it. Deprecated: `general.max_active_downloads`,
  `general.max_concurrent_moves`, `rclone.reinject_delay_seconds` (warn once, still win).
- Phase 2 (landed): capability views are live, not snapshots, so a config
  swapped late (tests, pre-start edits) always answers. Services are built in
  one root and started by the coordinator as before; `load_config`/`cli`
  behaviour is untouched.
- Phase 4 (landed): `local_move` supports `files_from` and whole-tree moves;
  rclone `--include` patterns are refused (no caller uses them) until a pattern
  matcher lands. Its destinations must be absolute and disjoint from staging.
  Cleanup is skipped whenever the chain adds no seed entry (disabled or
  adopt-only) — a remote verifier is still a later phase.
