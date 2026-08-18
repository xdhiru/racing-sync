# Racing Sync

Two-VPS torrent synchroniser for racing workflows.

- **VPS1** (source) — fast racing client (qBittorrent or Deluge) with autobrr.
- **VPS2** (destination) — long-term seed client (qBittorrent) with SSD cap + rclone offload to remote storage.

## Quickstart

Run straight from source — no install step, so `git pull` + restart is the
upgrade (third-party deps like `aiohttp`/`pydantic` must be in the env):

```bash
cp config.example.toml config.toml
# edit config.toml
python3 run.py run --config config.toml
# Ctrl+C stops gracefully
```

Help for every command:

```bash
python3 run.py --help
python3 run.py run --help
```

### Fresh start (`--reset`)

Deletes `state.db` (+WAL/SHM sidecars) and clears the log directory from the
loaded config, then starts normally. Use it instead of hand-deleting files:

```bash
python3 run.py run --config config.toml --reset
```

Note this clears **bookkeeping only**. Torrents sitting on the clients/SSD
are re-adopted by recovery on startup and resume (complete ones continue to
the rclone move, partial SSD ones resume downloading) — a startup warning
names how many rows were rebuilt. `--reset` never deletes torrent data.

Mid-testing clean slate (`--reset --full`): also drops every `racing`
client entry on dest **with files**, wipes SSD data (children only, never
the SSD root itself) and the cached `.torrent` blobs in
`watch_cross_seeds/`:

```bash
python3 run.py run --config config.toml --reset --full
```

Fuse/remote copies are never touched. Refuses unsafe SSD roots (symlink,
fuse mount, filesystem root, project checkout) instead of wiping.

### Abandon a torrent (`forget`)

Recovery gives the pipeline no way to *stop* wanting a torrent, so `forget`
is the off-switch: it drops the DB row, deletes the matching destination
client entries, removes the torrent's local SSD data **and** its cached
`watch_cross_seeds/<hash>/` blobs (fuse/remote copies are never touched).
Dry-run by default; `--apply` deletes:

```bash
python3 run.py forget --config config.toml <infohash|name>
python3 run.py forget --config config.toml <infohash|name> --apply
python3 run.py forget --config config.toml <infohash|name> --apply --keep-files
python3 run.py forget --config config.toml <infohash|name> --apply --ignore
```

`<infohash|name>` is a 40-char infohash (any known hash) or a unique name
substring — ambiguous names abort with the candidate list instead of
guessing. `--ignore` also records the release as cancelled so discovery,
recovery, re-injection and late-seed never pick it up again while it stays
on VPS1; lift with:

```bash
python3 run.py unignore --config config.toml --list
python3 run.py unignore --config config.toml <infohash|name>
```

Same operation is available at `POST /api/forget/{hash}?ignore=true` when
the control API is enabled.

From Telegram you don't need the CLI at all: the Active Tasks message
shows one `Cancel: /cancel_<hash>` line per torrent (10-char short hash;
full 40-char hashes also work). Copy-paste it into the chat and the bot
forgets + ignores it immediately (row, dest entries, SSD data, blob
cache) — no confirmation, the sent message is final. Unknown or
ambiguous prefixes get a reply telling you what to send instead.

Validate a config (schema + environment: paths, rclone binary) without
starting anything:

```bash
python3 run.py check-config --config config.toml
```

Unknown config keys are reported as warnings (they would otherwise be
silently ignored) — useful for catching typos like `max_active_download`.

## How it works (short version)

- **SSD budget, not just a cap.** `ssd.max_inflight_bytes` is a *global*
  budget shared by every concurrent download. Each torrent reserves its
  footprint before admission (`WAITING_DISK` parks the rest); batch
  footprints refine after classification; the ledger rebuilds from the DB
  after an abrupt stop. Two 32 GB + 16 GB arrivals never jointly exceed a
  40 GB budget, even if each fits free space alone.
- **Isolated batches.** Multi-file torrents stream through the SSD one
  batch at a time; after each verified move the torrent entry is deleted
  *with files* and re-added fresh for the next batch. Shared piece-boundary
  partials can never leak into the next batch, so only complete files reach
  the remote (at the cost of re-downloading boundary pieces).
- **Verified fuse injection.** Every fuse re-add is confirmed visible at
  the target mount before the row advances. The fuse index can lag while
  rclone is busy, so an accepted-but-invisible entry parks and retries —
  never fails, never touches moved files.
- **Manual fuse adoption.** Same infohash already seeding from fuse on VPS2
  (any category) with verified bytes fast-tracks `NEW/QUERYING/
  WAITING_INDEXER/WAITING_DISK` straight to `DONE` — no Prowlarr query, no
  SSD download. Workers check before querying; each tick also runs one
  batched hash lookup so parked rows are picked up within one poll interval.
  Ghosts (complete but bytes missing) never adopt.
- **Quiet waits.** `WAITING_DISK` rows re-check at most once a minute, and
  the log handlers survive a full disk instead of traceback-storming it.

See `docs/architecture.md` for the full design.

### Installed alternative

```bash
pip install -e ".[api,test]"
cp config.example.toml config.toml
# edit config.toml
racing-sync --config config.toml run
```

Editable installs also track `git pull` (restart only). Avoid non-editable
`pip install .` — it snapshots `src/` into site-packages and keeps running
stale code after a pull.

See `docs/architecture.md` for the full design.

## Layout

```
src/racing_sync/
  __main__.py         # CLI entrypoint
  config.py           # Pydantic config schema
  logging_setup.py    # Logging: file + sink + ring buffer
  prowlarr.py         # Prowlarr client
  classifier.py       # movie / episode / season
  batcher.py          # SSD-aware episode batching
  rclone_ops.py       # rclone subprocess wrapper
  sftp_source.py      # SSH / SFTP torrent export
  state.py            # SQLite-backed state machine
  coordinator.py      # Main async loop (tick, dispatch, transitions)
  coordinator_ssd.py  # SSD batch caps + global reservation ledger
  coordinator_picker.py # Cross-seed SSD-source picker (req #1/#2)
  coordinator_cleanup.py # VPS1 cleanup janitor
  coordinator_paths.py # Untrusted torrent-relative path guard
  coordinator_errors.py # Retryable WebUI / batch-move error contract
  coordinator_content.py # Stateless helpers (normalize, grace, notify filter)
  recovery.py         # Reconciler (req #4)
  forget.py           # Abandon a torrent (row + client entries + SSD data)
  watchdir.py         # Manual torrent drop scanner
  api.py              # Optional FastAPI control plane
  clients/
    abstract.py       # TorrentClient ABC + dataclasses
    http_base.py      # HTTP client base with auth
    qbittorrent.py    # qBittorrent WebUI wrapper
    deluge.py         # Deluge JSON-RPC wrapper
  telegram_bot.py     # Live status cards + active-tasks list
```