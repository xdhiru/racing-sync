# Racing Sync

Two-VPS torrent synchroniser for racing workflows.

- **VPS1** (source) — fast racing client (qBittorrent or Deluge) with autobrr.
- **VPS2** (destination) — long-term seed client (qBittorrent) with SSD cap + rclone offload to remote storage.

If you race torrents on a fast seedbox but don't want to pay for huge disks
there, this is for you: keep racing on VPS1, let racing-sync copy what
matters to VPS2, offload it to remote storage with rclone, and keep seeding
from there long-term — automatically.

## What problem does it solve?

Racing needs speed; long-term seeding needs cheap space. One box rarely
gives you both:

- VPS1 is fast with a small disk — great for winning the race, terrible for
  keeping 100s of torrents around.
- VPS2 has a small local SSD plus effectively unlimited remote storage
  (via an rclone mount) — great for seeding forever, terrible for racing.

Manually copying `.torrent` files between clients, watching SSD free space,
moving finished files, and re-adding everything to seed is tedious and
error-prone. racing-sync runs that loop for you, 24/7, with crash recovery.

## How it works

1. **Spot it:** a new torrent appears on VPS1.
2. **Find the best copy:** prefer a public copy when one exists, otherwise
   look the release up on your private indexers via Prowlarr, otherwise
   export it directly from VPS1.
3. **Stage it on SSD:** download it to the VPS2 SSD (movies in one go, big
   season packs in small batches so a 40 GB SSD can handle a 100 GB season).
4. **Offload it:** `rclone move` verified files to your remote
   (`remote:qbittorrent/`, single episodes go to `.../unsorted/`).
5. **Keep seeding:** re-add the torrent on VPS2 pointing at the fuse mount
   with instant-check, so you seed long-term without using SSD space.

A few guarantees underneath: one SSD budget shared by all downloads (no
joint overfill); batches move only after verification, so only complete
files reach the remote; every fuse re-add is confirmed visible (lagging
mounts park and retry); anything already on fuse skips the download and
goes straight to seeding; and all state lives in SQLite, so restarts resume
mid-pipeline. Public torrents land paused by default (see
`cross_seed.pause_public_torrents_on_fuse`). Full design in
`docs/architecture.md`.

## Quickstart

Requires Python 3.11+, plus system `rclone` binary (absolute path, probed by
`check-config`) and `sqlite3` CLI for manual DB inspection. Install into a
virtualenv — system-wide `pip install`
fails on modern distros (`externally-managed-environment`, don't bypass it
with `--break-system-packages`):

```bash
git clone https://github.com/xdhiru/racing-sync.git
cd racing-sync
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[api]"   # use -e "." without the API, -e ".[test]" for tests
```

Run straight from source — no install step beyond that, so `git pull` +
restart is the upgrade:

```bash
cp config.example.toml config.toml
# edit config.toml
python3 run.py check-config --config config.toml
python3 run.py run --config config.toml
# Ctrl+C stops gracefully
```

There is no config reload: restart the daemon after editing `config.toml`.

Help for every command:

```bash
python3 run.py --help
python3 run.py run --help
```

### Fresh start (`--reset`)

Wipes the bookkeeping (`state.db` + logs) and starts over — use it instead
of hand-deleting files. Torrents on the clients/SSD are picked back up by
recovery and resume where they left off, so torrent data is never deleted:

```bash
python3 run.py run --config config.toml --reset
# Full wipe for mid-testing (also drops dest entries with files,
# SSD data and cached .torrent blobs; fuse/remote copies stay untouched):
python3 run.py run --config config.toml --full --yes
```

### Abandon a torrent (`forget`)

The off-switch for a torrent the pipeline won't drop on its own. Removes
the DB row, the destination client entries, local SSD data and cached
`.torrent` blobs (fuse/remote copies are never touched). Dry-run by
default; `--apply` deletes:

```bash
python3 run.py forget --config config.toml <infohash|name>
python3 run.py forget --config config.toml <infohash|name> --apply
python3 run.py forget --config config.toml <infohash|name> --apply --keep-files
python3 run.py forget --config config.toml <infohash|name> --apply --ignore
```

`<infohash|name>` is a 40-char infohash or a unique name fragment
(ambiguous names show candidates instead of guessing). Forgetting an SSD
download also forgets the watch-dir rows waiting on it; `--ignore` blocks
it from ever coming back while listed on VPS1 (undo with `unignore`,
`--all` to undo every entry at once — both also lift the forget block so
re-dropped files reprocess immediately).
Same thing via API: `POST /api/forget/{hash}?ignore=true&delete_files=false`
(full 40-char hash required, always applies; `delete_files=false` = `--keep-files`).

From Telegram, tap the `/cancel_3` line under a file group (groups are
numbered; same-file copies share one heading), then pick which copy
(tracker buttons, 3 across — or `All`) and answer the keep question that
appears under the list: `Keep files` untracks + ignores with data left in
place (same as CLI `--keep-files`), `Delete files` wipes it. The number is
resolved once at tap into a locked copy list, so renumbering mid-flow can't
misroute — the question names the group, and every picker has a Cancel
button. Nothing is deleted without that explicit choice. `Fetch:`/`Prefer:`
lines appear only while a copy qualifies, and also open tracker buttons.
Torrents still
waiting on the indexer show `Fetch original: /fetch_<id>` instead: use
the VPS1 original right away rather than waiting out Prowlarr retries
(counts toward private-tracker ratio). Rows holding for a preferred copy
show `Prefer now: /prefer_<id>`: start their SSD download
immediately; waiting siblings then seed from fuse/remote after. The same fallback can trigger
automatically at the deadline with
`cross_seed.fallback_to_racing_torrent_on_prowlarr_timeout` (default off).

`check-config` also warns on unknown config keys (typo catcher).

### Installed alternative

Same virtualenv as above, then use the installed entrypoint instead of
`run.py`:

```bash
pip install -e ".[api,test]"
cp config.example.toml config.toml
# edit config.toml
racing-sync run --config config.toml
```

Editable installs also track `git pull` (restart only). Avoid plain
`pip install .` — it freezes a copy of the code and ignores later pulls.

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