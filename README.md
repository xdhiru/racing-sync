# Racing Sync

Win the race on one client, keep seeding from another — automatically.

- **VPS1 / source** — fast racing client (qBittorrent or Deluge, usually
  driven by autobrr).
- **VPS2 / dest** — long-term seed client (qBittorrent) with SSD staging
  and rclone offload to remote storage.

racing-sync watches the racing client, stages completed content on the
destination SSD, offloads it to remote storage, and re-adds the torrents on
the destination pointing at a fuse mount. Racing space can then be cleaned
up on its own schedule while the long-term copy keeps seeding.

## What it can do for you

The pipeline is a chain — observe → acquire → stage → transfer → seed — and
each link is pluggable, so the same daemon fits different setups:

| Shape | What it does | Main config |
| --- | --- | --- |
| **Two boxes** (default) | Downloads via a download-target indexer or VPS1's own export, rclone-moves to remote, seeds from fuse | `[transfer] provider = "rclone"`, `[seed] provider = "path"` |
| **One box** | Race and seed from one client; the racing copy is the staged copy, no second download | same host in `[client]` and `[dest]`, `[acquire] provider = "local"` |
| **NAS / HDD** | Verified move into a mounted directory instead of rclone | `[transfer] provider = "local_move"` + `path` / `unsorted` |
| **Adopt-only** | You place the bytes; the daemon verifies them at the seed path and never downloads | `[acquire] provider = "external"`, `[transfer] provider = "none"`, `[seed] provider = "existing"` |
| **Short chain** | Stop after the download or the move | `[transfer] provider = "none"` / `[seed] provider = "none"` |

Independent of the shape, optional pieces flip on with `config enable`:
Prowlarr lookup for a private download target, watch-directory intake, the
Telegram control plane, the HTTP API, and the VPS1 cleanup janitor.

## How it works

1. **Spot it:** a new torrent appears on the racing client (polling or a
   watch directory).
2. **Find the best copy:** prefer a public copy when one exists, otherwise
   look the release up on private indexers via Prowlarr, otherwise
   export it directly from VPS1.
3. **Stage it on SSD:** download it to the destination SSD (big season
   packs in small batches, so a 40 GB SSD can handle a 100 GB season).
4. **Offload it:** `rclone move` verified files to the remote
   (movies/seasons to `remote:qbittorrent/`, single episodes to
   `.../unsorted/`).
5. **Keep seeding:** re-add the torrent on VPS2 pointing at the fuse mount,
   so you seed long-term without using SSD space.

Only verified-complete files reach the remote; every fuse re-add is
confirmed visible before marking done; anything already on fuse skips
straight to seeding. State lives in SQLite, so restarts resume
mid-pipeline. Public torrents land paused by default. When a link is
missing the chain simply ends there — a finished download or move becomes
`DONE`. Full design in `docs/architecture.md`.

## Quickstart

Requires Python 3.11+. Rclone shapes also need the system `rclone` binary
(absolute path, probed by `check-config`); the `sqlite3` CLI is handy for
manual DB inspection. Install into a virtualenv — system-wide `pip install`
fails on modern distros (`externally-managed-environment`, don't bypass it
with `--break-system-packages`):

```bash
git clone https://github.com/xdhiru/racing-sync.git
cd racing-sync
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

`requirements.txt` is the primary environment (runtime + API deps, kept in
sync with `pyproject.toml`). Contributors who want the `racing-sync`
console script instead can `pip install -e ".[api]"` (`-e "."` without the
API, `-e ".[test]"` for tests) — same packages, installed editable.

Run straight from source — no install step beyond that, so `git pull` +
restart is the upgrade (re-run `pip install -r requirements.txt` only when
dependencies change).

### 1. Write and shape the config

```bash
python3 run.py config init --config config.toml
```

The starter is a one-screen single-box config. For the usual two-box setup,
point `[dest]` at VPS2, then fill the paths in `[storage]` (SSD staging),
`[remote]` (rclone destinations) and `[seed]` (fuse mount or plain seed
directory). For NAS/HDD or adopt-only, set the providers from the table
above — `config.example.toml` documents every key and provider.

### 2. Check, then run

```bash
python3 run.py check-config --config config.toml  # chain report + pre-flight
python3 run.py run --config config.toml
# Ctrl+C stops gracefully
```

`check-config` prints the resolved chain and warns on unknown config keys
(typo catcher) and on legal-but-foggy setups: an unmounted fuse path, a
watch glob that cannot match a `.torrent`, relative paths, a cap above the
disk, or a shared remote path.

There is no config reload: restart the daemon after editing `config.toml`.
`config.example.toml` remains the full reference for every key.

Optional features are one flip away: `config enable indexers` (or `watch`,
`telegram`, `api`, `cleanup`, `logging_sink`) appends its documented block,
`config disable <feature>` turns it off and keeps the keys, and
`config explain <feature>` prints the short version.

`python3 run.py config summary --config config.toml` prints what the file
resolves to — single-box or split topology, which capabilities are active,
how many keys are explicit vs defaulted, and a digest. Secrets are redacted,
so the output is safe to paste when asking for help. Add `--json` to diff.

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
python3 run.py run --config config.toml --reset --yes
# Full wipe for mid-testing (also drops dest entries with files,
# SSD data and cached .torrent blobs; fuse/remote copies stay untouched):
python3 run.py run --config config.toml --full --yes
```

### Abandon a torrent (`forget`)

The off-switch for a torrent the pipeline won't drop on its own. Removes
the DB row, destination client entries, local SSD data and cached
`.torrent` blobs (fuse/remote copies are never touched). Dry-run by
default; `--apply` deletes:

```bash
python3 run.py forget --config config.toml <infohash|name>
python3 run.py forget --config config.toml <infohash|name> --apply
python3 run.py forget --config config.toml <infohash|name> --apply --keep-files
python3 run.py forget --config config.toml <infohash|name> --apply --ignore
```

`<infohash|name>` is a 40-char infohash or a unique name fragment.
`--ignore` blocks it from ever coming back while listed on VPS1 (undo with
`unignore`, `--all` to undo every entry). Same thing via API:
`POST /api/forget/{hash}`. Full API surface in `docs/architecture.md`.

### Telegram

The bot is the main control plane: an Active Tasks card, one detail card per
torrent, and buttons for everything destructive. Typing `/` shows every
command with a description — the full list is in
`docs/telegram-commands.md`. The short version:

```
/cancel 3      cancel one release (no argument: cancel by lane)
/cancel_all    cancel every group whose title matches
/start 3       start now (no Prowlarr wait, no grace wait)
/hold 3        hold it;  /unhold <hash> resumes
/inject <hash> manual fuse seeding after you moved the bytes
/active /stats /tick /find <text> /files 3   dashboard + read-only tools
/pause /resume                               intake pause/resume
```

Targets accept a space (`/cancel 3`) or an underscore (`/cancel_3`).
Nothing is deleted without an explicit choice; fuse/remote copies are never
touched.

## Layout

```
src/racing_sync/   # coordinator*.py = the pipeline; state.py = SQLite state
                   # machine; content_keys/fuse_gate/path_ownership = the
                   # single rules; telegram_*.py = control plane; api.py =
                   # optional HTTP API; clients/ = qBittorrent/Deluge
```

Full module map in `docs/architecture.md`.