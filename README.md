# Racing Sync

Two-VPS torrent synchroniser for racing workflows.

- **VPS1** (source) — fast racing client (qBittorrent or Deluge) with autobrr.
- **VPS2** (destination) — long-term seed client (qBittorrent) with SSD cap + rclone offload to remote storage.

Race on VPS1, seed forever from VPS2: racing-sync copies what matters
to the VPS2 SSD, offloads it to remote storage with rclone, and keeps
seeding from a fuse mount — automatically, with crash recovery.

## How it works

1. **Spot it:** a new torrent appears on VPS1.
2. **Find the best copy:** prefer a public copy when one exists, otherwise
   look the release up on private indexers via Prowlarr, otherwise
   export it directly from VPS1.
3. **Stage it on SSD:** download it to the VPS2 SSD (big season packs in
   small batches, so a 40 GB SSD can handle a 100 GB season).
4. **Offload it:** `rclone move` verified files to the remote
   (movies/seasons to `remote:qbittorrent/`, single episodes to
   `.../unsorted/`).
5. **Keep seeding:** re-add the torrent on VPS2 pointing at the fuse mount,
   so you seed long-term without using SSD space.

Only verified-complete files reach the remote; every fuse re-add is
confirmed visible before marking done; anything already on fuse skips
straight to seeding. State lives in SQLite, so restarts resume
mid-pipeline. Public torrents land paused by default. Full design in
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

One message per torrent (detail card, edited in place) plus one
active-tasks list. Tap `/act_<n>` under a group for its action sheet:
Cancel, Start now, Inject, Skip/Resume. Cancelling asks two questions —
remember the release (`Ignore` blocks it from coming back, `Just forget`
leaves it re-addable) and keep or delete SSD data. `/cancel_match <text>`
cancels every group whose title contains `<text>` at once; `/now_<id>`
starts a waiting row immediately. Nothing is deleted without an explicit
choice; fuse/remote copies are never touched.

`check-config` also warns on unknown config keys (typo catcher).

## Layout

```
src/racing_sync/   # coordinator.py = main loop; classifier/batcher =
                   # what/how to download; rclone_ops = offload; state.py =
                   # SQLite state machine; telegram_bot.py = status cards;
                   # api.py = control plane; clients/ = qBittorrent/Deluge
```

Full module map in `docs/architecture.md`.