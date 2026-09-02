# Architecture

## Layout

```
src/racing_sync/
  __main__.py         CLI
  config.py           Pydantic schema, cross-validates everything
  logging_setup.py    Rotating files + JSONL + ring buffer + optional HTTP sink
  state.py            SQLite state machine (State, ALLOWED, StateStore)
  classifier.py       movie / episode / season
  batcher.py          SSD-aware episode batching
  rclone_ops.py       rclone subprocess wrapper
  sftp_source.py      paramiko-based .torrent export
  prowlarr.py         Prowlarr client (indexers, search, download)
  watchdir.py         Watch-dir scanner with bencoded torrent parser
  recovery.py         Startup reconciler
  coordinator.py      Main async loop + per-torrent workers
  telegram_bot.py     Live status + log forwarding
  api.py              Optional FastAPI control plane
  clients/
    abstract.py       TorrentClient ABC + dataclasses
    http_base.py      aiohttp + nginx auth helper
    qbittorrent.py    qBittorrent WebUI v2
    deluge.py         Deluge JSON-RPC
```

## State machine

```
NEW ──┬─> QUERYING ──> WAITING_DISK ──> QUEUED ──> DOWNLOADING ──> MOVING ──> RE_ADDING ──> DONE
      │       │                            │              │            │
      └───────┴────────────────────────────┴──────────────┴────────────┴──> (any) ──> FAILED
                                                                                  │
                                                                                  └──> QUEUED (retry)
```

`DONE` and `FAILED` are terminal-ish: `FAILED → QUEUED` is allowed for manual retry.

The state lives in `state.db` (SQLite, WAL journal). Every state transition is
audited in the `run_log` table.

## Per-torrent workflow

1. **Discover** — VPS1 racing client lists torrents with category=`racing`.
   Insert/update row in `state.db` at `state=NEW`.

2. **Decide SSD source** (`pick_ssd_source_for_racing`):
   - Multiple racing-client torrents for the same content?
     Prefer public. Try:
       - `cross_seed.refetch_public_via_prowlarr` → prowlarr → Seedpool
       - SFTP fallback (Deluge) or qB `export_torrent` (qB)
   - Only private? Map each racing torrent's announce URL to a prowlarr indexer
     via `[prowlarr.tracker_map]` (Beyond-HD, Aither, AnimeBytes, …). Search.
     First hit → use it.
   - No luck anywhere → SFTP-export the racing torrent.

3. **Add to VPS2**:
   - `save_path = dest.save_path` (local SSD)
   - `paused = True`, `skip_check = False` (req #3: we *want* the hash check on SSD)
   - `category = "racing"`

4. **Classify** (`classifier.classify`):
   - Single file → `movie`
   - All/most files carry `S00E00` → `season`
   - Otherwise → `mixed`
   - Skip movie if total size > `ssd.skip_movie_larger_than_bytes` (req #7)

5. **Batch & download**:
   - For `season`/`mixed`: `make_batches(episodes, cap=ssd_max_inflight_bytes)`.
     The cap is `min(ssd.max_inflight_bytes, free - safety_margin)` so we always
     respect actual disk headroom.
   - Set file priorities: priority 1 for batch N, priority 0 for everything else.
   - Resume; poll until `progress >= 0.999`.
   - After completion of batch N: wipe the season folder (req #8), prepare
     batch N+1, repeat.

6. **Move to remote** (`rclone_ops.move_local_to_remote`):
   - `rclone move <local> <remote> --size-only --checkers=8 …`
   - For season batch moves, append `--include=<exact filename>` patterns for
     only that batch's episodes (req #8).
   - Movies + full season roots → `rclone.remote.default`
   - Individual episodes → `rclone.remote.unsorted`

7. **Re-inject** (`coordinator._do_re_add`):
   - Delete the SSD torrent.
   - For each racing-client torrent for the content:
     - SFTP-export `.torrent` bytes.
     - `add_torrent(save_path=fuse.mount, skip_check=True, paused=False)`.
   - Also re-add the cross-seed torrent (the one that ran on SSD) on the
     fuse mount — same call, same `skip_check=True`.

8. **Mark DONE**.

## Recovery (req #4)

On startup `reconcile()` walks VPS2's torrents and:

- `state=downloading` + missing on VPS2 → re-add (orchestrator's responsibility).
- `state=moving` + missing → assume rclone completed, jump to `RE_ADDING`.
- `state=done` + missing → re-add to fuse (data already on remote).

The state DB is the source of truth; VPS2 + filesystem are reality. The
reconciler bridges them.

## SSD cap

`ssd_max_inflight_bytes()` is called every state transition:

```
min(ssd.max_inflight_bytes, ssd_free - general.disk_safety_margin_bytes)
```

So even if the user misconfigures the cap above their actual disk size, the
batcher still respects real free space.

## Logging

- `racing-sync.log` — human, rotated daily.
- `racing-sync.jsonl` — structured, for grep/jq/vector.
- `RingBufferHandler` — last 300 events, drained by the Telegram bot for the
  "Recent" section of the live status message.
- `[logging_sink]` — optional HTTPS POST to a central collector.

## Telegram

The bot edits one message in the chat every `status_update_interval` seconds:

```
racing-sync — 14:23:01
SSD free: /srv/qbittorrent/data

In flight
• abc12345 The.Movie.2024.1080p.WEB.mkv
   state=downloading size=4500MB batch=0/1
• def67890 Show.Name.S01
   state=moving size=20000MB batch=2/4

Recent
14:22:30 INFO  rclone ok in 23s
14:22:55 WARN  disk free 30 GB, parking
```

Pinned on startup. Log forwarding is opt-in; default is to only post
ERROR/CRITICAL events to avoid floods.

## FastAPI control plane

`POST /api/recover`, `POST /api/retry/{hash}`, `GET /api/state`, etc. Useful
when the Telegram bot isn't enough. Auth via nginx-injected
`X-Authenticated-User` header or a static token.

## Cross-seed tracker map

`[prowlarr.tracker_map]` is a flat dict where keys are announce-URL substrings
and values are prowlarr indexer names. First substring match wins. Resolution
order:

1. Built-ins (`beyond_hd`, `aither`, `animebytes`).
2. `overrides` (insertion order).

This is what `prowlarr.resolve_indexer_for_announce(url)` uses internally.

## Operational notes

- Always run the destination qBittorrent as a separate user; the SSD save_path
  must be writable by that user.
- The fuse mount (`/mnt/remote/...`) should be **read-only** to qBittorrent
  if possible, but qB doesn't care: it only reads from `save_path` after the
  files are there.
- rclone `--size-only` is the default because `--checksum` over a fuse mount
  can be very slow. Switch to `--checksum` if your releases frequently change
  piece sizes mid-race.
- The state DB is append-only-safe; it can be inspected with the `sqlite3` CLI:
  `sqlite3 /var/lib/racing-sync/state.db "select state, count(*) from torrent_state group by state"`.