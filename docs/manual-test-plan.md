# Manual test plan

Run these before trusting the daemon with a real race torrent.

## 0. Install + config

```bash
pip install -e ".[api,test]"
cp config.example.toml config.toml
$EDITOR config.toml  # adjust hosts/credentials; keep remote paths
racing-sync check-config --config config.toml
```

## 1. Unit tests

```bash
pytest -q
```

Expected: full suite passes (`pytest -q`).

## 2. qBittorrent auth probe (no race torrent yet)

Start the daemon in foreground with logging enabled, then verify in another
shell:

```bash
curl -s http://127.0.0.1:8080/api/v2/auth/login \
    --data 'username=admin&password=changeme'
# -> "Ok." if creds are right
```

If you have nginx basic-auth in front, expect a 401/302 to `/auth` and a POST
to it in the daemon logs. The HTTP client handles this automatically.

## 3. State DB smoke

```bash
racing-sync run --config config.toml &
# Ctrl-C after a few seconds (use your configured general.state_db,
# default /var/lib/racing-sync/state.db)
sqlite3 /var/lib/racing-sync/state.db ".schema"
sqlite3 /var/lib/racing-sync/state.db "select count(*) from torrent_state"
```

Expected: empty table (no torrents yet), but `run_log` has 1+ entries from
the startup reconciler.

## 4. End-to-end with one movie

On VPS1 racing client, add a small movie (1–2 GB public). Watch category
`racing`.

Expected behaviour on VPS2 within ~5 minutes:

1. `state.db` row appears with `state=new` (DB values are lowercase; enum names are uppercase).
2. Cross-seed picker logs `picked public-prowlarr` (`cross_seed_source: prowlarr|sftp|self`).
3. Transition `new -> querying -> waiting_indexer|waiting_disk -> queued -> downloading`.
4. After finish, log `rclone ok in Ns`.
5. Transition `downloading -> moving -> re_adding -> done`.
6. VPS2 qB has the movie on `fuse.mount`, status "seeding", `skip_check=true`.

## 5. End-to-end with a TV season

Add a 20 GB / 10-episode season torrent.

Expected:

1. Classifier returns `season`, 10 episodes.
2. Batcher splits into batches that each fit the frozen per-row cap (see
   the `batched N episodes into M batches (cap=…)` log).
3. After each verified batch move the torrent entry is deleted *with files*
   and re-added fresh for the next batch (isolated batches — watch for
   `isolated batch: ready for batch N/M`). The SSD holds at most one batch
   at a time.
4. After all batches: VPS2 has the full season on `fuse.mount`.

## 6. Recovery

Mid-way through step 5, `kill -9` racing-sync. Restart it. Expect:

1. Reconciler logs `kept=N resumed=M …`.
2. The torrent resumes from its persisted state (e.g. `moving`).
3. No duplicate downloads on SSD; no duplicate torrent entries on VPS2.

## 7. Watch-dir

Drop a .torrent for a non-racing public release into
`/srv/qbittorrent/watch`. Expect:

1. `watch-dir picked up: <name>` log.
2. Prowlarr search attempted (if enabled).
3. SSD → rclone → fuse injection within a few minutes.

## 8. Prowlarr fallback

Use a torrent that's only on a private tracker (Beta / Alpha). Expect:

1. `prowlarr search on Beta for <name>` log.
2. SSD download from the prowlarr-fetched `.torrent`.
3. Re-add of the private torrent on fuse with `skip_check=true`.

## 9. Recovery when SSD is full

Force the SSD path to be 99% full. Expect:

1. New torrents land in `state=waiting_disk` (re-checked quietly, about
   once a minute — no per-tick worker spam).
2. Live status message shows them queued behind the cap.
3. When space frees up, processing resumes automatically.

## 9b. SSD budget over-commit

With `max_inflight_bytes` well below free space (e.g. 5 GiB on an empty
disk), start two torrents whose *combined* size exceeds the budget but
each fit alone. Expect:

1. The first goes `queued`; the second parks in `waiting_disk` even though
   free space alone would fit it (check the `ssd budget in use` log).
2. No `ENOSPC` mid-download; the second starts after the first releases
   its reservation (`moving → re_adding`).

## 9c. Fuse index lag

While a move is running (remote busy), watch a re-add land. Expect:

1. `accepted but not yet visible on fuse …; parking re-add` instead of a
   failure — the row retries and reaches `done` once the index catches up.
2. Moved files are never deleted or replaced by the retry.

## 9d. Manual fuse adoption

Manually move a racing release's files to the remote and add the same
torrent on VPS2 pointing at the fuse mount with no category/tags. Expect:

1. The `NEW` / `WAITING_INDEXER` row goes straight to `done` within one
   poll interval, with `already completed on VPS2 fuse mount (manual add,
   category-agnostic)` in the log — no Prowlarr query.
2. A `skip_check` ghost (complete but bytes missing at the fuse target)
   never adopts; the row keeps its normal Prowlarr/SSD flow.

## 10. Telegram

Enable `[telegram]` in config. Restart. Expect:

1. A pinned "racing-sync online" message at the top of the chat.
2. The same message gets edited every `status_update_interval` seconds.
3. A `FAILED` transition posts a detail card in the chat (raw log lines
   are never forwarded — app logging goes to local files only).

## 11. FastAPI

Enable `[api]`. From another host:

```bash
curl -H "X-API-Token: $TOKEN" http://vps2:8765/api/active
curl -X POST -H "X-API-Token: $TOKEN" http://vps2:8765/api/recover
```

Expect JSON responses.