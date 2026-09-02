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

Expected: `test_batcher.py`, `test_classifier.py`, `test_state.py`, `test_sftp.py` pass.

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
# Ctrl-C after a few seconds
sqlite3 /var/lib/racing-sync/state.db ".schema"
sqlite3 /var/lib/racing-sync/state.db "select count(*) from torrent_state"
```

Expected: empty table (no torrents yet), but `run_log` has 1+ entries from
the startup reconciler.

## 4. End-to-end with one movie

On VPS1 racing client, add a small movie (1–2 GB public). Watch category
`racing`.

Expected behaviour on VPS2 within ~5 minutes:

1. `state.db` row appears with `state=new`.
2. Cross-seed picker logs `picked public-prowlarr` (or `public-sftp`).
3. Transition `new -> queued -> downloading`.
4. After finish, log `rclone ok in Ns`.
5. Transition `downloading -> moving -> re_adding -> done`.
6. VPS2 qB has the movie on `fuse.mount`, status "seeding", `skip_check=true`.

## 5. End-to-end with a TV season

Add a 20 GB / 10-episode season torrent.

Expected:

1. Classifier returns `season`, 10 episodes.
2. Batcher splits into 4 batches (each ≤ 500 GiB cap; trivial for this test).
3. After each batch completion: rclone `--include` log, then `wiping local
   tree` log. The season folder on SSD disappears between batches.
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

Use a torrent that's only on a private tracker (Beyond-HD / Aither). Expect:

1. `prowlarr search on Beyond-HD for <name>` log.
2. SSD download from the prowlarr-fetched `.torrent`.
3. Re-add of the private torrent on fuse with `skip_check=true`.

## 9. Recovery when SSD is full

Force the SSD path to be 99% full. Expect:

1. New torrents land in `state=waiting_disk`.
2. Live status message shows them queued behind the cap.
3. When space frees up, processing resumes automatically.

## 10. Telegram

Enable `[telegram]` in config. Restart. Expect:

1. A pinned "racing-sync online" message at the top of the chat.
2. The same message gets edited every `status_update_interval` seconds.
3. ERROR-level log lines cause a separate message in the chat.

## 11. FastAPI

Enable `[api]`. From another host:

```bash
curl -H "X-API-Token: $TOKEN" http://vps2:8765/api/active
curl -X POST -H "X-API-Token: $TOKEN" http://vps2:8765/api/recover
```

Expect JSON responses.