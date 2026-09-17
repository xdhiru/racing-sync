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

Validate a config without starting anything:

```bash
python3 run.py check-config --config config.toml
```

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
  coordinator.py      # Main async loop
  recovery.py         # Reconciler (req #4)
  watchdir.py         # Manual torrent drop scanner
  api.py              # Optional FastAPI control plane
  clients/
    abstract.py       # TorrentClient ABC + dataclasses
    http_base.py      # HTTP client base with auth
    qbittorrent.py    # qBittorrent WebUI wrapper
    deluge.py         # Deluge JSON-RPC wrapper
  telegram_bot.py     # Live status + log forwarder
```