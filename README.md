# Racing Sync

Two-VPS torrent synchroniser for racing workflows.

- **VPS1** (source) — fast racing client (qBittorrent or Deluge) with autobrr.
- **VPS2** (destination) — long-term seed client (qBittorrent) with SSD cap + rclone offload to remote storage.

## Quickstart

```bash
pip install -e ".[api,test]"
cp config.example.toml config.toml
# edit config.toml
racing-sync --config config.toml run
```

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
    base.py           # TorrentClient ABC
    qbittorrent.py    # qBittorrent WebUI wrapper
    deluge.py         # Deluge JSON-RPC wrapper
  telegram_bot.py     # Live status + log forwarder
```