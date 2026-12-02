"""The shipped example is the canonical spelling and still resolves the same.

`config.example.toml` is the full reference operators copy from, so it must
show the capability-chain names; the alias layer keeps every older file
loading, which the old-spelling fixtures pin separately.
"""

from __future__ import annotations

import logging
from pathlib import Path

from racing_sync.config import AppConfig

EXAMPLE = Path(__file__).parent.parent / "config.example.toml"

CANONICAL = (
    "[client]",
    "[storage]",
    "[remote]",
    "[transfer]",
    "[seed]",
    "[indexers]",
    "[acquire]",
    "[watch]",
)

RETIRED = (
    "[source]",
    "[source.nginx]",
    "[source.deluge_sftp]",
    "[ssd]",
    "[rclone]",
    "[rclone.remote]",
    "[rclone.fuse]",
    "[prowlarr]",
    "[prowlarr.download_indexers]",
    "[prowlarr.tracker_map]",
    "[cross_seed]",
    "[watch_dir]",
)


def test_example_uses_the_canonical_spelling():
    lines = {line.strip() for line in EXAMPLE.read_text(encoding="utf-8").splitlines()}

    for header in CANONICAL:
        assert header in lines, header
    for header in RETIRED:
        assert header not in lines, header


def test_example_carries_the_shared_daemon_defaults():
    """The shipped example uses the relaxed cadence for a shared Deluge daemon."""
    cfg = AppConfig.from_toml(EXAMPLE)

    assert cfg.general.source_poll_interval == 120
    assert cfg.cleanup.delete_stagger_seconds == 60


def test_model_defaults_match_the_shared_daemon_values():
    from racing_sync.config import CleanupConfig, GeneralConfig

    general = GeneralConfig()
    assert general.source_poll_interval == 120
    assert general.dest_poll_interval == 30
    assert CleanupConfig().delete_stagger_seconds == 60


def test_example_documents_no_retired_keys(caplog):
    with caplog.at_level(logging.WARNING):
        AppConfig.from_toml(EXAMPLE)
    assert not [r for r in caplog.records if "deprecated" in r.getMessage()]
