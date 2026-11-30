"""Composition root: which concrete services one config resolves to.

Nothing here may perform I/O — construction only wires objects, and the
coordinator keeps owning startup order. These tests pin the class each
fixture produces, the disabled-feature None results and the frozen
`Composition` record.
"""

from __future__ import annotations

import time
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from racing_sync.capabilities import BlobSource, WatchIntake
from racing_sync.clients.deluge import DelugeClient
from racing_sync.clients.qbittorrent import QBittorrentClient
from racing_sync.composition import (
    build_blob_source,
    build_dest_client,
    build_indexer_service,
    build_source_client,
    build_watch_intake,
    compose,
)
from racing_sync.config import AppConfig
from racing_sync.prowlarr import ProwlarrClient
from racing_sync.sftp_source import SFTPExporter
from racing_sync.watchdir import WatchDirScanner

FIXTURES = Path(__file__).parent / "fixtures" / "config"


def _load(name: str) -> AppConfig:
    return AppConfig.from_toml(FIXTURES / f"{name}.toml")


def test_compose_minimal_builds_only_the_clients():
    cfg = _load("minimal")

    comp = compose(cfg)

    assert type(comp.source) is QBittorrentClient
    assert type(comp.dest) is QBittorrentClient
    assert comp.indexers is None
    assert comp.blobs is None
    assert comp.watch is None
    assert comp.runtime._cfg is cfg


def test_compose_split_builds_the_deluge_graph():
    comp = compose(_load("split"))

    assert type(comp.source) is DelugeClient
    assert type(comp.dest) is QBittorrentClient
    assert type(comp.indexers) is ProwlarrClient
    assert type(comp.blobs) is SFTPExporter
    assert type(comp.watch) is WatchDirScanner
    assert isinstance(comp.blobs, BlobSource)
    assert isinstance(comp.watch, WatchIntake)


def test_legacy_off_keeps_optional_modules_disabled():
    comp = compose(_load("legacy-off"))

    assert type(comp.source) is QBittorrentClient
    assert comp.indexers is None
    assert comp.blobs is None
    assert comp.watch is None


@pytest.mark.parametrize("section", ["watch", "watch_dir"])
def test_watch_intake_is_built_without_indexers(tmp_path: Path, section: str):
    text = (FIXTURES / "minimal.toml").read_text(encoding="utf-8")
    text += f'\n[{section}]\npath = "/srv/watch"\n'
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(text, encoding="utf-8")

    comp = compose(AppConfig.from_toml(cfg_file))

    assert comp.indexers is None
    assert type(comp.watch) is WatchDirScanner
    assert comp.watch._prowlarr is None


def test_factories_return_none_for_disabled_features():
    cfg = _load("legacy-off")

    assert build_indexer_service(cfg) is None
    assert build_blob_source(cfg) is None
    assert build_watch_intake(cfg, None) is None


def test_factories_build_enabled_features():
    cfg = _load("split")

    indexers = build_indexer_service(cfg)
    watch = build_watch_intake(cfg, indexers)

    assert type(indexers) is ProwlarrClient
    assert type(build_blob_source(cfg)) is SFTPExporter
    assert type(watch) is WatchDirScanner
    assert watch._prowlarr is indexers


@pytest.mark.parametrize(
    ("name", "source_type"), [("minimal", QBittorrentClient), ("split", DelugeClient)]
)
def test_source_factory_follows_the_source_type(name: str, source_type: type):
    cfg = _load(name)

    assert type(build_source_client(cfg)) is source_type
    assert type(build_dest_client(cfg)) is QBittorrentClient


def test_compose_constructs_everything_promptly():
    start = time.monotonic()

    comp = compose(_load("split"))

    assert time.monotonic() - start < 5.0
    assert comp.source is not None
    assert comp.dest is not None
    assert comp.indexers is not None
    assert comp.blobs is not None
    assert comp.watch is not None


def test_composition_is_frozen():
    comp = compose(_load("minimal"))

    with pytest.raises(FrozenInstanceError):
        comp.watch = None
