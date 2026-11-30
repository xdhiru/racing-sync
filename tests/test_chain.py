"""The chain: which links run and where a shortened pipeline terminates.

Split/full is today's behavior and must be untouched. Download-only
(no transfer/seed) and move-only (no seed) terminate earlier, and the
shortcuts are derived from the same chain the engine uses.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from racing_sync.chain import Chain
from racing_sync.config import AppConfig
from racing_sync.runtime import Runtime
from racing_sync.state import State

FIXTURES = Path(__file__).parent / "fixtures" / "config"

_FULL = "sha256:181342c45a9d9087"

_DOWNLOAD_ONLY = """
[general]

[client]
type = "qbittorrent"
host = "http://127.0.0.1:8080"

[dest]
host = "http://127.0.0.1:8080"

[storage]
path = "/downloads"
save_path = "/downloads"
max_inflight_bytes = 1000000000
skip_movie_larger_than_bytes = 1000000000

[transfer]
enabled = false

[seed]
enabled = false
"""

_MOVE_ONLY = _DOWNLOAD_ONLY.replace(
    """[transfer]
enabled = false

[seed]
enabled = false
""",
    """[transfer]
enabled = true

[remote]
path = "remote:movies/"
unsorted = "remote:unsorted/"

[seed]
enabled = false
""",
)


def _load(tmp_path: Path, text: str) -> AppConfig:
    path = tmp_path / "config.toml"
    path.write_text(text, encoding="utf-8")
    return AppConfig.from_toml(path)


def _load_fixture(name: str) -> AppConfig:
    return AppConfig.from_toml(FIXTURES / f"{name}.toml")


@pytest.mark.parametrize("name", ["minimal", "single-fuse"])
def test_full_chain_is_todays_pipeline(name: str):
    chain = Chain.from_config(_load_fixture(name))

    assert chain.observe == ("source", "dest")
    assert (chain.acquire, chain.stage, chain.transfer, chain.seed) == (
        "auto",
        True,
        "rclone",
        "path",
    )
    assert chain.downloads and chain.moves and chain.seeds
    assert chain.after_download() is State.MOVING
    assert chain.after_move() is State.RE_ADDING
    assert chain.shortcuts() == frozenset()


def test_split_chain_observes_the_watch_dir():
    chain = Chain.from_config(_load_fixture("split"))

    assert chain.observe == ("source", "dest", "watch")
    assert chain.cleanup is True


def test_download_only_chain_terminates_at_done(tmp_path: Path):
    cfg = _load(tmp_path, _DOWNLOAD_ONLY)
    chain = Chain.from_config(cfg)

    assert (chain.transfer, chain.seed) == ("none", "none")
    assert not chain.moves and not chain.seeds
    assert chain.after_download() is State.DONE
    assert chain.shortcuts() == frozenset(
        {(State.DOWNLOADING, State.DONE), (State.MOVING, State.DONE)}
    )
    assert cfg.rclone.remote is None
    assert cfg.rclone.fuse is None


def test_move_only_chain_ends_after_the_transfer(tmp_path: Path):
    cfg = _load(tmp_path, _MOVE_ONLY)
    chain = Chain.from_config(cfg)

    assert (chain.transfer, chain.seed) == ("rclone", "none")
    assert chain.after_download() is State.MOVING
    assert chain.after_move() is State.DONE
    assert chain.shortcuts() == frozenset({(State.MOVING, State.DONE)})
    assert cfg.rclone.fuse is None


def test_seed_requires_transfer(tmp_path: Path):
    text = _DOWNLOAD_ONLY.replace("[seed]\nenabled = false\n", '[seed]\npath = "/mnt/fuse"\n')
    with pytest.raises(ValueError, match="transfer"):
        _load(tmp_path, text)


def test_disabled_links_keep_todays_digest(tmp_path: Path):
    """The full fixture's summary digest is unchanged by the new flags."""
    from racing_sync.config_summary import summarize

    path = FIXTURES / "minimal.toml"
    assert summarize(AppConfig.from_toml(path), path)["digest"] == _FULL


def test_runtime_exposes_the_chain_and_flags(tmp_path: Path):
    rt = Runtime(_load(tmp_path, _MOVE_ONLY))

    assert rt.chain.transfer == "rclone"
    assert rt.chain.seed == "none"
    assert rt.transfer.enabled is True
    assert rt.seed.enabled is False
    assert rt.acquire.enabled is True
    assert rt.seed.mount is None
    assert rt.seed.mounts() == []
    assert rt.transfer.remote_for(episode=True) == "remote:unsorted/"


def test_providers_report_the_chain():
    assert Chain.from_config(_load_fixture("minimal")).providers() == {
        "observe": "source, dest",
        "acquire": "auto",
        "stage": "ssd",
        "transfer": "rclone",
        "seed": "path",
        "cleanup": "off",
    }
