"""Shape regressions not covered by the golden files.

The golden fixtures pin full AppConfigs only. These two cases pin the
defensive path for partial config doubles and the legacy seed labels.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from racing_sync.config import AppConfig
from racing_sync.config_summary import _shape, summarize

_EXISTING_SEED = """
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

[acquire]
provider = "external"

[transfer]
provider = "external"

[seed]
provider = "existing"
path = "/mnt/fuse"
"""


def test_existing_seed_uses_the_existing_label(tmp_path: Path):
    path = tmp_path / "config.toml"
    path.write_text(_EXISTING_SEED, encoding="utf-8")

    shape = summarize(AppConfig.from_toml(path), path)["shape"]

    assert shape["acquire"] == ["external_copy"]
    assert shape["seed"]["provider"] == "existing"
    assert shape["seed"]["mount"] == "/mnt/fuse"


def test_partial_config_double_keeps_the_old_defaults():
    """A double without the optional sections still shapes as it always did."""
    cfg = SimpleNamespace(
        source=SimpleNamespace(host="http://box:8080"),
        dest=SimpleNamespace(host="http://box:8080"),
        ssd=SimpleNamespace(),
        rclone=SimpleNamespace(),
    )

    shape = _shape(cfg)

    assert shape["mode"] == "single"
    assert shape["observe"] == ["source", "dest"]
    assert shape["source"] == {
        "type": "",
        "host": "http://box:8080",
        "category": "",
        "min_age_seconds": 0,
    }
    assert shape["acquire"] == []
    assert shape["acquire_detail"]["prowlarr_enabled"] is False
    assert shape["acquire_detail"]["download_indexers"] == 0
    assert shape["transfer"] == {
        "provider": "rclone",
        "binary": "",
        "remote_default": "",
        "remote_unsorted": "",
        "max_concurrent_moves": 0,
        "move_timeout_seconds": 0.0,
    }
    assert shape["seed"]["provider"] == "none"
    assert shape["seed"]["mount"] == ""
    assert shape["cleanup"] == {"enabled": False, "dry_run": False}
    assert shape["control"] == {"telegram": False, "api": False, "logging_sink": False}
