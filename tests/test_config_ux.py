"""Config UX: starter file, toggles, explanations and the check-config report."""

from __future__ import annotations

from pathlib import Path

import pytest

from racing_sync.config import AppConfig
from racing_sync.config_disposition import FEATURE_SECTIONS
from racing_sync.config_ux import (
    FEATURES,
    ConfigUxError,
    check_report,
    explain,
    init_config,
    set_enabled,
)
from racing_sync.state import State, StateStore, TorrentState

FIXTURES = Path(__file__).parent / "fixtures" / "config"


def _starter(tmp_path: Path) -> Path:
    path = tmp_path / "config.toml"
    init_config(path)
    return path


def test_features_cover_the_section_registry_exactly():
    assert tuple(FEATURES) == FEATURE_SECTIONS
    for name, spec in FEATURES.items():
        assert spec.section == name
        assert spec.block.strip().startswith(f"[{name}]")
        assert spec.title.strip() and spec.summary.strip()
        assert name in explain(), name


@pytest.mark.parametrize("name", FEATURE_SECTIONS)
def test_every_registry_feature_can_be_toggled(tmp_path: Path, name: str):
    path = _starter(tmp_path)

    changed, missing = set_enabled(path, name, True)
    assert changed
    assert set(missing) <= set(FEATURES[name].required)
    assert f"[{name}]\nenabled = true" in path.read_text(encoding="utf-8")

    changed, _missing = set_enabled(path, name, False)
    assert changed
    assert f"[{name}]\nenabled = false" in path.read_text(encoding="utf-8")


def test_init_writes_a_loadable_single_box_starter(tmp_path):
    path = _starter(tmp_path)
    cfg = AppConfig.from_toml(path)

    assert cfg.pipeline.acquire == "auto"
    assert cfg.pipeline.transfer == "rclone"
    assert cfg.pipeline.seed == "path"
    assert cfg.watch_dir is None
    assert cfg.telegram.enabled is False
    assert cfg.cleanup.enabled is False
    text = path.read_text(encoding="utf-8")
    assert "[indexers]\nenabled = false" in text
    assert "[seed]" in text and 'provider = "path"' in text


def test_init_refuses_to_clobber(tmp_path):
    path = _starter(tmp_path)
    with pytest.raises(ConfigUxError, match="already exists"):
        init_config(path)
    init_config(path, force=True)


def test_enable_flips_the_existing_stub_and_keeps_comments(tmp_path):
    path = _starter(tmp_path)
    before = path.read_text(encoding="utf-8")

    changed, missing = set_enabled(path, "telegram", True)

    assert changed and missing == ["bot_token", "chat_id"]
    after = path.read_text(encoding="utf-8")
    assert 'provider = "path"' in after  # unrelated content kept
    assert "enabled = true" in after
    assert before.count("\n") == after.count("\n")


def test_enable_appends_the_documented_block(tmp_path):
    path = _starter(tmp_path)
    text = path.read_text(encoding="utf-8").replace("[watch]\nenabled = false\n", "")
    path.write_text(text, encoding="utf-8")

    changed, missing = set_enabled(path, "watch", True)

    assert changed
    assert missing == ["path"]
    body = path.read_text(encoding="utf-8")
    assert "[watch]\nenabled = true" in body
    assert 'glob = "*.torrent"' in body


def test_disable_keeps_the_keys(tmp_path):
    path = _starter(tmp_path)
    text = path.read_text(encoding="utf-8").replace(
        "[telegram]\nenabled = false", '[telegram]\nenabled = true\nchat_id = "-1001"'
    )
    path.write_text(text, encoding="utf-8")

    changed, _missing = set_enabled(path, "telegram", False)

    assert changed
    body = path.read_text(encoding="utf-8")
    assert "enabled = false" in body
    assert 'chat_id = "-1001"' in body


def test_disable_on_a_missing_section_is_a_noop(tmp_path):
    path = tmp_path / "config.toml"
    path.write_text("[general]\n", encoding="utf-8")

    changed, _missing = set_enabled(path, "watch", False)

    assert changed is False
    assert path.read_text(encoding="utf-8") == "[general]\n"


def test_unknown_feature_is_refused(tmp_path):
    path = _starter(tmp_path)
    with pytest.raises(ConfigUxError, match="unknown feature"):
        set_enabled(path, "bogus", True)


def test_explain_lists_and_describes():
    listing = explain()
    assert "indexers" in listing and "watch" in listing
    text = explain("watch")
    assert "[watch]" in text
    assert "Required when enabled: path" in text
    with pytest.raises(ConfigUxError):
        explain("nope")


def test_report_names_the_chain_and_disabled_features():
    cfg = AppConfig.from_toml(FIXTURES / "minimal.toml")
    lines = check_report(cfg, FIXTURES / "minimal.toml")

    assert lines[0].startswith("topology: single")
    assert "acquire=auto" in lines[1]
    assert "transfer=rclone" in lines[1]
    assert "seed=path" in lines[1]
    assert any(line.startswith("disabled:") and "indexers" in line for line in lines)


def test_report_shows_split_topology():
    cfg = AppConfig.from_toml(FIXTURES / "split.toml")
    lines = check_report(cfg, FIXTURES / "split.toml")

    assert lines[0].startswith("topology: split")


def test_report_lists_deprecated_keys(tmp_path):
    path = tmp_path / "config.toml"
    text = (FIXTURES / "minimal.toml").read_text(encoding="utf-8")
    text = text.replace("[general]\n", "[general]\nmax_active_downloads = 4\n", 1)
    path.write_text(text, encoding="utf-8")
    cfg = AppConfig.from_toml(path)

    lines = check_report(cfg, path)

    assert any("deprecated: `general.max_active_downloads`" in line for line in lines)


def _download_only_with_state(tmp_path: Path, dt_path: Path) -> Path:
    text = f"""
[general]
state_db = "{dt_path.as_posix()}"

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
    path = tmp_path / "config.toml"
    path.write_text(text, encoding="utf-8")
    return path


def test_report_warns_about_stranded_rows(tmp_path):
    db = tmp_path / "state.db"
    store = StateStore(db)
    try:
        store.upsert(
            TorrentState(source_infohash="a" * 40, source_name="Stuck", state=State.MOVING)
        )
    finally:
        store.close()
    path = _download_only_with_state(tmp_path, db)
    cfg = AppConfig.from_toml(path)

    lines = check_report(cfg, path)

    assert any("MOVING while transfer is off" in line for line in lines)


def test_cli_init_enable_disable_and_explain(tmp_path, capsys):
    from racing_sync.__main__ import main

    cfg = tmp_path / "config.toml"
    assert main(["config", "init", "--config", str(cfg)]) == 0
    assert main(["config", "init", "--config", str(cfg)]) == 2
    assert main(["config", "enable", "watch", "--config", str(cfg)]) == 0
    assert 'path = "/srv/qbittorrent/watch"' not in cfg.read_text(encoding="utf-8")
    assert "set before starting: path" in capsys.readouterr().out
    assert main(["config", "disable", "watch", "--config", str(cfg)]) == 0
    assert main(["config", "explain", "watch"]) == 0
    assert main(["config", "explain", "nope"]) == 2


def test_cli_check_config_prints_the_chain(tmp_path, capsys):
    from racing_sync.__main__ import main

    cfg = _starter(tmp_path)
    code = main(["check-config", "--config", str(cfg)])

    out = capsys.readouterr().out
    assert code in (0, 2)
    assert "topology: single" in out
    assert "chain: observe=source,dest" in out
