"""The new capability-chain spellings resolve onto the same AppConfig.

The `new-*.toml` fixtures are the `[client]`/`[storage]`/`[remote]`/`[seed]`
twins of the characterisation fixtures, so both spellings must produce an
identical model and summary. The rest pins the alias layer's own rules:
derived defaults, refusals, provider gating and leftover keys.
"""

from __future__ import annotations

import logging
import tomllib
from pathlib import Path

import pytest

from racing_sync import config_alias, config_disposition
from racing_sync.config import AppConfig
from racing_sync.config_alias import normalize_config
from racing_sync.config_disposition import (
    DEFAULT_PROVIDER,
    LINK_PROVIDERS,
    NEW_ONLY,
    SUPPORTED_PROVIDERS,
)
from racing_sync.config_summary import summarize

FIXTURES = Path(__file__).parent / "fixtures" / "config"

PAIRS = {
    "minimal": "new-minimal",
    "single-fuse": "new-single-fuse",
    "split": "new-split",
}


def _raw() -> dict:
    """Smallest new-spelling config that satisfies every core table."""
    return {
        "client": {"host": "http://source:8080"},
        "dest": {"host": "http://dest:8080"},
        "storage": {"path": "/ssd", "save_path": "/ssd/done"},
        "remote": {"path": "remote:movies/", "unsorted": "remote:unsorted/"},
        "seed": {"path": "/mnt/fuse/"},
    }


def _config_file(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "config.toml"
    path.write_text(text, encoding="utf-8")
    return path


@pytest.mark.parametrize(("old", "new"), sorted(PAIRS.items()))
def test_new_spelling_resolves_identically(old: str, new: str):
    old_path = FIXTURES / f"{old}.toml"
    new_path = FIXTURES / f"{new}.toml"

    old_cfg = AppConfig.from_toml(old_path)
    new_cfg = AppConfig.from_toml(new_path)

    assert new_cfg.model_dump() == old_cfg.model_dump()

    old_summary = summarize(old_cfg, old_path)
    new_summary = summarize(new_cfg, new_path)
    assert new_summary["shape"] == old_summary["shape"]
    assert new_summary["digest"] == old_summary["digest"]


@pytest.mark.parametrize("name", ["minimal", "single-fuse", "split", "legacy-off"])
def test_old_spelling_passes_through_untouched(name: str):
    with open(FIXTURES / f"{name}.toml", "rb") as fh:
        raw = tomllib.load(fh)

    normalized = normalize_config(raw)

    assert normalized.data == raw
    assert normalized.aliases == ()


def test_seed_unsorted_path_is_derived_from_the_mount():
    normalized = normalize_config(_raw())

    fuse = normalized.data["rclone"]["fuse"]
    assert fuse["mount"] == "/mnt/fuse/"
    assert fuse["mount_unsorted"] == "/mnt/fuse/unsorted"


def test_explicit_seed_unsorted_path_is_kept():
    raw = _raw()
    raw["seed"]["unsorted_path"] = "/elsewhere/unsorted"

    fuse = normalize_config(raw).data["rclone"]["fuse"]

    assert fuse["mount_unsorted"] == "/elsewhere/unsorted"


def test_conflicting_spellings_are_refused():
    raw = _raw()
    raw["source"] = {"host": "http://elsewhere:8080"}

    with pytest.raises(ValueError, match=r"client\.host.*source\.host"):
        normalize_config(raw)


def test_equal_spellings_merge_silently():
    raw = _raw()
    raw["source"] = {"host": "http://source:8080"}

    data = normalize_config(raw).data

    assert data["source"] == {"host": "http://source:8080"}


@pytest.mark.parametrize(
    ("section", "expected"),
    [
        ({"base_url": "http://prowlarr:9696"}, True),
        ({"enabled": True, "base_url": "http://prowlarr:9696"}, True),
        ({"enabled": False, "base_url": "http://prowlarr:9696"}, False),
        ({}, False),
    ],
)
def test_indexers_enabled_is_inferred_from_content(section: dict, expected: bool):
    raw = _raw()
    raw["indexers"] = section

    assert normalize_config(raw).data["prowlarr"]["enabled"] is expected


def test_watch_disabled_drops_the_section():
    raw = _raw()
    raw["watch"] = {"enabled": False, "path": "/watch"}

    assert "watch_dir" not in normalize_config(raw).data


@pytest.mark.parametrize("watch", [{"glob": "*.torrent"}, {"enabled": True}])
def test_watch_without_path_is_refused(watch: dict):
    raw = _raw()
    raw["watch"] = watch

    with pytest.raises(ValueError, match=r"\[watch\]\.path is required"):
        normalize_config(raw)


@pytest.mark.parametrize("link", ["transfer", "seed"])
def test_disabled_link_sets_the_pipeline_flag(link: str):
    raw = _raw()
    section = {"enabled": False}
    if link == "seed":
        section["path"] = "/mnt/fuse"
    raw[link] = section

    data = normalize_config(raw).data

    assert data["pipeline"][link] == "none"
    if link == "seed":
        # Keys survive the switch: re-enabling is a flip, not a rewrite.
        assert data["rclone"]["fuse"]["mount"] == "/mnt/fuse"


@pytest.mark.parametrize("link", ["transfer", "seed"])
def test_provider_none_disables_the_link(link: str):
    raw = _raw()
    raw.setdefault(link, {})["provider"] = "none"

    data = normalize_config(raw).data

    assert data["pipeline"][link] == "none"


def test_seed_existing_is_accepted():
    raw = _raw()
    raw["seed"]["provider"] = "existing"

    assert normalize_config(raw).data["pipeline"]["seed"] == "existing"


def test_disabling_acquire_is_refused_for_now():
    raw = _raw()
    raw["acquire"] = {"enabled": False}

    with pytest.raises(ValueError, match=r"\[acquire\]"):
        normalize_config(raw)


def test_switch_and_provider_must_agree():
    raw = _raw()
    raw["transfer"] = {"enabled": False, "provider": "rclone"}

    with pytest.raises(ValueError, match="contradicts provider"):
        normalize_config(raw)


@pytest.mark.parametrize(
    ("link", "provider"),
    [("acquire", "bogus"), ("transfer", "bogus"), ("seed", "bogus")],
)
def test_unknown_provider_is_refused(link: str, provider: str):
    raw = _raw()
    raw.setdefault(link, {})["provider"] = provider

    with pytest.raises(ValueError, match="not a known provider"):
        normalize_config(raw)


@pytest.mark.parametrize(
    ("link", "provider"),
    [("acquire", "cross_seed")],
)
def test_known_but_unimplemented_provider_is_refused(link: str, provider: str):
    raw = _raw()
    raw.setdefault(link, {})["provider"] = provider

    with pytest.raises(ValueError, match="not available yet"):
        normalize_config(raw)


# --- registry consistency: the alias layer owns no provider table of its own ---


def test_alias_imports_the_disposition_registry():
    assert config_alias.LINK_PROVIDERS is config_disposition.LINK_PROVIDERS
    assert config_alias.SUPPORTED_PROVIDERS is config_disposition.SUPPORTED_PROVIDERS
    assert config_alias.DEFAULT_PROVIDER is config_disposition.DEFAULT_PROVIDER
    for stale in ("_PROVIDERS", "_SUPPORTED", "_DEFAULT_PROVIDER"):
        assert not hasattr(config_alias, stale), f"config_alias still keeps a private {stale}"


@pytest.mark.parametrize(
    ("link", "provider"),
    [(link, name) for link, names in SUPPORTED_PROVIDERS.items() for name in names],
)
def test_every_supported_provider_is_accepted(link: str, provider: str):
    raw = _raw()
    raw.setdefault(link, {})["provider"] = provider

    assert normalize_config(raw).data["pipeline"][link] == provider


@pytest.mark.parametrize(
    ("link", "provider"),
    [
        (link, name)
        for link, names in LINK_PROVIDERS.items()
        for name in names
        if name not in SUPPORTED_PROVIDERS[link]
    ],
)
def test_every_unimplemented_link_provider_is_refused(link: str, provider: str):
    raw = _raw()
    raw.setdefault(link, {})["provider"] = provider

    with pytest.raises(ValueError, match="not available yet"):
        normalize_config(raw)


@pytest.mark.parametrize("link", sorted(DEFAULT_PROVIDER))
def test_the_default_provider_is_the_registry_default(link: str):
    raw = _raw()
    raw.setdefault(link, {})

    assert normalize_config(raw).data["pipeline"][link] == DEFAULT_PROVIDER[link]


def test_provider_errors_quote_the_registry():
    raw = _raw()
    raw.setdefault("transfer", {})["provider"] = "bogus"
    with pytest.raises(ValueError) as unknown:
        normalize_config(raw)
    assert "choose one of: " + ", ".join(LINK_PROVIDERS["transfer"]) in str(unknown.value)

    raw = _raw()
    raw.setdefault("acquire", {})["provider"] = "cross_seed"
    with pytest.raises(ValueError) as unavailable:
        normalize_config(raw)
    assert "this release runs: " + ", ".join(SUPPORTED_PROVIDERS["acquire"]) in str(
        unavailable.value
    )


def test_new_only_provider_hints_name_only_supported_providers():
    for link, names in SUPPORTED_PROVIDERS.items():
        assert NEW_ONLY[f"{link}.provider"] == " | ".join(names)


@pytest.mark.parametrize(
    ("link", "provider"),
    [("acquire", "auto"), ("transfer", "rclone"), ("seed", "path")],
)
def test_current_provider_is_accepted(link: str, provider: str):
    raw = _raw()
    raw.setdefault(link, {})["provider"] = provider

    data = normalize_config(raw).data

    assert link not in data
    assert data["rclone"]["fuse"]["mount"] == "/mnt/fuse/"


def test_unknown_key_in_a_new_section_is_reported():
    raw = _raw()
    raw["remote"]["hots"] = 1

    normalized = normalize_config(raw)

    assert normalized.unknown == ("remote.hots",)
    assert "hots" not in normalized.data["rclone"]["remote"]


def test_deprecated_key_still_applies_and_warns_once(tmp_path: Path, caplog):
    text = (
        (FIXTURES / "new-minimal.toml")
        .read_text(encoding="utf-8")
        .replace("[general]\n", "[general]\nmax_active_downloads = 4\n", 1)
    )
    cfg_file = _config_file(tmp_path, text)

    with caplog.at_level(logging.WARNING):
        cfg = AppConfig.from_toml(cfg_file)

    assert cfg.max_active_downloads == 4
    warnings = [
        r.getMessage() for r in caplog.records if "general.max_active_downloads" in r.getMessage()
    ]
    assert len(warnings) == 1


def test_deprecated_reinject_delay_wins_over_the_fuse_value(tmp_path: Path, caplog):
    text = (FIXTURES / "new-minimal.toml").read_text(encoding="utf-8") + (
        "\n[rclone]\nreinject_delay_seconds = 15\n\n[rclone.fuse]\nreinject_delay_seconds = 30\n"
    )
    cfg_file = _config_file(tmp_path, text)

    with caplog.at_level(logging.WARNING):
        cfg = AppConfig.from_toml(cfg_file)

    assert cfg.fuse_reinject_delay_seconds == 15
    warnings = [
        r.getMessage() for r in caplog.records if "rclone.reinject_delay_seconds" in r.getMessage()
    ]
    assert len(warnings) == 1


def test_unknown_client_key_is_reported_under_its_written_path(tmp_path: Path, caplog):
    text = (
        (FIXTURES / "new-minimal.toml")
        .read_text(encoding="utf-8")
        .replace('host = "http://127.0.0.1:8080"', 'host = "http://127.0.0.1:8080"\nhots = true', 1)
    )
    cfg_file = _config_file(tmp_path, text)

    with caplog.at_level(logging.WARNING):
        cfg = AppConfig.from_toml(cfg_file)

    assert cfg.source.host == "http://127.0.0.1:8080"
    assert any("client.hots" in r.getMessage() for r in caplog.records)


def test_new_spelling_without_dest_is_refused(tmp_path: Path):
    cfg_file = _config_file(
        tmp_path,
        """
[client]
host = "http://source:8080"

[storage]
path = "/ssd"
""",
    )

    with pytest.raises(ValueError, match=r"\[dest\]"):
        AppConfig.from_toml(cfg_file)


def test_storage_without_path_is_refused(tmp_path: Path):
    cfg_file = _config_file(
        tmp_path,
        """
[client]
host = "http://source:8080"

[dest]
host = "http://dest:8080"

[storage]
save_path = "/ssd/done"
""",
    )

    with pytest.raises(ValueError, match=r"\[storage\]\.path"):
        AppConfig.from_toml(cfg_file)


def test_storage_without_save_path_is_refused(tmp_path: Path):
    cfg_file = _config_file(
        tmp_path,
        """
[client]
host = "http://source:8080"

[dest]
host = "http://dest:8080"

[storage]
path = "/ssd"
""",
    )

    with pytest.raises(ValueError, match=r"\[storage\]\.save_path"):
        AppConfig.from_toml(cfg_file)


def test_seed_without_path_is_refused(tmp_path: Path):
    cfg_file = _config_file(
        tmp_path,
        """
[client]
host = "http://source:8080"

[dest]
host = "http://dest:8080"

[storage]
path = "/ssd"
save_path = "/ssd/done"

[seed]
unsorted_path = "/mnt/fuse/unsorted"
""",
    )

    with pytest.raises(ValueError, match=r"\[seed\]\.path"):
        AppConfig.from_toml(cfg_file)
