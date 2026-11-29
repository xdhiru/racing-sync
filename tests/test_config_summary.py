"""Phase 0 characterisation: the resolved config summary must not drift.

The golden files under `tests/fixtures/config/golden/` are the contract for
the config refactor: any change in how a config resolves (values, shape,
counts) fails here until deliberately re-baselined. Regenerate with:

    python - <<'PY'
    import json
    from pathlib import Path
    from racing_sync.config import AppConfig
    from racing_sync.config_summary import summarize
    root = Path("tests/fixtures/config")
    for path in [*sorted(root.glob("*.toml")), Path("config.example.toml")]:
        data = summarize(AppConfig.from_toml(path), path)
        data.pop("config", None)
        name = "config-example" if path.name == "config.example.toml" else path.stem
        (root / "golden" / f"{name}.json").write_text(
            json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
    PY
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from racing_sync.__main__ import main
from racing_sync.config import AppConfig
from racing_sync.config_summary import format_summary, summarize, summary_json

ROOT = Path(__file__).parent
FIXTURES = ROOT / "fixtures" / "config"

CASES = {
    "minimal": FIXTURES / "minimal.toml",
    "single-fuse": FIXTURES / "single-fuse.toml",
    "split": FIXTURES / "split.toml",
    "legacy-off": FIXTURES / "legacy-off.toml",
    "config-example": ROOT.parent / "config.example.toml",
}


def _summarize(path: Path) -> dict:
    data = summarize(AppConfig.from_toml(path), path)
    data.pop("config", None)
    return data


@pytest.mark.parametrize("name", sorted(CASES))
def test_summary_matches_the_golden(name: str):
    """Old-style configs resolve exactly as they did before the refactor."""
    expected = json.loads((FIXTURES / "golden" / f"{name}.json").read_text(encoding="utf-8"))
    assert _summarize(CASES[name]) == expected


def test_summary_never_leaks_secrets():
    """Secret values are replaced by a marker in both output forms."""
    path = CASES["legacy-off"]
    data = _summarize(path)
    blob = summary_json(data)
    human = format_summary({**data, "config": str(path)})
    for secret in (
        "prowlarr-api-key-0123456789",
        "999999999:ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ",
        "api-off-token-0123456789abcdef",
    ):
        assert secret not in blob
        assert secret not in human
    assert data["resolved"]["prowlarr"]["api_key"] == "***"
    assert data["resolved"]["telegram"]["bot_token"] == "***"
    assert data["resolved"]["api"]["api_token"] == "***"


def test_summary_reports_the_resolved_shape():
    split = _summarize(CASES["split"])
    assert split["shape"]["mode"] == "split"
    assert {"prowlarr_cross_seed", "sftp_export", "watch_dir"} <= set(split["shape"]["acquire"])
    assert split["shape"]["cleanup"] == {"enabled": True, "dry_run": True}
    assert split["shape"]["control"] == {"telegram": True, "api": True, "logging_sink": False}

    single = _summarize(CASES["single-fuse"])
    assert single["shape"]["mode"] == "single"
    assert single["shape"]["observe"] == ["source", "dest"]
    assert single["shape"]["acquire"] == ["client_export"]
    assert single["shape"]["cleanup"]["enabled"] is False

    off = _summarize(CASES["legacy-off"])
    assert off["shape"]["cleanup"]["enabled"] is False
    assert off["shape"]["control"]["telegram"] is False
    assert off["shape"]["control"]["api"] is False
    assert "prowlarr_cross_seed" not in off["shape"]["acquire"]


def test_summary_flags_unknown_sections(tmp_path: Path):
    text = CASES["minimal"].read_text(encoding="utf-8") + "\n[bogus]\nthing = 1\n"
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(text, encoding="utf-8")
    data = _summarize(cfg_file)
    assert data["keys"]["unknown"] == ["bogus.thing"]


def test_summary_is_read_only(tmp_path: Path):
    """A summary must never create the state DB or log directory."""
    text = CASES["minimal"].read_text(encoding="utf-8")
    state = tmp_path / "state" / "state.db"
    logs = tmp_path / "logs"
    text = text.replace(
        "[general]",
        f'[general]\nstate_db = "{state.as_posix()}"\nlog_dir = "{logs.as_posix()}"',
    )
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(text, encoding="utf-8")

    _summarize(cfg_file)

    assert not state.parent.exists()
    assert not logs.exists()


def test_digest_pins_shape_and_non_secret_values():
    base = CASES["split"].read_text(encoding="utf-8")
    same = base.replace(
        'api_key = "prowlarr-api-key-0123456789"', 'api_key = "another-key-0123456789"'
    )
    other_shape = base.replace('default = "remote:qbittorrent/"', 'default = "other:movies/"')

    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "a.toml").write_text(base, encoding="utf-8")
        (root / "b.toml").write_text(same, encoding="utf-8")
        (root / "c.toml").write_text(other_shape, encoding="utf-8")
        a = _summarize(root / "a.toml")
        b = _summarize(root / "b.toml")
        c = _summarize(root / "c.toml")

    assert a["digest"] == b["digest"], "redacted secrets must not change the digest"
    assert a["digest"] != c["digest"], "a different remote must change the digest"


def test_cli_prints_human_and_json(tmp_path: Path, capsys):
    cfg_file = FIXTURES / "split.toml"

    rc = main(["config", "summary", "--config", str(cfg_file)])
    human = capsys.readouterr().out
    assert rc == 0
    assert "Topology: split" in human
    assert "Digest:   sha256:" in human

    rc = main(["config", "summary", "--config", str(cfg_file), "--json"])
    payload = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert payload["shape"]["mode"] == "split"


def test_cli_reports_an_invalid_config(tmp_path: Path, capsys):
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text("not toml at all", encoding="utf-8")
    rc = main(["config", "summary", "--config", str(cfg_file)])
    assert rc == 2
    assert "Invalid config" in capsys.readouterr().err


_NEW_SPELLING_MINIMAL = """\
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

[remote]
path = "remote:movies/"
unsorted = "remote:unsorted/"

[seed]
path = "/mnt/fuse"
"""


def test_new_spelling_resolves_identically(tmp_path: Path):
    """The new names are aliases: same values in, same shape and digest out."""
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(_NEW_SPELLING_MINIMAL, encoding="utf-8")

    old = _summarize(CASES["minimal"])
    new = _summarize(cfg_file)

    assert new["digest"] == old["digest"]
    assert new["shape"] == old["shape"]
    keys = new["keys"]
    assert set(keys["by_section"]) == {"client", "dest", "remote", "seed", "storage"}
    assert keys["unknown"] == []
    assert keys["explicit"] == 10


def test_unknown_key_inside_new_section_is_reported(tmp_path: Path):
    text = _NEW_SPELLING_MINIMAL.replace(
        'host = "http://127.0.0.1:8080"\n\n[dest]',
        'host = "http://127.0.0.1:8080"\nhots = "http://typo"\n\n[dest]',
        count=1,
    )
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(text, encoding="utf-8")

    AppConfig.from_toml(cfg_file)  # a typo warns, it does not refuse to load
    assert _summarize(cfg_file)["keys"]["unknown"] == ["client.hots"]


def test_unknown_key_inside_nested_new_table(tmp_path: Path):
    text = _NEW_SPELLING_MINIMAL + '\n[client.nginx]\nmodee = "basic"\n'
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(text, encoding="utf-8")

    AppConfig.from_toml(cfg_file)  # the nested typo is ignored, not fatal
    data = _summarize(cfg_file)

    assert data["digest"].startswith("sha256:")
