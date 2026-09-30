"""Tests for SFTP single-path stat fallback."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

def test_disk_free_prefers_stat_over_df():
    """Single-path stat: no mount-table walk, parses avail blocks."""
    from racing_sync.sftp_source import _SFTPConnection

    conn = _SFTPConnection(MagicMock())
    seen = {}

    class _Chan:
        def settimeout(self, _t):
            pass

    class _Stdout:
        channel = _Chan()

        def read(self):
            seen["cmd"] = seen.get("cmd")
            return b"123456 4096\n"

    class _Client:
        def exec_command(self, cmd):
            seen["cmd"] = cmd
            return (None, _Stdout(), None)

    conn._client = _Client()
    assert conn._disk_free_via_df("/data") == 123456 * 4096
    assert "stat -f" in seen["cmd"]
    assert "df -kP" not in seen["cmd"]


def test_disk_free_falls_back_to_df():
    """stat missing: legacy df parse still works."""
    from racing_sync.sftp_source import _SFTPConnection

    conn = _SFTPConnection(MagicMock())
    calls = []

    class _Chan:
        def settimeout(self, _t):
            pass

    class _Stdout:
        def __init__(self, payload):
            self._payload = payload
            self.channel = _Chan()

        def read(self):
            return self._payload

    class _Client:
        def exec_command(self, cmd):
            calls.append(cmd)
            if cmd.startswith("stat -f"):
                raise OSError("stat: command not found")
            return (None, _Stdout(
                b"Filesystem 1024-blocks Used Available Capacity Mounted\n"
                b"/dev/sda1 1000 100 900 10% /data\n"), None)

    conn._client = _Client()
    assert conn._disk_free_via_df("/data") == 900 * 1024
    assert any(c.startswith("df -kP") for c in calls)



def test_fuse_stat_cached_serves_and_expires(tmp_path):
    """Shared stat cache: hit within TTL, fresh read after."""
    from racing_sync import coordinator_paths as _cp

    f = tmp_path / "f.mkv"
    f.write_bytes(b"x" * 10)
    try:
        assert _cp.fuse_stat_cached(f) == (True, 10)
        f.write_bytes(b"x" * 20)
        # Still the cached answer inside the TTL.
        assert _cp.fuse_stat_cached(f) == (True, 10)
        _cp.fuse_stat_invalidate(f)
        assert _cp.fuse_stat_cached(f) == (True, 20)
        assert _cp.fuse_stat_cached(tmp_path / "nope.mkv") == (False, -1)
        assert _cp.fuse_stat_cached(tmp_path) == (True, -1)
    finally:
        _cp.fuse_stat_invalidate(f)


def test_missing_fuse_files_uses_shared_cache(tmp_path):
    """Gate reads collapse to one stat per path per minute."""
    from racing_sync import coordinator_paths as _cp

    top = tmp_path / "rel"
    top.mkdir()
    (top / "a.mkv").write_bytes(b"a" * 5)
    calls = {"n": 0}
    real_stat = __import__("os").stat

    def _counting(path):
        calls["n"] += 1
        return real_stat(path)

    import os as _os
    _orig = _os.stat
    _os.stat = _counting
    try:
        _cp.fuse_stat_invalidate(top / "a.mkv")
        assert _cp.fuse_stat_cached(top / "a.mkv") == (True, 5)
        assert _cp.fuse_stat_cached(top / "a.mkv") == (True, 5)
        assert calls["n"] == 1
    finally:
        _os.stat = _orig
        _cp.fuse_stat_invalidate(top / "a.mkv")
