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

