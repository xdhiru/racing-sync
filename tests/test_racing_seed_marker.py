"""A DONE row whose racing torrent is still missing says so on the card.

The content torrent (a cross-seed copy) can be seeded while the racing
torrent itself failed to inject — its .torrent bytes were unreachable on
VPS1. The row is DONE, but the racing copy is missing and the card must
not look like a fully seeded release.
"""

from __future__ import annotations

from racing_sync.state import State, TorrentState
from racing_sync.telegram_views import render_detail

RACING = "a" * 40
CONTENT = "b" * 40


def _row(injected: str) -> TorrentState:
    return TorrentState(
        source_infohash=RACING,
        source_name="Example.Show.S01E01.mkv",
        dest_infohash=CONTENT,
        cross_seed_infohash=CONTENT,
        injected_private_hashes=injected,
        state=State.DONE,
    )


def test_done_card_flags_a_missing_racing_seed():
    text = render_detail(_row(CONTENT))

    assert "Racing torrent not injected yet" in text


def test_done_card_is_quiet_once_the_racing_seed_is_injected():
    text = render_detail(_row(f"{CONTENT},{RACING}"))

    assert "Racing torrent not injected yet" not in text
    assert "Seeded from fuse mount" in text
