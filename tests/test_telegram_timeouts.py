"""Tests for Telegram generous HTTP timeouts."""
from __future__ import annotations

import pytest

def test_make_bot_uses_generous_timeouts():
    """Telegram Bot carries 30s reads (slow routes stop flapping)."""
    from racing_sync.telegram_bot import _make_bot

    bot = _make_bot("123:ABC")
    try:
        req = bot.request
    except Exception:
        req = getattr(bot, "_request", None)
    assert req is not None
    try:
        assert float(req.read_timeout) >= 30.0
    finally:
        try:
            import asyncio

            async def _close():
                try:
                    await bot.shutdown()
                except Exception:
                    pass

            asyncio.get_event_loop().run_until_complete(_close())
        except Exception:
            pass
