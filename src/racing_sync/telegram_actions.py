"""Telegram action mixins (facade).

`ActionsMixin` is the composition `telegram_bot.py` imports: the
cancel, start and inject command families live in
`telegram_actions_cancel`, `telegram_actions_start` and
`telegram_actions_inject`, moved verbatim from this module.
"""

from .telegram_actions_cancel import CancelActionsMixin
from .telegram_actions_inject import InjectActionsMixin
from .telegram_actions_start import StartActionsMixin


class ActionsMixin(CancelActionsMixin, StartActionsMixin, InjectActionsMixin):
    """Cancel, start and inject command families for `TelegramBot`."""
