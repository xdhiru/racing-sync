"""Telegram action mixins (facade).

`ActionsMixin` is the composition `telegram_bot.py` imports: the
cancel, start, inject and push command families live in
`telegram_actions_cancel`, `telegram_actions_start`,
`telegram_actions_inject` and `telegram_actions_push`, moved verbatim
from this module.
"""

from .telegram_actions_cancel import CancelActionsMixin
from .telegram_actions_inject import InjectActionsMixin
from .telegram_actions_push import PushActionsMixin
from .telegram_actions_start import StartActionsMixin


class ActionsMixin(CancelActionsMixin, StartActionsMixin, InjectActionsMixin, PushActionsMixin):
    """Cancel, start, inject and push command families for `TelegramBot`."""
