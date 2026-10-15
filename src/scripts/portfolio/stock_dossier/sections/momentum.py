from __future__ import annotations

from ..base import DossierSection
from ..context import DossierContext


class MomentumSection(DossierSection):
    key = "momentum"
    title = "Momentum Signals"

    def fetch(self, ctx: DossierContext) -> str:
        from src.tools.yahoo_finance import get_price_momentum

        m = get_price_momentum.invoke({"input_str": f"{ctx.symbol}:{ctx.exchange}"})
        if m.get("error"):
            return f"*Momentum data unavailable: {m['error']}*"
        return (
            "```text\n"
            f"30d Return:  {m.get('return_30d_pct', 0):+.2f}%\n"
            f"90d Return:  {m.get('return_90d_pct', 0):+.2f}%\n"
            f"1y Return:   {m.get('return_1y_pct', 0):+.2f}%\n"
            f"Signal:      {m.get('momentum_signal', 'NEUTRAL')}\n"
            "```"
        )
