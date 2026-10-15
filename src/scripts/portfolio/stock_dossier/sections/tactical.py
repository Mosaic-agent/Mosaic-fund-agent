from __future__ import annotations

from ..base import DossierSection
from ..context import DossierContext


class TacticalPlaybookSection(DossierSection):
    """2-tranche execution playbook. Only one of the three deleted scripts
    (stock_research_report.py) implemented this narrative section; ported
    as-is but fed from ctx's canonical price history instead of a separate
    1-month yfinance intraday pull."""

    key = "tactical"
    title = "Tactical Action & Trade Execution Playbook"

    def fetch(self, ctx: DossierContext) -> str:
        cur_p, low_p, high_p = ctx.cur_price, ctx.low_price, ctx.high_price

        stop_loss = round(low_p * 0.96, 2)
        tranche_1_low, tranche_1_high = round(cur_p * 0.98, 2), round(cur_p * 1.01, 2)
        tranche_2_low, tranche_2_high = round(low_p * 1.01, 2), round(low_p * 1.04, 2)
        target_1, target_2 = round(high_p * 1.05, 2), round(high_p * 1.25, 2)
        downside_risk = round(cur_p - stop_loss, 2)
        upside_gain = round(target_2 - cur_p, 2)
        rr_ratio = round(upside_gain / (downside_risk + 1e-6), 1)

        return "\n".join([
            "```text",
            "┌──────────────────────────────────────────────────────────────────────────────────────────┐",
            "│                                TACTICAL TRADE BLUEPRINT                                  │",
            "├──────────────────────────────────────────────────────────────────────────────────────────┤",
            "│  • Signal Classification:     🟢 STAGGERED ACCUMULATION (2 Tranches)                     │",
            f"│  • Current Price (CMP):       ₹{cur_p:,.2f}",
            f"│  • Tranche 1 (40% Position):  ₹{tranche_1_low:,.2f} – ₹{tranche_1_high:,.2f} (CMP Entry Zone)",
            f"│  • Tranche 2 (60% Position):  ₹{tranche_2_low:,.2f} – ₹{tranche_2_high:,.2f} (Dip / Base Limit Add)",
            f"│  • Hard Invalidation / Stop:  Daily close below ₹{stop_loss:,.2f} (Risk: -₹{downside_risk:,.2f})",
            f"│  • Target 1 (Short-Term):     ₹{target_1:,.2f} (+{((target_1 - cur_p) / cur_p * 100):.1f}%)",
            f"│  • Target 2 (Medium-Term):    ₹{target_2:,.2f} (+{((target_2 - cur_p) / cur_p * 100):.1f}%)",
            f"│  • Asymmetric Risk/Reward:    1 : {rr_ratio}",
            "└──────────────────────────────────────────────────────────────────────────────────────────┘",
            "```",
        ])
