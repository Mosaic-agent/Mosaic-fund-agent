from __future__ import annotations

from ..base import DossierSection
from ..context import DossierContext


class DilutionAuditSection(DossierSection):
    """Promoter QIP/dilution check (feedback_qip_dilution_check memory rule).

    The deleted stock_quant_deepdive_cli.py implemented this by regex-parsing
    "Promoter: xx.xx%" out of a stringified tool-output blob — but
    get_shareholding_pattern already returns a structured dict
    (promoter_pct / promoter_pct_prev / promoter_pct_qoq_delta), which that
    regex never matched (no literal "Promoter:"+"%" text in a dict repr). That
    made the audit silently fall through to its "not parsed" branch on every
    real run. Fixed here to read the structured fields directly — same rule,
    working implementation."""

    key = "dilution"
    title = "Promoter Shareholding & Dilution Audit"

    def fetch(self, ctx: DossierContext) -> str:
        from src.tools.earnings_scraper import get_shareholding_pattern

        shareholding = get_shareholding_pattern.invoke({"symbol": ctx.symbol})
        if shareholding.get("error") or "promoter_pct" not in shareholding:
            return "*Promoter shareholding pattern unavailable — check the Financials section above.*"

        latest_pct = shareholding["promoter_pct"]
        prev_pct = shareholding.get("promoter_pct_prev", latest_pct)
        delta = shareholding.get("promoter_pct_qoq_delta", latest_pct - prev_pct)

        if abs(delta) < 1.0:
            return f"**Promoter % Stable (QoQ):** {latest_pct:.2f}% (change {delta:+.2f}pp — no dilution audit needed)"

        direction = "dropped" if delta < 0 else "rose"
        lines = [
            f"**Observed Promoter % Change (QoQ):** {prev_pct:.2f}% → {latest_pct:.2f}% "
            f"({direction} {abs(delta):.2f}pp)\n"
        ]
        if delta < 0:
            lines.append(
                "**Dilution Audit (QIP Rule):** A promoter-% drop does NOT confirm a "
                "sell-down. Verify whether total shares outstanding expanded via:\n"
                "  • **QIP** (Qualified Institutional Placement)\n"
                "  • **Preferential allotment** to investors\n"
                "  • **Rights/bonus issue** (bonus rarely dilutes %)\n"
                "  • **ESOP exercise** (minor dilution)\n\n"
                "> Rule: Promoter-% drop with **unchanged absolute share count** = dilution "
                "(not a sale). Promoter-% drop with **lower absolute share count** = actual "
                "sale (red flag). Cross-check `get_corporate_actions` / the balance sheet "
                "'Equity Capital' line for total shares outstanding before flagging a sale."
            )
        return "\n".join(lines)
