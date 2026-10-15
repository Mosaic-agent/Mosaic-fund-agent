from __future__ import annotations

from ..base import DossierSection
from ..context import DossierContext


class DilutionAuditSection(DossierSection):
    """Promoter QIP/dilution check (feedback_qip_dilution_check memory rule).

    Reads market_data.stock_shareholding_pattern (StockScans, multi-quarter —
    imported via `import --category stockscans_shareholding`) instead of a
    single live Screener.in scrape, which only ever exposed the latest vs. one
    prior quarter. Also cross-references stock_shareholding_entities
    (named promoter/public holders per quarter) so a promoter-% drop can be
    attributed to specific entities rather than only the aggregate number —
    e.g. an inter-group transfer between two named promoter entities looks
    very different from one entity's holding simply shrinking.

    Falls back to the Screener.in live scrape (get_shareholding_pattern) for
    symbols StockScans hasn't covered."""

    key = "dilution"
    title = "Promoter Shareholding & Dilution Audit"

    def fetch(self, ctx: DossierContext) -> str:
        return self._from_stockscans(ctx) or self._fallback(ctx)

    def _from_stockscans(self, ctx: DossierContext) -> str:
        from src.db.pool import get_pool

        pool = get_pool()
        df = pool.query_df(f"""
            SELECT quarter_date, promoter_pct
            FROM market_data.stock_shareholding_pattern FINAL
            WHERE symbol = '{ctx.symbol}'
            ORDER BY quarter_date DESC
            LIMIT 2
        """)
        if len(df) < 2:
            return ""

        latest_q, prev_q = df["quarter_date"].iloc[0], df["quarter_date"].iloc[1]
        latest_pct, prev_pct = float(df["promoter_pct"].iloc[0]), float(df["promoter_pct"].iloc[1])
        delta = latest_pct - prev_pct
        latest_q_str, prev_q_str = str(latest_q)[:10], str(prev_q)[:10]

        lines = [f"**Promoter % (QoQ, StockScans):** {prev_q_str} {prev_pct:.2f}% → {latest_q_str} {latest_pct:.2f}%"]
        if abs(delta) < 1.0:
            lines.append(f" (change {delta:+.2f}pp — no dilution audit needed)")
            return "\n".join(lines)
        lines[0] += f" ({'dropped' if delta < 0 else 'rose'} {abs(delta):.2f}pp)\n"

        if delta < 0:
            lines.append(
                "**Dilution Audit (QIP Rule):** A promoter-% drop does NOT confirm a "
                "sell-down. Verify whether total shares outstanding expanded via QIP, "
                "preferential allotment, rights/bonus issue, or ESOP exercise before "
                "flagging a sale. Named promoter-entity deltas below help distinguish "
                "an inter-group transfer from an open-market sale.\n"
            )
            lines.append(self._entity_deltas(ctx, latest_q, prev_q))

        return "\n".join(lines)

    def _entity_deltas(self, ctx: DossierContext, latest_q, prev_q) -> str:
        import pandas as pd
        from src.db.pool import get_pool

        pool = get_pool()
        df = pool.query_df(f"""
            SELECT holder_name, quarter_date, holding_pct
            FROM market_data.stock_shareholding_entities FINAL
            WHERE symbol = '{ctx.symbol}' AND category = 'promoter'
              AND quarter_date IN ('{str(latest_q)[:10]}', '{str(prev_q)[:10]}')
        """)
        if df.empty:
            return "*No named promoter-entity detail available for this quarter pair.*"

        latest_map = df[df["quarter_date"] == latest_q].set_index("holder_name")["holding_pct"]
        prev_map = df[df["quarter_date"] == prev_q].set_index("holder_name")["holding_pct"]

        rows = []
        for holder in sorted(set(latest_map.index) | set(prev_map.index)):
            lp, pp = float(latest_map.get(holder, 0.0)), float(prev_map.get(holder, 0.0))
            if abs(lp - pp) >= 0.05:
                rows.append((holder, pp, lp, lp - pp))
        if not rows:
            return "*No individual promoter entity moved ≥0.05pp between these two quarters.*"

        rows.sort(key=lambda r: r[3])
        out = pd.DataFrame(rows, columns=["holder_name", "prev_pct", "latest_pct", "delta_pp"]).set_index("holder_name")
        return "```text\n" + out.round(2).to_string() + "\n```"

    def _fallback(self, ctx: DossierContext) -> str:
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
