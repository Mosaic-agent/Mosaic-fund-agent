from __future__ import annotations

from ..base import DossierSection
from ..context import DossierContext


class FinancialsSection(DossierSection):
    """Multi-year P&L trend from market_data.stock_financial_statements
    (StockScans, imported via `import --category stockscans_fundamentals`) —
    real quarter-over-quarter and year-over-year history, not a single live
    scrape. Falls back to the Screener.in/Yahoo live scrape
    (`get_quarterly_results`) for symbols StockScans hasn't covered (e.g. very
    recent listings) — matches CLAUDE.md's documented scraping-fallback order.
    Free cash flow still comes from `get_stock_cashflow` (Yahoo) since
    StockScans' statements table has no OCF/FCF/capex fields."""

    key = "financials"
    title = "Multi-Year Financial Statements"

    def fetch(self, ctx: DossierContext) -> str:
        from src.tools.indian_equity_tools import get_stock_cashflow

        lines: list[str] = [self._pnl_trend(ctx) or self._pnl_fallback(ctx)]

        cf = get_stock_cashflow.invoke({"input_str": f"{ctx.symbol}:{ctx.exchange}"})
        if cf.get("error"):
            lines.append(f"*Cash flow data unavailable: {cf['error']}*")
        else:
            lines.append("### Free Cash Flow")
            lines.append("```text")
            lines.append(str({k: v for k, v in cf.items() if k != "symbol"}))
            lines.append("```")

        return "\n".join(lines)

    def _pnl_trend(self, ctx: DossierContext) -> str:
        from src.db.pool import get_pool

        df = get_pool().query_df(f"""
            SELECT statement_type, period_date, revenue, revenue_growth_yoy,
                   pat, pat_growth_yoy, eps, opm, npm
            FROM market_data.stock_financial_statements FINAL
            WHERE symbol = '{ctx.symbol}'
            ORDER BY statement_type, period_date DESC
        """)
        if df.empty:
            return ""

        out = []
        for stype, label in (("quarterly", "Quarterly Trend (₹ Cr)"), ("yearly", "Yearly Trend (₹ Cr)")):
            sub = df[df["statement_type"] == stype].head(4 if stype == "quarterly" else 3)
            if sub.empty:
                continue
            out.append(f"### {label} (StockScans)")
            out.append("```text")
            out.append(sub.drop(columns=["statement_type"]).to_string(index=False))
            out.append("```")
        return "\n".join(out)

    def _pnl_fallback(self, ctx: DossierContext) -> str:
        from src.tools.earnings_scraper import get_quarterly_results

        q = get_quarterly_results.invoke({"input_str": f"{ctx.symbol}:{ctx.exchange}"})
        if q.get("error"):
            return f"*Quarterly results unavailable: {q['error']}*"

        out = [
            f"### Latest Quarterly Results ({q.get('period', '')}, source: {q.get('source_url', '')})",
            "```text",
            f"Revenue:     ₹{q.get('revenue_cr', 0):,.2f} Cr (YoY {q.get('revenue_yoy_pct', 0):+.2f}%)\n"
            f"Net Profit:  ₹{q.get('net_profit_cr', 0):,.2f} Cr (YoY {q.get('profit_yoy_pct', 0):+.2f}%)\n"
            f"EPS:         ₹{q.get('eps', 0):.2f} (YoY {q.get('eps_yoy_pct', 0):+.2f}%)",
            "```",
        ]
        if q.get("guidance"):
            out.append(f"**Management Guidance/Commentary:** {q['guidance']}")
        return "\n".join(out)
