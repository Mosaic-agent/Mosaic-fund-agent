from __future__ import annotations

from ..base import DossierSection
from ..context import DossierContext


class FinancialsSection(DossierSection):
    """Screener.in primary, Yahoo fallback — matches CLAUDE.md's documented
    scraping-fallback order (the deleted stock_research_report.py used a raw
    yfinance income_stmt dump, i.e. the fallback tier as if it were primary)."""

    key = "financials"
    title = "Multi-Year Financial Statements"

    def fetch(self, ctx: DossierContext) -> str:
        from src.tools.earnings_scraper import get_quarterly_results
        from src.tools.indian_equity_tools import get_stock_cashflow

        inp = f"{ctx.symbol}:{ctx.exchange}"
        q = get_quarterly_results.invoke({"input_str": inp})
        cf = get_stock_cashflow.invoke({"input_str": inp})

        lines: list[str] = []
        if q.get("error"):
            lines.append(f"*Quarterly results unavailable: {q['error']}*")
        else:
            lines.append(f"### Latest Quarterly Results ({q.get('period', '')}, source: {q.get('source_url', '')})")
            lines.append("```text")
            lines.append(
                f"Revenue:     ₹{q.get('revenue_cr', 0):,.2f} Cr (YoY {q.get('revenue_yoy_pct', 0):+.2f}%)\n"
                f"Net Profit:  ₹{q.get('net_profit_cr', 0):,.2f} Cr (YoY {q.get('profit_yoy_pct', 0):+.2f}%)\n"
                f"EPS:         ₹{q.get('eps', 0):.2f} (YoY {q.get('eps_yoy_pct', 0):+.2f}%)"
            )
            lines.append("```")
            if q.get("guidance"):
                lines.append(f"**Management Guidance/Commentary:** {q['guidance']}")

        if cf.get("error"):
            lines.append(f"*Cash flow data unavailable: {cf['error']}*")
        else:
            lines.append("### Free Cash Flow")
            lines.append("```text")
            lines.append(str({k: v for k, v in cf.items() if k != "symbol"}))
            lines.append("```")

        return "\n".join(lines)
