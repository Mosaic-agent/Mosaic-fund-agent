from __future__ import annotations

from ..base import DossierSection
from ..context import DossierContext


class ExecutiveProfileSection(DossierSection):
    key = "profile"
    title = "Executive Profile & Capital Structure"

    def fetch(self, ctx: DossierContext) -> str:
        info = ctx.yf_info
        shares_cr = (info.get("sharesOutstanding") or 0) / 1e7
        float_cr = (info.get("floatShares") or 0) / 1e7
        float_pct = (float_cr / shares_cr * 100) if shares_cr else 0.0
        rev_cr = (info.get("totalRevenue") or 0) / 1e7
        ebitda_cr = (info.get("ebitda") or 0) / 1e7
        ebitda_margin = (info.get("ebitdaMargins") or 0) * 100
        pat_cr = (info.get("netIncomeToCommon") or 0) / 1e7
        pe_ttm = info.get("trailingPE") or 0.0
        ps_ttm = info.get("priceToSalesTrailing12Months") or 0.0
        summary = info.get("longBusinessSummary") or f"{ctx.company_name} operates in the Indian market."
        officers = info.get("companyOfficers") or []
        lead_officer = officers[0].get("name", "Management Team") if officers else "Management Team"
        lead_title = officers[0].get("title", "Promoter / Leadership") if officers else "Promoter / Leadership"

        return "\n".join([
            "```text",
            f"Company Name:           {ctx.company_name}",
            f"NSE / Yahoo Ticker:     {ctx.symbol} / {ctx.yf_symbol}",
            f"Key Leadership:         {lead_officer} ({lead_title})",
            f"Market Capitalization:  ₹{ctx.mcap_cr:,.2f} Crore",
            f"Shares Outstanding:     {shares_cr:.2f} Cr shares (Active Float: {float_cr:.2f} Cr / {float_pct:.1f}%)",
            f"TTM Revenue / P/S:      ₹{rev_cr:,.2f} Crore / {ps_ttm:.2f}×",
            f"TTM EBITDA / Margin:    ₹{ebitda_cr:,.2f} Crore / {ebitda_margin:.2f}%",
            f"TTM PAT / Trailing P/E: ₹{pat_cr:,.2f} Crore / {pe_ttm:.2f}×",
            "```",
            "",
            "### Business Model & Value Chain Summary",
            "",
            summary,
        ])
