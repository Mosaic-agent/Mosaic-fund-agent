"""
src/tools/indian_equity_tools.py
─────────────────────────────────
Supplementary LangChain tools for Indian equity deep-dive research.

Tools
-----
get_mf_holdings_for_stock  — which DSP mutual funds hold a given stock (ClickHouse)
get_stock_cashflow         — annual FCF, operating CF, capex via Yahoo Finance
get_fii_dii_summary        — latest FII/DII institutional net flows from ClickHouse
"""
from __future__ import annotations

import logging
from typing import Any

from langchain_core.tools import tool

log = logging.getLogger(__name__)


@tool
def get_mf_holdings_for_stock(company_name_or_symbol: str) -> str:
    """
    Look up which DSP mutual funds hold a given Indian stock.

    Queries ``market_data.mf_holdings`` in ClickHouse which covers 62 DSP
    funds from Sep 2023–Mar 2026 (Top 10 funds back to Jun 2022).

    Accepts either a company name (e.g. "Adani Enterprises") or NSE symbol
    (e.g. "ADANIENT").  Returns a Markdown table of fund_name, % of NAV,
    month, and market value in Crore INR.

    If no results are found, returns a message indicating the stock is not
    held by any DSP fund in the database.
    """
    query = company_name_or_symbol.strip()
    # Build LIKE pattern — split into words and require all words to appear
    words = [w for w in query.lower().split() if len(w) > 2]
    if not words:
        return f"Invalid input: {company_name_or_symbol!r}"

    # Use the longest single word for the primary filter
    primary = max(words, key=len)

    sql = f"""
SELECT
    fund_name,
    security_name,
    round(pct_of_nav, 2)       AS weight_pct,
    toString(as_of_month)      AS month,
    round(market_value_cr, 1)  AS market_value_cr
FROM market_data.mf_holdings FINAL
WHERE lower(security_name) LIKE '%{primary}%'
ORDER BY as_of_month DESC, pct_of_nav DESC
LIMIT 40
"""
    try:
        from src.db.pool import get_pool
        ch = get_pool().get_client()
        r  = ch.query(sql)
        if not r.result_rows:
            return (
                f"No DSP fund holdings found for '{company_name_or_symbol}'. "
                "The stock may not be held by any DSP fund or may not be in the database."
            )

        # Deduplicate: keep latest month per fund
        seen: dict[str, list] = {}
        for row in r.result_rows:
            fund = row[0]
            if fund not in seen:
                seen[fund] = list(row)

        lines = ["| Fund Name | Security | Weight (% NAV) | Month | Value (₹ Cr) |"]
        lines.append("|---|---|---|---|---|")
        for row in list(seen.values())[:20]:
            fund, sec, wt, month, val = row
            lines.append(f"| {fund} | {sec} | {wt}% | {month} | ₹{val}Cr |")

        header = f"**DSP Fund Holdings for '{company_name_or_symbol}'** ({len(seen)} funds)\n\n"
        return header + "\n".join(lines)
    except Exception as exc:
        log.error("get_mf_holdings_for_stock failed: %s", exc)
        return f"Error querying ClickHouse: {exc}"


@tool
def get_stock_cashflow(input_str: str) -> dict[str, Any]:
    """
    Fetch annual free cash flow, operating cash flow, and capital expenditure
    for an Indian (or US) stock using Yahoo Finance.

    Input format: ``"SYMBOL"`` or ``"SYMBOL:EXCHANGE"``

    Examples:
      "ADANIENT:NSE"  — Adani Enterprises (NSE)
      "RELIANCE:NSE"  — Reliance Industries
      "AAPL"          — Apple (US)

    Returns a dict with up to 3 years of annual FCF data plus the latest
    trailing twelve-months (TTM) free cash flow if available.
    """
    import time
    import yfinance as yf

    parts    = input_str.strip().split(":")
    symbol   = parts[0].strip().upper()
    exchange = parts[1].strip().upper() if len(parts) > 1 else "NSE"

    # Build Yahoo Finance ticker symbol
    if exchange in ("NSE", "BSE"):
        suffix = ".BO" if exchange == "BSE" else ".NS"
        yf_sym = f"{symbol}{suffix}"
    else:
        yf_sym = symbol      # US stock — no suffix

    log.info("Fetching cash flow for %s", yf_sym)
    time.sleep(0.3)

    try:
        ticker = yf.Ticker(yf_sym)
        cf = ticker.cash_flow  # Annual cash flow DataFrame
    except Exception as exc:
        return {"symbol": yf_sym, "error": f"Yahoo Finance error: {exc}"}

    if cf is None or cf.empty:
        return {"symbol": yf_sym, "error": "No cash flow data available"}

    # Normalise index to lowercase for robust key lookup
    cf.index = cf.index.str.replace(" ", "").str.lower()

    def _get_row(keys: list[str]) -> list[float]:
        for k in keys:
            k_norm = k.replace(" ", "").lower()
            if k_norm in cf.index:
                row = cf.loc[k_norm]
                return [round(float(v) / 1e6, 1) if v == v else None for v in row[:3]]
        return []

    fcf    = _get_row(["FreeCashFlow", "freecashflow"])
    op_cf  = _get_row(["OperatingCashFlow", "operatingcashflow", "CashFlowFromContinuingOperatingActivities"])
    capex  = _get_row(["CapitalExpenditure", "capitalexpenditure"])

    # Column labels (fiscal year ends)
    cols = [str(c.date()) if hasattr(c, "date") else str(c) for c in cf.columns[:3]]

    years_data = []
    for i, col in enumerate(cols):
        years_data.append({
            "fiscal_year_end":          col,
            "free_cash_flow_usd_m":     fcf[i]   if fcf   and i < len(fcf)   else None,
            "operating_cash_flow_usd_m": op_cf[i] if op_cf and i < len(op_cf) else None,
            "capex_usd_m":              capex[i] if capex and i < len(capex) else None,
        })

    return {
        "symbol":    yf_sym,
        "exchange":  exchange,
        "currency":  "INR (millions)" if exchange in ("NSE", "BSE") else "USD (millions)",
        "annual_cashflows": years_data,
        "note": (
            "Values in INR millions for Indian stocks, USD millions for US stocks. "
            "Positive FCF = healthy cash generation; negative FCF = investment phase or concern."
        ),
    }


@tool
def get_fii_dii_summary(days: int = 7) -> str:
    """
    Fetch the latest FII (Foreign Institutional Investor) and DII (Domestic
    Institutional Investor) net flow data from ClickHouse.

    Covers NSDL/CDSL daily net buy/sell figures for Indian equity markets.
    Returns a Markdown table of the last N trading days with net flows in
    Crore INR and a brief sentiment interpretation.

    Args:
        days: Number of recent trading days to show (default 7).
    """
    sql = f"""
SELECT
    toString(trade_date)  AS date,
    round(fii_net_cr, 0)  AS fii_net_cr,
    round(dii_net_cr, 0)  AS dii_net_cr
FROM market_data.fii_dii_flows FINAL
ORDER BY trade_date DESC
LIMIT {min(days, 30)}
"""
    try:
        from src.db.pool import get_pool
        ch = get_pool().get_client()
        r  = ch.query(sql)
        if not r.result_rows:
            return "No FII/DII flow data available in ClickHouse."

        lines = ["| Date | FII Net (₹ Cr) | DII Net (₹ Cr) | Combined |"]
        lines.append("|---|---|---|---|")
        total_fii = total_dii = 0.0
        for date_, fii, dii in r.result_rows:
            combined = (fii or 0) + (dii or 0)
            sig = "+" if combined > 0 else "-"
            lines.append(
                f"| {date_} | {fii:+,.0f} | {dii:+,.0f} | {sig}{abs(combined):,.0f} |"
            )
            total_fii += fii or 0
            total_dii += dii or 0

        summary = (
            f"\n**{days}-day totals:** FII {total_fii:+,.0f} Cr  |  DII {total_dii:+,.0f} Cr"
        )
        signal = (
            "Bullish (FII buying)" if total_fii > 500
            else "Bearish (FII selling)" if total_fii < -500
            else "Neutral"
        )
        summary += f"  |  Signal: **{signal}**"
        return "\n".join(lines) + summary
    except Exception as exc:
        log.error("get_fii_dii_summary failed: %s", exc)
        return f"Error fetching FII/DII data: {exc}"


# Convenience list for agent tool registration
INDIAN_EQUITY_TOOLS = [
    get_mf_holdings_for_stock,
    get_stock_cashflow,
    get_fii_dii_summary,
]
