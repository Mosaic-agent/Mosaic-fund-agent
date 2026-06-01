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


@tool
def get_db_price_summary(symbol: str) -> dict[str, Any]:
    """
    Fetch price trend summary from ClickHouse for an Indian stock.

    Returns 30/60/90/365-day close, high, low, avg volume, and percentage
    change — all computed from ``market_data.daily_prices``.

    Args:
        symbol: NSE ticker e.g. ADVENZYMES, RELIANCE, GOLDBEES

    Example: get_db_price_summary("ADVENZYMES")
    """
    try:
        from src.db.pool import query_df

        sym = symbol.upper().strip()
        df = query_df(f"""
            SELECT
                count()                                          AS trading_days,
                argMax(close, trade_date)                        AS latest_close,
                max(trade_date)                                  AS latest_date,
                min(trade_date)                                  AS earliest_date,

                -- 30-day window
                maxIf(high, trade_date >= today() - 30)          AS high_30d,
                minIf(low,  trade_date >= today() - 30)          AS low_30d,
                avgIf(close, trade_date >= today() - 30)         AS avg_close_30d,
                avgIf(volume, trade_date >= today() - 30)        AS avg_vol_30d,

                -- 60-day window
                maxIf(high, trade_date >= today() - 60)          AS high_60d,
                minIf(low,  trade_date >= today() - 60)          AS low_60d,
                avgIf(close, trade_date >= today() - 60)         AS avg_close_60d,

                -- 90-day window
                maxIf(high, trade_date >= today() - 90)          AS high_90d,
                minIf(low,  trade_date >= today() - 90)          AS low_90d,
                avgIf(close, trade_date >= today() - 90)         AS avg_close_90d,

                -- 365-day window
                maxIf(high, trade_date >= today() - 365)         AS high_1y,
                minIf(low,  trade_date >= today() - 365)         AS low_1y,
                avgIf(close, trade_date >= today() - 365)        AS avg_close_1y
            FROM market_data.daily_prices FINAL
            WHERE symbol = '{sym}'
              AND trade_date >= today() - 365
        """)

        if df.empty or df.iloc[0]["trading_days"] == 0:
            # Auto-import: trigger data refresh then retry the query
            try:
                from src.tools.agent_tools import check_and_refresh_symbol_data
                status = check_and_refresh_symbol_data.invoke({"symbol": sym})
                log.info("get_db_price_summary auto-import for %s: %s", sym, status)
                if "REFRESHED" in str(status) or "FRESH" in str(status) or "UNCHANGED" in str(status):
                    df = query_df(f"""
                        SELECT
                            count()                                          AS trading_days,
                            argMax(close, trade_date)                        AS latest_close,
                            max(trade_date)                                  AS latest_date,
                            min(trade_date)                                  AS earliest_date,
                            maxIf(high, trade_date >= today() - 30)          AS high_30d,
                            minIf(low,  trade_date >= today() - 30)          AS low_30d,
                            avgIf(close, trade_date >= today() - 30)         AS avg_close_30d,
                            avgIf(volume, trade_date >= today() - 30)        AS avg_vol_30d,
                            maxIf(high, trade_date >= today() - 60)          AS high_60d,
                            minIf(low,  trade_date >= today() - 60)          AS low_60d,
                            avgIf(close, trade_date >= today() - 60)         AS avg_close_60d,
                            maxIf(high, trade_date >= today() - 90)          AS high_90d,
                            minIf(low,  trade_date >= today() - 90)          AS low_90d,
                            avgIf(close, trade_date >= today() - 90)         AS avg_close_90d,
                            maxIf(high, trade_date >= today() - 365)         AS high_1y,
                            minIf(low,  trade_date >= today() - 365)         AS low_1y,
                            avgIf(close, trade_date >= today() - 365)        AS avg_close_1y
                        FROM market_data.daily_prices FINAL
                        WHERE symbol = '{sym}'
                          AND trade_date >= today() - 365
                    """)
                if df.empty or df.iloc[0]["trading_days"] == 0:
                    return {"symbol": sym, "error": f"No price data after auto-import ({status})."}
            except Exception as imp_exc:
                log.warning("get_db_price_summary auto-import failed for %s: %s", sym, imp_exc)
                return {"symbol": sym, "error": f"No data and auto-import failed: {imp_exc}"}

        r = df.iloc[0]

        # Compute % changes from window-start close
        def _pct(window_days: int) -> float | None:
            start_df = query_df(f"""
                SELECT close FROM market_data.daily_prices FINAL
                WHERE symbol = '{sym}'
                  AND trade_date >= today() - {window_days}
                ORDER BY trade_date ASC LIMIT 1
            """)
            if start_df.empty:
                return None
            start = float(start_df.iloc[0]["close"])
            if start == 0:
                return None
            return round((float(r["latest_close"]) - start) / start * 100, 2)

        return {
            "symbol": sym,
            "source": "ClickHouse daily_prices",
            "latest_close": round(float(r["latest_close"]), 2),
            "latest_date": str(r["latest_date"]),
            "trading_days": int(r["trading_days"]),
            "30d": {
                "change_pct": _pct(30),
                "high": round(float(r["high_30d"]), 2),
                "low": round(float(r["low_30d"]), 2),
                "avg_close": round(float(r["avg_close_30d"]), 2),
                "avg_volume": int(r["avg_vol_30d"]),
            },
            "60d": {
                "change_pct": _pct(60),
                "high": round(float(r["high_60d"]), 2),
                "low": round(float(r["low_60d"]), 2),
                "avg_close": round(float(r["avg_close_60d"]), 2),
            },
            "90d": {
                "change_pct": _pct(90),
                "high": round(float(r["high_90d"]), 2),
                "low": round(float(r["low_90d"]), 2),
                "avg_close": round(float(r["avg_close_90d"]), 2),
            },
            "1y": {
                "change_pct": _pct(365),
                "high": round(float(r["high_1y"]), 2),
                "low": round(float(r["low_1y"]), 2),
                "avg_close": round(float(r["avg_close_1y"]), 2),
            },
        }
    except Exception as exc:
        log.error("get_db_price_summary failed for %s: %s", symbol, exc)
        return {"symbol": symbol, "error": str(exc)}


# Convenience list for agent tool registration
INDIAN_EQUITY_TOOLS = [
    get_mf_holdings_for_stock,
    get_stock_cashflow,
    get_fii_dii_summary,
]
