"""
src/agents/fast_path_router.py
───────────────────────────────
Zero-Latency Deterministic Fast-Path Router for Mosaic Agent.

Intercepts direct data lookups (Shoonya quotes, ETF iNAV spreads, DSP mutual fund holdings,
and intraday snapshots) before executing LLM ReAct planning loops.

Returns a structured dict with `{"handled": True, "response": "... "}` when a match is found,
or `None` to pass execution to the agentic LLM planner.
"""

from __future__ import annotations

import re
import logging
from typing import Any

logger = logging.getLogger(__name__)

# Patterns for fast-path intercept
_QUOTE_PATTERN = re.compile(
    r"^(?:get\s+)?(?:live\s+)?(?:shoonya\s+)?(?:quote|price|ltp)\s+(?:for\s+)?([a-zA-Z0-9_-]+)$",
    re.IGNORECASE
)
_INAV_PATTERN = re.compile(
    r"^(?:get\s+)?(?:live\s+)?inav\s+(?:for\s+)?([a-zA-Z0-9_-]+)$",
    re.IGNORECASE
)
_DSP_HOLDINGS_PATTERN = re.compile(
    r"^(?:dsp\s+)?(?:fund\s+)?holdings\s+(?:for\s+)?([a-zA-Z0-9_-]+)$",
    re.IGNORECASE
)
_INTRADAY_PATTERN = re.compile(
    r"^(?:intraday\s+)(?:data\s+)?(?:for\s+)?([a-zA-Z0-9_-]+)$",
    re.IGNORECASE
)


_COMEX_PATTERN = re.compile(
    r"^(?:comex|comex\s+gold|pre\s*market\s*gold|metals\s*scorecard|gold\s*futures|silver\s*futures|copper\s*futures)$",
    re.IGNORECASE
)
_GOLDBEES_PATTERN = re.compile(
    r"^(?:goldbees|goldbees\s+signal|goldbees\s+pipeline|today's\s+goldbees|goldbees\s+report)$",
    re.IGNORECASE
)
_MACD_PATTERN = re.compile(
    r"^(?:plot\s+)?(?:macd|mcad)\s+(?:chart\s+)?(?:for\s+)?([a-zA-Z0-9_.:-]+)$|"
    r"^(?:plot\s+)?([a-zA-Z0-9_.:-]+)\s+(?:macd|mcad)(?:\s+chart)?$",
    re.IGNORECASE
)


def try_fast_path(question: str) -> dict[str, Any] | None:
    """
    Attempt to fulfill the user's query deterministically without LLM calls.

    Returns:
        dict with {"handled": True, "intent": "fast_path", "response": "... "} if handled.
        None if question requires LLM reasoning/planning.
    """
    q_clean = question.strip().lower()

    # 1. Direct COMEX / Metals Lookup
    if _COMEX_PATTERN.match(q_clean):
        res = _handle_comex_lookup()
        if res:
            return {"handled": True, "intent": "fast_path_comex", "response": res}

    # 2. Direct GOLDBEES Pipeline Signal Lookup
    if _GOLDBEES_PATTERN.match(q_clean):
        res = _handle_goldbees_lookup()
        if res:
            return {"handled": True, "intent": "fast_path_goldbees", "response": res}

    # 3. Direct Quote / LTP Lookup
    match_quote = _QUOTE_PATTERN.match(q_clean)
    if match_quote:
        symbol = match_quote.group(1).upper()
        res = _handle_quote_lookup(symbol)
        if res:
            return {"handled": True, "intent": "fast_path_quote", "response": res}

    # 4. Direct iNAV Lookup
    match_inav = _INAV_PATTERN.match(q_clean)
    if match_inav:
        symbol = match_inav.group(1).upper()
        res = _handle_inav_lookup(symbol)
        if res:
            return {"handled": True, "intent": "fast_path_inav", "response": res}

    # 5. DSP Holdings Lookup
    match_dsp = _DSP_HOLDINGS_PATTERN.match(q_clean)
    if match_dsp:
        symbol = match_dsp.group(1).upper()
        res = _handle_dsp_holdings_lookup(symbol)
        if res:
            return {"handled": True, "intent": "fast_path_dsp", "response": res}

    # 6. Intraday Snapshot Lookup
    match_intra = _INTRADAY_PATTERN.match(q_clean)
    if match_intra:
        symbol = match_intra.group(1).upper()
        res = _handle_intraday_lookup(symbol)
        if res:
            return {"handled": True, "intent": "fast_path_intraday", "response": res}

    # 7. MACD / MCAD Chart Lookup
    match_macd = _MACD_PATTERN.match(q_clean)
    if match_macd:
        symbol = (match_macd.group(1) or match_macd.group(2)).upper()
        res = _handle_macd_lookup(symbol)
        if res:
            return {"handled": True, "intent": "fast_path_macd", "response": res}

    # 8. Single-Metric Stock Lookup (Market Cap, P/E, P/B, Dividend Yield, 52-Week Range via Yahoo Finance)
    try:
        from src.agents.sub_agents.india_equity import try_quick_stat_answer
        quick_stat_res = try_quick_stat_answer(question)
        if quick_stat_res:
            return {"handled": True, "intent": "fast_path_quick_stat", "response": quick_stat_res}
    except Exception as exc:
        logger.debug("Fast-path quick stat lookup failed for %s: %s", question, exc)

    return None



def _handle_quote_lookup(symbol: str) -> str | None:
    """Fetch direct quote via Shoonya API or yfinance fallback."""
    try:
        from src.importer.fetchers.shoonya_fetcher import get_shoonya_api
        api = get_shoonya_api()
        if api:
            res = api.searchscrip(exchange="NSE", searchtext=symbol)
            if res and res.get("values"):
                tok = res["values"][0]["token"]
                tsym = res["values"][0].get("tsym", symbol)
                cname = res["values"][0].get("cname", symbol)
                q = api.get_quotes(exchange="NSE", token=tok)
                if q and q.get("stat") == "Ok":
                    lp = float(q.get("lp", 0))
                    c = float(q.get("c", 0))
                    o = float(q.get("o", 0))
                    h = float(q.get("h", 0))
                    l = float(q.get("l", 0))
                    ap = float(q.get("ap", 0))
                    v = int(q.get("v", 0))
                    chg = lp - c if c > 0 else 0
                    pct = (chg / c) * 100 if c > 0 else 0

                    return f"""### **Live Quote: {tsym} ({cname})**
*Source: Shoonya REST API (Zero-Latency Deterministic Router)*

* **Last Price (LTP)**: **₹{lp:.2f}** ({pct:+.2f}%)
* **Previous Close**: ₹{c:.2f}
* **Day's Range**: ₹{l:.2f} – ₹{h:.2f} (Open: ₹{o:.2f})
* **Intraday VWAP**: ₹{ap:.2f}
* **Volume**: {v:,} shares
* **Depth (B/S)**: {q.get('tbq', 0):,} buy qty / {q.get('tsq', 0):,} sell qty
"""
    except Exception as exc:
        logger.debug("Shoonya quote lookup failed for %s: %s", symbol, exc)

    # Fallback to Yahoo Finance quote
    try:
        from src.tools.yahoo_finance import get_yahoo_finance_data
        yf_data = get_yahoo_finance_data.invoke({"symbol": symbol})
        if yf_data and yf_data.get("current_price_inr"):
            price = yf_data.get("current_price_inr")
            yoy = yf_data.get("price_yoy_change_pct")
            yoy_str = f" ({yoy:+.2f}% YoY)" if yoy is not None else ""
            return f"""### **Quote: {symbol}**
*Source: Yahoo Finance API (Zero-Latency Fast-Path)*

* **Current Price**: **₹{price:,.2f}**{yoy_str}
* **Market Cap**: {yf_data.get('market_cap_formatted', 'N/A')}
* **52-Week Range**: ₹{yf_data.get('52_week_low', 0):,.2f} – ₹{yf_data.get('52_week_high', 0):,.2f}
* **P/E Ratio**: {yf_data.get('pe_ratio', 0):.2f} | **P/B Ratio**: {yf_data.get('pb_ratio', 0):.2f}
* **Dividend Yield**: {yf_data.get('dividend_yield_pct', 0):.2f}%
* **Sector / Industry**: {yf_data.get('sector', '')} / {yf_data.get('industry', '')}
"""
    except Exception as exc:
        logger.debug("Fast-path Yahoo quote fallback failed for %s: %s", symbol, exc)

    return None


def _handle_inav_lookup(symbol: str) -> str | None:
    """Fetch live iNAV and calculate premium/discount percentage."""
    try:
        from src.importer.fetchers.shoonya_fetcher import get_shoonya_api
        from src.importer.fetchers.nse_inav_fetcher import fetch_inav_snapshots

        snaps = fetch_inav_snapshots([symbol])
        inav_val = None
        if snaps:
            inav_val = snaps[0].get("inav")

        api = get_shoonya_api()
        ltp = None
        if api:
            res = api.searchscrip(exchange="NSE", searchtext=symbol)
            if res and res.get("values"):
                q = api.get_quotes(exchange="NSE", token=res["values"][0]["token"])
                if q and q.get("stat") == "Ok":
                    ltp = float(q.get("lp", 0))

        if ltp and inav_val and inav_val > 0:
            diff = ltp - inav_val
            prem_pct = (diff / inav_val) * 100
            status = "🔴 PREMIUM" if prem_pct > 0 else "🟢 DISCOUNT"
            return f"""### **Live iNAV Snapshot: {symbol}**
*Source: NSE iNAV API & Shoonya Feed*

* **Market Price (LTP)**: **₹{ltp:.2f}**
* **Indicative NAV (iNAV)**: **₹{inav_val:.2f}**
* **Spread**: **{prem_pct:+6.2f}%** ({status})
"""
    except Exception as exc:
        logger.debug("Fast-path iNAV lookup failed for %s: %s", symbol, exc)
    return None


def _handle_dsp_holdings_lookup(symbol: str) -> str | None:
    """Query DSP active fund holdings for symbol."""
    try:
        from src.db.pool import query_df
        df = query_df(
            f"""
            SELECT fund_name, as_of_month, market_value_cr, pct_of_nav
            FROM market_data.mf_holdings FINAL
            WHERE security_name LIKE '%{symbol}%' OR isin LIKE '%{symbol}%'
            ORDER BY as_of_month DESC, market_value_cr DESC
            LIMIT 10
            """
        )
        if not df.empty:
            rows_str = "\n".join([
                f"* **{r['fund_name']}** ({r['as_of_month'][:7]}): ₹{r['market_value_cr']:.2f} Cr ({r['pct_of_nav']:.2f}% of NAV)"
                for _, r in df.iterrows()
            ])
            return f"""### **DSP Active Fund Holdings: {symbol}**
*Source: ClickHouse `market_data.mf_holdings`*

{rows_str}
"""
    except Exception as exc:
        logger.debug("Fast-path DSP holdings lookup failed for %s: %s", symbol, exc)
    return None


def _handle_intraday_lookup(symbol: str) -> str | None:
    """Fetch live intraday order flow and tick metrics."""
    try:
        from src.importer.fetchers.shoonya_fetcher import get_shoonya_api
        api = get_shoonya_api()
        if api:
            res = api.searchscrip(exchange="NSE", searchtext=symbol)
            if res and res.get("values"):
                tok = res["values"][0]["token"]
                q = api.get_quotes(exchange="NSE", token=tok)
                if q and q.get("stat") == "Ok":
                    lp = float(q.get("lp", 0))
                    c = float(q.get("c", 0))
                    ap = float(q.get("ap", 0))
                    v = int(q.get("v", 0))
                    tbq = int(q.get("tbq", 0))
                    tsq = int(q.get("tsq", 0))
                    pct = ((lp - c) / c) * 100 if c > 0 else 0

                    return f"""### **Intraday Snapshot: {symbol}**
*Source: Shoonya Intraday Tick Stream*

* **LTP**: **₹{lp:.2f}** ({pct:+.2f}%)
* **Intraday VWAP**: ₹{ap:.2f}
* **Traded Volume**: {v:,} shares
* **Order Depth**: Buy {tbq:,} shares vs Sell {tsq:,} shares
* **Best Bid / Ask**: ₹{q.get('bp1', 0)} ({q.get('bq1', 0)}) / ₹{q.get('sp1', 0)} ({q.get('sq1', 0)})
"""
    except Exception as exc:
        logger.debug("Fast-path intraday lookup failed for %s: %s", symbol, exc)
    return None


def _handle_comex_lookup() -> str | None:
    """Fetch pre-market COMEX metals scorecard (Gold, Silver, Copper) instantly."""
    try:
        from src.tools.skills_tools import run_comex_analysis
        res = run_comex_analysis.invoke({})
        return f"### **COMEX Pre-Market Metals Scorecard**\n*Source: COMEX Futures & CFTC COT (Zero-Latency Fast-Path)*\n\n{res}"
    except Exception as exc:
        logger.debug("Fast-path COMEX lookup failed: %s", exc)
    return None


def _handle_goldbees_lookup() -> str | None:
    """Fetch GOLDBEES ML prediction & position sizing report instantly."""
    try:
        from src.tools.skills_tools import run_goldbees_pipeline
        res = run_goldbees_pipeline.invoke({})
        return f"### **GOLDBEES ML Signal & Position Sizing Report**\n*Source: GOLDBEES ML Pipeline (Zero-Latency Fast-Path)*\n\n{res}"
    except Exception as exc:
        logger.debug("Fast-path GOLDBEES lookup failed: %s", exc)
    return None


def _handle_macd_lookup(symbol: str) -> str | None:
    """Render MACD chart directly via plot_macd_chart tool."""
    try:
        from src.tools.chart_tools import plot_macd_chart, inject_chart_placeholders
        raw_res = plot_macd_chart.invoke({"symbol": symbol, "days": 180})
        if raw_res:
            return inject_chart_placeholders(raw_res)
    except Exception as exc:
        logger.debug("Fast-path MACD lookup failed for %s: %s", symbol, exc)
    return None


