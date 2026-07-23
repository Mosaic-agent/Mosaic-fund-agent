"""
src/agents/sub_agents/equity_gatherer.py
────────────────────────────────────────
Programmatic Indian equity data gatherer for the fallback path.

`_gather_indian_equity_data` is used by `IndianEquityResearchSubAgent._fallback()`
when the configured LLM does not support tool-calling.  It collects Yahoo
overview + price momentum + 1-year chart, quarterly results, annual cash
flow, DSP mutual-fund holdings and recent news, then optionally runs an LLM
synthesis pass when context_window ≥ 12 000 tokens.
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def _gather_indian_equity_data(symbol: str, exchange: str, company_name: str, llm: Any = None) -> str:
    """
    Gather comprehensive Indian equity data via direct Python function calls.

    This is the tool-calling-free fallback path for models like gemma4 that
    do not support function/tool use.  Calls each data-source function directly
    and assembles a formatted Markdown research note.
    """
    from datetime import date as _date
    parts: list[str] = [
        f"# {company_name} ({symbol})\n"
        f"*Exchange: {exchange} \u2022 Research date: {_date.today()}*\n"
    ]

    # 1. Yahoo Finance overview + price momentum
    try:
        from src.tools.yahoo_finance import fetch_yahoo_data, fetch_price_history
        yf   = fetch_yahoo_data(symbol, exchange)
        hist = fetch_price_history(symbol, exchange, "1y")
        mc   = f"₹{yf.market_cap / 1e7:,.0f} Cr" if yf.market_cap else "N/A"

        yoy_change_str = "—"
        if len(hist) >= 2:
            latest = hist[-1]["close"]
            prev1y = hist[0]["close"]
            r1y = round((latest - prev1y) / prev1y * 100, 2) if prev1y else 0
            yoy_change_str = f"{r1y:+.2f}%"

        parts.append(
            f"## Company Snapshot\n"
            f"| Metric | Value |\n|---|---|\n"
            f"| Sector | {yf.sector or 'N/A'} |\n"
            f"| Industry | {yf.industry or 'N/A'} |\n"
            f"| Market Cap | {mc} (YoY Change: {yoy_change_str}) |\n"
            f"| P/E (Trailing) | {round(yf.pe_ratio, 1) if yf.pe_ratio else 'N/A'} |\n"
            f"| P/B | {round(yf.pb_ratio, 1) if yf.pb_ratio else 'N/A'} |\n"
            f"| Current Price | ₹{yf.current_price:,.2f} (YoY Change: {yoy_change_str}) |\n"
            f"| 52-Week High | ₹{yf.fifty_two_week_high:,.2f} |\n"
            f"| 52-Week Low | ₹{yf.fifty_two_week_low:,.2f} |\n"
        )
        if yf.description:
            parts.append(f"**Business:** {yf.description[:500]}…")
        if len(hist) >= 2:
            latest  = hist[-1]["close"]
            idx_30d = max(0, len(hist) - 22)
            idx_90d = max(0, len(hist) - 66)
            prev30  = hist[idx_30d]["close"]
            prev90  = hist[idx_90d]["close"]
            prev1y  = hist[0]["close"]
            r30 = round((latest - prev30) / prev30 * 100, 2) if prev30 else 0
            r90 = round((latest - prev90) / prev90 * 100, 2) if prev90 else 0
            r1y = round((latest - prev1y) / prev1y * 100, 2) if prev1y else 0
            sig = "BULLISH" if r30 > 5 else "BEARISH" if r30 < -5 else "NEUTRAL"
            parts.append(f"**Price Momentum:** 30d {r30:+.2f}% │ 90d {r90:+.2f}% │ 1y (YoY) {r1y:+.2f}% │ Signal: **{sig}**")

            # Volume pattern analysis
            recent_vols = [r.get("volume", 0) for r in hist if r.get("volume")]
            if len(recent_vols) >= 20:
                avg_vol_20d = sum(recent_vols[-20:]) / 20.0
                latest_vol = recent_vols[-1]
                vol_ratio = (latest_vol / avg_vol_20d) if avg_vol_20d > 0 else 1.0
                avg_vol_30d = sum(recent_vols[-30:]) / min(len(recent_vols), 30) if recent_vols else 0
                vol_sig = "HIGH EXPANSION" if vol_ratio >= 1.8 else "ACCUMULATION/DISTRIBUTION" if vol_ratio >= 1.3 else "NORMAL"
                parts.append(
                    f"**Volume Pattern:** 30d ADV: {int(avg_vol_30d):,} shares │ "
                    f"Latest Vol vs 20d MA: {vol_ratio:.2f}x │ Activity: **{vol_sig}**"
                )

            try:
                from src.tools.chart_tools import plot_price_chart
                chart_str = plot_price_chart(symbol, days=365)
                if chart_str and "No price data found" not in chart_str and "Error" not in chart_str:
                    parts.append(f"### 1-Year Price Chart\n{chart_str}")
            except Exception as chart_exc:
                logger.warning("Failed to add price chart to programmatic output: %s", chart_exc)
    except Exception as exc:
        parts.append(f"## Company Snapshot\n*Yahoo Finance unavailable: {exc}*")

    # 2. Quarterly results
    try:
        from src.tools.earnings_scraper import fetch_from_screener, fetch_from_yahoo_financials
        q = fetch_from_screener(symbol) or fetch_from_yahoo_financials(symbol, exchange)
        if q:
            parts.append(
                f"## Latest Quarterly Results ({q.period})\n"
                f"| Metric | Value |\n|---|---|\n"
                f"| Revenue | ₹{q.revenue_cr:,.0f} Cr |\n"
                f"| Net Profit | ₹{q.net_profit_cr:,.0f} Cr |\n"
                f"| EPS | ₹{q.eps:.2f} |\n"
                f"| Revenue Growth YoY | {q.revenue_yoy_pct:+.1f}% |\n"
                f"| Profit Growth YoY | {q.profit_yoy_pct:+.1f}% |\n"
                f"| EPS Growth YoY | {q.eps_yoy_pct:+.1f}% |\n"
            )
        else:
            parts.append("## Quarterly Results\n*Not available via Screener.in for this symbol.*")
    except Exception as exc:
        parts.append(f"## Quarterly Results\n*Unavailable: {exc}*")

    # 3. Cash flow
    try:
        from src.tools.indian_equity_tools import get_stock_cashflow
        cf_result = get_stock_cashflow.invoke({"input_str": f"{symbol}:{exchange}"})
        if isinstance(cf_result, dict) and cf_result.get("annual_cashflows"):
            rows = cf_result["annual_cashflows"]
            lines = ["## Annual Cash Flows\n| FY End | FCF (\u20b9M) | Op CF (\u20b9M) | Capex (\u20b9M) |", "|---|---|---|---|"]
            for r in rows:
                lines.append(
                    f"| {r['fiscal_year_end']} "
                    f"| {r.get('free_cash_flow_usd_m') or 'N/A'} "
                    f"| {r.get('operating_cash_flow_usd_m') or 'N/A'} "
                    f"| {r.get('capex_usd_m') or 'N/A'} |"
                )
            parts.append("\n".join(lines))
        else:
            parts.append("## Cash Flow\n*No cash flow data available.*")
    except Exception as exc:
        parts.append(f"## Cash Flow\n*Unavailable: {exc}*")

    # 4. DSP mutual fund holdings
    try:
        from src.tools.indian_equity_tools import get_mf_holdings_for_stock
        mf_result = get_mf_holdings_for_stock.invoke({"company_name_or_symbol": company_name})
        parts.append(
            f"## DSP Mutual Fund Holdings\n{mf_result}"
            if mf_result and "Error" not in mf_result
            else f"## DSP Mutual Fund Holdings\n*{mf_result or 'No data'}*"
        )
    except Exception as exc:
        parts.append(f"## DSP Fund Holdings\n*Unavailable: {exc}*")

    # 5. Recent news
    try:
        from src.tools.news_search import fetch_news_for_symbol
        news = fetch_news_for_symbol(symbol, company_name)
        if news:
            lines = ["## Recent News\n| Headline | Source | Sentiment |", "|---|---|---|"]
            for n in news[:8]:
                lines.append(f"| {n.title[:70]} | {n.source} | {n.sentiment.value} |")
            parts.append("\n".join(lines))
        else:
            parts.append("## Recent News\n*No recent news found.*")
    except Exception as exc:
        parts.append(f"## News\n*Unavailable: {exc}*")

    raw_data = "\n\n".join(parts)

    # LLM synthesis — only attempted when the model has enough context headroom.
    # For small-context models (e.g. gemma4 at 4k) we return the raw tables directly;
    # the data is already complete and actionable without an extra LLM call.
    from config.settings import settings
    if llm is not None and settings.llm_context_window >= 12000:
        try:
            from langchain_core.messages import HumanMessage, SystemMessage
            budget = settings.llm_prompt_budget  # chars
            data_budget = max(500, budget - 300)
            truncated = raw_data[:data_budget]
            if len(raw_data) > data_budget:
                truncated += "\n\n*[data truncated to fit context window]*"

            synthesis_prompt = (
                f"You are a senior Indian equity analyst. Below is live market data for "
                f"{company_name} ({symbol}, {exchange}). "
                f"In 3-5 concise paragraphs synthesise: (1) business quality, "
                f"(2) valuation vs sector, (3) cash flow trend, "
                f"(4) institutional sentiment, (5) a clear BUY/HOLD/SELL/WATCH verdict with one-line rationale. "
                f"Never invent numbers — use only the data provided.\n\n"
                f"--- DATA ---\n{truncated}"
            )
            from src.utils.caveman import get_caveman_prompt
            res = llm.invoke([
                SystemMessage(content="You are a concise Indian equity research analyst." + get_caveman_prompt()),
                HumanMessage(content=synthesis_prompt),
            ])
            synthesis = str(res.content).strip()
            parts.append(f"## Analyst Synthesis\n{synthesis}")
            logger.info("_gather_indian_equity_data: LLM synthesis complete (%d chars)", len(synthesis))
        except Exception as exc:
            logger.warning("_gather_indian_equity_data: LLM synthesis failed: %s", exc)
        return "\n\n".join(parts)

    return raw_data
