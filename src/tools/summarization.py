"""
src/tools/summarization.py
───────────────────────────
LangChain tool for LLM-based summarization and scoring of enriched
stock data.

Given a stock's news articles, financial results, and market metrics,
this tool calls the configured LLM to generate:
  • 5 bullet investment insights
  • Risk score (1–10)
  • Sentiment score (-1.0 to +1.0)
  • Key risk signals
  • Concise investment summary

[SENSITIVE] Requires OPENAI_API_KEY or ANTHROPIC_API_KEY in .env.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.tools import tool

from config.settings import settings

logger = logging.getLogger(__name__)


# ── LLM Factory ───────────────────────────────────────────────────────────────

def _get_llm() -> Any:
    """
    Return a LangChain LLM based on settings.

    Priority:
      1. LLM_BASE_URL set → local OpenAI-compatible server (Ollama, LM Studio, etc.)
      2. LLM_PROVIDER=anthropic → Anthropic cloud
      3. Default → OpenAI cloud

    [SENSITIVE] API keys loaded from config/settings.py → .env
    """
    from langchain_openai import ChatOpenAI

    # ── Local / custom OpenAI-compatible endpoint ─────────────────────────────
    if settings.llm_base_url:
        return ChatOpenAI(
            model=settings.llm_model,
            base_url=settings.llm_base_url,
            api_key=settings.openai_api_key or "local",
            temperature=0.2,
            max_tokens=settings.llm_token_budget,
        )

    # ── OpenRouter cloud ──────────────────────────────────────────────────────
    if settings.llm_provider.lower() == "openrouter":
        return ChatOpenAI(
            model=settings.llm_model,
            base_url="https://openrouter.ai/api/v1",
            api_key=settings.openrouter_api_key,
            temperature=0.2,
            max_tokens=settings.llm_token_budget,
        )

    # ── Anthropic cloud ───────────────────────────────────────────────────────
    if settings.llm_provider.lower() == "anthropic":
        from langchain_anthropic import ChatAnthropic
        # [SENSITIVE] anthropic_api_key loaded from .env
        return ChatAnthropic(
            model=settings.llm_model,
            api_key=settings.anthropic_api_key,
            temperature=0.2,
            max_tokens=settings.llm_token_budget,
            extra_headers={"anthropic-beta": "prompt-caching-2024-07-31"},
        )

    # ── Google Gemini cloud ───────────────────────────────────────────────────
    if settings.llm_provider.lower() == "google":
        from langchain_google_genai import ChatGoogleGenerativeAI
        from src.utils.google_limiter import gemini_rate_limiter
        return ChatGoogleGenerativeAI(
            model=settings.llm_model,
            google_api_key=settings.google_api_key,
            temperature=0.2,
            max_output_tokens=settings.llm_token_budget,
            rate_limiter=gemini_rate_limiter,
        )

    # ── OpenAI cloud (default) ────────────────────────────────────────────────
    # [SENSITIVE] openai_api_key loaded from .env
    return ChatOpenAI(
        model=settings.llm_model,
        api_key=settings.openai_api_key,
        temperature=0.2,
        max_tokens=settings.llm_token_budget,
    )


# ── Prompt Template ────────────────────────────────────────────────────────────

ASSET_ANALYSIS_PROMPT = ChatPromptTemplate.from_messages(
    [
        (
            "system",
            """You are an expert Indian equity market analyst with deep knowledge of NSE and BSE listed companies.
Analyze the provided stock data and generate structured investment insights.

Always respond with valid JSON only — no markdown, no explanation outside JSON.""",
        ),
        (
            "human",
            """Analyze this Indian stock and generate a structured report.

## Stock Data
Symbol: {symbol}
Exchange: {exchange}
Instrument Type: {instrument_type}
Quantity Held: {quantity}
Average Buy Price: ₹{avg_price}
Current Price: ₹{current_price}
P&L: {pnl_pct}%

## Company Overview
Sector: {sector}
Industry: {industry}
Market Cap: {market_cap}
P/E Ratio: {pe_ratio}
P/B Ratio: {pb_ratio}
52-Week High: ₹{high_52w}
52-Week Low: ₹{low_52w}
Business Summary: {description}

## Price Momentum
30-Day Return: {return_30d}%
90-Day Return: {return_90d}%
Momentum Signal: {momentum_signal}

## Latest Quarterly Results
Period: {qr_period}
Revenue: ₹{qr_revenue} Crore
Net Profit: ₹{qr_profit} Crore
EPS: ₹{qr_eps}
Revenue Growth YoY: {qr_revenue_yoy}%
Profit Growth YoY: {qr_profit_yoy}%

## Recent News (last {news_days} days)
{news_summary}

## US Deep-Dive Report (SEC Filings, XBRL, Jobs, Peers)
{deepdive_report}

---
Generate a JSON response with exactly this structure:
{{
  "summary": "2-3 sentence overall investment summary",
  "key_insights": [
    "Insight 1",
    "Insight 2",
    "Insight 3",
    "Insight 4",
    "Insight 5"
  ],
  "risk_signals": ["risk signal 1", "risk signal 2"],
  "sentiment_score": <float between -1.0 and 1.0>,
  "risk_score": <float between 1 and 10>,
  "recommendation": "HOLD / BUY / SELL / WATCH"
}}

Risk score guide: 1=very low risk, 10=very high risk.
Sentiment score guide: -1=very bearish, 0=neutral, +1=very bullish.
Consider market context: SEBI regulations (Indian), SEC filings (US), FII flows, sector cycles, promoter holding, etc.
If a US Deep-Dive Report is provided, prioritize it for financial accuracy and long-term moat assessment.""",
        ),
    ]
)

PORTFOLIO_SUMMARY_PROMPT = ChatPromptTemplate.from_messages(
    [
        (
            "system",
            """You are a senior Indian portfolio manager and risk analyst.
Generate a portfolio-level intelligence report based on provided holdings analysis.
Respond with valid JSON only.""",
        ),
        (
            "human",
            """Analyze this portfolio of Indian equity holdings and generate portfolio-level insights.

## Portfolio Overview
Total Invested: ₹{total_invested}
Total Current Value: ₹{total_current_value}
Total P&L: ₹{total_pnl} ({total_pnl_pct}%)
Number of Holdings: {num_holdings}
ETF Count: {etf_count}
Stock Count: {stock_count}

## Sector Allocation
{sector_allocation}

## Holdings Summary
{holdings_summary}

## COMEX Pre-Market Commodity Signals
{comex_summary}

## Institutional Flow Context (Last 5 Trading Days)
{fii_dii_summary}

---
Generate a JSON response:
{{
  "health_score": <float 0-100>,
  "diversification_score": <float 0-100>,
  "portfolio_risks": [
    "risk 1",
    "risk 2",
    "risk 3"
  ],
  "actionable_insights": [
    "action 1",
    "action 2",
    "action 3",
    "action 4",
    "action 5"
  ],
  "rebalancing_signals": ["signal 1", "signal 2"]
}}

Health score guide: 100=perfectly healthy, 0=critical issues.
Diversification score: 100=well diversified, 0=heavily concentrated.
Focus on Indian market context: NSE/BSE sector cycles, FII/DII patterns, RBI policy impact.
Use COMEX signals to adjust risk assessment for commodity-linked ETFs (e.g. GOLDBEES tracks XAU Gold).
A bullish Gold (XAU) COMEX signal is a direct pre-market tailwind for GOLDBEES/gold ETFs.
A bearish Gold (XAU) COMEX signal is a headwind and increases risk for gold ETF positions.
Use the institutional flow context to assess FII/DII-driven market pressure: sustained FII
selling (net negative for ≥3 days) is a meaningful risk signal for broad equity positions.""",
        ),
    ]
)


# ── COMEX prompt formatter ─────────────────────────────────────────────────────

def _format_comex_for_prompt(comex_signals: dict[str, Any]) -> str:
    """
    Render COMEX signals as a compact, LLM-readable string for injection
    into the portfolio summary prompt.

    Returns a plain string (never raises).
    """
    if not comex_signals or "error" in comex_signals or "commodities" not in comex_signals:
        return "COMEX data unavailable (no API key or fetch failed)."

    commodities = comex_signals.get("commodities", {})
    overall     = comex_signals.get("overall_signal", "UNKNOWN")
    pre_market  = comex_signals.get("pre_market", False)
    timing      = "pre-market" if pre_market else "intraday"

    lines = [f"Overall COMEX signal: {overall} ({timing})"]
    for sym, cdata in commodities.items():
        pct     = f"{cdata['change_pct']:+.2f}%" if cdata.get("change_pct") is not None else "N/A"
        etfs    = ", ".join(cdata.get("nse_etfs", [])) or "no direct NSE ETF"
        emoji   = cdata.get("emoji", "")
        signal  = cdata.get("signal", "UNKNOWN")
        lines.append(
            f"  {emoji} {cdata.get('name', sym)} ({sym}): {signal} {pct}"
            f"  |  NSE exposure: {etfs}"
        )
    return "\n".join(lines)


def _format_fii_dii_for_prompt(institutional_flows: dict[str, Any]) -> str:
    """
    Render FII/DII institutional flow context as a compact, LLM-readable string.

    Returns a plain string (never raises).
    """
    if not institutional_flows:
        return "FII/DII institutional flow data unavailable."
    summary = institutional_flows.get("summary_str", "")
    return summary if summary else "FII/DII institutional flow data unavailable."


def _format_deepdive_for_llm(deepdive_data: dict[str, Any]) -> str:
    """Render structured deepdive ClickHouse data as a Markdown context block."""
    lines: list[str] = []
    report_date = deepdive_data.get("report_date", "unknown")
    age_days = deepdive_data.get("age_days", 0)
    lines.append(
        f"**SEC Filing Structured Data (ClickHouse, report date: {report_date}, {age_days} days ago)**"
    )

    financials = deepdive_data.get("financials") or []
    if financials:
        lines.append("")
        lines.append("**Annual Financials (USD millions)**")
        lines.append("| FY | Revenue | Gross Margin | Op. Margin | Net Income | FCF | R&D |")
        lines.append("|---|---|---|---|---|---|---|")
        for f in financials[-5:]:
            fy  = f.get("fiscal_year", "")
            rev = f.get("revenue_usd_m") or 0
            gm  = f.get("gross_margin_pct") or 0
            om  = f.get("operating_margin_pct") or 0
            ni  = f.get("net_income_usd_m") or 0
            fcf = f.get("free_cash_flow_usd_m") or 0
            rd  = f.get("rd_expense_usd_m") or 0
            lines.append(
                f"| {fy} | ${rev:,.0f}M | {gm:.1f}% | {om:.1f}% "
                f"| ${ni:,.0f}M | ${fcf:,.0f}M | ${rd:,.0f}M |"
            )

    val = deepdive_data.get("valuation") or {}
    if val:
        lines.append("")
        lines.append("**Valuation Multiples**")
        pe_t   = val.get("pe_trailing") or 0
        pe_f   = val.get("pe_forward") or 0
        ev_rev = val.get("ev_revenue") or 0
        ev_eb  = val.get("ev_ebitda") or 0
        fcf_y  = val.get("fcf_yield_pct") or 0
        mktcap = val.get("market_cap_usd_b") or 0
        lines.append(
            f"P/E (trailing): {pe_t:.1f}x | Forward P/E: {pe_f:.1f}x | "
            f"EV/Revenue: {ev_rev:.1f}x | EV/EBITDA: {ev_eb:.1f}x | "
            f"FCF Yield: {fcf_y:.1f}% | Market Cap: ${mktcap:.1f}B"
        )
        peer_pe     = val.get("peer_pe_median") or 0
        peer_ev_eb  = val.get("peer_ev_ebitda_median") or 0
        peer_ev_rev = val.get("peer_ev_revenue_median") or 0
        if peer_pe or peer_ev_eb or peer_ev_rev:
            lines.append(
                f"Peer medians \u2014 P/E: {peer_pe:.1f}x | EV/EBITDA: {peer_ev_eb:.1f}x "
                f"| EV/Revenue: {peer_ev_rev:.1f}x"
            )

    segments = deepdive_data.get("segments") or []
    if segments:
        lines.append("")
        lines.append("**Revenue Segments (latest fiscal year)**")
        for s in segments[:8]:
            name = s.get("name", "")
            rev  = s.get("revenue_usd_m") or 0
            yoy  = s.get("yoy_growth_pct") or 0
            lines.append(f"- {name}: ${rev:,.0f}M ({yoy:+.1f}% YoY)")

    return "\n".join(lines)


def _build_deepdive_context(asset_data: dict[str, Any]) -> str:
    """
    Combine structured deepdive table data and/or the Gemini narrative report
    into a single context block for the LLM prompt.
    """
    parts: list[str] = []

    dd = asset_data.get("deepdive_data")
    if dd:
        parts.append(_format_deepdive_for_llm(dd))

    text_report = asset_data.get("deepdive_report")
    if text_report:
        max_chars = 3000
        excerpt = text_report[:max_chars]
        if len(text_report) > max_chars:
            excerpt += "\n\u2026 [report truncated]"
        parts.append("\n**Narrative Report Excerpt**\n" + excerpt)

    return "\n\n".join(parts) if parts else "No deep-dive report available for this asset."


def summarize_asset(asset_data: dict[str, Any]) -> dict[str, Any]:
    """
    Generate AI investment insights for a single stock/ETF.

    Args:
        asset_data: Dict containing all enriched data for one holding.

    Returns:
        Dict with summary, key_insights, risk_signals, sentiment_score, risk_score.
    """
    llm = _get_llm()
    parser = JsonOutputParser()
    chain = ASSET_ANALYSIS_PROMPT | llm | parser

    # Format news into a readable string
    news_items = asset_data.get("news_items", [])
    if news_items:
        news_summary = "\n".join(
            f"- [{item.get('sentiment', 'NEUTRAL')}] {item.get('title', '')} "
            f"({item.get('source', '')})"
            for item in news_items[:settings.news_articles_per_stock]
        )
    else:
        news_summary = "No recent news available."

    qr = asset_data.get("quarterly_result", {}) or {}
    yf_data = asset_data.get("yahoo_data", {}) or {}
    momentum = asset_data.get("momentum", {}) or {}

    try:
        result = chain.invoke(
            {
                "symbol": asset_data.get("symbol", ""),
                "exchange": asset_data.get("exchange", "NSE"),
                "instrument_type": asset_data.get("instrument_type", "STOCK"),
                "quantity": asset_data.get("quantity", 0),
                "avg_price": round(asset_data.get("average_buy_price", 0), 2),
                "current_price": round(asset_data.get("current_price", 0), 2),
                "pnl_pct": round(asset_data.get("pnl_percent", 0), 2),
                "sector": yf_data.get("sector", "N/A"),
                "industry": yf_data.get("industry", "N/A"),
                "market_cap": yf_data.get("market_cap", "N/A"),
                "pe_ratio": yf_data.get("pe_ratio", "N/A"),
                "pb_ratio": yf_data.get("pb_ratio", "N/A"),
                "high_52w": yf_data.get("52_week_high", "N/A"),
                "low_52w": yf_data.get("52_week_low", "N/A"),
                "description": (yf_data.get("description", "") or "")[:min(1200, settings.llm_prompt_budget // 20)],
                "return_30d": momentum.get("return_30d_pct", "N/A"),
                "return_90d": momentum.get("return_90d_pct", "N/A"),
                "momentum_signal": momentum.get("momentum_signal", "N/A"),
                "qr_period": qr.get("period", "N/A"),
                "qr_revenue": qr.get("revenue_cr", "N/A"),
                "qr_profit": qr.get("net_profit_cr", "N/A"),
                "qr_eps": qr.get("eps", "N/A"),
                "qr_revenue_yoy": qr.get("revenue_yoy_pct", "N/A"),
                "qr_profit_yoy": qr.get("profit_yoy_pct", "N/A"),
                "news_days": settings.news_lookback_days,
                "news_summary": news_summary,
                "deepdive_report": _build_deepdive_context(asset_data),
            }
        )
        return result

    except Exception as exc:
        logger.error("LLM summarization failed for %s: %s", asset_data.get("symbol"), exc)
        return {
            "summary": "Analysis could not be completed.",
            "key_insights": [],
            "risk_signals": ["LLM analysis failed"],
            "sentiment_score": 0.0,
            "risk_score": 5.0,
            "recommendation": "HOLD",
        }


def summarize_portfolio(portfolio_data: dict[str, Any]) -> dict[str, Any]:
    """
    Generate portfolio-level intelligence from aggregated holding analyses.

    Args:
        portfolio_data: Dict with portfolio totals and per-holding summaries.

    Returns:
        Dict with health_score, diversification_score, portfolio_risks, actionable_insights.
    """
    llm = _get_llm()
    parser = JsonOutputParser()
    chain = PORTFOLIO_SUMMARY_PROMPT | llm | parser

    # Format sector allocation
    sector_alloc = portfolio_data.get("sector_allocation", {})
    sector_str = "\n".join(
        f"  {sector}: {pct:.1f}%" for sector, pct in sorted(
            sector_alloc.items(), key=lambda x: x[1], reverse=True
        )
    ) or "No sector data available."

    # Format COMEX signals
    comex_str = _format_comex_for_prompt(portfolio_data.get("comex_signals", {}))

    # Format FII/DII institutional flow context
    fii_dii_str = _format_fii_dii_for_prompt(portfolio_data.get("institutional_flows", {}))

    # Format holdings summary
    holdings = portfolio_data.get("holdings_analysis", [])
    holdings_str = "\n".join(
        f"  {h.get('symbol', '')}: {h.get('instrument_type', 'STOCK')}, "
        f"P&L={h.get('pnl_percent', 0):.1f}%, "
        f"Risk={h.get('risk_score', 5):.0f}/10, "
        f"Sentiment={h.get('sentiment_score', 0):.2f}"
        for h in holdings
    ) or "No holdings data."

    try:
        result = chain.invoke(
            {
                "total_invested": f"{portfolio_data.get('total_invested', 0):,.0f}",
                "total_current_value": f"{portfolio_data.get('total_current_value', 0):,.0f}",
                "total_pnl": f"{portfolio_data.get('total_pnl', 0):,.0f}",
                "total_pnl_pct": f"{portfolio_data.get('total_pnl_pct', 0):.2f}",
                "num_holdings": portfolio_data.get("num_holdings", 0),
                "etf_count": portfolio_data.get("etf_count", 0),
                "stock_count": portfolio_data.get("stock_count", 0),
                "sector_allocation": sector_str,
                "holdings_summary": holdings_str,
                "comex_summary": comex_str,
                "fii_dii_summary": fii_dii_str,
            }
        )
        return result

    except Exception as exc:
        logger.error("Portfolio LLM summarization failed: %s", exc)
        return {
            "health_score": 50.0,
            "diversification_score": 50.0,
            "portfolio_risks": ["Portfolio analysis could not be completed."],
            "actionable_insights": [],
            "rebalancing_signals": [],
        }


# ── LangChain Tools ───────────────────────────────────────────────────────────

@tool
def analyze_stock_with_llm(asset_data_json: str) -> str:
    """
    Use LLM to generate investment insights for a single Indian stock.

    Input: JSON string with enriched asset data (symbol, news, quarterly results,
           Yahoo Finance metrics, momentum data, P&L).

    Returns: JSON string with summary, key_insights (5 bullets), risk_signals,
             sentiment_score (-1 to +1), risk_score (1-10), recommendation.
    """
    try:
        data = json.loads(asset_data_json)
        result = summarize_asset(data)
        return json.dumps(result)
    except Exception as exc:
        logger.error("analyze_stock_with_llm failed: %s", exc)
        return json.dumps({"error": str(exc)})


@tool
def analyze_portfolio_with_llm(portfolio_data_json: str) -> str:
    """
    Use LLM to generate portfolio-level intelligence and risk assessment.

    Input: JSON string with portfolio totals, sector allocation, and
           per-holding analyses.

    Returns: JSON string with health_score (0-100), diversification_score (0-100),
             portfolio_risks list, actionable_insights list, rebalancing_signals list.
    """
    try:
        data = json.loads(portfolio_data_json)
        result = summarize_portfolio(data)
        return json.dumps(result)
    except Exception as exc:
        logger.error("analyze_portfolio_with_llm failed: %s", exc)
        return json.dumps({"error": str(exc)})


# Convenience list
SUMMARIZATION_TOOLS = [analyze_stock_with_llm, analyze_portfolio_with_llm]
