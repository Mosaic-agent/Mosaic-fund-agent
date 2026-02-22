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
            max_tokens=1024,
        )

    # ── Anthropic cloud ───────────────────────────────────────────────────────
    if settings.llm_provider.lower() == "anthropic":
        from langchain_anthropic import ChatAnthropic
        # [SENSITIVE] anthropic_api_key loaded from .env
        return ChatAnthropic(
            model=settings.llm_model,
            api_key=settings.anthropic_api_key,
            temperature=0.2,
            max_tokens=1024,
        )

    # ── OpenAI cloud (default) ────────────────────────────────────────────────
    # [SENSITIVE] openai_api_key loaded from .env
    return ChatOpenAI(
        model=settings.llm_model,
        api_key=settings.openai_api_key,
        temperature=0.2,
        max_tokens=1024,
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
Consider Indian market context: SEBI regulations, FII flows, sector cycles, promoter holding, etc.""",
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
Focus on Indian market context: NSE/BSE sector cycles, FII/DII patterns, RBI policy impact.""",
        ),
    ]
)


# ── Core summarization functions ──────────────────────────────────────────────

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
            for item in news_items[:5]
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
                "description": (yf_data.get("description", "") or "")[:300],
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


# ── Demo / Rule-based Scoring (no LLM required) ──────────────────────────────

def summarize_asset_demo(asset_data: dict[str, Any]) -> dict[str, Any]:
    """
    Rule-based investment scoring — no LLM API call required.
    Used by the --demo CLI flag to test the full pipeline without API keys.

    Scoring formula:
      sentiment_score = momentum contribution + news contribution + pnl contribution
      risk_score      = base (ETF=3.5, STOCK=5.0) + volatility adjustment
    """
    symbol = asset_data.get("symbol", "UNKNOWN")
    instrument_type = asset_data.get("instrument_type", "STOCK")
    pnl_pct = float(asset_data.get("pnl_percent", 0.0) or 0.0)
    momentum = asset_data.get("momentum", {}) or {}
    news_items = asset_data.get("news_items", [])
    qr = asset_data.get("quarterly_result", {}) or {}
    yf = asset_data.get("yahoo_data", {}) or {}

    ret_30d = float(momentum.get("return_30d_pct", 0.0) or 0.0)
    ret_90d = float(momentum.get("return_90d_pct", 0.0) or 0.0)
    mom_signal = momentum.get("momentum_signal", "NEUTRAL")

    # News sentiment tally
    sentiments = [n.get("sentiment", "NEUTRAL") for n in news_items[:5]]
    pos = sum(1 for s in sentiments if s == "POSITIVE")
    neg = sum(1 for s in sentiments if s == "NEGATIVE")
    total_news = len(sentiments)

    # Sentiment components
    mom_contrib = max(-0.5, min(0.5, ret_30d / 20.0 * 0.4))
    news_contrib = ((pos - neg) / total_news * 0.3) if total_news else 0.0
    pnl_contrib = max(-0.3, min(0.3, pnl_pct / 15.0 * 0.2))
    sentiment_score = round(max(-1.0, min(1.0, mom_contrib + news_contrib + pnl_contrib)), 3)

    # Risk score
    base_risk = 3.5 if instrument_type == "ETF" else 5.0
    volatility_adj = min(3.0, abs(ret_90d) / 30.0)
    risk_score = round(max(1.0, min(10.0, base_risk + volatility_adj)), 1)

    # Risk signals
    risk_signals: list[str] = []
    if mom_signal == "BEARISH":
        risk_signals.append(f"Negative momentum: {ret_30d:+.1f}% (30d)")
    if pnl_pct < -10:
        risk_signals.append(f"Loss position: {pnl_pct:.1f}% P&L")
    profit_yoy = qr.get("profit_yoy_pct") if qr else None
    if profit_yoy and float(profit_yoy) < -20:
        risk_signals.append("Declining profitability YoY")
    if neg > pos and total_news > 0:
        risk_signals.append(f"Negative news bias ({neg} neg vs {pos} pos articles)")
    risk_signals = risk_signals or ["No significant risk signals detected"]

    # Recommendation
    if sentiment_score > 0.3 and risk_score < 5:
        recommendation = "BUY"
    elif pnl_pct < -20 or risk_score > 7.5:
        recommendation = "SELL"
    elif sentiment_score < -0.2 or risk_score > 6.5:
        recommendation = "WATCH"
    else:
        recommendation = "HOLD"

    # Build key insights from available data
    pe = yf.get("pe_ratio") or "N/A"
    sector = yf.get("sector") or "N/A"
    industry = yf.get("industry") or "N/A"
    market_cap = yf.get("market_cap") or "N/A"
    high_52w = yf.get("52_week_high") or "N/A"
    low_52w = yf.get("52_week_low") or "N/A"

    key_insights = [
        f"Price momentum: {ret_30d:+.1f}% (30d) | {ret_90d:+.1f}% (90d) → {mom_signal}",
        f"Sector: {sector} | Industry: {industry}",
        f"P/E: {pe} | Market Cap: {market_cap} | 52W H/L: ₹{high_52w}/₹{low_52w}",
        (
            f"Quarterly profit growth YoY: {profit_yoy}%"
            if profit_yoy is not None
            else "Quarterly results not available"
        ),
        (
            f"News: {pos} positive, {neg} negative, {total_news - pos - neg} neutral "
            f"({total_news} articles)"
            if total_news
            else "No recent news found"
        ),
    ]

    summary = (
        f"{symbol} ({instrument_type}) — {mom_signal} momentum with "
        f"{ret_30d:+.1f}% 30-day return. "
        f"P&L: {pnl_pct:+.1f}%. Rule-based assessment: {recommendation}."
    )

    return {
        "summary": summary,
        "key_insights": key_insights,
        "risk_signals": risk_signals,
        "sentiment_score": sentiment_score,
        "risk_score": risk_score,
        "recommendation": recommendation,
    }


def summarize_portfolio_demo(portfolio_data: dict[str, Any]) -> dict[str, Any]:
    """
    Rule-based portfolio scoring — no LLM API call required.
    Used by the --demo CLI flag to test the full pipeline without API keys.
    """
    holdings = portfolio_data.get("holdings_analysis", [])
    sector_alloc: dict[str, float] = portfolio_data.get("sector_allocation", {})
    total_pnl_pct = float(portfolio_data.get("total_pnl_pct", 0.0) or 0.0)
    num_holdings = int(portfolio_data.get("num_holdings", 0) or 0)

    # Diversification score from sector spread
    if sector_alloc:
        equity_sectors = {k: v for k, v in sector_alloc.items()
                         if k not in ("Index ETF", "Liquid ETF", "Commodity ETF", "Sectoral ETF")}
        num_sectors = len(equity_sectors)
        max_sector = max(sector_alloc.values(), default=100.0)
        div_score = min(100.0, (min(num_sectors, 8) / 8) * 60 + (1 - max_sector / 100) * 40)
    else:
        div_score = 50.0

    # Health score
    avg_risk = sum(h.get("risk_score", 5) for h in holdings) / max(len(holdings), 1)
    health_base = 100.0 - (avg_risk - 1.0) * 10.0
    pnl_bonus = max(-20.0, min(20.0, total_pnl_pct))
    health_score = round(max(0.0, min(100.0, health_base + pnl_bonus)), 1)
    div_score = round(div_score, 1)

    # Portfolio risks
    risks: list[str] = []
    if sector_alloc:
        max_sector_name = max(sector_alloc, key=sector_alloc.get)
        max_pct = sector_alloc[max_sector_name]
        if max_pct > 40:
            risks.append(f"High concentration in {max_sector_name}: {max_pct:.1f}% of portfolio")
    if num_holdings < 5:
        risks.append(f"Under-diversified: only {num_holdings} holdings")
    if total_pnl_pct < -10:
        risks.append(f"Portfolio in significant loss: {total_pnl_pct:.1f}% overall P&L")
    bearish = sum(1 for h in holdings if h.get("sentiment_score", 0) < -0.2)
    if bearish > len(holdings) // 2:
        risks.append(f"{bearish}/{len(holdings)} holdings showing bearish sentiment")
    risks = risks or ["No critical portfolio risks identified"]

    # Actionable insights
    insights: list[str] = []
    if holdings:
        top_g = max(holdings, key=lambda h: h.get("pnl_percent", 0))
        top_l = min(holdings, key=lambda h: h.get("pnl_percent", 0))
        insights.append(
            f"Best performer: {top_g['symbol']} ({top_g.get('pnl_percent', 0):+.1f}% P&L) "
            f"— review if target allocation is exceeded"
        )
        if top_l.get("pnl_percent", 0) < -5:
            insights.append(
                f"Underperformer: {top_l['symbol']} ({top_l.get('pnl_percent', 0):.1f}%) "
                f"— reassess fundamentals"
            )

    etf_pct = sum(1 for h in holdings if h.get("instrument_type") == "ETF") / max(num_holdings, 1) * 100
    if etf_pct < 10:
        insights.append("Consider increasing ETF allocation for core portfolio stability")
    if len(sector_alloc) < 4:
        insights.append("Add holdings from more sectors to improve diversification")
    high_risk_names = [h["symbol"] for h in holdings if h.get("risk_score", 5) > 7]
    if high_risk_names:
        insights.append(f"High-risk holdings ({', '.join(high_risk_names)}) — verify position sizing")
    insights = insights or ["Portfolio appears balanced. Monitor earnings and macro events."]

    # Rebalancing signals
    rebalancing: list[str] = []
    if sector_alloc and max(sector_alloc.values(), default=0) > 35:
        top_s = max(sector_alloc, key=sector_alloc.get)
        rebalancing.append(f"Trim {top_s} sector below 30% of portfolio")
    watch = [h["symbol"] for h in holdings if h.get("risk_score", 5) > 6 and h.get("pnl_percent", 0) < 0]
    if watch:
        rebalancing.append(f"Re-evaluate risk/reward for: {', '.join(watch)}")
    rebalancing = rebalancing or ["No immediate rebalancing required"]

    return {
        "health_score": health_score,
        "diversification_score": div_score,
        "portfolio_risks": risks[:3],
        "actionable_insights": insights[:5],
        "rebalancing_signals": rebalancing[:3],
    }


# ── LangChain Tool ────────────────────────────────────────────────────────────

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
