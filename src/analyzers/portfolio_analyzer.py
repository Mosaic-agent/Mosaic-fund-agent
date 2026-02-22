"""
src/analyzers/portfolio_analyzer.py
─────────────────────────────────────
Portfolio-level aggregation and analysis.

Given a list of AssetAnalysis results, this module computes:
  • Sector allocation breakdown (% of portfolio value)
  • Concentration risk (top holdings domination %)
  • ETF vs direct equity ratio
  • Diversification score
  • Overall portfolio health score
  • Rebalancing signals via LLM

NSE sector classifications are derived from Yahoo Finance sector tags
and a curated symbol → sector fallback map.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any

from src.models.portfolio import (
    AssetAnalysis,
    InstrumentType,
    Portfolio,
    PortfolioReport,
    PortfolioSummary,
)
from src.tools.summarization import summarize_portfolio, summarize_portfolio_demo

logger = logging.getLogger(__name__)

# ── Fallback sector map for well-known NSE symbols ────────────────────────────
# Used when Yahoo Finance sector info is missing
SYMBOL_SECTOR_FALLBACK: dict[str, str] = {
    "RELIANCE": "Energy",
    "TCS": "Technology",
    "INFY": "Technology",
    "WIPRO": "Technology",
    "HCLTECH": "Technology",
    "TECHM": "Technology",
    "LTIM": "Technology",
    "MPHASIS": "Technology",
    "PERSISTENT": "Technology",
    "COFORGE": "Technology",
    "HDFCBANK": "Financial Services",
    "ICICIBANK": "Financial Services",
    "KOTAKBANK": "Financial Services",
    "AXISBANK": "Financial Services",
    "SBIN": "Financial Services",
    "BAJFINANCE": "Financial Services",
    "BAJAJFINSV": "Financial Services",
    "INDUSINDBK": "Financial Services",
    "IDFCFIRSTB": "Financial Services",
    "PNB": "Financial Services",
    "BANKBARODA": "Financial Services",
    "CANBK": "Financial Services",
    "UNIONBANK": "Financial Services",
    "SBILIFE": "Financial Services",
    "HDFCLIFE": "Financial Services",
    "ICICIPRULI": "Financial Services",
    "MUTHOOTFIN": "Financial Services",
    "CHOLAFIN": "Financial Services",
    "HINDUNILVR": "FMCG",
    "ITC": "FMCG",
    "NESTLEIND": "FMCG",
    "BRITANNIA": "FMCG",
    "MARICO": "FMCG",
    "DABUR": "FMCG",
    "GODREJCP": "FMCG",
    "COLPAL": "FMCG",
    "EMAMILTD": "FMCG",
    "VBL": "FMCG",
    "SUNPHARMA": "Healthcare",
    "DRREDDY": "Healthcare",
    "CIPLA": "Healthcare",
    "DIVISLAB": "Healthcare",
    "LUPIN": "Healthcare",
    "BIOCON": "Healthcare",
    "TORNTPHARM": "Healthcare",
    "AUROPHARMA": "Healthcare",
    "ALKEM": "Healthcare",
    "APOLLOHOSP": "Healthcare",
    "TATAMOTORS": "Automobile",
    "MARUTI": "Automobile",
    "BAJAJ-AUTO": "Automobile",
    "HEROMOTOCO": "Automobile",
    "EICHERMOT": "Automobile",
    "TVSMOTOR": "Automobile",
    "ASHOKLEY": "Automobile",
    "MRF": "Automobile",
    "APOLLOTYRE": "Automobile",
    "LT": "Infrastructure",
    "SIEMENS": "Infrastructure",
    "ABB": "Infrastructure",
    "CUMMINSIND": "Infrastructure",
    "ADANIPORTS": "Infrastructure",
    "TATASTEEL": "Metals & Mining",
    "JSWSTEEL": "Metals & Mining",
    "HINDALCO": "Metals & Mining",
    "COALINDIA": "Metals & Mining",
    "ASIANPAINT": "Chemicals",
    "PIDILITIND": "Chemicals",
    "BERGEPAINT": "Chemicals",
    "ULTRACEMCO": "Cement",
    "GRASIM": "Cement",
    "NTPC": "Power & Utilities",
    "POWERGRID": "Power & Utilities",
    "ADANIGREEN": "Power & Utilities",
    "ADANIPOWER": "Power & Utilities",
    "ONGC": "Oil & Gas",
    "BPCL": "Oil & Gas",
    "IOC": "Oil & Gas",
    "GAIL": "Oil & Gas",
    "PETRONET": "Oil & Gas",
    "BHARTIARTL": "Telecom",
    "TITAN": "Consumer Discretionary",
    "HAVELLS": "Consumer Discretionary",
    "M&M": "Automobile",
    "TATACONSUM": "FMCG",
    "ADANIENT": "Conglomerate",
    # ETFs → mapped to their benchmark
    "NIFTYBEES": "Index ETF",
    "JUNIORBEES": "Index ETF",
    "GOLDBEES": "Commodity ETF",
    "LIQUIDBEES": "Liquid ETF",
    "BANKBEES": "Sectoral ETF",
    "NETFIT": "Sectoral ETF",
    "PSUBNKBEES": "Sectoral ETF",
    "ICICIB22": "Index ETF",
}


def _get_sector(analysis: AssetAnalysis) -> str:
    """Determine sector for an asset, using Yahoo Finance data with fallback."""
    if analysis.yahoo_data and analysis.yahoo_data.sector:
        return analysis.yahoo_data.sector
    return SYMBOL_SECTOR_FALLBACK.get(analysis.symbol.upper(), "Unknown")


def compute_sector_allocation(
    analyses: list[AssetAnalysis],
    total_value: float,
) -> dict[str, float]:
    """
    Compute sector-wise allocation as % of total portfolio value.

    Args:
        analyses:    List of per-asset analyses.
        total_value: Total portfolio market value in INR.

    Returns:
        Dict mapping sector name → percentage of portfolio.
    """
    sector_values: dict[str, float] = defaultdict(float)
    for analysis in analyses:
        sector = _get_sector(analysis)
        sector_values[sector] += analysis.current_value

    if total_value <= 0:
        return {}

    return {
        sector: round((value / total_value) * 100, 2)
        for sector, value in sorted(sector_values.items(), key=lambda x: x[1], reverse=True)
    }


def compute_concentration_risk(
    analyses: list[AssetAnalysis],
    total_value: float,
) -> dict[str, Any]:
    """
    Compute concentration metrics for the portfolio.

    Returns:
        Dict with top_holding_pct, top3_holdings_pct, herfindahl_index,
        and concentration_level (LOW / MEDIUM / HIGH / VERY HIGH).
    """
    if not analyses or total_value <= 0:
        return {}

    weights = sorted(
        [(a.symbol, a.current_value / total_value * 100) for a in analyses],
        key=lambda x: x[1],
        reverse=True,
    )

    top1 = weights[0][1] if weights else 0
    top3 = sum(w[1] for w in weights[:3])

    # Herfindahl-Hirschman Index (HHI) – sum of squared weights
    hhi = sum((w / 100) ** 2 for _, w in weights)

    if top1 > 40:
        level = "VERY HIGH"
    elif top1 > 25:
        level = "HIGH"
    elif top1 > 15:
        level = "MEDIUM"
    else:
        level = "LOW"

    return {
        "top_holding": weights[0][0] if weights else "",
        "top_holding_pct": round(top1, 2),
        "top3_holdings_pct": round(top3, 2),
        "herfindahl_index": round(hhi, 4),
        "concentration_level": level,
        "top_holdings": [{"symbol": s, "weight_pct": round(w, 2)} for s, w in weights[:5]],
    }


def compute_diversification_score(
    analyses: list[AssetAnalysis],
    sector_allocation: dict[str, float],
    concentration: dict[str, Any],
) -> float:
    """
    Compute a diversification score (0–100).

    Higher = better diversified. Factors:
      • Number of sectors (ideal: 6+)
      • Max sector concentration (ideal: <25%)
      • Number of holdings (ideal: 10+)
      • ETF presence (adds diversification)

    Returns:
        Float score between 0 and 100.
    """
    score = 100.0

    # Penalise for few sectors
    num_sectors = len([s for s in sector_allocation if s not in ("Index ETF", "Liquid ETF")])
    if num_sectors < 3:
        score -= 30
    elif num_sectors < 5:
        score -= 15
    elif num_sectors < 7:
        score -= 5

    # Penalise for max sector concentration
    max_sector_pct = max(sector_allocation.values(), default=0)
    if max_sector_pct > 60:
        score -= 30
    elif max_sector_pct > 40:
        score -= 15
    elif max_sector_pct > 25:
        score -= 8

    # Penalise for top holding concentration
    top1_pct = concentration.get("top_holding_pct", 0)
    if top1_pct > 40:
        score -= 20
    elif top1_pct > 25:
        score -= 10
    elif top1_pct > 15:
        score -= 5

    # Penalise for few holdings
    num_holdings = len(analyses)
    if num_holdings < 3:
        score -= 25
    elif num_holdings < 6:
        score -= 10

    return round(max(0, min(100, score)), 1)


def compute_portfolio_health(
    analyses: list[AssetAnalysis],
    diversification_score: float,
    total_pnl_pct: float,
) -> float:
    """
    Compute an overall portfolio health score (0–100).

    Factors:
      • Diversification (40% weight)
      • Average risk score of holdings (30% weight)
      • Average sentiment of holdings (20% weight)
      • P&L performance (10% weight)

    Returns:
        Float score between 0 and 100.
    """
    # Diversification component (40%)
    div_component = diversification_score * 0.4

    # Risk component (30%) – lower avg risk → higher score
    if analyses:
        avg_risk = sum(a.risk_score for a in analyses) / len(analyses)
        risk_component = ((10 - avg_risk) / 9) * 100 * 0.3
    else:
        risk_component = 50 * 0.3

    # Sentiment component (20%) – map -1..+1 to 0..100
    if analyses:
        avg_sentiment = sum(a.sentiment_score for a in analyses) / len(analyses)
        sentiment_component = ((avg_sentiment + 1) / 2) * 100 * 0.2
    else:
        sentiment_component = 50 * 0.2

    # P&L component (10%)
    if total_pnl_pct > 20:
        pnl_component = 100 * 0.1
    elif total_pnl_pct > 5:
        pnl_component = 75 * 0.1
    elif total_pnl_pct > -5:
        pnl_component = 50 * 0.1
    elif total_pnl_pct > -15:
        pnl_component = 25 * 0.1
    else:
        pnl_component = 0

    total = div_component + risk_component + sentiment_component + pnl_component
    return round(max(0, min(100, total)), 1)


def build_portfolio_report(
    portfolio: Portfolio,
    analyses: list[AssetAnalysis],
    demo_mode: bool = False,
) -> PortfolioReport:
    """
    Aggregate all asset analyses and portfolio metrics into a final report.

    Args:
        portfolio: Raw portfolio from Zerodha (for totals).
        analyses:  List of enriched AssetAnalysis per holding.

    Returns:
        PortfolioReport matching the required JSON output schema.
    """
    from datetime import datetime

    total_invested = portfolio.total_invested
    total_value = sum(a.current_value for a in analyses) or portfolio.total_current_value
    total_pnl = total_value - total_invested
    total_pnl_pct = (total_pnl / total_invested * 100) if total_invested else 0

    etf_list = [a for a in analyses if a.instrument_type == InstrumentType.ETF]
    stock_list = [a for a in analyses if a.instrument_type != InstrumentType.ETF]

    etf_value = sum(a.current_value for a in etf_list)
    direct_value = sum(a.current_value for a in stock_list)
    etf_pct = (etf_value / total_value * 100) if total_value else 0
    direct_pct = (direct_value / total_value * 100) if total_value else 0

    # Sector allocation
    sector_allocation = compute_sector_allocation(analyses, total_value)

    # Concentration
    concentration = compute_concentration_risk(analyses, total_value)

    # Scores
    diversification_score = compute_diversification_score(
        analyses, sector_allocation, concentration
    )
    health_score = compute_portfolio_health(analyses, diversification_score, total_pnl_pct)

    # LLM portfolio-level insights
    holdings_summary_for_llm = [
        {
            "symbol": a.symbol,
            "instrument_type": a.instrument_type.value,
            "pnl_percent": a.pnl_percent,
            "risk_score": a.risk_score,
            "sentiment_score": a.sentiment_score,
            "sector": _get_sector(a),
            "summary": a.summary,
        }
        for a in analyses
    ]

    _summarize_fn = summarize_portfolio_demo if demo_mode else summarize_portfolio
    llm_portfolio = _summarize_fn(
        {
            "total_invested": total_invested,
            "total_current_value": total_value,
            "total_pnl": total_pnl,
            "total_pnl_pct": total_pnl_pct,
            "num_holdings": len(analyses),
            "etf_count": len(etf_list),
            "stock_count": len(stock_list),
            "sector_allocation": sector_allocation,
            "holdings_analysis": holdings_summary_for_llm,
        }
    )

    # Build final PortfolioReport
    summary = PortfolioSummary(
        total_value=f"₹{total_value:,.2f}",
        total_invested=f"₹{total_invested:,.2f}",
        total_pnl=f"₹{total_pnl:,.2f}",
        total_pnl_percent=f"{total_pnl_pct:.2f}%",
        health_score=llm_portfolio.get("health_score", health_score),
        diversification_score=llm_portfolio.get("diversification_score", diversification_score),
        num_holdings=len(analyses),
        etf_count=len(etf_list),
        stock_count=len(stock_list),
        etf_allocation_pct=round(etf_pct, 2),
        direct_equity_allocation_pct=round(direct_pct, 2),
    )

    holdings_analysis_out: list[dict[str, Any]] = [
        {
            "symbol": a.symbol,
            "exchange": a.exchange,
            "instrument_type": a.instrument_type.value,
            "quantity": a.quantity,
            "average_buy_price": a.average_buy_price,
            "current_price": a.current_price,
            "invested_value_inr": a.invested_value,
            "current_value_inr": a.current_value,
            "pnl_percent": a.pnl_percent,
            "sector": _get_sector(a),
            "sentiment_score": a.sentiment_score,
            "risk_score": a.risk_score,
            "summary": a.summary,
            "key_news": [
                {
                    "title": n.title,
                    "source": n.source,
                    "published_at": n.published_at,
                    "sentiment": n.sentiment.value,
                    "url": n.url,
                }
                for n in a.news_items[:5]
            ],
            "latest_results": a.quarterly_result.model_dump() if a.quarterly_result else {},
            "key_insights": a.key_insights,
            "risk_signals": a.risk_signals,
            "inav_analysis": a.inav_data,
            "historic_inav": a.historic_inav_data,
        }
        for a in analyses
    ]

    return PortfolioReport(
        generated_at=datetime.now().isoformat(),
        portfolio_summary=summary,
        holdings_analysis=holdings_analysis_out,
        sector_allocation=sector_allocation,
        portfolio_risks=llm_portfolio.get("portfolio_risks", []),
        actionable_insights=llm_portfolio.get("actionable_insights", []),
    )
