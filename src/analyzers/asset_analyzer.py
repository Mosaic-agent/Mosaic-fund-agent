"""
src/analyzers/asset_analyzer.py
────────────────────────────────
Orchestrates per-asset data collection and analysis for a single holding.

For each holding, this module:
  1. Fetches Yahoo Finance company data and price metrics
  2. Fetches recent news via NewsAPI
  3. Scrapes latest quarterly results from Screener.in / Yahoo Finance
  4. Calls the LLM summarization layer to produce risk/sentiment scores

Returns a fully populated AssetAnalysis model per holding.
"""

from __future__ import annotations

import logging
from typing import Any

from src.models.portfolio import (
    AssetAnalysis,
    Holding,
    NewsItem,
    QuarterlyResult,
    YahooFinanceData,
)
from src.tools.earnings_scraper import fetch_from_screener, fetch_from_yahoo_financials
from src.tools.garch_position_sizer import size_position
from src.tools.inav_fetcher import get_etf_inav, is_etf
from src.tools.historic_inav import get_historic_inav
from src.tools.news_search import fetch_news_for_symbol
from src.tools.summarization import summarize_asset
from src.tools.yahoo_finance import fetch_price_history, fetch_yahoo_data
from src.utils.symbol_mapper import get_company_name
from src.deepdive.clickhouse import DeepDiveStore

logger = logging.getLogger(__name__)


def analyze_holding(holding: Holding) -> AssetAnalysis:
    """
    Perform full enrichment and analysis for a single Zerodha holding.

    Steps:
      1. Yahoo Finance – company overview, P/E, sector, description
      2. Yahoo Finance – 3-month price momentum
      3. NewsAPI – recent Indian financial news
      4. Screener.in / Yahoo – quarterly results
      4b. iNAV for ETFs
      4c. Deep-dive report from ClickHouse (US stocks only)
      5. LLM – investment insights, risk score, sentiment score

    Args:
        holding:         A Holding model from Zerodha Kite MCP.

    Returns:
        AssetAnalysis with all enriched data and AI-generated scores.
    """
    symbol = holding.tradingsymbol
    exchange = holding.exchange
    company_name = get_company_name(symbol)

    logger.info("Analyzing holding: %s (%s)", symbol, exchange)

    # ── Step 1: Yahoo Finance company data ────────────────────────────────────
    yf_data: YahooFinanceData = fetch_yahoo_data(symbol, exchange)
    current_price = yf_data.current_price or holding.last_price

    # ── Step 2: Price momentum ────────────────────────────────────────────────
    history = fetch_price_history(symbol, exchange, period="3mo")
    momentum: dict[str, Any] = {}
    if len(history) >= 2:
        latest = history[-1]["close"]
        oldest = history[0]["close"]
        idx_30d = max(0, len(history) - 22)
        close_30d = history[idx_30d]["close"]
        ret_30d = ((latest - close_30d) / close_30d * 100) if close_30d else 0
        ret_90d = ((latest - oldest) / oldest * 100) if oldest else 0
        signal = "BULLISH" if ret_30d > 5 else ("BEARISH" if ret_30d < -5 else "NEUTRAL")
        momentum = {
            "return_30d_pct": round(ret_30d, 2),
            "return_90d_pct": round(ret_90d, 2),
            "momentum_signal": signal,
        }

    # ── Step 3: News ──────────────────────────────────────────────────────────
    news_items: list[NewsItem] = fetch_news_for_symbol(symbol, company_name)

    # ── Step 4: Quarterly results ─────────────────────────────────────────────
    quarterly: QuarterlyResult | None = fetch_from_screener(symbol)
    if quarterly is None:
        quarterly = fetch_from_yahoo_financials(symbol, exchange)

    # ── Step 4b: iNAV for ETFs ────────────────────────────────────────────────
    inav_data: dict | None = None
    if holding.instrument_type.value == "ETF" or is_etf(symbol):
        logger.info("Fetching iNAV for ETF: %s", symbol)
        inav_data = get_etf_inav(symbol)
        if inav_data.get("market_price"):
            current_price = inav_data["market_price"]

    # ── Step 4c: Historic iNAV (30-day AMFI) for ETFs ─────────────────────────
    historic_inav_data: dict | None = None
    if inav_data and inav_data.get("is_etf"):
        logger.info("Fetching 30-day historic iNAV for ETF: %s", symbol)
        result = get_historic_inav(symbol, days=30)
        if not result.get("error"):
            historic_inav_data = result

    # ── Step 4d: Deep-dive report from ClickHouse (US stocks only) ─────────────
    deepdive_report: str | None = None
    if exchange.upper() in ("US", "NASDAQ", "NYSE"):
        try:
            ch_store = DeepDiveStore()
            # Try to load the most recent report from ClickHouse
            reports = ch_store.load_report(symbol, "")  # empty date gets latest from FINAL query in store
            if not reports:
                # If no date specified, load_report might fail if it expects a date.
                # Let's check most recent report_date first.
                r = ch_store._client.query(
                    "SELECT max(report_date) FROM market_data.deepdive_reports WHERE ticker = {t:String}",
                    parameters={"t": symbol}
                )
                if r.result_rows and r.result_rows[0][0]:
                    max_date = str(r.result_rows[0][0])
                    reports = ch_store.load_report(symbol, max_date)
            
            if reports and "__full__" in reports:
                deepdive_report = reports["__full__"]
                logger.info("Deep-dive report found in ClickHouse for %s", symbol)
        except Exception as exc:
            logger.warning("Failed to load deep-dive report for %s: %s", symbol, exc)

    # ── Step 5: LLM Analysis ──────────────────────────────────────────────────
    asset_payload: dict[str, Any] = {
        "symbol": symbol,
        "exchange": exchange,
        "instrument_type": holding.instrument_type.value,
        "quantity": holding.quantity,
        "average_buy_price": holding.average_price,
        "current_price": current_price,
        "pnl_percent": holding.pnl_percent,
        "yahoo_data": {
            "sector": yf_data.sector,
            "industry": yf_data.industry,
            "market_cap": yf_data.market_cap,
            "pe_ratio": yf_data.pe_ratio,
            "pb_ratio": yf_data.pb_ratio,
            "52_week_high": yf_data.fifty_two_week_high,
            "52_week_low": yf_data.fifty_two_week_low,
            "description": yf_data.description,
        },
        "momentum": momentum,
        "news_items": [
            {
                "title": n.title,
                "source": n.source,
                "sentiment": n.sentiment.value,
            }
            for n in news_items
        ],
        "quarterly_result": quarterly.model_dump() if quarterly else {},
        "deepdive_report": deepdive_report,
    }

    llm_result = summarize_asset(asset_payload)

    # ── Step 6: GARCH position sizing ─────────────────────────────────────────
    risk_decision_data: dict | None = None
    try:
        rd = size_position(symbol, exchange)
        if rd is not None:
            risk_decision_data = {
                "garch_vol_pct":      rd.garch_annual_vol_pct,
                "vol_target_pct":     rd.vol_target_pct,
                "regime":             rd.regime,
                "recommended_weight": rd.final_weight,
                "tier":               rd.tier,
                "tier_label":         rd.tier_label,
                "price_below_ema50":  rd.trend_mult < 1.0,
                "alerts":             rd.alerts,
            }
    except Exception as exc:
        logger.warning("Risk governor failed for %s: %s", symbol, exc)

    # ── Build AssetAnalysis model ─────────────────────────────────────────────
    return AssetAnalysis(
        symbol=symbol,
        exchange=exchange,
        instrument_type=holding.instrument_type,
        quantity=holding.quantity,
        average_buy_price=holding.average_price,
        current_price=current_price,
        invested_value=round(holding.invested_value, 2),
        current_value=round(holding.quantity * current_price, 2),
        pnl_percent=round(holding.pnl_percent, 2),
        yahoo_data=yf_data,
        news_items=news_items,
        quarterly_result=quarterly,
        inav_data=inav_data,
        historic_inav_data=historic_inav_data,
        risk_decision=risk_decision_data,
        sentiment_score=float(llm_result.get("sentiment_score", 0.0)),
        risk_score=float(llm_result.get("risk_score", 5.0)),
        summary=llm_result.get("summary", ""),
        key_insights=llm_result.get("key_insights", []),
        risk_signals=llm_result.get("risk_signals", []),
        recommendation=llm_result.get("recommendation", ""),
    )
