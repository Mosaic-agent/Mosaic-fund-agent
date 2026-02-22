"""
src/tools/yahoo_finance.py
──────────────────────────
LangChain tool wrapper around yfinance for Indian equity market data.

Provides end-of-day prices, financial metrics, sector info, and
52-week range for NSE/BSE listed stocks using Yahoo Finance's
free API (no API key required).

Symbol conventions:
  • NSE stocks: RELIANCE.NS, TCS.NS, INFY.NS
  • BSE stocks: RELIANCE.BO, TCS.BO
  • Indian ETFs: NIFTYBEES.NS, GOLDBEES.NS
"""

from __future__ import annotations

import logging
import time
from typing import Any

import yfinance as yf
from langchain_core.tools import tool

from config.settings import settings
from src.models.portfolio import YahooFinanceData

logger = logging.getLogger(__name__)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _build_yf_symbol(symbol: str, exchange: str = "NSE") -> str:
    """
    Convert a Zerodha trading symbol to Yahoo Finance ticker.

    Examples:
      RELIANCE + NSE  →  RELIANCE.NS
      RELIANCE + BSE  →  RELIANCE.BO
    """
    # Already has suffix – return as-is
    if symbol.endswith(".NS") or symbol.endswith(".BO"):
        return symbol

    suffix = settings.bse_suffix if exchange.upper() == "BSE" else settings.nse_suffix
    return f"{symbol}{suffix}"


def _safe_float(value: Any, default: float = 0.0) -> float:
    """Safely coerce a value to float, returning default on failure."""
    try:
        if value is None:
            return default
        f = float(value)
        return f if not (f != f) else default  # NaN guard
    except (TypeError, ValueError):
        return default


def fetch_yahoo_data(symbol: str, exchange: str = "NSE") -> YahooFinanceData:
    """
    Fetch financial data for a single Indian stock from Yahoo Finance.

    Args:
        symbol:   Zerodha trading symbol e.g. 'RELIANCE'
        exchange: 'NSE' (default) or 'BSE'

    Returns:
        YahooFinanceData model populated from Yahoo Finance info dict.
    """
    yf_symbol = _build_yf_symbol(symbol, exchange)
    logger.info("Fetching Yahoo Finance data for %s", yf_symbol)

    try:
        ticker = yf.Ticker(yf_symbol)
        info: dict[str, Any] = ticker.info or {}

        return YahooFinanceData(
            symbol=yf_symbol,
            sector=info.get("sector", ""),
            industry=info.get("industry", ""),
            # Market cap: Yahoo returns in USD for Indian stocks sometimes;
            # we keep as-is since it's comparative rather than absolute here
            market_cap=_safe_float(info.get("marketCap")),
            pe_ratio=_safe_float(info.get("trailingPE")),
            pb_ratio=_safe_float(info.get("priceToBook")),
            dividend_yield=_safe_float(info.get("dividendYield")) * 100,
            fifty_two_week_high=_safe_float(info.get("fiftyTwoWeekHigh")),
            fifty_two_week_low=_safe_float(info.get("fiftyTwoWeekLow")),
            current_price=_safe_float(
                info.get("currentPrice") or info.get("regularMarketPrice")
            ),
            description=info.get("longBusinessSummary", ""),
        )

    except Exception as exc:
        logger.warning("Yahoo Finance fetch failed for %s: %s", yf_symbol, exc)
        return YahooFinanceData(symbol=yf_symbol)


def fetch_price_history(
    symbol: str,
    exchange: str = "NSE",
    period: str = "3mo",
) -> list[dict[str, Any]]:
    """
    Fetch historical OHLCV data for an Indian stock.

    Args:
        symbol:   Zerodha trading symbol e.g. 'RELIANCE'
        exchange: 'NSE' (default) or 'BSE'
        period:   yfinance period string: '1mo', '3mo', '6mo', '1y'

    Returns:
        List of dicts with date, open, high, low, close, volume.
    """
    yf_symbol = _build_yf_symbol(symbol, exchange)
    try:
        ticker = yf.Ticker(yf_symbol)
        hist = ticker.history(period=period)
        if hist.empty:
            return []
        records = []
        for date, row in hist.iterrows():
            records.append(
                {
                    "date": str(date.date()),
                    "open": round(_safe_float(row.get("Open")), 2),
                    "high": round(_safe_float(row.get("High")), 2),
                    "low": round(_safe_float(row.get("Low")), 2),
                    "close": round(_safe_float(row.get("Close")), 2),
                    "volume": int(_safe_float(row.get("Volume"))),
                }
            )
        return records
    except Exception as exc:
        logger.warning("Price history fetch failed for %s: %s", yf_symbol, exc)
        return []


# ── LangChain Tool ────────────────────────────────────────────────────────────

@tool
def get_yahoo_finance_data(input_str: str) -> dict[str, Any]:
    """
    Fetch financial metrics and company overview for an Indian stock
    from Yahoo Finance (free, no API key required).

    Input format: "SYMBOL" or "SYMBOL:EXCHANGE"
    Examples:
      "RELIANCE"        → fetches RELIANCE.NS from NSE
      "RELIANCE:BSE"    → fetches RELIANCE.BO from BSE
      "NIFTYBEES:NSE"   → fetches NIFTYBEES.NS (ETF)

    Returns a dict with sector, industry, P/E, P/B, 52-week range,
    market cap, dividend yield, current price, and company description.
    """
    # Parse input
    parts = input_str.strip().split(":")
    symbol = parts[0].strip().upper()
    exchange = parts[1].strip().upper() if len(parts) > 1 else "NSE"

    # Small delay to be polite to Yahoo Finance
    time.sleep(0.5)

    data = fetch_yahoo_data(symbol, exchange)

    return {
        "symbol": data.symbol,
        "sector": data.sector,
        "industry": data.industry,
        "market_cap": data.market_cap,
        "pe_ratio": data.pe_ratio,
        "pb_ratio": data.pb_ratio,
        "dividend_yield_pct": data.dividend_yield,
        "52_week_high": data.fifty_two_week_high,
        "52_week_low": data.fifty_two_week_low,
        "current_price_inr": data.current_price,
        "description": data.description[:500] if data.description else "",
    }


@tool
def get_price_momentum(input_str: str) -> dict[str, Any]:
    """
    Fetch 3-month price history and compute momentum signals for an
    Indian stock using Yahoo Finance (free).

    Input format: "SYMBOL" or "SYMBOL:EXCHANGE"

    Returns recent close prices, 30-day return %, 90-day return %,
    and a simple momentum signal (BULLISH / BEARISH / NEUTRAL).
    """
    parts = input_str.strip().split(":")
    symbol = parts[0].strip().upper()
    exchange = parts[1].strip().upper() if len(parts) > 1 else "NSE"

    time.sleep(0.5)
    history = fetch_price_history(symbol, exchange, period="3mo")

    if len(history) < 2:
        return {"symbol": symbol, "error": "Insufficient price history"}

    latest_close = history[-1]["close"]
    oldest_close = history[0]["close"]

    # 30-day return (approx last 22 trading days)
    idx_30d = max(0, len(history) - 22)
    close_30d_ago = history[idx_30d]["close"]

    ret_30d = ((latest_close - close_30d_ago) / close_30d_ago * 100) if close_30d_ago else 0
    ret_90d = ((latest_close - oldest_close) / oldest_close * 100) if oldest_close else 0

    if ret_30d > 5:
        signal = "BULLISH"
    elif ret_30d < -5:
        signal = "BEARISH"
    else:
        signal = "NEUTRAL"

    return {
        "symbol": symbol,
        "current_price": latest_close,
        "return_30d_pct": round(ret_30d, 2),
        "return_90d_pct": round(ret_90d, 2),
        "momentum_signal": signal,
        "data_points": len(history),
    }


# Convenience list of Yahoo Finance tools
YAHOO_TOOLS = [get_yahoo_finance_data, get_price_momentum]
