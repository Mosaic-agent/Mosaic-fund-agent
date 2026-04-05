"""
src/importer/fetchers/nse_quote_fetcher.py
───────────────────────────────────────────
Fetches today's OHLCV bar for NSE-listed ETFs and stocks directly from the
NSE Quote API — available immediately after market close (3:30 PM IST),
without waiting for Yahoo Finance's ~1-hour publication delay.

Endpoint used:
    https://www.nseindia.com/api/quote-equity?symbol=<SYMBOL>

The response contains today's open, high, low, last traded price (LTP),
previous close, and volume in priceInfo / industryInfo / marketDeptOrderBook.

Suitable for writing into the existing `daily_prices` ClickHouse table via
ClickHouseImporter.insert_prices() — identical schema to yfinance rows.
"""

from __future__ import annotations

import logging
import time
from datetime import date
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_NSE_QUOTE_URL = "https://www.nseindia.com/api/quote-equity"
_NSE_WARMUP    = "https://www.nseindia.com/"
_NSE_HEADERS   = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
    "Connection": "keep-alive",
}
_TIMEOUT = 15
_INTER_REQUEST_DELAY = 0.5  # seconds between per-symbol requests


def _safe(val: Any, default: float = 0.0) -> float:
    try:
        return float(str(val).replace(",", ""))
    except (TypeError, ValueError):
        return default


def fetch_nse_eod(
    symbols: list[tuple[str, str]],  # [(nse_symbol, yahoo_ticker), ...] — yahoo_ticker ignored
    category: str,
    trade_date: date | None = None,
) -> list[dict[str, Any]]:
    """
    Fetch today's OHLCV from NSE Quote API for each symbol.

    Parameters
    ----------
    symbols    : list of (nse_symbol, _) tuples — same format as yfinance fetcher
    category   : category string to embed in each row (e.g. 'etfs', 'stocks')
    trade_date : the date to tag the row with; defaults to today

    Returns
    -------
    list of dicts with keys: symbol, category, trade_date, open, high, low,
    close, volume — ready for ClickHouseImporter.insert_prices().

    Symbols for which NSE returns no data (outside market hours, suspended,
    or ETF-only on a different endpoint) are silently skipped.
    """
    if not symbols:
        return []

    today = trade_date or date.today()
    rows: list[dict[str, Any]] = []

    try:
        with httpx.Client(
            headers=_NSE_HEADERS,
            follow_redirects=True,
            timeout=_TIMEOUT,
        ) as client:
            # Warm-up to obtain NSE session cookies
            client.get(_NSE_WARMUP, timeout=10)
            time.sleep(0.8)

            for nse_sym, _ in symbols:
                try:
                    resp = client.get(
                        _NSE_QUOTE_URL,
                        params={"symbol": nse_sym.upper()},
                        timeout=_TIMEOUT,
                    )
                    resp.raise_for_status()
                    data = resp.json()
                except Exception as exc:
                    logger.debug("NSE quote failed for %s: %s", nse_sym, exc)
                    time.sleep(_INTER_REQUEST_DELAY)
                    continue

                price_info = data.get("priceInfo", {})
                intrinsic  = price_info.get("intrinsicValue") or {}

                open_  = _safe(price_info.get("open"))
                high   = _safe(price_info.get("intraDayHighLow", {}).get("max") or
                               price_info.get("high"))
                low    = _safe(price_info.get("intraDayHighLow", {}).get("min") or
                               price_info.get("low"))
                close  = _safe(price_info.get("lastPrice") or
                               price_info.get("close"))
                prev   = _safe(price_info.get("previousClose"))

                # Volume is in marketDeptOrderBook → tradeInfo
                trade_info = (data.get("marketDeptOrderBook") or {}).get("tradeInfo") or {}
                volume = _safe(trade_info.get("totalTradingVolume") or
                               trade_info.get("tradedVolume"))

                # Skip rows where price data is missing / zero
                if close <= 0:
                    logger.debug("No price data for %s — skipping", nse_sym)
                    time.sleep(_INTER_REQUEST_DELAY)
                    continue

                # If open is missing (e.g. pre-open), fall back to prev close
                if open_ <= 0:
                    open_ = prev if prev > 0 else close
                if high <= 0:
                    high = close
                if low <= 0:
                    low = close

                rows.append({
                    "symbol":     nse_sym.upper(),
                    "category":   category,
                    "trade_date": today,
                    "open":       open_,
                    "high":       high,
                    "low":        low,
                    "close":      close,
                    "volume":     volume,
                })
                logger.debug("NSE quote %s  O=%.2f H=%.2f L=%.2f C=%.2f V=%.0f",
                             nse_sym, open_, high, low, close, volume)
                time.sleep(_INTER_REQUEST_DELAY)

    except Exception as exc:
        logger.warning("NSE quote session failed: %s", exc)

    logger.info("NSE EOD: fetched %d/%d symbols for %s", len(rows), len(symbols), today)
    return rows
