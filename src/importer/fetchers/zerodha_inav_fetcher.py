"""
src/importer/fetchers/zerodha_inav_fetcher.py
──────────────────────────────────────────────
Fetches live iNAV snapshots for Zerodha Fund House ETFs (GOLDCASE, SILVERCASE,
LIQUIDCASE, etc.) directly from the Zerodha AMC API.

Endpoint: https://api.zerodhafundhouse.com/api/v1/schemes
which returns live scheme stats including the real-time iNAV values.

Market prices (LTP) are fetched from yfinance to calculate the live premium/discount.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import httpx
import yfinance as yf

logger = logging.getLogger(__name__)

_ZERODHA_API_URL = "https://api.zerodhafundhouse.com/api/v1/schemes"
_TIMEOUT = 15

# Tracked Zerodha symbols in our registry (all 8 ETFs with live iNAV from the API)
ZERODHA_SYMBOLS = {
    "GOLDCASE",    "SILVERCASE",  "LIQUIDCASE",
    "TOP100CASE",  "MID150CASE",  "LTGILTCASE",
    "NIFTYCASE",   "SML100CASE",
}


def _safe(val: Any, default: float = 0.0) -> float:
    try:
        return float(str(val).replace(",", "").strip())
    except (TypeError, ValueError):
        return default


def _parse_zerodha_datetime(ts_str: str) -> datetime:
    """Parse Zerodha's ISO UTC timestamp (e.g. '2026-07-03T10:29:49Z') and return naive UTC datetime."""
    try:
        return datetime.fromisoformat(ts_str.replace("Z", "+00:00")).astimezone(timezone.utc).replace(tzinfo=None)
    except Exception as exc:
        logger.debug("Failed to parse Zerodha datetime string '%s': %s. Falling back to current UTC.", ts_str, exc)
        return datetime.now(timezone.utc).replace(tzinfo=None)


def fetch_inav_zerodha(symbols: list[str]) -> list[dict[str, Any]]:
    """
    Fetch live iNAV snapshots for Zerodha ETFs from the Zerodha AMC API.

    Parameters
    ----------
    symbols : list of internal symbols, e.g. ["GOLDCASE", "SILVERCASE"]

    Returns
    -------
    list of dicts with keys:
        symbol, snapshot_at (datetime UTC), inav, market_price,
        premium_discount_pct, source
    """
    requested_clean = {s.upper().replace(".NS", "") for s in symbols}
    target_symbols = [s for s in requested_clean if s in ZERODHA_SYMBOLS]

    if not target_symbols:
        return []

    logger.info("Zerodha iNAV: fetching live data for target symbols: %s", target_symbols)

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json",
    }

    # 1. Fetch live iNAV data from Zerodha API
    try:
        with httpx.Client(headers=headers, follow_redirects=True, timeout=_TIMEOUT) as client:
            resp = client.get(_ZERODHA_API_URL)
            resp.raise_for_status()
            response_json = resp.json()
    except Exception as exc:
        logger.warning("Zerodha iNAV schemes GET failed: %s", exc)
        return []

    schemes_list = response_json.get("data", [])
    if not schemes_list:
        logger.warning("Zerodha iNAV API returned empty data list")
        return []

    # Map target tickers to their raw API items
    matched_items: dict[str, dict[str, Any]] = {}
    for item in schemes_list:
        ticker = str(item.get("ticker", "")).upper()
        if ticker in target_symbols:
            matched_items[ticker] = item

    if not matched_items:
        logger.info("No matching Zerodha ETFs found in API response")
        return []

    # 2. Fetch corresponding live market prices from yfinance
    yf_symbols = [f"{sym}.NS" for sym in matched_items.keys()]
    market_prices: dict[str, float] = {}

    try:
        logger.debug("Zerodha iNAV: fetching live market prices for: %s", yf_symbols)
        yf_data = yf.download(yf_symbols, period="1d", progress=False)
        
        if not yf_data.empty and "Close" in yf_data.columns:
            close_df = yf_data["Close"]
            for sym in matched_items.keys():
                yf_sym = f"{sym}.NS"
                series = None
                
                if hasattr(close_df, "columns"):  # DataFrame
                    if yf_sym in close_df.columns:
                        series = close_df[yf_sym]
                    elif len(yf_symbols) == 1:
                        series = close_df.iloc[:, 0]
                else:  # Series
                    series = close_df
                
                if series is not None:
                    series = series.dropna()
                    if not series.empty:
                        val = series.iloc[-1]
                        if hasattr(val, "iloc"):
                            val = val.iloc[-1]
                        market_prices[sym] = float(val)
    except Exception as exc:
        logger.warning("Zerodha iNAV: failed to fetch market prices from yfinance: %s", exc)

    # 3. Build snapshot rows
    rows: list[dict[str, Any]] = []
    for sym, item in matched_items.items():
        stats = item.get("schemeStats", {})
        inav_info = stats.get("inav", {})
        raw_inav = inav_info.get("val")
        if raw_inav is None:
            continue

        inav = _safe(raw_inav)
        if inav <= 0:
            continue

        # Use yfinance market price if available; fallback to declared NAV or current iNAV
        market_price = market_prices.get(sym)
        if market_price is None or market_price <= 0:
            raw_nav = stats.get("nav")
            market_price = _safe(raw_nav) if raw_nav is not None else inav

        prem_disc = ((market_price - inav) / inav * 100)
        
        ts_str = inav_info.get("ts") or ""
        snapshot_at = _parse_zerodha_datetime(ts_str)

        rows.append({
            "symbol":               sym,
            "snapshot_at":          snapshot_at,
            "inav":                 inav,
            "market_price":         market_price,
            "premium_discount_pct": round(prem_disc, 4),
            "source":               "zerodha_amc_live",
        })

    logger.info("Zerodha iNAV: successfully compiled %d snapshot(s)", len(rows))
    return rows
