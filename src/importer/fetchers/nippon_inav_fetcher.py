"""
src/importer/fetchers/nippon_inav_fetcher.py
─────────────────────────────────────────────
Fetches live iNAV snapshots for Nippon India Mutual Fund ETFs (like GOLDBEES,
SILVERBEES, NIFTYBEES, etc.) directly from the Nippon India AMC website.

This fetcher hits the RealtimeNAV endpoint:
https://etf.nipponindiaim.com/RealtimeNAV/Nav/DetailsFill
which returns live intraday iNAV values calculated by the AMC.

Since the AMC site does not provide market prices, we batch-fetch the latest
market prices (LTP) from yfinance to calculate the live premium/discount.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Any

import httpx
import yfinance as yf

logger = logging.getLogger(__name__)

_NIPPON_DETAILS_URL = "https://etf.nipponindiaim.com/RealtimeNAV/Nav/DetailsFill"
_TIMEOUT = 15

# Map our internal symbols to the scheme names used on the Nippon AMC website
NIPPON_SYMBOL_MAP: dict[str, str] = {
    "GOLDBEES":   "Nippon India ETF Gold BeES",
    "SILVERBEES": "Nippon India Silver ETF",
    "NIFTYBEES":  "Nippon India ETF Nifty 50 BeES",
    "JUNIORBEES": "Nippon India ETF Nifty Next 50 Junior BeES",
    "LIQUIDBEES": "Nippon India ETF Nifty 1D Rate Liquid BeES",
    "HNGSNGBEES": "Nippon India ETF Hang Seng BeES",
    "BANKBEES":   "Nippon India ETF Nifty Bank BeES",
    "PSUBNKBEES": "Nippon India ETF Nifty PSU Bank BeES",
    "CPSEETF":    "CPSE ETF",
    "ITBEES":     "Nippon India ETF Nifty IT",
    "PHARMABEES": "Nippon India Nifty Pharma ETF",
    "AUTOBEES":   "Nippon India Nifty Auto ETF",
    "INFRABEES":  "Nippon India ETF Nifty Infrastructure BeES",
    "SHARIABEES": "Nippon India ETF Nifty 50 Shariah BeES",
}

# Reverse mapping for fast lookups
_SCHEME_TO_SYMBOL = {v: k for k, v in NIPPON_SYMBOL_MAP.items()}


def _safe(val: Any, default: float = 0.0) -> float:
    try:
        return float(str(val).replace(",", "").replace("%", "").strip())
    except (TypeError, ValueError):
        return default


def _parse_nippon_datetime(realdt_str: str) -> datetime:
    """Parse Nippon's Realdt string (e.g. 'Friday, 03 July 2026 11:21:32 PM') and return naive UTC datetime."""
    try:
        # Nippon returns dates in IST
        dt_local = datetime.strptime(realdt_str.strip(), "%A, %d %B %Y %I:%M:%S %p")
        ist_tz = timezone(timedelta(hours=5, minutes=30))
        dt_local = dt_local.replace(tzinfo=ist_tz)
        return dt_local.astimezone(timezone.utc).replace(tzinfo=None)
    except Exception as exc:
        logger.debug("Failed to parse Nippon datetime string '%s': %s. Falling back to current UTC.", realdt_str, exc)
        return datetime.now(timezone.utc).replace(tzinfo=None)


def fetch_inav_nippon(symbols: list[str]) -> list[dict[str, Any]]:
    """
    Fetch live iNAV snapshots for Nippon ETFs from the Nippon India AMC website.

    Parameters
    ----------
    symbols : list of internal symbols, e.g. ["GOLDBEES", "SILVERBEES"]

    Returns
    -------
    list of dicts with keys:
        symbol, snapshot_at (datetime UTC), inav, market_price,
        premium_discount_pct, source
    """
    # Filter requested symbols that are actually managed by Nippon
    requested_clean = {s.upper().replace(".NS", "") for s in symbols}
    target_symbols = [s for s in requested_clean if s in NIPPON_SYMBOL_MAP]

    if not target_symbols:
        return []

    logger.info("Nippon iNAV: fetching live data for target symbols: %s", target_symbols)

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Content-Type": "application/json",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
    }

    # 1. Fetch live iNAV data from Nippon AMC
    try:
        with httpx.Client(headers=headers, follow_redirects=True, timeout=_TIMEOUT) as client:
            resp = client.post(_NIPPON_DETAILS_URL, json={})
            resp.raise_for_status()
            data = resp.json()
    except Exception as exc:
        logger.warning("Nippon iNAV details fill POST failed: %s", exc)
        return []

    details_list = data.get("RVDetailsList", [])
    if not details_list:
        logger.warning("Nippon iNAV API returned empty details list")
        return []

    # Map target scheme names to their raw API items
    matched_items: dict[str, dict[str, Any]] = {}
    for item in details_list:
        sch_name = item.get("SchName", "")
        if sch_name in _SCHEME_TO_SYMBOL:
            sym = _SCHEME_TO_SYMBOL[sch_name]
            if sym in target_symbols:
                matched_items[sym] = item

    if not matched_items:
        logger.info("No matching Nippon ETFs found in DetailsFill response")
        return []

    # 2. Fetch corresponding live market prices from yfinance
    yf_symbols = [f"{sym}.NS" for sym in matched_items.keys()]
    market_prices: dict[str, float] = {}

    try:
        logger.debug("Nippon iNAV: fetching live market prices for: %s", yf_symbols)
        # Fetch EOD/live quote using period="1d"
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
        logger.warning("Nippon iNAV: failed to fetch market prices from yfinance: %s", exc)

    # 3. Build snapshot rows
    rows: list[dict[str, Any]] = []
    for sym, item in matched_items.items():
        raw_inav = item.get("CNav")
        if raw_inav is None:
            continue

        inav = _safe(raw_inav)
        if inav <= 0:
            continue

        # Use yfinance market price if available; fallback to previous day's NAV or current iNAV
        market_price = market_prices.get(sym)
        if market_price is None or market_price <= 0:
            raw_pnav = item.get("PNav")
            market_price = _safe(raw_pnav) if raw_pnav is not None else inav

        prem_disc = ((market_price - inav) / inav * 100)
        
        realdt = item.get("Realdt") or ""
        snapshot_at = _parse_nippon_datetime(realdt)

        rows.append({
            "symbol":               sym,
            "snapshot_at":          snapshot_at,
            "inav":                 inav,
            "market_price":         market_price,
            "premium_discount_pct": round(prem_disc, 4),
            "source":               "NIPPON_AMC",
        })

    logger.info("Nippon iNAV: successfully compiled %d snapshot(s)", len(rows))
    return rows
