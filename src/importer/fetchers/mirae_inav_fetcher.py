"""
src/importer/fetchers/mirae_inav_fetcher.py
────────────────────────────────────────────
Fetches live iNAV snapshots for Mirae Asset ETFs (MAFANG, MAHKTECH, MASPTOP50)
directly from the Mirae Asset AMC API.

Endpoint: https://miraeassetetf.co.in/api/ticker
which returns a list of all schemes with their real-time iNAV values.

Market prices (LTP) are fetched from yfinance to calculate the live premium/discount.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import httpx
import pytz
import yfinance as yf

logger = logging.getLogger(__name__)

_MIRAE_API_URL = "https://miraeassetetf.co.in/api/ticker"
_TIMEOUT = 15

# Tracked Mirae Asset symbols in our registry (all 38 ETFs available via the API)
MIRAE_SYMBOLS = {
    # ── International / US / HK ETFs (prev-session close adjusted for FX) ──
    "MAFANG",     "MAHKTECH",   "MASPTOP50",
    # ── Broad Market ──────────────────────────────────────────────────
    "NIFTYETF",  "NEXT50",     "MIDCAPETF",  "SMALL250",   "MULTICAP",
    "EQUAL50",   "EQUAL200",   "SENSEXETF",
    # ── Sectoral / Thematic ─────────────────────────────────────────
    "BANKETF",   "BANKPSU",    "BFSI",       "ITETF",      "INTERNET",
    "MAKEINDIA", "EVINDIA",    "ENERGY",     "METAL",      "INFRA",
    "HEALTHCARE","DEFENCE",    "CONSUMER",   "MIDSMALL",   "SMALLCAP",
    "ALPHAETF",  "LOWVOL",     "VALUE",      "DIVIDEND",   "TOP20",
    "ESG",       "SELECTIPO",
    # ── Commodities ──────────────────────────────────────────────────
    "GOLDETF",   "SILVERAG",
    # ── Debt / Liquid ──────────────────────────────────────────────
    "LIQUID",    "LIQUIDPLUS", "GSEC10YEAR",
}


def _safe(val: Any, default: float = 0.0) -> float:
    try:
        return float(str(val).replace(",", "").strip())
    except (TypeError, ValueError):
        return default


def _parse_mirae_datetime(ts_str: str) -> datetime:
    """
    Parse Mirae's timestamp and return naive UTC datetime.
    Supports ISO UTC format (e.g. '2026-07-03T10:29:59.985Z') and
    local date format (e.g. '03-Jul-2026 23:41:28') assumed to be in IST.
    """
    ts_str = ts_str.strip()
    if not ts_str:
        return datetime.now(timezone.utc).replace(tzinfo=None)

    if ts_str.endswith("Z"):
        try:
            return datetime.fromisoformat(ts_str.replace("Z", "+00:00")).astimezone(timezone.utc).replace(tzinfo=None)
        except Exception:
            pass

    # Try parsing format: "03-Jul-2026 23:41:28" in IST
    try:
        dt_ist = datetime.strptime(ts_str, "%d-%b-%Y %H:%M:%S")
        ist_tz = pytz.timezone("Asia/Kolkata")
        dt_with_tz = ist_tz.localize(dt_ist)
        dt_utc = dt_with_tz.astimezone(timezone.utc)
        return dt_utc.replace(tzinfo=None)
    except Exception as exc:
        logger.debug("Failed to parse Mirae datetime string '%s': %s. Falling back to current UTC.", ts_str, exc)
        return datetime.now(timezone.utc).replace(tzinfo=None)


def fetch_inav_mirae(symbols: list[str]) -> list[dict[str, Any]]:
    """
    Fetch live iNAV snapshots for Mirae Asset ETFs from the Mirae AMC API.

    Parameters
    ----------
    symbols : list of internal symbols, e.g. ["MAFANG", "MAHKTECH"]

    Returns
    -------
    list of dicts with keys:
        symbol, snapshot_at (datetime UTC), inav, market_price,
        premium_discount_pct, source
    """
    requested_clean = {s.upper().replace(".NS", "") for s in symbols}
    target_symbols = [s for s in requested_clean if s in MIRAE_SYMBOLS]

    if not target_symbols:
        return []

    logger.info("Mirae iNAV: fetching live data for target symbols: %s", target_symbols)

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json",
    }

    # 1. Fetch live iNAV data from Mirae Asset API
    try:
        with httpx.Client(headers=headers, follow_redirects=True, timeout=_TIMEOUT) as client:
            resp = client.get(_MIRAE_API_URL)
            resp.raise_for_status()
            response_json = resp.json()
    except Exception as exc:
        logger.warning("Mirae iNAV ticker GET failed: %s", exc)
        return []

    if not isinstance(response_json, list):
        logger.warning("Mirae iNAV API returned non-list data structure")
        return []

    # Map target tickers to their raw API items
    matched_items: dict[str, dict[str, Any]] = {}
    for item in response_json:
        nse_symbol = str(item.get("NSE_Symbol") or item.get("nse_symbol") or "").upper()
        if nse_symbol in target_symbols:
            matched_items[nse_symbol] = item

    if not matched_items:
        logger.info("No matching Mirae ETFs found in API response")
        return []

    # 2. Fetch corresponding live market prices from yfinance
    yf_symbols = [f"{sym}.NS" for sym in matched_items.keys()]
    market_prices: dict[str, float] = {}

    try:
        logger.debug("Mirae iNAV: fetching live market prices for: %s", yf_symbols)
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
        logger.warning("Mirae iNAV: failed to fetch market prices from yfinance: %s", exc)

    # 3. Build snapshot rows
    rows: list[dict[str, Any]] = []
    for sym, item in matched_items.items():
        raw_inav = item.get("INAV") or item.get("inav")
        if raw_inav is None:
            continue

        inav = _safe(raw_inav)
        if inav <= 0:
            continue

        # Use yfinance market price if available; fallback to declared NAV or current iNAV
        market_price = market_prices.get(sym)
        if market_price is None or market_price <= 0:
            raw_nav = item.get("NAV") or item.get("nav")
            market_price = _safe(raw_nav) if raw_nav is not None else inav

        prem_disc = ((market_price - inav) / inav * 100)
        
        ts_str = item.get("timestamp") or ""
        snapshot_at = _parse_mirae_datetime(ts_str)

        rows.append({
            "symbol":               sym,
            "snapshot_at":          snapshot_at,
            "inav":                 inav,
            "market_price":         market_price,
            "premium_discount_pct": round(prem_disc, 4),
            "source":               "mirae_amc_live",
        })

    logger.info("Mirae iNAV: successfully compiled %d snapshot(s)", len(rows))
    return rows
