"""
src/tools/historic_inav.py
───────────────────────────
Fetches historic daily iNAV (Net Asset Value) for Indian ETFs and computes
the historic premium / discount vs market close price for each day.

Primary data source: MFAPI.in — free JSON API over official AMFI data
  - Official daily NAV published for all ETFs / mutual funds
  - Free, no authentication required
  - URL: https://api.mfapi.in/mf/{scheme_code}

Market close prices for premium/discount: Yahoo Finance (yfinance)

Usage:
    from src.tools.historic_inav import get_historic_inav

    data = get_historic_inav("GOLDBEES", days=30)
    # data = {
    #   "symbol":        "GOLDBEES",
    #   "from_date":     "2026-01-23",
    #   "to_date":       "2026-02-22",
    #   "records":       [{"date": "2026-02-22", "nav": 127.31, "market_close": 127.54,
    #                       "premium_discount_pct": 0.18, "label": "FAIR VALUE"}, ...],
    #   "avg_premium_discount_pct": 0.35,
    #   "trend":         "NARROWING",   # WIDENING / NARROWING / STABLE
    #   "max_premium":   {"date": "...", "pct": 1.2},
    #   "max_discount":  {"date": "...", "pct": -0.5},
    #   "sparkline":     "▂▃▅▆▅▄▃▂▁▂",  # 10-char Rich-compatible sparkline
    # }
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Optional

import requests
import yfinance as yf

logger = logging.getLogger(__name__)

# ── MFAPI Scheme Codes — [NON-SENSITIVE] ─────────────────────────────────────
# Sourced from MFAPI.in (https://api.mfapi.in/) — free JSON API over AMFI data
# Codes verified by live API calls: GET https://api.mfapi.in/mf/{scheme_code}
AMFI_SCHEME_CODES: dict[str, str] = {
    "GOLDBEES":   "140088",   # Nippon India ETF Gold BeES  (nav≈127.57 ✓)
    "NIFTYBEES":  "140084",   # Nippon India ETF Nifty 50 BeES
    "BANKBEES":   "140087",   # Nippon India ETF Nifty Bank BeES
    "JUNIORBEES": "140085",   # Nippon India ETF Nifty Next 50 Junior BeES
    "LIQUIDBEES": "140086",   # Nippon India ETF Nifty 1D Rate Liquid BeES
    "SILVERBEES": "149758",   # Nippon India Silver ETF
    "HNGSNGBEES": "140095",   # Nippon India ETF Hang Seng BeES
    "PSUBNKBEES": "140089",   # Nippon India ETF Nifty PSU Bank BeES
    "CPSETF":     "128751",   # CPSE ETF
    "CPSEETF":    "128751",   # alias
    "MAFANG":     "148927",   # Mirae Asset NYSE FANG+ ETF  (nav≈128.99 ✓)
    "MAHKTECH":   "149379",   # Mirae Asset Hang Seng TECH ETF
    "KOTAKGOLD":  "106193",   # Kotak Gold ETF
    "HDFCNIFTY":  "135853",   # HDFC Nifty 50 ETF
    "SETFNIF50":  "135106",   # SBI ETF Nifty 50
}

# MFAPI.in base URL — returns full NAV history as JSON — [NON-SENSITIVE]
_MFAPI_BASE = "https://api.mfapi.in/mf"

# Sparkline chars (8 levels, Unicode block elements)
_SPARK_CHARS = "▁▂▃▄▅▆▇█"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _pct_label(pct: float) -> str:
    if pct > 0.25:
        return "PREMIUM"
    if pct < -0.25:
        return "DISCOUNT"
    return "FAIR VALUE"


def _build_sparkline(values: list[float], width: int = 20) -> str:
    """
    Build a Unicode block sparkline from a list of floats.
    Positive = block above midline, negative = below. Normalised to 8 levels.
    """
    if not values:
        return ""
    lo, hi = min(values), max(values)
    span = hi - lo
    chars = []
    for v in values[-width:]:            # take latest `width` points
        if span == 0:
            idx = 3                      # midpoint when all values identical
        else:
            idx = int((v - lo) / span * 7)
        chars.append(_SPARK_CHARS[idx])
    return "".join(chars)


def _fetch_mfapi_nav(scheme_code: str, from_date: str, to_date: str) -> list[dict]:
    """
    Fetch daily NAV history from MFAPI.in (free JSON API over AMFI data).

    Args:
        scheme_code: MFAPI scheme code (e.g. "140088" for GOLDBEES)
        from_date:   "YYYY-MM-DD" lower bound (inclusive)
        to_date:     "YYYY-MM-DD" upper bound (inclusive)

    Returns:
        List of {"date": "YYYY-MM-DD", "nav": float} dicts, newest first.
    """
    try:
        url  = f"{_MFAPI_BASE}/{scheme_code}"
        resp = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        payload = resp.json()

        # MFAPI response: {"meta": {...}, "data": [{"date": "DD-MM-YYYY", "nav": "127.57"}, ...]}
        raw_data = payload.get("data", [])
        records: list[dict] = []
        for item in raw_data:
            date_str = item.get("date", "")
            nav_str  = item.get("nav", "")
            try:
                # MFAPI date format is DD-MM-YYYY  (e.g. "20-02-2026")
                dt  = datetime.strptime(date_str, "%d-%m-%Y")
                iso = dt.strftime("%Y-%m-%d")
                if not (from_date <= iso <= to_date):
                    continue
                records.append({"date": iso, "nav": round(float(nav_str), 4)})
            except (ValueError, TypeError):
                continue

        return sorted(records, key=lambda r: r["date"], reverse=True)

    except Exception as exc:
        logger.debug("MFAPI NAV history fetch failed for scheme %s: %s", scheme_code, exc)
        return []


def _fetch_yahoo_closes(symbol: str, from_date: str, to_date: str) -> dict[str, float]:
    """
    Fetch daily closing prices from Yahoo Finance for a date range.

    Returns:
        Dict mapping "YYYY-MM-DD" → close price.
    """
    try:
        ticker = yf.Ticker(f"{symbol}.NS")
        hist = ticker.history(start=from_date, end=to_date, interval="1d")
        return {
            str(idx.date()): round(float(row["Close"]), 4)
            for idx, row in hist.iterrows()
        }
    except Exception as exc:
        logger.debug("Yahoo close fetch failed for %s: %s", symbol, exc)
        return {}


# ── Public API ────────────────────────────────────────────────────────────────

def get_historic_inav(symbol: str, days: int = 30) -> dict:
    """
    Fetch historic daily iNAV and compute premium / discount vs market close.

    Args:
        symbol: NSE trading symbol (e.g. "GOLDBEES")
        days:   Number of calendar days to look back (default 30)

    Returns a dict:
    {
        "symbol":                  "GOLDBEES",
        "from_date":               "2026-01-23",
        "to_date":                 "2026-02-22",
        "records":                 [
            {
                "date": "2026-02-20",
                "nav": 127.31,
                "market_close": 127.54,
                "premium_discount_pct": 0.18,
                "label": "FAIR VALUE"
            }, ...
        ],
        "avg_premium_discount_pct": 0.42,
        "trend":                   "WIDENING",    # or NARROWING / STABLE
        "max_premium":             {"date": "...", "pct": 1.5},
        "max_discount":            {"date": "...", "pct": -0.3},
        "sparkline":               "▁▂▃▄▅▄▃▂▃▄▅▆▅▄▃▄▅▆▇▆",
        "source":                  "AMFI + Yahoo Finance",
        "note":                    "...",
    }

    Returns {"error": "..."} dict on failure.
    """
    clean = symbol.upper().replace(".NS", "").replace(".BO", "").strip()
    scheme_code = AMFI_SCHEME_CODES.get(clean)
    if not scheme_code:
        return {
            "symbol": clean,
            "error": f"No AMFI scheme code found for {clean}. Historic iNAV unavailable.",
        }

    to_dt   = datetime.today()
    from_dt = to_dt - timedelta(days=days)

    to_str_yf   = to_dt.strftime("%Y-%m-%d")
    from_str_yf = from_dt.strftime("%Y-%m-%d")

    logger.info("Fetching historic iNAV for %s (%s \u2192 %s)", clean, from_str_yf, to_str_yf)

    # Fetch from both sources (sequential is fine for 30 days)
    nav_records  = _fetch_mfapi_nav(scheme_code, from_str_yf, to_str_yf)
    close_prices = _fetch_yahoo_closes(clean, from_str_yf, to_str_yf)

    if not nav_records:
        return {
            "symbol": clean,
            "error": f"MFAPI returned no NAV records for {clean} (scheme {scheme_code}) in the requested period.",
        }

    # ── Merge and compute premium / discount ──────────────────────────────────
    records: list[dict] = []
    for r in nav_records:
        date       = r["date"]
        nav        = r["nav"]
        mkt_close  = close_prices.get(date)
        if mkt_close is None:
            continue                # skip days with no market data (weekends/holidays)

        pct = round(((mkt_close - nav) / nav) * 100, 2)
        records.append({
            "date":                  date,
            "nav":                   nav,
            "market_close":          mkt_close,
            "premium_discount_pct":  pct,
            "label":                 _pct_label(pct),
        })

    if not records:
        return {
            "symbol": clean,
            "error": "No overlapping AMFI + Yahoo dates found.",
        }

    # Sort oldest → newest for trend analysis and sparkline
    records.sort(key=lambda r: r["date"])

    pcts = [r["premium_discount_pct"] for r in records]
    avg_pct = round(sum(pcts) / len(pcts), 2)

    # Trend: compare first-half avg vs second-half avg
    mid = len(pcts) // 2
    first_half  = sum(pcts[:mid]) / max(mid, 1)
    second_half = sum(pcts[mid:]) / max(len(pcts) - mid, 1)
    delta = second_half - first_half
    if abs(delta) < 0.1:
        trend = "STABLE"
    elif delta > 0:
        trend = "WIDENING"     # premium growing over time
    else:
        trend = "NARROWING"    # premium shrinking (good if was high)

    max_prem_rec = max(records, key=lambda r: r["premium_discount_pct"])
    max_disc_rec = min(records, key=lambda r: r["premium_discount_pct"])

    return {
        "symbol":                   clean,
        "from_date":                records[0]["date"],
        "to_date":                  records[-1]["date"],
        "records":                  records,
        "avg_premium_discount_pct": avg_pct,
        "avg_label":                _pct_label(avg_pct),
        "trend":                    trend,
        "max_premium":              {"date": max_prem_rec["date"], "pct": max_prem_rec["premium_discount_pct"]},
        "max_discount":             {"date": max_disc_rec["date"], "pct": max_disc_rec["premium_discount_pct"]},
        "data_points":              len(records),
        "sparkline":                _build_sparkline(pcts),
        "source":                   "MFAPI.in (AMFI data) + Yahoo Finance",
        "note": (
            "nav = official AMFI daily NAV via MFAPI.in. "
            "market_close = Yahoo Finance daily close. "
            "premium_discount_pct = (market_close - nav) / nav * 100."
        ),
    }
