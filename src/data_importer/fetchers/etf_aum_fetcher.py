"""
src/importer/fetchers/etf_aum_fetcher.py
─────────────────────────────────────────
Tracks daily AUM (total assets) for major global gold ETFs as a proxy for
retail / institutional fund flows.

Source: yfinance (totalAssets field) — no API key needed.

AUM flow signals:
  AUM ↑ + price ↑  → normal appreciation (no signal)
  AUM ↑ + price ↓  → net inflows / accumulation (contrarian buy signal)
  AUM ↓ + price ↑  → redemptions / profit-taking (watch for reversal)
  AUM ↓ + price ↓  → panic outflows / capitulation (potential bottom)

Implied tonnes formula:
  gold_price_per_troy_oz × 32_150.7 troy_oz/tonne = USD per tonne
  implied_tonnes = aum_usd / (gold_price_per_oz * 32_150.7)
"""
from __future__ import annotations

import logging
import time
from datetime import date

import yfinance as yf

_GOLD_ETF_SYMBOLS  = ["GLD", "IAU", "SGOL", "PHYS"]
_GOLD_FUTURES_SYM  = "GC=F"
_TROY_OZ_PER_TONNE = 32_150.7

log = logging.getLogger(__name__)


def fetch_etf_aum(
    symbols: list[str] | None = None,
    snap_date: date | None = None,
) -> list[dict]:
    """
    Fetch today's (or snap_date's) AUM snapshot for each ETF symbol.

    Parameters
    ----------
    symbols   : Yahoo Finance tickers to fetch (default: GLD, IAU, SGOL, PHYS)
    snap_date : trade_date stored in ClickHouse (default: today)

    Returns
    -------
    List of dicts matching the etf_aum ClickHouse schema.
    Symbols that fail to fetch are skipped with a WARNING log.
    """
    if symbols is None:
        symbols = _GOLD_ETF_SYMBOLS
    if snap_date is None:
        snap_date = date.today()

    # Current gold price for implied-tonnes calculation
    gold_price = 0.0
    try:
        info = yf.Ticker(_GOLD_FUTURES_SYM).info
        gold_price = float(info.get("regularMarketPrice") or
                           info.get("previousClose") or 0)
    except Exception as exc:
        log.warning("Could not fetch gold spot price: %s", exc)

    rows: list[dict] = []
    for sym in symbols:
        try:
            info     = yf.Ticker(sym).info
            aum_usd  = float(info.get("totalAssets") or 0)
            price    = float(info.get("regularMarketPrice") or
                            info.get("navPrice") or
                            info.get("previousClose") or 0)

            implied_tonnes = (
                round(aum_usd / (gold_price * _TROY_OZ_PER_TONNE), 2)
                if gold_price > 0 and aum_usd > 0 else 0.0
            )

            rows.append({
                "trade_date":      snap_date,
                "symbol":          sym,
                "aum_usd":         round(aum_usd, 2),
                "price":           round(price, 4),
                "implied_tonnes":  implied_tonnes,
                "source":          "yfinance",
            })
            log.debug("ETF AUM %s: $%.0fM, %.1ft implied", sym, aum_usd / 1e6, implied_tonnes)
            time.sleep(0.35)   # polite delay
        except Exception as exc:
            log.warning("ETF AUM fetch failed for %s: %s", sym, exc)

    return rows
