"""
src/importer/fetchers/nselib_fetcher.py
────────────────────────────────────────
Fetches daily OHLCV data for NSE-listed ETFs using nselib — a direct
NSE data source with no authentication required.

nselib returns data from NSE's official price/volume API which is more
reliable than Yahoo Finance for Indian market symbols.

Limitations
───────────
NSE-listed symbols only.  Global indices (^GSPC, ^TNX), commodity
futures (GC=F), US ETFs (GLD), and FX pairs (USDINR=X) still require
Yahoo Finance.
"""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any

log = logging.getLogger(__name__)


def fetch_nselib_ohlcv(
    symbols: list[tuple[str, str]],   # [(nse_symbol, yahoo_ticker), ...]
    category: str,
    from_date: date,
    to_date: date,
) -> list[dict[str, Any]]:
    """
    Fetch daily OHLCV for NSE-listed symbols via nselib.

    Accepts the same (nse_symbol, yahoo_ticker) tuple format as
    yfinance_fetcher — yahoo_ticker is ignored; only nse_symbol is used.

    Returns rows with keys: symbol, category, trade_date, open, high, low, close, volume
    Symbols that fail are skipped and logged; caller should fall back to yfinance.
    """
    try:
        from nselib import capital_market  # type: ignore
    except ImportError:
        log.warning("nselib not installed — run: pip install nselib")
        return []

    from_str = from_date.strftime("%d-%m-%Y")
    to_str   = to_date.strftime("%d-%m-%Y")

    rows: list[dict[str, Any]] = []

    for idx, (nse_sym, _yahoo_sym) in enumerate(symbols, 1):
        pct = (idx / len(symbols)) * 100
        print(f"    [{idx}/{len(symbols)} - {pct:.1f}%] Fetching {nse_sym} via nselib...", flush=True)
        try:
            df = capital_market.price_volume_and_deliverable_position_data(
                nse_sym,
                from_date=from_str,
                to_date=to_str,
            )
        except Exception as exc:
            log.warning("nselib: fetch failed for %s: %s", nse_sym, exc)
            continue

        if df is None or df.empty:
            log.debug("nselib: no data for %s (%s → %s)", nse_sym, from_str, to_str)
            continue

        # Strip BOM from column names (nselib CSV quirk)
        df.columns = [c.strip().lstrip("﻿").strip('"') for c in df.columns]

        for _, row in df.iterrows():
            try:
                trade_date = _parse_date(str(row.get("Date", "")))
                if trade_date is None:
                    continue
                close = _safe_float(row.get("ClosePrice"))
                if not close:
                    continue
                rows.append({
                    "symbol":     nse_sym,
                    "category":   category,
                    "trade_date": trade_date,
                    "open":       _safe_float(row.get("OpenPrice")) or close,
                    "high":       _safe_float(row.get("HighPrice")) or close,
                    "low":        _safe_float(row.get("LowPrice"))  or close,
                    "close":      close,
                    "volume":     _safe_float(row.get("TotalTradedQuantity")) or 0.0,
                })
            except Exception as exc:
                log.debug("nselib: bad row for %s: %s — %s", nse_sym, dict(row), exc)
                continue

    log.info("nselib: fetched %d rows for %s (%s → %s)", len(rows), category, from_str, to_str)
    return rows


def _parse_date(raw: str) -> date | None:
    """Parse nselib date strings: 'DD-Mon-YYYY' or 'DD-MM-YYYY'."""
    for fmt in ("%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw.strip(), fmt).date()
        except ValueError:
            continue
    return None


def _safe_float(val) -> float:
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0
