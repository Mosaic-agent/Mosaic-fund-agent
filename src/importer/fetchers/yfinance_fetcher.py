"""
src/importer/fetchers/yfinance_fetcher.py
─────────────────────────────────────────
Fetches daily OHLCV data from Yahoo Finance using yfinance.download().

Fetches up to BATCH_SIZE symbols per download call to avoid HTTP 429s
while keeping the number of round-trips low.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)

# Max symbols per yf.download() call — stay well below the soft limit
BATCH_SIZE = 40


def fetch_ohlcv(
    symbols: list[tuple[str, str]],   # [(nse_symbol, yahoo_ticker), ...]
    category: str,
    from_date: date,
    to_date: date,
) -> list[dict[str, Any]]:
    """
    Download daily OHLCV bars for the given symbols between from_date and to_date.

    Returns a list of dicts with keys:
        symbol, category, trade_date (date), open, high, low, close, volume

    Uses batch downloads for efficiency.  Only rows with valid close prices
    are returned (NaN rows are silently dropped).
    """
    if not symbols:
        return []

    rows: list[dict[str, Any]] = []

    # Split into batches
    for batch_start in range(0, len(symbols), BATCH_SIZE):
        batch = symbols[batch_start : batch_start + BATCH_SIZE]
        nse_map = {yahoo: nse for nse, yahoo in batch}
        tickers = list(nse_map.keys())

        try:
            df = yf.download(
                tickers,
                start=from_date.isoformat(),
                end=(to_date + timedelta(days=1)).isoformat(),  # end is exclusive
                auto_adjust=True,
                progress=False,
                threads=True,
            )
        except Exception as exc:
            logger.warning("yfinance download failed for batch %s: %s", tickers[:3], exc)
            continue

        if df.empty:
            continue

        # yf.download always returns MultiIndex columns (Price × Ticker) in
        # modern yfinance, even for a single ticker.  Only fall back to
        # constructing the MultiIndex if the columns are genuinely flat
        # (older yfinance behaviour when a single ticker was given).
        if not isinstance(df.columns, pd.MultiIndex):
            df.columns = pd.MultiIndex.from_product([df.columns, tickers])

        for yahoo_sym in tickers:
            nse_sym = nse_map[yahoo_sym]
            try:
                sym_df = df.xs(yahoo_sym, axis=1, level=1).dropna(subset=["Close"])
            except KeyError:
                logger.debug("No data for %s (%s)", nse_sym, yahoo_sym)
                continue

            for ts, row in sym_df.iterrows():
                trade_date = ts.date() if hasattr(ts, "date") else ts
                rows.append({
                    "symbol":     nse_sym,
                    "category":   category,
                    "trade_date": trade_date,
                    "open":       float(row.get("Open", 0) or 0),
                    "high":       float(row.get("High", 0) or 0),
                    "low":        float(row.get("Low", 0) or 0),
                    "close":      float(row["Close"]),
                    "volume":     float(row.get("Volume", 0) or 0),
                })

        logger.debug(
            "Fetched %d rows for batch %s..%s (%s→%s)",
            len(rows),
            tickers[0],
            tickers[-1],
            from_date,
            to_date,
        )

    return rows
