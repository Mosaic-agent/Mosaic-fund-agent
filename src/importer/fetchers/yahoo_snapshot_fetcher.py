"""
src/importer/fetchers/yahoo_snapshot_fetcher.py
───────────────────────────────────────────────
Fetches live price snapshots from Yahoo Finance using yfinance.

This is used for international indices and commodities where a
high-frequency (e.g. 5-min) time-series is required to build
intraday charts or alerts in ClickHouse.

Returns a list of dicts suitable for ClickHouseImporter.insert_inav_snapshots().
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import yfinance as yf

logger = logging.getLogger(__name__)


def fetch_yahoo_snapshots(symbols: list[tuple[str, str]]) -> list[dict[str, Any]]:
    """
    Fetch a live price snapshot for each symbol using yfinance.

    Parameters
    ----------
    symbols : list of (nse_symbol, yahoo_ticker) tuples,
              e.g. [("DXY", "DX-Y.NYB")]

    Returns
    -------
    list of dicts with keys:
        symbol, snapshot_at (datetime UTC), inav, market_price,
        premium_discount_pct, source
    """
    if not symbols:
        return []

    snapshot_at = datetime.now(timezone.utc).replace(tzinfo=None)  # ClickHouse naive UTC
    tickers = [yahoo for _, yahoo in symbols]
    nse_map = {yahoo: nse for nse, yahoo in symbols}

    try:
        # Fetch 1 minute interval for the last day to get the most recent tick
        df = yf.download(
            tickers,
            period="1d",
            interval="1m",
            progress=False,
            auto_adjust=True,
        )
    except Exception as exc:
        logger.warning("Yahoo snapshot fetch failed: %s", exc)
        return []

    if df.empty:
        logger.warning("Yahoo snapshot fetch returned empty DataFrame")
        return []

    rows: list[dict[str, Any]] = []

    # Handle MultiIndex columns (yf.download default)
    import pandas as pd
    if not isinstance(df.columns, pd.MultiIndex):
        # Single ticker case
        last_row = df.iloc[-1]
        price = float(last_row["Close"])
        sym = symbols[0][0]
        rows.append({
            "symbol": sym,
            "snapshot_at": snapshot_at,
            "inav": price,
            "market_price": price,
            "premium_discount_pct": 0.0,
            "source": "Yahoo",
        })
    else:
        # Multi ticker case
        for yahoo_sym in tickers:
            try:
                sym_df = df.xs(yahoo_sym, axis=1, level=1).dropna(subset=["Close"])
                if sym_df.empty:
                    continue
                last_row = sym_df.iloc[-1]
                price = float(last_row["Close"])
                rows.append({
                    "symbol": nse_map[yahoo_sym],
                    "snapshot_at": snapshot_at,
                    "inav": price,
                    "market_price": price,
                    "premium_discount_pct": 0.0,
                    "source": "Yahoo",
                })
            except KeyError:
                continue

    logger.info("Yahoo snapshot: captured %d snapshot(s) at %s UTC", len(rows), snapshot_at)
    return rows
