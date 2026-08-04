"""
src/importer/fetchers/nse_index_fetcher.py
──────────────────────────────────────────
Fetches daily OHLCV data for NSE indices via nselib.capital_market.index_data().

Used for indices not available on Yahoo Finance (midcap/smallcap variants,
thematic, factor indices etc.).  Stores data in daily_prices with
category='indices'.
"""
from __future__ import annotations

import logging
import time
from datetime import date
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


def fetch_nse_indices(
    symbols: list[tuple[str, str]],   # [(internal_symbol, nse_api_name), ...]
    from_date: date,
    to_date: date,
) -> list[dict[str, Any]]:
    """
    Download daily OHLCV for NSE indices using nselib.

    nselib.capital_market.index_data() returns a DataFrame with columns:
        INDEX_NAME, OPEN_INDEX_VAL, HIGH_INDEX_VAL, CLOSE_INDEX_VAL,
        LOW_INDEX_VAL, TURN_OVER, TRADED_QTY, TIMESTAMP

    Returns rows compatible with daily_prices schema.
    """
    from nselib.capital_market import index_data

    rows: list[dict[str, Any]] = []
    fmt = "%d-%m-%Y"

    for internal_sym, nse_name in symbols:
        try:
            df = index_data(
                index=nse_name,
                from_date=from_date.strftime(fmt),
                to_date=to_date.strftime(fmt),
            )
        except Exception as exc:
            logger.warning(
                "nselib index_data failed for %s (%s): %s",
                internal_sym, nse_name, exc,
            )
            time.sleep(0.5)
            continue

        if df is None or df.empty:
            logger.debug("No data for index %s (%s)", internal_sym, nse_name)
            time.sleep(0.3)
            continue

        for _, row in df.iterrows():
            try:
                trade_date = pd.to_datetime(row["TIMESTAMP"]).date()
                rows.append({
                    "symbol":     internal_sym,
                    "category":   "indices",
                    "trade_date": trade_date,
                    "open":       float(row.get("OPEN_INDEX_VAL", 0) or 0),
                    "high":       float(row.get("HIGH_INDEX_VAL", 0) or 0),
                    "low":        float(row.get("LOW_INDEX_VAL", 0) or 0),
                    "close":      float(row.get("CLOSE_INDEX_VAL", 0) or 0),
                    "volume":     float(row.get("TRADED_QTY", 0) or 0),
                })
            except Exception as exc:
                logger.debug("Skipping row for %s: %s", internal_sym, exc)

        logger.debug(
            "Fetched %d rows for index %s (%s)",
            len([r for r in rows if r["symbol"] == internal_sym]),
            internal_sym, nse_name,
        )
        time.sleep(0.3)  # Rate limit: ~3 req/sec to avoid NSE throttling

    return rows
