"""
src/db/queries.py
──────────────────
Typed, parameterized ClickHouse query accessors (Repository Pattern).

All functions use ``src.db.pool.query_df`` internally with ``FINAL`` (to
deduplicate ReplacingMergeTree rows) and parameterized arguments (to prevent
SQL injection).

Previously, equivalent inline SQL blocks were duplicated across:
  • src/tools/market/correlation_tools.py  (3 inline queries)
  • src/tools/indian_equity_tools.py
  • src/tools/premium_alerts.py
  • src/tools/garch_position_sizer.py
  • src/tools/quant_scorecard.py

Usage
-----
    from src.db.queries import fetch_ohlcv, fetch_benchmark, fetch_fx_rates

    df = fetch_ohlcv("GOLDBEES", days=365)
    bench = fetch_benchmark("NIFTYBEES", days=365)
    fx = fetch_fx_rates("USDINR", days=365)
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


def _cutoff_date(days: int) -> str:
    """Return the ISO-8601 date string for *days* before today."""
    return (date.today() - timedelta(days=days)).isoformat()


def fetch_ohlcv(
    symbol: str,
    days: int = 365,
) -> pd.DataFrame:
    """
    Fetch full OHLCV history for *symbol* from ``market_data.daily_prices``.

    Args:
        symbol: NSE/BSE ticker (e.g. ``"GOLDBEES"``, ``"RELIANCE"``).
        days:   Lookback window in calendar days (default 365).

    Returns:
        DataFrame with columns ``[trade_date, open, high, low, close, volume]``,
        sorted ascending by ``trade_date``.  Empty DataFrame on error.
    """
    from src.db.pool import query_df

    try:
        df = query_df(
            """
            SELECT trade_date,
                   toFloat64(argMax(open,   imported_at)) AS open,
                   toFloat64(argMax(high,   imported_at)) AS high,
                   toFloat64(argMax(low,    imported_at)) AS low,
                   toFloat64(argMax(close,  imported_at)) AS close,
                   toFloat64(argMax(volume, imported_at)) AS volume
            FROM market_data.daily_prices FINAL
            WHERE symbol = {sym:String}
              AND trade_date >= {cutoff:String}
            GROUP BY trade_date
            ORDER BY trade_date ASC
            """,
            parameters={"sym": symbol.strip().upper(), "cutoff": _cutoff_date(days)},
        )
        if not df.empty:
            df["trade_date"] = pd.to_datetime(df["trade_date"])
        return df
    except Exception as exc:
        logger.warning("fetch_ohlcv(%s, days=%d): %s", symbol, days, exc)
        return pd.DataFrame()


def fetch_close(
    symbol: str,
    days: int = 365,
    table: str = "daily_prices",
) -> pd.DataFrame:
    """
    Fetch close-price history for *symbol*.

    Supports both ``daily_prices`` (equities/ETFs) and ``fx_rates`` (FX pairs).

    Args:
        symbol: Ticker or currency pair (e.g. ``"NIFTYBEES"``, ``"USDINR"``).
        days:   Lookback window in calendar days.
        table:  ClickHouse table name (``"daily_prices"`` or ``"fx_rates"``).

    Returns:
        DataFrame with columns ``[trade_date, close]``, ascending.
    """
    from src.db.pool import query_df

    valid_tables = {"daily_prices", "fx_rates", "mf_nav"}
    if table not in valid_tables:
        logger.error("fetch_close: unknown table %r (allowed: %s)", table, valid_tables)
        return pd.DataFrame()

    try:
        df = query_df(
            f"""
            SELECT trade_date,
                   toFloat64(argMax(close, imported_at)) AS close
            FROM market_data.{table} FINAL
            WHERE symbol = {{sym:String}}
              AND trade_date >= {{cutoff:String}}
            GROUP BY trade_date
            ORDER BY trade_date ASC
            """,
            parameters={"sym": symbol.strip().upper(), "cutoff": _cutoff_date(days)},
        )
        if not df.empty:
            df["trade_date"] = pd.to_datetime(df["trade_date"])
        return df
    except Exception as exc:
        logger.warning("fetch_close(%s, table=%s, days=%d): %s", symbol, table, days, exc)
        return pd.DataFrame()


def fetch_benchmark(symbol: str = "NIFTYBEES", days: int = 365) -> pd.DataFrame:
    """
    Convenience wrapper: fetch close-price history for a benchmark ETF.

    Args:
        symbol: Benchmark ticker (default ``"NIFTYBEES"``).
        days:   Lookback window.

    Returns:
        DataFrame with columns ``[trade_date, close]``.
    """
    return fetch_close(symbol, days=days, table="daily_prices")


def fetch_fx_rates(pair: str = "USDINR", days: int = 365) -> pd.DataFrame:
    """
    Convenience wrapper: fetch close-rate history for an FX pair.

    Args:
        pair: Currency pair (e.g. ``"USDINR"``, ``"EURUSD"``).
        days: Lookback window.

    Returns:
        DataFrame with columns ``[trade_date, close]``.
    """
    return fetch_close(pair, days=days, table="fx_rates")


def fetch_latest_signal(symbol: str) -> Optional[dict]:
    """
    Fetch the most recent composite signal row for *symbol*.

    Args:
        symbol: NSE/BSE ticker.

    Returns:
        Dict of the latest ``signal_composite`` row, or ``None`` if not found.
    """
    from src.db.pool import query_df

    try:
        df = query_df(
            """
            SELECT *
            FROM market_data.signal_composite FINAL
            WHERE symbol = {sym:String}
            ORDER BY trade_date DESC
            LIMIT 1
            """,
            parameters={"sym": symbol.strip().upper()},
        )
        if df.empty:
            return None
        return df.iloc[0].to_dict()
    except Exception as exc:
        logger.warning("fetch_latest_signal(%s): %s", symbol, exc)
        return None
