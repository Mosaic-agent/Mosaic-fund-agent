"""
src/importer/fetchers/fx_rates_fetcher.py
──────────────────────────────────────────
Fetches daily USD exchange rate OHLC data via Yahoo Finance (yfinance).

Source: Yahoo Finance — no API key required.
Cadence: Daily (markets open on trading days for each pair).

Pairs tracked:
  USDINR  — US Dollar / Indian Rupee
  USDCNY  — US Dollar / Chinese Yuan
  USDAED  — US Dollar / UAE Dirham
  USDSAR  — US Dollar / Saudi Riyal
  USDKWD  — US Dollar / Kuwaiti Dinar

Relevance to the gold fund:
  • Gold is priced in USD globally; INR depreciation amplifies local gold returns.
  • CNY rate tracks Chinese demand-side FX dynamics.
  • AED + SAR are pegged to USD (~3.67 and ~3.75 respectively) — peg breaks
    or peg stress show up as volatility spikes and are rare tail-risk indicators.
  • KWD is one of the strongest currencies vs USD — used as a Gulf diversification signal.

Schema stored: trade_date, symbol, open, high, low, close, source
"""
from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

import pandas as pd
import yfinance as yf

log = logging.getLogger(__name__)

# Yahoo Finance ticker → short symbol stored in ClickHouse
FX_PAIRS: list[tuple[str, str]] = [
    ("USDINR", "USDINR=X"),
    ("USDCNY", "USDCNY=X"),
    ("USDAED", "USDAED=X"),
    ("USDSAR", "USDSAR=X"),
    ("USDKWD", "USDKWD=X"),
]

_BATCH_SIZE = 10   # all 5 fit in one call; keep headroom


def fetch_fx_rates(
    pairs: list[tuple[str, str]] | None = None,
    from_date: date | None = None,
    to_date: date | None = None,
) -> list[dict[str, Any]]:
    """
    Download daily OHLC for the given USD FX pairs.

    Parameters
    ----------
    pairs     : list of (symbol, yahoo_ticker) — defaults to FX_PAIRS
    from_date : start date (inclusive) — defaults to 2 years ago
    to_date   : end date (inclusive)  — defaults to today

    Returns
    -------
    List of dicts with keys:
        symbol, trade_date (date), open, high, low, close, source="yfinance"
    Only rows with a valid close are returned (NaN drops silently).
    """
    if pairs is None:
        pairs = FX_PAIRS
    if to_date is None:
        to_date = date.today()
    if from_date is None:
        from_date = to_date - timedelta(days=730)

    if not pairs:
        return []

    yahoo_map = {yahoo: sym for sym, yahoo in pairs}
    tickers   = list(yahoo_map.keys())

    try:
        df = yf.download(
            tickers,
            start=from_date.isoformat(),
            end=(to_date + timedelta(days=1)).isoformat(),   # end is exclusive
            auto_adjust=True,
            progress=False,
            threads=True,
        )
    except Exception as exc:
        log.warning("yfinance FX download failed: %s", exc)
        return []

    if df.empty:
        log.warning("yfinance returned empty FX data for %s", tickers)
        return []

    rows: list[dict[str, Any]] = []

    # yfinance returns MultiIndex columns (field, ticker) when multiple tickers
    if isinstance(df.columns, pd.MultiIndex):
        for yahoo_ticker, short_sym in yahoo_map.items():
            try:
                sym_df = df.xs(yahoo_ticker, axis=1, level=1)[["Open", "High", "Low", "Close"]]
            except KeyError:
                log.warning("FX ticker %s missing from yfinance response", yahoo_ticker)
                continue
            _append_rows(rows, sym_df, short_sym)
    else:
        # Single ticker — flat columns
        if len(pairs) == 1:
            short_sym = pairs[0][0]
            _append_rows(rows, df[["Open", "High", "Low", "Close"]], short_sym)

    rows.sort(key=lambda r: (r["symbol"], r["trade_date"]))
    log.info("FX rates: %d rows fetched (%d pairs, %s→%s)", len(rows), len(pairs), from_date, to_date)
    return rows


def _append_rows(rows: list, df: "pd.DataFrame", symbol: str) -> None:
    """Append non-NaN rows from a single-ticker OHLC DataFrame."""
    df = df.dropna(subset=["Close"])
    for idx, row in df.iterrows():
        rows.append({
            "symbol":     symbol,
            "trade_date": idx.date() if hasattr(idx, "date") else idx,
            "open":       float(row["Open"])  if pd.notna(row["Open"])  else float(row["Close"]),
            "high":       float(row["High"])  if pd.notna(row["High"])  else float(row["Close"]),
            "low":        float(row["Low"])   if pd.notna(row["Low"])   else float(row["Close"]),
            "close":      float(row["Close"]),
            "source":     "yfinance",
        })
