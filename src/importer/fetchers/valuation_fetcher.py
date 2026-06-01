"""
src/importer/fetchers/valuation_fetcher.py
───────────────────────────────────────────
Fetch current valuation snapshot (PE, PB, margins, analyst consensus, etc.)
via yfinance .info for a list of symbols.
"""

from __future__ import annotations

import logging
from datetime import date

import yfinance as yf

logger = logging.getLogger(__name__)

_FLOAT_FIELDS = {
    "market_cap":        "marketCap",
    "trailing_pe":       "trailingPE",
    "forward_pe":        "forwardPE",
    "peg_ratio":         "pegRatio",
    "price_to_book":     "priceToBook",
    "price_to_sales":    "priceToSalesTrailing12Months",
    "debt_to_equity":    "debtToEquity",
    "return_on_equity":  "returnOnEquity",
    "profit_margin":     "profitMargins",
    "operating_margin":  "operatingMargins",
    "gross_margin":      "grossMargins",
    "revenue_growth":    "revenueGrowth",
    "earnings_growth":   "earningsGrowth",
    "free_cashflow":     "freeCashflow",
    "beta":              "beta",
    "target_price":      "targetMeanPrice",
}


def fetch_valuation(symbols: list[tuple[str, str]]) -> list[dict]:
    """
    Fetch today's valuation snapshot for each symbol.

    Parameters
    ----------
    symbols : list of (internal_name, yahoo_ticker)

    Returns
    -------
    list with one dict per symbol:
        symbol, snapshot_date, market_cap, trailing_pe, forward_pe, peg_ratio,
        price_to_book, price_to_sales, debt_to_equity, return_on_equity,
        profit_margin, operating_margin, gross_margin, revenue_growth,
        earnings_growth, free_cashflow, beta, target_price,
        recommendation, analyst_count
    """
    rows: list[dict] = []
    today = date.today()

    for internal, yahoo in symbols:
        try:
            info = yf.Ticker(yahoo).info
            if not info or not isinstance(info, dict):
                logger.warning("No valid valuation info returned for %s (rate-limited or unavailable)", yahoo)
                continue
            row: dict = {"symbol": internal, "snapshot_date": today}

            for col, key in _FLOAT_FIELDS.items():
                val = info.get(key)
                row[col] = float(val) if val is not None else 0.0

            row["recommendation"] = str(info.get("recommendationKey") or "")
            row["analyst_count"] = int(info.get("numberOfAnalystOpinions") or 0)

            rows.append(row)
            logger.info("Fetched valuation snapshot for %s", yahoo)

        except Exception as e:
            logger.error("Error fetching valuation for %s: %s", yahoo, e)

    return rows
