"""
src/importer/fetchers/earnings_fetcher.py
──────────────────────────────────────────
Fetch quarterly earnings history + upcoming earnings dates via yfinance.
"""

from __future__ import annotations

import logging
from datetime import date

import yfinance as yf

logger = logging.getLogger(__name__)


def fetch_earnings(symbols: list[tuple[str, str]]) -> list[dict]:
    """
    Fetch earnings calendar (past + upcoming) for the given symbol list.

    Parameters
    ----------
    symbols : list of (internal_name, yahoo_ticker)

    Returns
    -------
    list of dicts with keys:
        symbol, earnings_date, eps_estimate, eps_actual, surprise_pct
    """
    rows: list[dict] = []

    for internal, yahoo in symbols:
        try:
            ticker = yf.Ticker(yahoo)
            df = ticker.earnings_dates

            if df is None or df.empty:
                logger.warning("No earnings data for %s", yahoo)
                continue

            df = df.reset_index()
            df.columns = [c.strip() for c in df.columns]

            for _, row in df.iterrows():
                try:
                    ed = row.get("Earnings Date")
                    if ed is None:
                        continue
                    edate = ed.date() if hasattr(ed, "date") else date.fromisoformat(str(ed)[:10])
                    eps_est = float(row.get("EPS Estimate") or 0.0)
                    eps_act = float(row.get("Reported EPS") or 0.0)
                    surprise = float(row.get("Surprise(%)") or 0.0)

                    rows.append({
                        "symbol": internal,
                        "earnings_date": edate,
                        "eps_estimate": eps_est,
                        "eps_actual": eps_act,
                        "surprise_pct": surprise,
                    })
                except Exception as e:
                    logger.debug("Skipping earnings row for %s: %s", yahoo, e)

            logger.info("Fetched %d earnings records for %s", len([r for r in rows if r["symbol"] == internal]), yahoo)

        except Exception as e:
            logger.error("Error fetching earnings for %s: %s", yahoo, e)

    return rows
