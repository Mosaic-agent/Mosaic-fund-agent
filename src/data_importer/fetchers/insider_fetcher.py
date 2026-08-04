"""
src/importer/fetchers/insider_fetcher.py
─────────────────────────────────────────
Fetch insider transaction history via yfinance.
"""

from __future__ import annotations

import logging
from datetime import date

import yfinance as yf

logger = logging.getLogger(__name__)


def fetch_insider_trades(symbols: list[tuple[str, str]]) -> list[dict]:
    """
    Fetch insider buy/sell transactions for the given symbols.

    Parameters
    ----------
    symbols : list of (internal_name, yahoo_ticker)

    Returns
    -------
    list of dicts with keys:
        symbol, insider_name, relation, transaction_date,
        transaction_type, shares, value
    """
    rows: list[dict] = []

    for internal, yahoo in symbols:
        try:
            ticker = yf.Ticker(yahoo)
            df = ticker.insider_transactions

            if df is None or df.empty:
                logger.warning("No insider transaction data for %s", yahoo)
                continue

            df = df.reset_index(drop=True)

            for _, row in df.iterrows():
                try:
                    start_date = row.get("Start Date")
                    if start_date is None:
                        continue
                    tdate = start_date.date() if hasattr(start_date, "date") else date.fromisoformat(str(start_date)[:10])

                    rows.append({
                        "symbol": internal,
                        "insider_name": str(row.get("Insider", "") or ""),
                        "relation": str(row.get("Position", "") or ""),
                        "transaction_date": tdate,
                        "transaction_type": str(row.get("Transaction", "") or ""),
                        "shares": float(row.get("Shares", 0) or 0),
                        "value": float(row.get("Value", 0) or 0),
                    })
                except Exception as e:
                    logger.debug("Skipping insider row for %s: %s", yahoo, e)

            logger.info(
                "Fetched %d insider records for %s",
                len([r for r in rows if r["symbol"] == internal]),
                yahoo,
            )

        except Exception as e:
            logger.error("Error fetching insider trades for %s: %s", yahoo, e)

    return rows
