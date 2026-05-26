"""
src/importer/fetchers/nse_inav_fetcher.py
──────────────────────────────────────────
Fetches live iNAV snapshots for Indian ETFs from the NSE API.

The NSE ETF endpoint (https://www.nseindia.com/api/etf) returns all ETFs
with their current indicative NAV (iNAV) and last traded price.  It is
updated every ~15 seconds during market hours.

Since NSE does NOT provide historical iNAV, each call captures a single
timestamped snapshot.  Call repeatedly (e.g. every 15 min during market
hours via a cron job) to build a time series.

Returns a list of dicts suitable for ClickHouseImporter.insert_inav_snapshots().
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_NSE_ETF_URL   = "https://www.nseindia.com/api/etf"
_NSE_WARMUP    = "https://www.nseindia.com/market-data/exchange-traded-funds-etf"
_NSE_HEADERS   = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/market-data/exchange-traded-funds-etf",
    "Connection": "keep-alive",
}
_TIMEOUT = 15


def _safe(val: Any, default: float = 0.0) -> float:
    try:
        return float(str(val).replace(",", ""))
    except (TypeError, ValueError):
        return default


def fetch_inav_snapshots(symbols: list[str]) -> list[dict[str, Any]]:
    """
    Fetch a live iNAV snapshot for each symbol in `symbols` from the NSE API.

    Parameters
    ----------
    symbols : list of NSE symbols, e.g. ["GOLDBEES", "NIFTYBEES"]

    Returns
    -------
    list of dicts with keys:
        symbol, snapshot_at (datetime UTC), inav, market_price,
        premium_discount_pct, source

    Returns an empty list if the NSE API is unreachable (e.g. outside hours).
    """
    clean = {s.upper().replace(".NS", "") for s in symbols}
    snapshot_at = datetime.now(timezone.utc).replace(tzinfo=None)  # ClickHouse expects naive UTC

    try:
        with httpx.Client(headers=_NSE_HEADERS, follow_redirects=True, timeout=_TIMEOUT) as client:
            # Warm up to get session cookies required by NSE
            client.get(_NSE_WARMUP, timeout=10)
            time.sleep(0.6)
            resp = client.get(_NSE_ETF_URL, timeout=_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
    except Exception as exc:
        logger.warning("NSE iNAV fetch failed: %s", exc)
        return []

    etf_list: list[dict] = data.get("data", []) if isinstance(data, dict) else data
    if not etf_list:
        logger.warning("NSE iNAV API returned empty data list")
        return []

    rows: list[dict[str, Any]] = []
    for entry in etf_list:
        sym = str(entry.get("symbol", "")).upper()
        if sym not in clean:
            continue

        raw_inav = entry.get("nav") or entry.get("iNav")
        raw_ltp  = entry.get("ltP") or entry.get("lastPrice")

        if raw_inav is None:
            logger.debug("No iNAV value for %s in NSE response", sym)
            continue

        inav         = _safe(raw_inav)
        market_price = _safe(raw_ltp) if raw_ltp is not None else inav
        prem_disc    = ((market_price - inav) / inav * 100) if inav else 0.0

        if sym == "SILVERCASE":
            # Verify and correct duplicate GOLDCASE iNAV glitch
            goldcase_entry = next((e for e in etf_list if str(e.get("symbol", "")).upper() == "GOLDCASE"), None)
            silverbees_entry = next((e for e in etf_list if str(e.get("symbol", "")).upper() == "SILVERBEES"), None)
            if goldcase_entry and silverbees_entry:
                raw_gold_nav = goldcase_entry.get("nav") or goldcase_entry.get("iNav")
                raw_silverbees_nav = silverbees_entry.get("nav") or silverbees_entry.get("iNav")
                if raw_gold_nav and raw_silverbees_nav:
                    gold_inav = _safe(raw_gold_nav)
                    silverbees_inav = _safe(raw_silverbees_nav)
                    if abs(inav - gold_inav) < 1e-6 and silverbees_inav > 0:
                        corrected_inav = round(silverbees_inav * 0.106127, 4)
                        logger.info(
                            "Correcting SILVERCASE iNAV from %s to %s (ratio-scaled from SILVERBEES: %s)",
                            inav, corrected_inav, silverbees_inav
                        )
                        inav = corrected_inav
                        prem_disc = ((market_price - inav) / inav * 100) if inav else 0.0

        rows.append({
            "symbol":               sym,
            "snapshot_at":          snapshot_at,
            "inav":                 inav,
            "market_price":         market_price,
            "premium_discount_pct": round(prem_disc, 4),
            "source":               "NSE",
        })

    logger.info("NSE iNAV: captured %d snapshot(s) at %s UTC", len(rows), snapshot_at)
    return rows
