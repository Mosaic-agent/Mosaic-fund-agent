"""
src/importer/fetchers/zerodha_inav_fetcher.py
──────────────────────────────────────────────
Fetches live iNAV snapshots for Zerodha Fund House ETFs (GOLDCASE, SILVERCASE,
LIQUIDCASE, etc.) directly from the Zerodha AMC API.

Endpoint: GET https://api.zerodhafundhouse.com/api/v1/schemes
Response: {"data": [{ticker, schemeStats: {inav: {val, ts}, nav}}, ...]}

Market prices (LTP) are fetched from yfinance via the shared base class.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import httpx

from src.data_importer.fetchers.base_inav_fetcher import BaseInavFetcher, _COMMON_HEADERS

logger = logging.getLogger(__name__)

_ZERODHA_API_URL = "https://api.zerodhafundhouse.com/api/v1/schemes"
_TIMEOUT = 15

# All 8 Zerodha Fund House ETFs with live iNAV from the API
ZERODHA_SYMBOLS = frozenset({
    "GOLDCASE",    "SILVERCASE",  "LIQUIDCASE",
    "TOP100CASE",  "MID150CASE",  "LTGILTCASE",
    "NIFTYCASE",   "SML100CASE",
})


def _parse_zerodha_datetime(ts_str: str) -> datetime:
    """Parse Zerodha's ISO UTC timestamp (e.g. '2026-07-03T10:29:49Z') → naive UTC datetime."""
    try:
        return datetime.fromisoformat(ts_str.replace("Z", "+00:00")).astimezone(timezone.utc).replace(tzinfo=None)
    except Exception as exc:
        logger.debug("Failed to parse Zerodha datetime '%s': %s", ts_str, exc)
        return datetime.now(timezone.utc).replace(tzinfo=None)


class _ZerodhaInavFetcher(BaseInavFetcher):
    source_label = "zerodha_amc_live"
    symbols = ZERODHA_SYMBOLS

    def _fetch_raw(self) -> Any:
        resp = httpx.get(
            _ZERODHA_API_URL,
            headers=_COMMON_HEADERS,
            follow_redirects=True,
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        schemes = resp.json().get("data", [])
        if not schemes:
            raise ValueError("Zerodha API returned empty data list")
        return schemes

    def _match_symbols(self, raw: Any, target: set[str]) -> dict[str, Any]:
        return {
            str(item.get("ticker", "")).upper(): item
            for item in raw
            if str(item.get("ticker", "")).upper() in target
        }

    def _extract_inav(self, item: Any) -> float | None:
        return item.get("schemeStats", {}).get("inav", {}).get("val")

    def _extract_timestamp(self, item: Any) -> datetime:
        ts = item.get("schemeStats", {}).get("inav", {}).get("ts") or ""
        return _parse_zerodha_datetime(ts)

    def _extract_fallback_price(self, item: Any) -> float | None:
        return item.get("schemeStats", {}).get("nav")


_fetcher = _ZerodhaInavFetcher()


def fetch_inav_zerodha(symbols: list[str]) -> list[dict[str, Any]]:
    """
    Fetch live iNAV snapshots for Zerodha Fund House ETFs.

    Parameters
    ----------
    symbols : list of NSE symbols, e.g. ["GOLDCASE", "SILVERCASE"]

    Returns
    -------
    list of dicts: symbol, snapshot_at (naive UTC), inav, market_price,
    premium_discount_pct, source
    """
    return _fetcher.fetch_inav(symbols)
