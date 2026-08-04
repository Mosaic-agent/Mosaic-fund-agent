"""
src/importer/fetchers/nippon_inav_fetcher.py
─────────────────────────────────────────────
Fetches live iNAV snapshots for Nippon India Mutual Fund ETFs (like GOLDBEES,
SILVERBEES, NIFTYBEES, etc.) directly from the Nippon India AMC website.

Endpoint: POST https://etf.nipponindiaim.com/RealtimeNAV/Nav/DetailsFill (body: {})
Response: {"RVDetailsList": [{SchName, CNav, PNav, Realdt, ...}, ...]}

Market prices (LTP) are fetched from yfinance via the shared base class.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta
from typing import Any

import httpx

from src.data_importer.fetchers.base_inav_fetcher import BaseInavFetcher, _COMMON_HEADERS

logger = logging.getLogger(__name__)

_NIPPON_DETAILS_URL = "https://etf.nipponindiaim.com/RealtimeNAV/Nav/DetailsFill"
_TIMEOUT = 15

# Map our internal symbols to the scheme names used on the Nippon AMC website
NIPPON_SYMBOL_MAP: dict[str, str] = {
    # ── Existing tracked ETFs ──────────────────────────────────────────────
    "GOLDBEES":    "Nippon India ETF Gold BeES",
    "SILVERBEES":  "Nippon India Silver ETF",
    "NIFTYBEES":   "Nippon India ETF Nifty 50 BeES",
    "JUNIORBEES":  "Nippon India ETF Nifty Next 50 Junior BeES",
    "LIQUIDBEES":  "Nippon India ETF Nifty 1D Rate Liquid BeES",
    "HNGSNGBEES":  "Nippon India ETF Hang Seng BeES",
    "BANKBEES":    "Nippon India ETF Nifty Bank BeES",
    "PSUBNKBEES":  "Nippon India ETF Nifty PSU Bank BeES",
    "CPSEETF":     "CPSE ETF",
    "ITBEES":      "Nippon India ETF Nifty IT",
    "PHARMABEES":  "Nippon India Nifty Pharma ETF",
    "AUTOBEES":    "Nippon India Nifty Auto ETF",
    "INFRABEES":   "Nippon India ETF Nifty Infrastructure BeES",
    "SHARIABEES":  "Nippon India ETF Nifty 50 Shariah BeES",
    # ── Additional ETFs discovered via API coverage audit ──────────────────
    "NIF100BEES":  "Nippon India ETF Nifty 100",
    "NV20BEES":    "Nippon India ETF Nifty 50 Value 20",
    "MID150BEES":  "Nippon India ETF Nifty Midcap 150",
    "DIVOPPBEES":  "Nippon India ETF Nifty Dividend Opportunities 50",
    "CONSUMBEES":  "Nippon India ETF Nifty India Consumption",
    "MANUFGBEES":  "Nippon India ETF Nifty India Manufacturing",
    "LTGILTBEES":  "Nippon India ETF Nifty 8-13 yr G-Sec Long Term Gilt",
    "GILT5YBEES":  "Nippon India ETF Nifty 5 yr Benchmark G-Sec",
    "LIQGRWBEES":  "Nippon India Nifty 1D Rate Liquid ETF \u2013 Growth",
    "SENSEXIETF":  "Nippon India ETF BSE Sensex",
    "SNXT30BEES":  "Nippon India ETF BSE Sensex Next 30",
    "SNXT50BETA":  "Nippon India ETF BSE Sensex Next 50",
}

# Reverse mapping: scheme name → NSE symbol
_SCHEME_TO_SYMBOL: dict[str, str] = {v: k for k, v in NIPPON_SYMBOL_MAP.items()}


def _parse_nippon_datetime(realdt_str: str) -> datetime:
    """Parse Nippon's Realdt string (e.g. 'Friday, 03 July 2026 11:21:32 PM') → naive UTC datetime."""
    try:
        dt_local = datetime.strptime(realdt_str.strip(), "%A, %d %B %Y %I:%M:%S %p")
        ist_tz = timezone(timedelta(hours=5, minutes=30))
        return dt_local.replace(tzinfo=ist_tz).astimezone(timezone.utc).replace(tzinfo=None)
    except Exception as exc:
        logger.debug("Failed to parse Nippon datetime '%s': %s", realdt_str, exc)
        return datetime.now(timezone.utc).replace(tzinfo=None)


class _NipponInavFetcher(BaseInavFetcher):
    source_label = "nippon_amc_live"
    symbols = frozenset(NIPPON_SYMBOL_MAP)

    _nippon_headers = {
        **_COMMON_HEADERS,
        "Content-Type": "application/json",
        "X-Requested-With": "XMLHttpRequest",
    }

    def _fetch_raw(self) -> Any:
        resp = httpx.post(
            _NIPPON_DETAILS_URL,
            json={},
            headers=self._nippon_headers,
            follow_redirects=True,
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        details = resp.json().get("RVDetailsList", [])
        if not details:
            raise ValueError("Nippon API returned empty RVDetailsList")
        return details

    def _match_symbols(self, raw: Any, target: set[str]) -> dict[str, Any]:
        matched: dict[str, Any] = {}
        for item in raw:
            sch_name = item.get("SchName", "")
            sym = _SCHEME_TO_SYMBOL.get(sch_name)
            if sym and sym in target:
                matched[sym] = item
        return matched

    def _extract_inav(self, item: Any) -> float | None:
        return item.get("CNav")

    def _extract_timestamp(self, item: Any) -> datetime:
        return _parse_nippon_datetime(item.get("Realdt") or "")

    def _extract_fallback_price(self, item: Any) -> float | None:
        return item.get("PNav")


_fetcher = _NipponInavFetcher()


def fetch_inav_nippon(symbols: list[str]) -> list[dict[str, Any]]:
    """
    Fetch live iNAV snapshots for Nippon India ETFs.

    Parameters
    ----------
    symbols : list of NSE symbols, e.g. ["GOLDBEES", "SILVERBEES"]

    Returns
    -------
    list of dicts: symbol, snapshot_at (naive UTC), inav, market_price,
    premium_discount_pct, source
    """
    return _fetcher.fetch_inav(symbols)
