"""
src/importer/fetchers/mirae_inav_fetcher.py
────────────────────────────────────────────
Fetches live iNAV snapshots for Mirae Asset ETFs (MAFANG, MAHKTECH, MASPTOP50, etc.)
directly from the Mirae Asset AMC API.

Endpoint: GET https://miraeassetetf.co.in/api/ticker
Returns a JSON list of all schemes with their real-time iNAV values.

All 38 ETFs tracked by this fetcher are available via the single endpoint.
Market prices (LTP) are fetched from yfinance via the shared base class.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import httpx
import pytz

from src.data_importer.fetchers.base_inav_fetcher import BaseInavFetcher, _COMMON_HEADERS

logger = logging.getLogger(__name__)

_MIRAE_API_URL = "https://miraeassetetf.co.in/api/ticker"
_TIMEOUT = 15

# All 38 Mirae Asset ETFs available via the API
MIRAE_SYMBOLS = frozenset({
    # ── International / US / HK ETFs (prev-session close adjusted for FX) ──
    "MAFANG",     "MAHKTECH",   "MASPTOP50",
    # ── Broad Market ──────────────────────────────────────────────────
    "NIFTYETF",  "NEXT50",     "MIDCAPETF",  "SMALL250",   "MULTICAP",
    "EQUAL50",   "EQUAL200",   "SENSEXETF",
    # ── Sectoral / Thematic ─────────────────────────────────────────
    "BANKETF",   "BANKPSU",    "BFSI",       "ITETF",      "INTERNET",
    "MAKEINDIA", "EVINDIA",    "ENERGY",     "METAL",      "INFRA",
    "HEALTHCARE","DEFENCE",    "CONSUMER",   "MIDSMALL",   "SMALLCAP",
    "ALPHAETF",  "LOWVOL",     "VALUE",      "DIVIDEND",   "TOP20",
    "ESG",       "SELECTIPO",
    # ── Commodities ──────────────────────────────────────────────────
    "GOLDETF",   "SILVERAG",
    # ── Debt / Liquid ──────────────────────────────────────────────
    "LIQUID",    "LIQUIDPLUS", "GSEC10YEAR",
})


def _parse_mirae_datetime(ts_str: str) -> datetime:
    """
    Parse Mirae's timestamp and return naive UTC datetime.
    Supports ISO UTC format (e.g. '2026-07-03T10:29:59.985Z') and
    local date format (e.g. '03-Jul-2026 23:41:28') assumed to be in IST.
    """
    ts_str = ts_str.strip()
    if not ts_str:
        return datetime.now(timezone.utc).replace(tzinfo=None)
    if ts_str.endswith("Z"):
        try:
            return datetime.fromisoformat(ts_str.replace("Z", "+00:00")).astimezone(timezone.utc).replace(tzinfo=None)
        except Exception:
            pass
    try:
        dt_ist = datetime.strptime(ts_str, "%d-%b-%Y %H:%M:%S")
        ist_tz = pytz.timezone("Asia/Kolkata")
        dt_utc = ist_tz.localize(dt_ist).astimezone(timezone.utc)
        return dt_utc.replace(tzinfo=None)
    except Exception as exc:
        logger.debug("Failed to parse Mirae datetime '%s': %s", ts_str, exc)
        return datetime.now(timezone.utc).replace(tzinfo=None)


class _MiraeInavFetcher(BaseInavFetcher):
    source_label = "mirae_amc_live"
    symbols = MIRAE_SYMBOLS

    def _fetch_raw(self) -> Any:
        resp = httpx.get(
            _MIRAE_API_URL,
            headers=_COMMON_HEADERS,
            follow_redirects=True,
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        raw = resp.json()
        if not isinstance(raw, list):
            raise ValueError("Mirae API returned non-list response")
        return raw

    def _match_symbols(self, raw: Any, target: set[str]) -> dict[str, Any]:
        return {
            str(item.get("NSE_Symbol") or item.get("nse_symbol") or "").upper(): item
            for item in raw
            if str(item.get("NSE_Symbol") or item.get("nse_symbol") or "").upper() in target
        }

    def _extract_inav(self, item: Any) -> float | None:
        return item.get("INAV") or item.get("inav")

    def _extract_timestamp(self, item: Any) -> datetime:
        return _parse_mirae_datetime(item.get("timestamp") or "")

    def _extract_fallback_price(self, item: Any) -> float | None:
        return item.get("NAV") or item.get("nav")


_fetcher = _MiraeInavFetcher()


def fetch_inav_mirae(symbols: list[str]) -> list[dict[str, Any]]:
    """
    Fetch live iNAV snapshots for Mirae Asset ETFs.

    Parameters
    ----------
    symbols : list of NSE symbols, e.g. ["MAFANG", "MAHKTECH"]

    Returns
    -------
    list of dicts: symbol, snapshot_at (naive UTC), inav, market_price,
    premium_discount_pct, source
    """
    return _fetcher.fetch_inav(symbols)
