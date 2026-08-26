"""
src/tools/amc_inav_manager.py
─────────────────────────────
High-performance in-memory manager for real-time AMC direct iNAV feeds.

Aggregates:
  - Nippon India Mutual Fund (Nippon AMC API)
  - Zerodha Fund House (Zerodha AMC API)
  - Mirae Asset Mutual Fund (Mirae AMC API)
  - Motilal Oswal Mutual Fund (Motilal AMC API)

Features:
  - Background auto-refresh thread (configurable interval, default 60s)
  - Instant O(1) in-memory lookups
  - Category tagging (Domestic Largecap, Midcap, Sectoral, Commodity, International, Debt)
  - Zero scraping of NSE web pages — 100% block-proof official REST calls
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any

from src.data_importer.fetchers.nippon_inav_fetcher import (
    NIPPON_SYMBOL_MAP,
    _SCHEME_TO_SYMBOL,
    _parse_nippon_datetime,
    _NIPPON_DETAILS_URL,
    _TIMEOUT as NIPPON_TIMEOUT,
)
from src.data_importer.fetchers.zerodha_inav_fetcher import (
    ZERODHA_SYMBOLS,
    _parse_zerodha_datetime,
    _ZERODHA_API_URL,
    _TIMEOUT as ZERODHA_TIMEOUT,
)
from src.data_importer.fetchers.mirae_inav_fetcher import (
    MIRAE_SYMBOLS,
    _parse_mirae_datetime,
    _MIRAE_API_URL,
    _TIMEOUT as MIRAE_TIMEOUT,
)
from src.data_importer.fetchers.motilal_inav_fetcher import (
    MOTILAL_SYMBOLS,
    _parse_motilal_datetime,
    _MOTILAL_API_URL,
    _TIMEOUT as MOTILAL_TIMEOUT,
)
from src.data_importer.fetchers.base_inav_fetcher import _COMMON_HEADERS, _safe
import httpx

logger = logging.getLogger(__name__)

# Category mapping for tracked ETFs
ETF_CATEGORIES: dict[str, str] = {
    # Commodities
    "GOLDBEES": "Commodity (Gold)",
    "GOLDCASE": "Commodity (Gold)",
    "SILVERBEES": "Commodity (Silver)",
    "SILVERCASE": "Commodity (Silver)",
    "GOLDETF": "Commodity (Gold)",
    "SILVERAG": "Commodity (Silver)",
    "MOSILVER": "Commodity (Silver)",
    "MOGOLD": "Commodity (Gold)",
    # Domestic Broad Market
    "NIFTYBEES": "Equity (Largecap)",
    "SETFNIF50": "Equity (Largecap)",
    "HDFCNIFTY": "Equity (Largecap)",
    "TOP100CASE": "Equity (Largecap)",
    "JUNIORBEES": "Equity (Next 50)",
    "MID150CASE": "Equity (Midcap)",
    "MID150BEES": "Equity (Midcap)",
    "NIF100BEES": "Equity (Largecap)",
    "MONIFTY500": "Equity (Broad 500)",
    "MONIFTY100": "Equity (Largecap)",
    "MONEXT50": "Equity (Next 50)",
    "MON50EQUAL": "Equity (Equal Weight)",
    # Domestic Sectoral / Factor
    "BANKBEES": "Equity (Banking)",
    "PSUBNKBEES": "Equity (PSU Bank)",
    "ITBEES": "Equity (IT)",
    "PHARMABEES": "Equity (Pharma)",
    "AUTOBEES": "Equity (Auto)",
    "INFRABEES": "Equity (Infra)",
    "CPSEETF": "Equity (CPSE / PSU)",
    "MOM100": "Equity (Momentum)",
    "MOM50": "Equity (Momentum)",
    "MOMOMENTUM": "Equity (Momentum)",
    "CONSUMBEES": "Equity (Consumption)",
    "DIVOPPBEES": "Equity (Dividend)",
    # International
    "MON100": "International (US Tech)",
    "MONQ50": "International (US Tech)",
    "MAFANG": "International (US Tech)",
    "MAHKTECH": "International (HK Tech)",
    "MASPTOP50": "International (US Top 50)",
    "HNGSNGBEES": "International (Hang Seng)",
    # Debt / Liquid
    "LIQUIDBEES": "Debt (Liquid)",
    "LIQUIDCASE": "Debt (Liquid)",
    "LTGILTCASE": "Debt (Gilt)",
    "LTGILTBEES": "Debt (Gilt)",
}

# Thresholds for buy discount alert by category
DISCOUNT_THRESHOLDS: dict[str, float] = {
    "Commodity": -1.0,     # Alert if discount <= -1.0%
    "Equity": -0.6,        # Alert if discount <= -0.6%
    "International": -2.0, # Alert if discount <= -2.0%
    "Debt": -0.15,         # Alert if discount <= -0.15%
}


class AMCInavManager:
    """Thread-safe singleton/class for fetching and caching live AMC direct iNAVs."""

    def __init__(self, refresh_interval_secs: int = 60):
        self.refresh_interval = refresh_interval_secs
        self._cache: dict[str, dict[str, Any]] = {}
        self._last_refresh: float = 0.0
        self._lock = threading.Lock()
        self._running = False
        self._thread: threading.Thread | None = None

    def start_background_refresh(self):
        """Start daemon thread for periodic background refresh."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._refresh_loop, daemon=True)
        self._thread.start()
        logger.info("Started AMCInavManager background refresh (every %ds)", self.refresh_interval)

    def stop_background_refresh(self):
        """Stop background refresh thread."""
        self._running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)

    def _refresh_loop(self):
        # Initial immediate fetch
        self.refresh_all_now()
        while self._running:
            time.sleep(self.refresh_interval)
            try:
                self.refresh_all_now()
            except Exception as e:
                logger.warning("AMC iNAV background refresh error: %s", e)

    def refresh_all_now(self) -> dict[str, dict[str, Any]]:
        """Fetch fresh iNAVs from all supported AMC APIs concurrently."""
        fresh_data: dict[str, dict[str, Any]] = {}

        # 1. Nippon AMC
        try:
            nippon = self._fetch_nippon()
            fresh_data.update(nippon)
        except Exception as e:
            logger.debug("Nippon iNAV fetch error: %s", e)

        # 2. Zerodha AMC
        try:
            zerodha = self._fetch_zerodha()
            fresh_data.update(zerodha)
        except Exception as e:
            logger.debug("Zerodha iNAV fetch error: %s", e)

        # 3. Mirae AMC
        try:
            mirae = self._fetch_mirae()
            fresh_data.update(mirae)
        except Exception as e:
            logger.debug("Mirae iNAV fetch error: %s", e)

        # 4. Motilal AMC
        try:
            motilal = self._fetch_motilal()
            fresh_data.update(motilal)
        except Exception as e:
            logger.debug("Motilal iNAV fetch error: %s", e)

        with self._lock:
            self._cache.update(fresh_data)
            self._last_refresh = time.time()

        logger.debug("AMCInavManager refreshed %d ETF iNAVs", len(self._cache))
        return self.get_all_inavs()

    def get_inav(self, symbol: str) -> dict[str, Any] | None:
        """Get cached iNAV info for a specific symbol."""
        sym = symbol.strip().upper().replace(".NS", "")
        with self._lock:
            return self._cache.get(sym)

    def get_all_inavs(self) -> dict[str, dict[str, Any]]:
        """Get all cached iNAVs."""
        with self._lock:
            return dict(self._cache)

    @property
    def last_refresh_age_seconds(self) -> float:
        return time.time() - self._last_refresh if self._last_refresh > 0 else 9999.0

    # ── AMC Specific Fetchers (Direct Raw Parsing) ────────────────────────────

    def _fetch_nippon(self) -> dict[str, dict[str, Any]]:
        headers = {
            **_COMMON_HEADERS,
            "Content-Type": "application/json",
            "X-Requested-With": "XMLHttpRequest",
        }
        with httpx.Client(headers=headers, timeout=NIPPON_TIMEOUT) as client:
            resp = client.post(_NIPPON_DETAILS_URL, json={})
            resp.raise_for_status()
            details = resp.json().get("RVDetailsList", [])
            out = {}
            for item in details:
                sch_name = item.get("SchName", "")
                sym = _SCHEME_TO_SYMBOL.get(sch_name)
                cnav = _safe(item.get("CNav"))
                if sym and cnav > 0:
                    dt = _parse_nippon_datetime(item.get("Realdt") or "")
                    cat = ETF_CATEGORIES.get(sym, "Domestic ETF")
                    out[sym] = {
                        "symbol": sym,
                        "inav": cnav,
                        "timestamp": dt,
                        "amc": "Nippon India",
                        "source": "nippon_amc_live",
                        "category": cat,
                    }
            return out

    def _fetch_zerodha(self) -> dict[str, dict[str, Any]]:
        with httpx.Client(headers=_COMMON_HEADERS, timeout=ZERODHA_TIMEOUT) as client:
            resp = client.get(_ZERODHA_API_URL)
            resp.raise_for_status()
            schemes = resp.json().get("data", [])
            out = {}
            for sc in schemes:
                sym = str(sc.get("ticker", "")).upper()
                if sym not in ZERODHA_SYMBOLS:
                    continue
                stats = sc.get("schemeStats", {})
                inav_obj = stats.get("inav", {})
                val = _safe(inav_obj.get("val") if isinstance(inav_obj, dict) else None)
                ts_str = inav_obj.get("ts", "") if isinstance(inav_obj, dict) else ""
                if val <= 0:
                    val = _safe(stats.get("nav"))
                if val > 0:
                    dt = _parse_zerodha_datetime(ts_str) if ts_str else datetime.now(timezone.utc).replace(tzinfo=None)
                    cat = ETF_CATEGORIES.get(sym, "Domestic ETF")
                    out[sym] = {
                        "symbol": sym,
                        "inav": val,
                        "timestamp": dt,
                        "amc": "Zerodha Fund House",
                        "source": "zerodha_amc_live",
                        "category": cat,
                    }
            return out

    def _fetch_mirae(self) -> dict[str, dict[str, Any]]:
        with httpx.Client(headers=_COMMON_HEADERS, timeout=MIRAE_TIMEOUT) as client:
            resp = client.get(_MIRAE_API_URL)
            resp.raise_for_status()
            schemes = resp.json()
            out = {}
            if isinstance(schemes, list):
                for sc in schemes:
                    # Mirae API returns NSE_Symbol, INAV, NAV, timestamp
                    sym = str(sc.get("NSE_Symbol") or sc.get("symbol") or "").upper().strip()
                    if not sym:
                        continue
                    inav = _safe(sc.get("INAV") or sc.get("inav") or sc.get("NAV") or sc.get("nav"))
                    if inav > 0:
                        dt = _parse_mirae_datetime(str(sc.get("timestamp") or sc.get("dateTime", "")))
                        cat = ETF_CATEGORIES.get(sym, "Thematic ETF")
                        out[sym] = {
                            "symbol": sym,
                            "inav": inav,
                            "timestamp": dt,
                            "amc": "Mirae Asset",
                            "source": "mirae_amc_live",
                            "category": cat,
                        }
            return out

    def _fetch_motilal(self) -> dict[str, dict[str, Any]]:
        with httpx.Client(headers=_COMMON_HEADERS, timeout=MOTILAL_TIMEOUT) as client:
            resp = client.post(_MOTILAL_API_URL, json={"apiName": "GetINAVandPrice"})
            resp.raise_for_status()
            res_json = resp.json()
            out = {}
            # Motilal bundles under data.data -> m50M100Data (domestic) and n100Data (intl)
            inner_data = res_json.get("data", {})
            if isinstance(inner_data, dict) and "data" in inner_data:
                inner_data = inner_data["data"]

            entries = []
            if isinstance(inner_data, dict):
                entries = inner_data.get("m50M100Data", []) + inner_data.get("n100Data", [])
            elif isinstance(inner_data, list):
                entries = inner_data

            for item in entries:
                sym = str(item.get("nseSymbol") or item.get("symbol") or "").upper().strip()
                if not sym:
                    continue
                inav = _safe(item.get("currNav") or item.get("inav"))
                if inav > 0:
                    dt = _parse_motilal_datetime(str(item.get("currNavDate", "")))
                    cat = ETF_CATEGORIES.get(sym, "Thematic ETF")
                    out[sym] = {
                        "symbol": sym,
                        "inav": inav,
                        "timestamp": dt,
                        "amc": "Motilal Oswal",
                        "source": "motilal_amc_live",
                        "category": cat,
                    }
            return out


# Global singleton instance
_manager_instance: AMCInavManager | None = None
_manager_lock = threading.Lock()


def get_amc_inav_manager(refresh_interval: int = 60) -> AMCInavManager:
    """Retrieve global thread-safe AMCInavManager singleton."""
    global _manager_instance
    with _manager_lock:
        if _manager_instance is None:
            _manager_instance = AMCInavManager(refresh_interval_secs=refresh_interval)
            _manager_instance.refresh_all_now()
            _manager_instance.start_background_refresh()
        return _manager_instance
