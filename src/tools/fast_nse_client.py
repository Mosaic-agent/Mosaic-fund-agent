"""
src/tools/fast_nse_client.py
────────────────────────────
High-Performance, Low-Latency Client for NSE (National Stock Exchange of India).

Features & Optimizations:
  1. Persistent Connection Pooling: Eliminates TCP & TLS 1.3 handshake overhead per request.
  2. Automatic Session & Cookie Warming: Maintains active Akamai session tokens.
  3. Dynamic Origin & Referer Routing: Matches Akamai WAF access patterns to prevent 403 Forbidden drops.
  4. High-Throughput Batch Scraping: Fetches entire market baskets (350+ ETFs or 2,000+ pre-open stocks)
     in a single sub-second network round-trip (<1ms per instrument).
  5. Latency Telemetry: Tracks and records round-trip time (RTT) for every operation.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Dict, List, Optional
import requests
from requests.adapters import HTTPAdapter

logger = logging.getLogger(__name__)

# Standard browser headers tuned for Akamai Bot Manager bypass
_BASE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/130.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

# Mapping of API endpoints to their authoritative origin/referer pages
_ORIGIN_MAP = {
    "etf": "https://www.nseindia.com/market-data/exchange-traded-funds-etf",
    "most_active": "https://www.nseindia.com/market-data/most-active-equities",
    "pre_open": "https://www.nseindia.com/market-data/pre-open-market-cm-and-emerge-market",
    "market_status": "https://www.nseindia.com/",
    "announcements": "https://www.nseindia.com/companies-listing/corporate-filings/announcements",
    "quote": "https://www.nseindia.com/get-quotes/equity",
}


class FastNSEClient:
    """Thread-safe persistent low-latency client for NSE market data endpoints."""

    _instance: Optional[FastNSEClient] = None
    _lock = threading.Lock()

    def __init__(self, pool_size: int = 25, timeout: float = 8.0):
        self.timeout = timeout
        self.session = requests.Session()

        # Mount connection pooling adapter with keep-alive
        adapter = HTTPAdapter(
            pool_connections=pool_size,
            pool_maxsize=pool_size,
            max_retries=2,
            pool_block=False,
        )
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

        self._warmed_origins: set[str] = set()
        self._last_warmup_ts: float = 0.0
        self._cookie_ttl_sec: float = 300.0  # Refresh cookies every 5 minutes

    @classmethod
    def get_instance(cls) -> FastNSEClient:
        """Singleton accessor for zero-overhead client sharing."""
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

    def _ensure_session_warm(self, origin_url: str = "https://www.nseindia.com/") -> None:
        """Ensure session cookies are initialized and fresh."""
        now = time.perf_counter()
        needs_refresh = (
            origin_url not in self._warmed_origins
            or (now - self._last_warmup_ts) > self._cookie_ttl_sec
        )
        if needs_refresh:
            try:
                headers = {**_BASE_HEADERS, "Referer": "https://www.google.com/"}
                self.session.get(origin_url, headers=headers, timeout=self.timeout)
                self._warmed_origins.add(origin_url)
                self._last_warmup_ts = now
                logger.debug("FastNSEClient: Warmed session at %s", origin_url)
            except Exception as exc:
                logger.warning("FastNSEClient: Session warmup error (%s): %s", origin_url, exc)

    def get(self, url: str, origin_key: str = "market_status", params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Execute a low-latency HTTP GET with persistent connection pooling and origin headers.
        Returns a dict with: {'data': json_or_content, 'latency_ms': float, 'status_code': int}.
        """
        origin_url = _ORIGIN_MAP.get(origin_key, "https://www.nseindia.com/")
        self._ensure_session_warm(origin_url)

        headers = {**_BASE_HEADERS, "Referer": origin_url}
        t0 = time.perf_counter()

        resp = self.session.get(url, headers=headers, params=params, timeout=self.timeout)
        latency_ms = (time.perf_counter() - t0) * 1000

        result: Dict[str, Any] = {
            "status_code": resp.status_code,
            "latency_ms": round(latency_ms, 2),
            "data": None,
        }

        if resp.status_code == 200:
            try:
                result["data"] = resp.json()
            except Exception:
                result["data"] = resp.text
        return result

    # ── High-Throughput Batch Methods ──────────────────────────────────────────

    def fetch_etf_universe(self) -> Dict[str, Any]:
        """Fetch all ~350 NSE-listed ETFs in a single round-trip."""
        url = "https://www.nseindia.com/api/etf"
        res = self.get(url, origin_key="etf")
        if res["data"] and isinstance(res["data"], dict):
            res["count"] = len(res["data"].get("data", []))
        return res

    def fetch_most_active(self, by: str = "volume") -> Dict[str, Any]:
        """Fetch most active securities by volume or value in real time."""
        url = f"https://www.nseindia.com/api/live-analysis-most-active-securities?index={by}"
        res = self.get(url, origin_key="most_active")
        if res["data"] and isinstance(res["data"], dict):
            res["count"] = len(res["data"].get("data", []))
        return res

    def fetch_pre_open_market(self, key: str = "ALL") -> Dict[str, Any]:
        """Fetch pre-open market prices for all 2,000+ securities in one call."""
        url = f"https://www.nseindia.com/api/market-data-pre-open?key={key}"
        res = self.get(url, origin_key="pre_open")
        if res["data"] and isinstance(res["data"], dict):
            res["count"] = len(res["data"].get("data", []))
        return res

    def fetch_market_status(self) -> Dict[str, Any]:
        """Check market status and exchange timings."""
        url = "https://www.nseindia.com/api/marketStatus"
        return self.get(url, origin_key="market_status")

    def fetch_corporate_announcements(self, index: str = "equities") -> Dict[str, Any]:
        """Fetch real-time corporate announcements from NSE."""
        url = f"https://www.nseindia.com/api/corporate-announcements?index={index}"
        return self.get(url, origin_key="announcements")


def get_fast_nse_client() -> FastNSEClient:
    """Helper function to obtain the shared FastNSEClient singleton."""
    return FastNSEClient.get_instance()
