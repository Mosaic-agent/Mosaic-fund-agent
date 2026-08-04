"""
src/importer/fetchers/base_inav_fetcher.py
───────────────────────────────────────────
Template Method base class for AMC iNAV fetchers.

All four AMC fetchers (Nippon, Zerodha, Mirae, Motilal) share the same
pipeline structure:

    1. Normalise requested symbols against the fetcher's known set
    2. HTTP call to the AMC API  →  raw response
    3. Parse raw response  →  {nse_symbol: api_item} map
    4. Batch-fetch market prices from yfinance
    5. Build output rows (inav, market_price, premium_discount_pct, source)

Steps 1, 4, and 5 are identical across fetchers and live here.
Steps 2 and 3 differ per AMC and are implemented by concrete subclasses.

Public API (unchanged from before):
    fetch_inav_nippon / fetch_inav_mirae / fetch_inav_zerodha / fetch_inav_motilal
    — each is a thin module-level wrapper calling the corresponding class.

Adding a new AMC fetcher:
    1. Subclass BaseInavFetcher
    2. Implement the 4 abstract methods
    3. Export a module-level function that calls instance.fetch_inav()
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any

import yfinance as yf

logger = logging.getLogger(__name__)

_COMMON_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}


def _safe(val: Any, default: float = 0.0) -> float:
    """Parse a possibly-stringified number, stripping commas and percent signs."""
    try:
        return float(str(val).replace(",", "").replace("%", "").strip())
    except (TypeError, ValueError):
        return default


def _fetch_yfinance_prices(symbols: list[str]) -> dict[str, float]:
    """
    Batch-fetch the most recent closing price for each NSE symbol.

    Parameters
    ----------
    symbols : bare NSE symbols, e.g. ["GOLDBEES", "SILVERBEES"]

    Returns
    -------
    dict mapping symbol → last close price (absent if unavailable)
    """
    if not symbols:
        return {}

    yf_syms = [f"{s}.NS" for s in symbols]
    prices: dict[str, float] = {}
    try:
        data = yf.download(yf_syms, period="1d", progress=False)
        if data.empty or "Close" not in data.columns:
            return prices
        close_df = data["Close"]
        for sym in symbols:
            yf_sym = f"{sym}.NS"
            series = None
            if hasattr(close_df, "columns"):
                if yf_sym in close_df.columns:
                    series = close_df[yf_sym]
                elif len(yf_syms) == 1:
                    series = close_df.iloc[:, 0]
            else:
                series = close_df
            if series is not None:
                series = series.dropna()
                if not series.empty:
                    val = series.iloc[-1]
                    if hasattr(val, "iloc"):
                        val = val.iloc[-1]
                    prices[sym] = float(val)
    except Exception as exc:
        logger.warning("yfinance batch price fetch failed: %s", exc)
    return prices


class BaseInavFetcher(ABC):
    """
    Template Method base for AMC iNAV fetchers.

    Subclasses must set `source_label` and `symbols` as class attributes,
    and implement the four abstract methods below.
    """

    #: Source label written into each output row, e.g. "nippon_amc_live"
    source_label: str

    #: Set of NSE symbols this fetcher covers
    symbols: frozenset[str]

    # ── Abstract hooks ────────────────────────────────────────────────────

    @abstractmethod
    def _fetch_raw(self) -> Any:
        """
        Perform the HTTP request(s) to the AMC API.

        Returns the parsed JSON body (dict or list).
        Must raise an exception on failure (caught by the template method).
        """

    @abstractmethod
    def _match_symbols(self, raw: Any, target: set[str]) -> dict[str, Any]:
        """
        Extract the subset of items from the raw API response that correspond
        to symbols in `target`.

        Returns {nse_symbol (upper): api_item}.
        """

    @abstractmethod
    def _extract_inav(self, item: Any) -> float | None:
        """
        Pull the iNAV value out of a single api_item.

        Return None to skip the row entirely.
        """

    @abstractmethod
    def _extract_timestamp(self, item: Any) -> datetime:
        """
        Parse the timestamp from a single api_item and return a naive UTC datetime.
        """

    def _extract_fallback_price(self, item: Any) -> float | None:
        """
        Return the declared NAV (or any other price) to use when yfinance has
        no data.  Default: None (falls back to iNAV itself).
        """
        return None

    def _staleness_check(self, sym: str, snapshot_at: datetime, inav: float) -> bool:
        """
        Return True to *keep* this row, False to discard it.

        Default: always keep.  Override to add staleness gates (e.g. Motilal).
        """
        return True

    # ── Template method ───────────────────────────────────────────────────

    def fetch_inav(self, symbols: list[str]) -> list[dict[str, Any]]:
        """
        Full pipeline: filter → fetch raw → match → yfinance prices → build rows.

        Parameters
        ----------
        symbols : NSE symbols requested by the caller

        Returns
        -------
        list of row dicts: symbol, snapshot_at, inav, market_price,
        premium_discount_pct, source
        """
        target = {s.upper().replace(".NS", "") for s in symbols} & set(self.symbols)
        if not target:
            return []

        logger.info("%s iNAV: fetching for %s", self.source_label, sorted(target))

        try:
            raw = self._fetch_raw()
        except Exception as exc:
            logger.warning("%s iNAV: fetch failed: %s", self.source_label, exc)
            return []

        matched = self._match_symbols(raw, target)
        if not matched:
            logger.info("%s iNAV: no matching symbols in API response", self.source_label)
            return []

        prices = _fetch_yfinance_prices(list(matched))

        rows: list[dict[str, Any]] = []
        for sym, item in matched.items():
            raw_inav = self._extract_inav(item)
            if raw_inav is None:
                continue
            inav = _safe(raw_inav)
            if inav <= 0:
                continue

            snapshot_at = self._extract_timestamp(item)

            if not self._staleness_check(sym, snapshot_at, inav):
                continue

            market_price = prices.get(sym)
            if market_price is None or market_price <= 0:
                fb = self._extract_fallback_price(item)
                market_price = _safe(fb) if fb is not None else inav

            prem_disc = (market_price - inav) / inav * 100

            rows.append({
                "symbol":               sym,
                "snapshot_at":          snapshot_at,
                "inav":                 inav,
                "market_price":         market_price,
                "premium_discount_pct": round(prem_disc, 4),
                "source":               self.source_label,
            })

        logger.info("%s iNAV: compiled %d row(s)", self.source_label, len(rows))
        return rows
