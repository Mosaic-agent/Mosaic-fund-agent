"""
src/tools/inav_fetcher.py
──────────────────────────
Fetches the Indicative NAV (iNAV) for Indian ETFs and calculates
the premium / discount vs the live market price.

Data sources (in priority order):
  1. NSE API  — https://www.nseindia.com/api/etf  (updated every 15s in market hours)
  2. Yahoo Finance navPrice / regularMarketPrice   (delayed fallback)

Usage:
    from src.tools.inav_fetcher import get_etf_inav, get_portfolio_etf_inav

    # Single ETF
    result = get_etf_inav("GOLDBEES")
    # result = {
    #   "symbol": "GOLDBEES", "is_etf": True,
    #   "inav": 61.23, "market_price": 61.80,
    #   "premium_discount_pct": 0.93, "premium_discount_label": "PREMIUM",
    #   "source": "NSE"
    # }

    # Batch — auto-filters non-ETFs from a portfolio list
    batch = get_portfolio_etf_inav(["GOLDBEES", "NIFTYBEES", "RELIANCE", "TCS"])
    # Returns only: {"GOLDBEES": {...}, "NIFTYBEES": {...}}
"""

from __future__ import annotations

import logging
import time
from typing import Optional

import httpx
import yfinance as yf

logger = logging.getLogger(__name__)

# ── Known Indian ETF symbols — [NON-SENSITIVE] ────────────────────────────────
# Sourced from NSE ETF list; any symbol not in this set falls back
# to a Yahoo Finance quoteType check.
KNOWN_ETF_SYMBOLS: set[str] = {
    "GOLDBEES", "NIFTYBEES", "BANKBEES", "JUNIORBEES", "LIQUIDBEES",
    "SILVERBEES", "ITBEES", "PHARMABEES", "INFRABEES", "SHARIABEES",
    "PSUBNKBEES", "HNGSNGBEES", "CONSUMBEES", "DIVOPPBEES", "AUTOBEES",
    "MOM100", "CPSEETF", "NETFIT", "ICICIB22", "SETFNIF50",
    "MAFSETF", "EBBETF0423", "HDFCSENSEX", "ABSLLIQUID", "KOTAKPSUBN",
    "LICNETFN50", "LICNETFSEN", "UTINIFTETF", "AXISNIFTY", "SBINIFTY",
    # International / thematic ETFs
    "MAFANG",     # Mirae Asset NYSE FANG+ ETF
    "MAHKTECH",   # Mirae Asset Hang Seng TECH ETF
    # Motilal Oswal international ETFs (often at large premium due to SEBI overseas cap)
    "MON100",     # Motilal Oswal NASDAQ 100 ETF
    "MASPTOP50",  # Motilal Oswal S&P 500 Top 50 ETF
    "MONQ50",     # Motilal Oswal NASDAQ Q-50 ETF
    "MONIFTY500", # Motilal Oswal Nifty 500 ETF
    "MONIFTY100", # Motilal Oswal Nifty 100 ETF
    "MONEXT50",   # Motilal Oswal Nifty Next 50 ETF
    "MON50EQUAL", # Motilal Oswal Nifty 50 Equal Weight ETF
    # Zerodha Case series
    "LIQUIDCASE", "GOLDCASE", "SILVERCASE",
}

# NSE iNAV endpoint — [NON-SENSITIVE]
_NSE_ETF_URL = "https://www.nseindia.com/api/etf"

# Headers required to pass NSE's bot protection — [NON-SENSITIVE]
_NSE_HEADERS = {
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

# Warm-up URL to obtain NSE session cookies — [NON-SENSITIVE]
_NSE_WARMUP_URL = "https://www.nseindia.com/market-data/exchange-traded-funds-etf"

# Module-level cache: populated on first _fetch_inav_nse call, reused for the
# rest of the process (one HTTP round-trip for all ETFs in a portfolio run).
_NSE_ETF_CACHE: list[dict] = []
_NSE_CACHE_LOADED: bool = False


# ── Symbol utilities ──────────────────────────────────────────────────────────

def _clean(symbol: str) -> str:
    """Strip .NS / .BO suffixes and upper-case."""
    return symbol.upper().replace(".NS", "").replace(".BO", "").strip()


def _nse_etf_symbols() -> frozenset[str]:
    """
    Return the set of all ETF symbols listed on NSE by loading the live API.

    Result is cached in _NSE_ETF_CACHE for the process lifetime so the
    network call only happens once.  Falls back to KNOWN_ETF_SYMBOLS if the
    NSE API is unreachable.
    """
    global _NSE_ETF_CACHE, _NSE_CACHE_LOADED
    if not _NSE_CACHE_LOADED:
        try:
            with httpx.Client(
                headers=_NSE_HEADERS,
                follow_redirects=True,
                timeout=12,
            ) as client:
                client.get(_NSE_WARMUP_URL, timeout=10)
                time.sleep(0.5)
                resp = client.get(_NSE_ETF_URL, timeout=10)
                resp.raise_for_status()
                data = resp.json()
                _NSE_ETF_CACHE = data.get("data", []) if isinstance(data, dict) else data
                _NSE_CACHE_LOADED = True

                # Apply SILVERCASE iNAV correction if glitch is present (NSE repeats GOLDCASE iNAV for SILVERCASE)
                goldcase_entry = next((x for x in _NSE_ETF_CACHE if str(x.get("symbol")).upper() == "GOLDCASE"), None)
                silvercase_entry = next((x for x in _NSE_ETF_CACHE if str(x.get("symbol")).upper() == "SILVERCASE"), None)
                silverbees_entry = next((x for x in _NSE_ETF_CACHE if str(x.get("symbol")).upper() == "SILVERBEES"), None)

                if goldcase_entry and silvercase_entry and silverbees_entry:
                    raw_gold_nav = goldcase_entry.get("nav")
                    raw_silver_nav = silvercase_entry.get("nav")
                    raw_silverbees_nav = silverbees_entry.get("nav")

                    if raw_gold_nav and raw_silver_nav and raw_silverbees_nav:
                        try:
                            gold_nav = float(str(raw_gold_nav).replace(",", ""))
                            silver_nav = float(str(raw_silver_nav).replace(",", ""))
                            silverbees_nav = float(str(raw_silverbees_nav).replace(",", ""))

                            if abs(gold_nav - silver_nav) < 1e-6 and silverbees_nav > 0:
                                corrected_silver_nav = round(silverbees_nav * 0.106127, 4)
                                silvercase_entry["nav"] = str(corrected_silver_nav)
                                logger.info(
                                    "Corrected SILVERCASE iNAV from %s to %s using SILVERBEES iNAV %s",
                                    raw_silver_nav, corrected_silver_nav, silverbees_nav
                                )
                        except (TypeError, ValueError):
                            pass

                logger.info("NSE ETF list loaded: %d symbols", len(_NSE_ETF_CACHE))
        except Exception as exc:
            logger.warning("NSE ETF list unavailable (%s) — falling back to static set", exc)
            return frozenset(KNOWN_ETF_SYMBOLS)
    return frozenset(str(r.get("symbol", "")).upper() for r in _NSE_ETF_CACHE)


def is_etf(symbol: str) -> bool:
    """
    Return True if `symbol` is a listed Indian ETF.

    Check order (first match wins):
      1. NSE ETF API — authoritative; covers all 320+ listed ETFs including
         newly-listed ones (e.g. MON100, MASPTOP50) that Yahoo misclassifies.
      2. Static KNOWN_ETF_SYMBOLS — fast offline fallback if NSE is down.
      3. Yahoo Finance quoteType — last resort; unreliable for Indian ETFs
         (returns "EQUITY" for many NSE ETFs).
    """
    clean = _clean(symbol)
    # 1. NSE authoritative list
    if clean in _nse_etf_symbols():
        return True
    # 2. Static whitelist (offline guard)
    if clean in KNOWN_ETF_SYMBOLS:
        return True
    # 3. Yahoo fallback (least reliable)
    try:
        qt = yf.Ticker(f"{clean}.NS").info.get("quoteType", "")
        return qt.upper() == "ETF"
    except Exception:
        return False


# ── iNAV fetchers ─────────────────────────────────────────────────────────────

def _fetch_inav_nse(symbol: str) -> Optional[tuple[float, float]]:
    """
    Primary source: NSE ETF API.

    The endpoint returns ALL ETFs as a list under a 'data' key.
    The full list is fetched once and cached for the lifetime of the process;
    subsequent calls for different symbols use the cached data.

    Returns (inav, market_price) tuple, or None on failure.
    """
    global _NSE_ETF_CACHE, _NSE_CACHE_LOADED
    try:
        # Reuse the cache populated by _nse_etf_symbols(); load it if not yet done
        if not _NSE_CACHE_LOADED:
            _nse_etf_symbols()   # populates _NSE_ETF_CACHE as a side-effect

        # Filter from cache by symbol
        clean_sym = symbol.upper()
        matched = next(
            (r for r in _NSE_ETF_CACHE if str(r.get("symbol", "")).upper() == clean_sym),
            None,
        )
        if matched is None:
            logger.debug("NSE ETF list has no entry for %s", symbol)
            return None

        raw_nav = matched.get("nav")
        raw_ltp = matched.get("ltP")
        if raw_nav is None:
            return None

        inav = float(str(raw_nav).replace(",", ""))
        market_price = float(str(raw_ltp).replace(",", "")) if raw_ltp else None
        return (inav, market_price)

    except Exception as exc:
        logger.debug("NSE iNAV fetch failed for %s: %s", symbol, exc)
        return None


def _fetch_inav_yahoo(symbol: str) -> Optional[float]:
    """
    Fallback source: Yahoo Finance.
    Returns navPrice if available, otherwise regularMarketPrice as proxy.
    Note: Yahoo data is delayed (15 min) and navPrice may be end-of-day.
    """
    try:
        info = yf.Ticker(f"{symbol}.NS").info
        return info.get("navPrice") or info.get("regularMarketPrice")
    except Exception as exc:
        logger.debug("Yahoo iNAV fallback failed for %s: %s", symbol, exc)
        return None


def _fetch_market_price(symbol: str) -> Optional[float]:
    """Fetch the latest market (traded) price for a symbol via Yahoo Finance."""
    try:
        info = yf.Ticker(f"{symbol}.NS").info
        return info.get("regularMarketPrice") or info.get("currentPrice")
    except Exception:
        return None


# ── Public API ────────────────────────────────────────────────────────────────

def get_etf_inav(symbol: str) -> dict:
    """
    Fetch iNAV for a single Indian ETF and compute premium / discount.

    Returns a dict:
    {
        "symbol":                  "GOLDBEES",
        "is_etf":                  True,
        "inav":                    61.23,       # iNAV per unit in ₹
        "market_price":            61.80,       # last traded price in ₹
        "premium_discount_pct":    0.93,        # +ve = premium, -ve = discount
        "premium_discount_label":  "PREMIUM",   # PREMIUM / DISCOUNT / FAIR VALUE
        "source":                  "NSE",
        "note":                    "..."
    }

    If symbol is not an ETF, returns {"is_etf": False, ...} without fetching.
    """
    clean = _clean(symbol)

    if not is_etf(clean):
        return {
            "symbol": clean,
            "is_etf": False,
            "inav": None,
            "market_price": None,
            "premium_discount_pct": None,
            "premium_discount_label": None,
            "source": None,
            "note": f"{clean} is not identified as an ETF — iNAV skipped",
        }

    logger.info("Fetching iNAV for ETF: %s", clean)

    # 1. Primary: NSE API — returns (inav, market_price) or None
    nse_result = _fetch_inav_nse(clean)
    source = "NSE"

    if nse_result is not None:
        inav, nse_market_price = nse_result
    else:
        inav = None
        nse_market_price = None

    # 2. Fallback: Yahoo Finance for iNAV
    if inav is None:
        inav = _fetch_inav_yahoo(clean)
        source = "Yahoo Finance (fallback)"

    # 3. Market price: prefer NSE ltP (same data source as iNAV); fallback to Yahoo
    market_price = nse_market_price if nse_market_price else _fetch_market_price(clean)

    # 4. Premium / discount calculation
    premium_discount_pct: Optional[float] = None
    premium_discount_label = "UNKNOWN"

    if inav and market_price:
        pct = round(((market_price - inav) / inav) * 100, 2)
        premium_discount_pct = pct
        if pct > 0.25:
            premium_discount_label = "PREMIUM"
        elif pct < -0.25:
            premium_discount_label = "DISCOUNT"
        else:
            premium_discount_label = "FAIR VALUE"

    return {
        "symbol": clean,
        "is_etf": True,
        "inav": round(inav, 4) if inav else None,
        "market_price": round(market_price, 4) if market_price else None,
        "premium_discount_pct": premium_discount_pct,
        "premium_discount_label": premium_discount_label,
        "source": source,
        "note": (
            "Positive premium_discount_pct = ETF trading above iNAV (premium). "
            "Negative = trading below iNAV (discount)."
        ),
    }


def get_portfolio_etf_inav(symbols: list[str]) -> dict[str, dict]:
    """
    Batch iNAV lookup for all ETFs found in a portfolio symbol list.
    Non-ETF symbols are silently skipped.

    Args:
        symbols: List of NSE trading symbols (e.g. from Zerodha holdings).

    Returns:
        Dict mapping ETF symbol → iNAV result dict.
        Only ETFs are included; stocks are excluded.

    Example:
        >>> get_portfolio_etf_inav(["GOLDBEES", "NIFTYBEES", "RELIANCE"])
        {"GOLDBEES": {...}, "NIFTYBEES": {...}}   # RELIANCE excluded
    """
    results: dict[str, dict] = {}
    for symbol in symbols:
        clean = _clean(symbol)
        data = get_etf_inav(clean)
        if data.get("is_etf"):
            results[clean] = data
    return results
