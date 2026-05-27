"""
src/importer/fetchers/kite_inav_fetcher.py
───────────────────────────────────────────
Fetch live iNAV for Indian ETFs via Kite Connect's public instrument quote.

NSE publishes iNAV as separate non-tradeable instruments (e.g. GOLDBEINAV,
SILVERINAV, MON100INAV).  These are live, AMC-published values updated every
~15 seconds during market hours — the true intraday iNAV, not the previous
day's declared NAV returned by NSE's /api/etf endpoint.

No Kite Connect subscription is needed for the instrument lookup.
The quote API does require a valid Kite session (api_key + access_token).
Falls back gracefully when Kite is not configured.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

# NSE iNAV instrument symbols for every ETF we track.
# Source: Kite instruments dump — https://api.kite.trade/instruments/NSE
# Update this map when new ETFs are added.
INAV_INSTRUMENT_MAP: dict[str, str] = {
    # Domestic commodity ETFs
    "GOLDBEES":    "GOLDBEINAV",
    "SILVERBEES":  "SILVERINAV",
    "SILVERCASE":  "SILVERINAV",     # same underlying silver
    "GOLDCASE":    "GOLDBEINAV",     # same underlying gold

    # Domestic equity / fixed-income ETFs
    "NIFTYBEES":   "NIFTYBINAV",
    "JUNIORBEES":  "MID150INAV",     # closest proxy
    "BANKBEES":    "BANKBINAV",
    # PSUBNKBEES has no dedicated Kite iNAV instrument
    "LIQUIDBEES":  "LIQUIDINAV",
    "MID150BEES":  "MID150INAV",
    "PHARMABEES":  "PHARMAINAV",
    "AUTOBEES":    "AUTOBEINAV",
    "LTGILTBEES":  "LTGILTINAV",

    # International ETFs (overseas market closed during Indian hours;
    # prev-day NAV is the correct reference — no dedicated Kite iNAV instrument)
    "MAFANG":      "MAFANGINAV",
    "MON100":      "MON100INAV",
    "MASPTOP50":   "MASPTOINAV",
    "MAHKTECH":    "MAHKTEINAV",
    "MONQ50":      "MONQ50INAV",
    # HNGSNGBEES and PSUBNKBEES have no Kite iNAV instrument — fall back to NSE API
}

# Pre-resolved instrument tokens (avoids an instruments API call on every fetch).
# Source: Kite /instruments/NSE dump.  Refresh if tokens ever change.
INAV_TOKEN_MAP: dict[str, int] = {
    "GOLDBEINAV":  2106113,
    "SILVERINAV":  2108929,
    "NIFTYBINAV":  5641473,
    "BANKBINAV":   5629185,
    "LIQUIDINAV":  2593793,
    "MID150INAV":  2594305,
    "PHARMAINAV":  2598145,
    "AUTOBEINAV":  2598401,
    "LTGILTINAV":  2594049,
    "MAFANGINAV":  2590209,
    "MON100INAV":  2593281,
    "MASPTOINAV":  2590465,
    "MAHKTEINAV":  2586881,
    "MONQ50INAV":  2592769,
}


def _build_kite() -> Any | None:
    """Return a KiteConnect instance using env/settings credentials, or None."""
    try:
        from kiteconnect import KiteConnect
        from config.settings import settings
        api_key     = getattr(settings, "kite_api_key",     None)
        access_token = getattr(settings, "kite_access_token", None)
        if not api_key or not access_token:
            logger.debug("kite_inav_fetcher: Kite credentials not set — skipping")
            return None
        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(access_token)
        return kite
    except ImportError:
        logger.debug("kite_inav_fetcher: kiteconnect package not installed")
        return None
    except Exception as exc:
        logger.warning("kite_inav_fetcher: Kite init failed: %s", exc)
        return None


def fetch_inav_kite(symbols: list[str]) -> list[dict[str, Any]]:
    """
    Fetch live iNAV snapshots for *symbols* via Kite Connect quote API.

    Each returned dict has the same schema as nse_inav_fetcher rows:
        symbol, snapshot_at (naive UTC), inav, market_price,
        premium_discount_pct, source

    Returns an empty list if Kite is not configured or the API call fails.
    """
    kite = _build_kite()
    if kite is None:
        return []

    clean   = {s.upper().replace(".NS", "") for s in symbols}
    snapshot_at = datetime.now(timezone.utc).replace(tzinfo=None)

    # Build the list of iNAV instruments to quote
    instrument_keys: list[str] = []
    etf_for_inav:    dict[str, str] = {}   # inav_sym → etf_sym

    for etf in clean:
        inav_sym = INAV_INSTRUMENT_MAP.get(etf)
        if inav_sym:
            nse_key = f"NSE:{inav_sym}"
            instrument_keys.append(nse_key)
            etf_for_inav[inav_sym] = etf
        else:
            logger.debug("kite_inav_fetcher: no iNAV mapping for %s", etf)

    if not instrument_keys:
        return []

    # Also quote the ETF market prices in one call
    etf_keys = [f"NSE:{e}" for e in clean if e in INAV_INSTRUMENT_MAP]
    all_keys  = instrument_keys + etf_keys

    try:
        quotes = kite.quote(all_keys)
    except Exception as exc:
        logger.warning("kite_inav_fetcher: quote API failed: %s", exc)
        return []

    # Build ETF → market_price lookup
    ltp_map: dict[str, float] = {}
    for etf in clean:
        key = f"NSE:{etf}"
        if key in quotes:
            ltp_map[etf] = float(quotes[key].get("last_price", 0))

    rows: list[dict[str, Any]] = []
    for inav_sym, etf in etf_for_inav.items():
        key = f"NSE:{inav_sym}"
        if key not in quotes:
            logger.debug("kite_inav_fetcher: no quote returned for %s", key)
            continue

        inav_val = float(quotes[key].get("last_price", 0))
        if inav_val <= 0:
            logger.debug("kite_inav_fetcher: zero iNAV for %s", inav_sym)
            continue

        market_price = ltp_map.get(etf, inav_val)
        prem_disc    = ((market_price - inav_val) / inav_val * 100) if inav_val else 0.0

        rows.append({
            "symbol":               etf,
            "snapshot_at":          snapshot_at,
            "inav":                 round(inav_val, 4),
            "market_price":         round(market_price, 4),
            "premium_discount_pct": round(prem_disc, 4),
            "source":               "Kite",
        })
        logger.debug(
            "kite_inav_fetcher: %s iNAV=%.4f LTP=%.4f prem=%.4f%%",
            etf, inav_val, market_price, prem_disc,
        )

    logger.info(
        "kite_inav_fetcher: fetched %d iNAV snapshots via Kite at %s UTC",
        len(rows), snapshot_at,
    )
    return rows
