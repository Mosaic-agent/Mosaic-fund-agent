"""
src/importer/fetchers/motilal_inav_fetcher.py
──────────────────────────────────────────────
Fetches live iNAV snapshots for Motilal Oswal AMC ETFs (MON100, MONQ50, etc.)
directly from the Motilal Oswal AMC internal API.

Endpoint: POST https://www.motilaloswalmf.com/mutualfund/api/v1/someFunc
Body:     {"apiName": "GetINAVandPrice"}

Returns all ETF iNAV + price records grouped under m50M100Data and n100Data.
iNAV entries are identified by "iNAV" in the secname field.

Market prices (LTP) are fetched from yfinance to calculate the live
premium/discount. The iNAV for international ETFs (MON100, MONQ50) reflects
the previous US session's close adjusted for current USDINR — not a live
intraday value — since the Nasdaq is closed during Indian trading hours.

Staleness gate: rows whose currNavDate is older than _MAX_STALENESS_DAYS (2)
calendar days are silently dropped so that the NSE step-1 data prevails.
This guards against Motilal's batch refresh job stalling (observed Jul 2026:
all 32 ETFs frozen for 28+ days). Domestic ETFs are typically refreshed
every market session; international ETFs only refresh after each Nasdaq close.

Tracked symbols (32 total):
  Domestic (m50M100Data): MOM50, MOM100, MOALPHA50, MOBANK10, MOCAPITAL,
    MODEFENCE, MOENERGY, MOGSEC, MOGOLD, MOINFRA, MOIPO, MOMENTUM50, MOMGF,
    MOMIDMTM, MOMNC, MOLOWVOL, MONIFTY500, MON50EQUAL, MONEXT50, MOMOMENTUM,
    MOPSE, MOREALTY, MOSERVICE, MOSILVER, MOSMALL250, MOVALUE, MOHEALTH,
    MOQUALITY, MOTOUR, MONIFTY100
  International (n100Data): MON100, MONQ50
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta
from typing import Any

import httpx
import yfinance as yf

logger = logging.getLogger(__name__)

_MOTILAL_API_URL = "https://www.motilaloswalmf.com/mutualfund/api/v1/someFunc"
_TIMEOUT = 15

# All ETFs managed by Motilal Oswal AMC that expose live iNAV via their API.
# Domestic ETFs come from m50M100Data; international ETFs from n100Data.
MOTILAL_SYMBOLS = {
    # Domestic
    "MOM50", "MOM100", "MOALPHA50", "MOBANK10", "MOCAPITAL",
    "MODEFENCE", "MOENERGY", "MOGSEC", "MOGOLD", "MOINFRA",
    "MOIPO", "MOMENTUM50", "MOMGF", "MOMIDMTM", "MOMNC",
    "MOLOWVOL", "MONIFTY500", "MON50EQUAL", "MONEXT50", "MOMOMENTUM",
    "MOPSE", "MOREALTY", "MOSERVICE", "MOSILVER", "MOSMALL250",
    "MOVALUE", "MOHEALTH", "MOQUALITY", "MOTOUR", "MONIFTY100",
    # International (iNAV reflects prev US session close + USDINR — not live intraday)
    "MON100", "MONQ50",
}

# Map NSE symbol → the secname suffix used by Motilal's API to identify iNAV rows.
# Both MON100 and MONQ50 iNAV rows contain "iNAV" in secname.
_INAV_SECNAME_KEYWORDS = {"iNAV", "inav"}


def _safe(val: Any, default: float = 0.0) -> float:
    try:
        return float(str(val).replace(",", "").strip())
    except (TypeError, ValueError):
        return default


def _parse_motilal_datetime(dt_str: str) -> datetime:
    """
    Parse Motilal's datetime string (e.g. '06/05/2026 16:30:50' IST) and
    return a naive UTC datetime.
    """
    dt_str = (dt_str or "").strip()
    if not dt_str:
        return datetime.now(timezone.utc).replace(tzinfo=None)

    # Format: MM/DD/YYYY HH:MM:SS (treated as IST)
    try:
        dt_ist_naive = datetime.strptime(dt_str, "%m/%d/%Y %H:%M:%S")
        ist_offset = timedelta(hours=5, minutes=30)
        dt_utc = dt_ist_naive - ist_offset
        return dt_utc
    except ValueError:
        pass

    logger.debug("Failed to parse Motilal datetime '%s'. Falling back to current UTC.", dt_str)
    return datetime.now(timezone.utc).replace(tzinfo=None)


def fetch_inav_motilal(symbols: list[str]) -> list[dict[str, Any]]:
    """
    Fetch live iNAV snapshots for Motilal Oswal ETFs.

    Parameters
    ----------
    symbols : list of internal symbols, e.g. ["MON100", "MONQ50"]

    Returns
    -------
    list of dicts with keys:
        symbol, snapshot_at (datetime UTC naive), inav, market_price,
        premium_discount_pct, source
    """
    requested = {s.upper().replace(".NS", "") for s in symbols}
    target_symbols = requested & MOTILAL_SYMBOLS

    if not target_symbols:
        return []

    logger.info("Motilal iNAV: fetching live data for: %s", sorted(target_symbols))

    # 1. Fetch iNAV data from Motilal AMC API
    try:
        with httpx.Client(follow_redirects=True, timeout=_TIMEOUT) as client:
            resp = client.post(
                _MOTILAL_API_URL,
                json={"apiName": "GetINAVandPrice"},
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "WEB/MultipleCampaign",
                    "UserAgent": "WEB/MultipleCampaign",
                    "appid": "27820BB4MEC3DA4D65MAC74CDFF81E020A60",
                },
            )
            resp.raise_for_status()
            payload = resp.json()
    except Exception as exc:
        logger.warning("Motilal iNAV: API request failed: %s", exc)
        return []

    # 2. Flatten all entry groups into a single list
    raw_data: dict[str, Any] = {}
    try:
        inner = payload["data"]["data"]
        for group_entries in inner.values():
            if not isinstance(group_entries, list):
                continue
            for entry in group_entries:
                nse_sym = str(entry.get("nseSymbol") or "").upper()
                if nse_sym not in target_symbols:
                    continue
                secname = str(entry.get("secname") or "")
                # Keep only iNAV rows (secname contains "iNAV")
                is_inav = any(kw.lower() in secname.lower() for kw in _INAV_SECNAME_KEYWORDS)
                if is_inav:
                    # Last write wins — API may duplicate symbols across groups
                    raw_data[nse_sym] = entry
    except (KeyError, TypeError) as exc:
        logger.warning("Motilal iNAV: unexpected response structure: %s", exc)
        return []

    if not raw_data:
        logger.info("Motilal iNAV: no matching iNAV entries found for %s", sorted(target_symbols))
        return []

    # 3. Fetch live market prices from yfinance
    yf_symbols = [f"{sym}.NS" for sym in raw_data]
    market_prices: dict[str, float] = {}
    try:
        yf_data = yf.download(yf_symbols, period="1d", progress=False)
        if not yf_data.empty and "Close" in yf_data.columns:
            close_df = yf_data["Close"]
            for sym in raw_data:
                yf_sym = f"{sym}.NS"
                series = None
                if hasattr(close_df, "columns"):
                    if yf_sym in close_df.columns:
                        series = close_df[yf_sym]
                    elif len(yf_symbols) == 1:
                        series = close_df.iloc[:, 0]
                else:
                    series = close_df
                if series is not None:
                    series = series.dropna()
                    if not series.empty:
                        val = series.iloc[-1]
                        if hasattr(val, "iloc"):
                            val = val.iloc[-1]
                        market_prices[sym] = float(val)
    except Exception as exc:
        logger.warning("Motilal iNAV: yfinance price fetch failed: %s", exc)

    # 4. Build snapshot rows
    now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
    _MAX_STALENESS_DAYS = 2  # reject iNAV older than 2 calendar days (market weekends tolerated)

    rows: list[dict[str, Any]] = []
    for sym, entry in raw_data.items():
        raw_inav = entry.get("currNav")
        if raw_inav is None:
            continue
        inav = _safe(raw_inav)
        if inav <= 0:
            continue

        snapshot_at = _parse_motilal_datetime(entry.get("currNavDate", ""))

        # Staleness gate: reject rows whose iNAV timestamp is too old so NSE
        # data (step 1 in the waterfall) remains authoritative.
        age_days = (now_utc - snapshot_at).total_seconds() / 86400
        if age_days > _MAX_STALENESS_DAYS:
            logger.debug(
                "Motilal iNAV: skipping %s — iNAV is %.1f days old (ts=%s)",
                sym, age_days, entry.get("currNavDate"),
            )
            continue

        market_price = market_prices.get(sym)
        if market_price is None or market_price <= 0:
            # Fallback to prevNAV if yfinance has no data
            raw_prev = entry.get("prevNAV")
            market_price = _safe(raw_prev) if raw_prev is not None else inav

        prem_disc = ((market_price - inav) / inav * 100) if inav else 0.0

        rows.append({
            "symbol":               sym,
            "snapshot_at":          snapshot_at,
            "inav":                 inav,
            "market_price":         market_price,
            "premium_discount_pct": round(prem_disc, 4),
            "source":               "motilal_amc_live",
        })

    logger.info("Motilal iNAV: compiled %d snapshot(s): %s", len(rows), [r["symbol"] for r in rows])
    return rows
