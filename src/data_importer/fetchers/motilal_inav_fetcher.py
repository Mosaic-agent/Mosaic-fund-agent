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

from src.data_importer.fetchers.base_inav_fetcher import BaseInavFetcher

logger = logging.getLogger(__name__)

_MOTILAL_API_URL = "https://www.motilaloswalmf.com/mutualfund/api/v1/someFunc"
_TIMEOUT = 15
_MAX_STALENESS_DAYS = 2  # reject iNAV older than 2 calendar days (weekends tolerated)

# All ETFs managed by Motilal Oswal AMC that expose live iNAV via their API.
# Domestic ETFs come from m50M100Data; international ETFs from n100Data.
MOTILAL_SYMBOLS = frozenset({
    # Domestic
    "MOM50", "MOM100", "MOALPHA50", "MOBANK10", "MOCAPITAL",
    "MODEFENCE", "MOENERGY", "MOGSEC", "MOGOLD", "MOINFRA",
    "MOIPO", "MOMENTUM50", "MOMGF", "MOMIDMTM", "MOMNC",
    "MOLOWVOL", "MONIFTY500", "MON50EQUAL", "MONEXT50", "MOMOMENTUM",
    "MOPSE", "MOREALTY", "MOSERVICE", "MOSILVER", "MOSMALL250",
    "MOVALUE", "MOHEALTH", "MOQUALITY", "MOTOUR", "MONIFTY100",
    # International (iNAV reflects prev US session close + USDINR — not live intraday)
    "MON100", "MONQ50",
})

_INAV_SECNAME_KEYWORDS = {"iNAV", "inav"}

_MOTILAL_HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "WEB/MultipleCampaign",
    "UserAgent": "WEB/MultipleCampaign",
    "appid": "27820BB4MEC3DA4D65MAC74CDFF81E020A60",
}


def _parse_motilal_datetime(dt_str: str) -> datetime:
    """
    Parse Motilal's datetime string (e.g. '06/05/2026 16:30:50' IST) →
    naive UTC datetime.
    """
    dt_str = (dt_str or "").strip()
    if not dt_str:
        return datetime.now(timezone.utc).replace(tzinfo=None)
    try:
        dt_ist_naive = datetime.strptime(dt_str, "%m/%d/%Y %H:%M:%S")
        return dt_ist_naive - timedelta(hours=5, minutes=30)
    except ValueError:
        pass
    logger.debug("Failed to parse Motilal datetime '%s'. Falling back to current UTC.", dt_str)
    return datetime.now(timezone.utc).replace(tzinfo=None)


class _MotilalInavFetcher(BaseInavFetcher):
    source_label = "motilal_amc_live"
    symbols = MOTILAL_SYMBOLS

    def _fetch_raw(self) -> Any:
        """
        Returns a flat dict {nse_symbol: api_entry} for iNAV rows only,
        so that _match_symbols can simply intersect with target.
        """
        resp = httpx.post(
            _MOTILAL_API_URL,
            json={"apiName": "GetINAVandPrice"},
            headers=_MOTILAL_HEADERS,
            follow_redirects=True,
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        payload = resp.json()
        # Flatten all groups (m50M100Data, n100Data, …) and keep only iNAV rows
        flat: dict[str, Any] = {}
        inner = payload["data"]["data"]
        for group_entries in inner.values():
            if not isinstance(group_entries, list):
                continue
            for entry in group_entries:
                nse_sym = str(entry.get("nseSymbol") or "").upper()
                secname = str(entry.get("secname") or "")
                if any(kw.lower() in secname.lower() for kw in _INAV_SECNAME_KEYWORDS):
                    flat[nse_sym] = entry  # last write wins if duplicate
        return flat

    def _match_symbols(self, raw: Any, target: set[str]) -> dict[str, Any]:
        # raw is already a {nse_sym: entry} dict filtered to iNAV rows
        return {sym: entry for sym, entry in raw.items() if sym in target}

    def _extract_inav(self, item: Any) -> float | None:
        return item.get("currNav")

    def _extract_timestamp(self, item: Any) -> datetime:
        return _parse_motilal_datetime(item.get("currNavDate", ""))

    def _extract_fallback_price(self, item: Any) -> float | None:
        return item.get("prevNAV")

    def _staleness_check(self, sym: str, snapshot_at: datetime, inav: float) -> bool:
        now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
        age_days = (now_utc - snapshot_at).total_seconds() / 86400
        if age_days > _MAX_STALENESS_DAYS:
            logger.debug(
                "Motilal iNAV: skipping %s — iNAV is %.1f days old (snapshot_at=%s)",
                sym, age_days, snapshot_at,
            )
            return False
        return True


_fetcher = _MotilalInavFetcher()


def fetch_inav_motilal(symbols: list[str]) -> list[dict[str, Any]]:
    """
    Fetch live iNAV snapshots for Motilal Oswal ETFs.

    Parameters
    ----------
    symbols : list of NSE symbols, e.g. ["MON100", "MONQ50"]

    Returns
    -------
    list of dicts: symbol, snapshot_at (naive UTC), inav, market_price,
    premium_discount_pct, source
    """
    return _fetcher.fetch_inav(symbols)
