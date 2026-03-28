"""
src/importer/fetchers/imf_reserves_fetcher.py
──────────────────────────────────────────────
Fetches central bank gold reserves via the World Gold Council (WGC) Goldhub API.

Primary:  https://fsapi.gold.org/api/cbd/v11/charts/getPage
  — Returns year-end holdings in metric tonnes (direct, no conversion needed)
  — Updated monthly with ~6-week lag; typically has data through prior month's year-end
  — Latest available as of early 2026: Dec 2025 for most countries

Fallback: World Bank WDI (annual, ~2-year lag)
  — Used only if WGC API is unreachable

9 countries tracked: CN, IN, RU, US, DE, TR, GB, JP, PL
"""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Optional

import requests

log = logging.getLogger(__name__)

_WGC_URL = "https://fsapi.gold.org/api/cbd/v11/charts/getPage"
_WGC_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36",
    "Accept": "application/json",
    "Origin": "https://www.gold.org",
}
_TIMEOUT = 20

# ISO3 codes for the 9 tracked central banks
_WGC_COUNTRIES = "CHN,IND,RUS,USA,DEU,TUR,GBR,JPN,POL"

_ISO3_TO_NAME: dict[str, str] = {
    "CHN": "China", "IND": "India", "RUS": "Russia", "USA": "United States",
    "DEU": "Germany", "TUR": "Turkey", "GBR": "United Kingdom",
    "JPN": "Japan", "POL": "Poland",
}

_ISO3_TO_ISO2: dict[str, str] = {
    "CHN": "CN", "IND": "IN", "RUS": "RU", "USA": "US",
    "DEU": "DE", "TUR": "TR", "GBR": "GB", "JPN": "JP", "POL": "PL",
}


def _fetch_wgc(from_year: int, to_year: int) -> list[dict]:
    """
    Fetch year-end gold holdings in metric tonnes from WGC Goldhub fsapi.

    Response structure:
        chartData.linechart.LAST_YEAR_END.gold_reserves_tns.data
        = [{"name": "CHN", "data": [[epoch_ms, tonnes], ...]}, ...]
    """
    params = {
        "page": "date_range",
        "countries": _WGC_COUNTRIES,
        "periodicity": "monthly",
        "startDate": f"{from_year}-01-01",
        "endDate": f"{to_year}-12-31",
    }
    try:
        r = requests.get(_WGC_URL, params=params, headers=_WGC_HEADERS, timeout=_TIMEOUT)
        r.raise_for_status()
    except requests.RequestException as exc:
        log.warning("WGC CBD API request failed: %s", exc)
        return []

    try:
        lc = r.json()["chartData"]["linechart"]["LAST_YEAR_END"]
        series = lc["gold_reserves_tns"]["data"]
    except (KeyError, TypeError) as exc:
        log.warning("WGC CBD API unexpected response structure: %s", exc)
        return []

    rows: list[dict] = []
    for country_entry in series:
        iso3 = country_entry.get("name", "")
        for point in country_entry.get("data", []):
            epoch_ms, tonnes = point[0], point[1]
            if tonnes is None:
                continue
            ref_date = datetime.fromtimestamp(epoch_ms / 1000).date()
            # API returns year-end dates; store as first day of that year for
            # consistency with the ClickHouse schema (ref_period = month/year marker)
            rows.append({
                "ref_period":      date(ref_date.year, 12, 1),
                "country_code":    _ISO3_TO_ISO2.get(iso3, iso3[:2]),
                "country_name":    _ISO3_TO_NAME.get(iso3, iso3),
                "reserves_tonnes": round(tonnes, 1),
                "source":          "wgc_goldhub",
            })

    rows.sort(key=lambda r: (r["ref_period"], r["country_name"]))
    log.info("WGC Goldhub CB reserves: %d rows (%d–%d)", len(rows), from_year, to_year)
    return rows


def _fetch_worldbank_fallback(from_year: int, to_year: int) -> list[dict]:
    """World Bank fallback — annual, ~2-year lag, requires wbgapi."""
    try:
        import wbgapi as wb  # type: ignore
    except ImportError:
        log.warning("wbgapi not installed — World Bank fallback unavailable")
        return []

    _IND_TOTL = "FI.RES.TOTL.CD"
    _IND_XGLD = "FI.RES.XGLD.CD"
    _GOLD_PRICE: dict[int, float] = {
        2010: 1224.52, 2011: 1571.52, 2012: 1668.86, 2013: 1411.23,
        2014: 1266.40, 2015: 1160.06, 2016: 1250.74, 2017: 1257.15,
        2018: 1268.49, 2019: 1392.60, 2020: 1769.64, 2021: 1798.61,
        2022: 1800.99, 2023: 1940.54, 2024: 2386.77, 2025: 2940.0,
    }
    TROY_OZ = 32_150.7
    iso3_codes = list(_ISO3_TO_ISO2.keys())
    years = list(range(from_year, to_year + 1))

    def _fetch(ind: str) -> dict:
        out: dict = {}
        try:
            for item in wb.data.fetch(ind, iso3_codes, time=years):
                if item["value"] is None:
                    continue
                iso3 = item["economy"]
                year = int("".join(c for c in item["time"] if c.isdigit()))
                out[(iso3, year)] = float(item["value"])
        except Exception as exc:
            log.warning("World Bank %s failed: %s", ind, exc)
        return out

    totl, xgld = _fetch(_IND_TOTL), _fetch(_IND_XGLD)
    rows = []
    for (iso3, year), totl_usd in totl.items():
        xgld_usd = xgld.get((iso3, year))
        if not xgld_usd:
            continue
        gold_usd = totl_usd - xgld_usd
        if gold_usd <= 0:
            continue
        price = _GOLD_PRICE.get(year, _GOLD_PRICE[max(k for k in _GOLD_PRICE if k <= year)])
        rows.append({
            "ref_period":      date(year, 12, 1),
            "country_code":    _ISO3_TO_ISO2.get(iso3, iso3[:2]),
            "country_name":    _ISO3_TO_NAME.get(iso3, iso3),
            "reserves_tonnes": round(gold_usd / (price * TROY_OZ), 1),
            "source":          "world_bank_wdi",
        })
    rows.sort(key=lambda r: (r["ref_period"], r["country_name"]))
    log.info("World Bank CB reserves fallback: %d rows", len(rows))
    return rows


def fetch_cb_reserves(
    from_year: int = 2010,
    to_year: Optional[int] = None,
) -> list[dict]:
    """
    Fetch central bank gold holdings in metric tonnes.

    Tries WGC Goldhub first (year-end, up to ~1 month lag, exact tonnes).
    Falls back to World Bank WDI (year-end, ~2-year lag, derived from USD/price).

    Parameters
    ----------
    from_year : first year to include (default 2010)
    to_year   : last year to include (default current year)

    Returns
    -------
    List of dicts: ref_period, country_code, country_name, reserves_tonnes, source
    """
    if to_year is None:
        to_year = date.today().year

    rows = _fetch_wgc(from_year, to_year)
    if not rows:
        log.info("WGC unavailable — falling back to World Bank")
        rows = _fetch_worldbank_fallback(from_year, to_year)

    if not rows:
        log.warning("No CB reserve data returned from any source.")
    return rows


