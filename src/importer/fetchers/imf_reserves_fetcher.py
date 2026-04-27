"""
src/importer/fetchers/imf_reserves_fetcher.py
──────────────────────────────────────────────
Fetches central bank gold reserves from two sources (merged):

Primary:   WGC Goldhub API  — exact tonnes, year-end, ~6-week lag
           Latest available: Dec 2025 (Jan-Mar 2026 publishes ~May 2026)

Fallback:  World Bank WDI REST API  — derived from total-reserves minus ex-gold USD,
           annual, ~12-month lag, no external library needed

Both sources are fetched and merged; WGC rows take precedence for any overlapping year.

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


_WB_BASE = "https://api.worldbank.org/v2"
_GOLD_PRICE_BY_YEAR: dict[int, float] = {
    2010: 1224.52, 2011: 1571.52, 2012: 1668.86, 2013: 1411.23,
    2014: 1266.40, 2015: 1160.06, 2016: 1250.74, 2017: 1257.15,
    2018: 1268.49, 2019: 1392.60, 2020: 1769.64, 2021: 1798.61,
    2022: 1800.99, 2023: 1940.54, 2024: 2386.77, 2025: 2940.0,
}
_TROY_OZ_PER_TONNE = 32_150.7


def _fetch_worldbank(from_year: int, to_year: int) -> list[dict]:
    """
    World Bank WDI REST API — no external library, direct HTTP.
    Derives gold tonnes from (total_reserves_usd - ex_gold_reserves_usd) / annual_gold_price.
    Annual cadence, ~12-month lag (2024 data available in 2025).
    """
    iso2_list = ";".join(_ISO3_TO_ISO2.values())
    date_range = f"{from_year}:{to_year}"

    def _wb_fetch(indicator: str) -> dict[tuple[str, int], float]:
        out: dict[tuple[str, int], float] = {}
        page, per_page = 1, 500
        while True:
            try:
                r = requests.get(
                    f"{_WB_BASE}/country/{iso2_list}/indicator/{indicator}",
                    params={"format": "json", "date": date_range, "per_page": per_page, "page": page},
                    timeout=_TIMEOUT,
                )
                r.raise_for_status()
            except requests.RequestException as exc:
                log.warning("World Bank %s page %d failed: %s", indicator, page, exc)
                break
            payload = r.json()
            if len(payload) < 2 or not payload[1]:
                break
            for item in payload[1]:
                if item["value"] is None:
                    continue
                iso3 = item.get("countryiso3code", "")
                iso2 = _ISO3_TO_ISO2.get(iso3, "")
                if not iso2:
                    continue
                try:
                    year = int(item["date"])
                except (ValueError, TypeError):
                    continue
                out[(iso2, year)] = float(item["value"])
            total_pages = payload[0].get("pages", 1)
            if page >= total_pages:
                break
            page += 1
        return out

    totl = _wb_fetch("FI.RES.TOTL.CD")
    xgld = _wb_fetch("FI.RES.XGLD.CD")

    rows: list[dict] = []
    for (iso2, year), totl_usd in totl.items():
        xgld_usd = xgld.get((iso2, year))
        if not xgld_usd:
            continue
        gold_usd = totl_usd - xgld_usd
        if gold_usd <= 0:
            continue
        price_key = year if year in _GOLD_PRICE_BY_YEAR else max(k for k in _GOLD_PRICE_BY_YEAR if k <= year)
        price = _GOLD_PRICE_BY_YEAR[price_key]
        country_code = iso2
        country_name = next((n for c, n in _ISO3_TO_NAME.items() if _ISO3_TO_ISO2.get(c) == iso2), iso2)
        rows.append({
            "ref_period":      date(year, 12, 1),
            "country_code":    country_code,
            "country_name":    country_name,
            "reserves_tonnes": round(gold_usd / (price * _TROY_OZ_PER_TONNE), 1),
            "source":          "world_bank_wdi",
        })

    rows.sort(key=lambda r: (r["ref_period"], r["country_name"]))
    log.info("World Bank CB reserves: %d rows (%d–%d)", len(rows), from_year, to_year)
    return rows


def fetch_cb_reserves(
    from_year: int = 2010,
    to_year: Optional[int] = None,
) -> list[dict]:
    """
    Fetch central bank gold holdings in metric tonnes.

    Merges two sources (WGC takes precedence for any overlapping year):
    - WGC Goldhub: exact tonnes, year-end, ~6-week lag (Dec 2025 = latest in Apr 2026)
    - World Bank WDI: derived from USD reserves, annual, ~12-month lag (fills historic gaps)

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

    wgc_rows = _fetch_wgc(from_year, to_year)
    wb_rows = _fetch_worldbank(from_year, to_year)

    if not wgc_rows and not wb_rows:
        log.warning("No CB reserve data returned from any source.")
        return []

    # WGC rows keyed by (country_code, ref_period) — takes precedence
    wgc_keys: set[tuple] = {(r["country_code"], r["ref_period"]) for r in wgc_rows}
    wb_fill = [r for r in wb_rows if (r["country_code"], r["ref_period"]) not in wgc_keys]

    merged = wgc_rows + wb_fill
    merged.sort(key=lambda r: (r["ref_period"], r["country_name"]))
    log.info("CB reserves merged: %d WGC + %d WB fill = %d total rows", len(wgc_rows), len(wb_fill), len(merged))
    return merged


