"""
src/tools/goldhub_intelligence.py
──────────────────────────────────
World Gold Council (WGC) Goldhub intelligence fetcher.

API:  https://fsapi.gold.org/api/cbd/v11/charts/getPage  (no auth required)
      Discovered from the WGC Central Bank Data app (apps.gold.org/cbd-app).

Provides:
  1. fetch_cb_reserves_wgc()  — year-end gold holdings in metric tonnes
                                 for up to 200+ countries; latest ≈ Dec 2025
  2. fetch_etf_aum_wgc()      — Gold ETF AUM & implied tonnes via yfinance
                                 (WGC ETF page is 404; yfinance is the live proxy)
  3. fetch_gold_intelligence() — combined summary dict for quick CLI/UI display

Data cadence:
  Central banks : year-end snapshots, updated monthly (~6-week lag after each year-end)
  ETF AUM       : daily via Yahoo Finance
"""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Optional

import requests

log = logging.getLogger(__name__)

_WGC_API   = "https://fsapi.gold.org/api/cbd/v11/charts/getPage"
_WGC_HDR   = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36",
    "Accept": "application/json",
    "Origin": "https://www.gold.org",
}
_TIMEOUT   = 20

# Default country set: top 9 central bank gold holders we track
_DEFAULT_COUNTRIES = "CHN,IND,RUS,USA,DEU,TUR,GBR,JPN,POL"

_ISO3_TO_NAME: dict[str, str] = {
    "CHN": "China",      "IND": "India",         "RUS": "Russia",
    "USA": "United States", "DEU": "Germany",     "TUR": "Turkey",
    "GBR": "United Kingdom", "JPN": "Japan",      "POL": "Poland",
    "AUS": "Australia",  "CAN": "Canada",         "ITA": "Italy",
    "FRA": "France",     "NLD": "Netherlands",    "SWZ": "Switzerland",
    "SAU": "Saudi Arabia", "KAZ": "Kazakhstan",
}

_ISO3_TO_ISO2: dict[str, str] = {
    "CHN": "CN", "IND": "IN", "RUS": "RU", "USA": "US", "DEU": "DE",
    "TUR": "TR", "GBR": "GB", "JPN": "JP", "POL": "PL",
}


# ── 1. Central bank reserves ──────────────────────────────────────────────────

def fetch_cb_reserves_wgc(
    from_year: int = 2010,
    to_year: Optional[int] = None,
    countries: str = _DEFAULT_COUNTRIES,
) -> list[dict]:
    """
    Fetch year-end central bank gold holdings (metric tonnes) from WGC Goldhub.

    Source: fsapi.gold.org/api/cbd/v11/charts/getPage
    Data:   gold_reserves_tns series — direct metric tonnes, no conversion needed
    Lag:    ~6 weeks after each year-end; Dec 2025 typically available by Feb 2026

    Parameters
    ----------
    from_year : first year to include (default 2010)
    to_year   : last year to include (default current year)
    countries : comma-separated ISO3 codes (default: 9 tracked central banks)

    Returns
    -------
    List of dicts: ref_period (date), country_code (ISO2), country_name,
                   reserves_tonnes (float), source="wgc_goldhub"
    """
    if to_year is None:
        to_year = date.today().year

    params = {
        "page":        "date_range",
        "countries":   countries,
        "periodicity": "monthly",
        "startDate":   f"{from_year}-01-01",
        "endDate":     f"{to_year}-12-31",
    }
    try:
        resp = requests.get(_WGC_API, params=params, headers=_WGC_HDR, timeout=_TIMEOUT)
        resp.raise_for_status()
    except requests.RequestException as exc:
        log.warning("WGC Goldhub request failed: %s", exc)
        return []

    try:
        lc = resp.json()["chartData"]["linechart"]["LAST_YEAR_END"]
        series = lc["gold_reserves_tns"]["data"]
    except (KeyError, TypeError) as exc:
        log.warning("WGC unexpected response structure: %s", exc)
        return []

    rows: list[dict] = []
    for entry in series:
        iso3 = entry.get("name", "")
        for point in entry.get("data", []):
            epoch_ms, tonnes = point[0], point[1]
            if tonnes is None:
                continue
            year = datetime.fromtimestamp(epoch_ms / 1000).year
            rows.append({
                "ref_period":      date(year, 12, 1),   # year-end month
                "country_code":    _ISO3_TO_ISO2.get(iso3, iso3[:2]),
                "country_name":    _ISO3_TO_NAME.get(iso3, iso3),
                "reserves_tonnes": round(tonnes, 1),
                "source":          "wgc_goldhub",
            })

    rows.sort(key=lambda r: (r["ref_period"], r["country_name"]))
    log.info("WGC Goldhub: %d CB reserve rows fetched (%d→%d)", len(rows), from_year, to_year)
    return rows


def _latest_per_country(rows: list[dict]) -> list[dict]:
    """Return the most recent row per country, sorted by descending tonnes."""
    latest: dict[str, dict] = {}
    for r in rows:
        k = r["country_code"]
        if k not in latest or r["ref_period"] > latest[k]["ref_period"]:
            latest[k] = r
    return sorted(latest.values(), key=lambda r: -r["reserves_tonnes"])


# ── 2. Gold ETF AUM (yfinance proxy) ─────────────────────────────────────────

def fetch_etf_aum_wgc(
    symbols: Optional[list[str]] = None,
) -> list[dict]:
    """
    Daily Gold ETF AUM snapshot via Yahoo Finance (WGC ETF page is not publicly
    accessible as a data URL; yfinance provides the same underlying data).

    Returns
    -------
    List of dicts: trade_date, symbol, aum_usd, price, implied_tonnes, source
    """
    from src.importer.fetchers.etf_aum_fetcher import fetch_etf_aum  # type: ignore
    return fetch_etf_aum(symbols=symbols)


# ── 3. Combined intelligence summary ─────────────────────────────────────────

def fetch_gold_intelligence(
    from_year: int = 2010,
    verbose: bool = True,
) -> dict:
    """
    Fetch and summarise WGC gold market intelligence.

    Returns a dict with:
      status          : "success" | "partial" | "error"
      source          : "wgc_goldhub"
      as_of           : ISO date of latest CB data point
      cb_reserves     : list of latest-per-country rows (sorted by tonnes desc)
      cb_rows_total   : total rows fetched
      etf_aum         : live ETF AUM rows
      etf_total_tonnes: combined implied tonnes across all ETFs
      signals         : list of plain-English signal strings
    """
    result: dict = {
        "status": "error",
        "source": "wgc_goldhub + yfinance",
        "as_of": None,
        "cb_reserves": [],
        "cb_rows_total": 0,
        "etf_aum": [],
        "etf_total_tonnes": 0.0,
        "signals": [],
    }

    # ── CB reserves ───────────────────────────────────────────────────────────
    cb_rows = fetch_cb_reserves_wgc(from_year=from_year)
    if cb_rows:
        latest = _latest_per_country(cb_rows)
        result["cb_reserves"]   = latest
        result["cb_rows_total"] = len(cb_rows)
        result["as_of"]         = max(r["ref_period"] for r in cb_rows).isoformat()
        result["status"]        = "partial"

        # Signals: countries that increased reserves YoY
        by_country: dict[str, list[dict]] = {}
        for r in cb_rows:
            by_country.setdefault(r["country_name"], []).append(r)
        for country, pts in by_country.items():
            pts_sorted = sorted(pts, key=lambda x: x["ref_period"])
            if len(pts_sorted) >= 2:
                prev, curr = pts_sorted[-2], pts_sorted[-1]
                delta = curr["reserves_tonnes"] - prev["reserves_tonnes"]
                pct   = delta / max(prev["reserves_tonnes"], 1) * 100
                if abs(pct) >= 1.0:
                    direction = "▲ +ACCUMULATED" if delta > 0 else "▼ −SOLD"
                    result["signals"].append(
                        f"{country}: {direction} {abs(delta):.1f}t "
                        f"({curr['ref_period'].strftime('%b %Y')} vs "
                        f"{prev['ref_period'].strftime('%b %Y')}, {pct:+.1f}%)"
                    )

        if verbose:
            log.info("WGC CB reserves: %d countries, latest data as of %s",
                     len(latest), result["as_of"])

    # ── ETF AUM ───────────────────────────────────────────────────────────────
    try:
        etf_rows = fetch_etf_aum_wgc()
        if etf_rows:
            result["etf_aum"]          = etf_rows
            result["etf_total_tonnes"] = round(sum(r["implied_tonnes"] for r in etf_rows), 1)
            if result["status"] == "partial":
                result["status"] = "success"
            result["signals"].append(
                f"Gold ETFs combined: {result['etf_total_tonnes']:.0f}t implied holdings "
                f"(GLD + IAU + SGOL + PHYS)"
            )
    except Exception as exc:
        log.warning("ETF AUM fetch failed: %s", exc)

    if not cb_rows and not result["etf_aum"]:
        result["status"] = "error"

    return result


if __name__ == "__main__":
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    intel = fetch_gold_intelligence(from_year=2020)
    print(f"\nStatus  : {intel['status']}")
    print(f"As of   : {intel['as_of']}")
    print(f"CB rows : {intel['cb_rows_total']}")
    print(f"\nLatest holdings (metric tonnes):")
    for r in intel["cb_reserves"]:
        print(f"  {r['country_code']}  {r['country_name']:20s}  {r['reserves_tonnes']:>8.1f}t  ({r['ref_period'].strftime('%b %Y')})")
    print(f"\nETF AUM:")
    for r in intel["etf_aum"]:
        print(f"  {r['symbol']:6s}  ${r['aum_usd']/1e9:.1f}B  {r['implied_tonnes']:.0f}t")
    print(f"\nSignals:")
    for s in intel["signals"]:
        print(f"  • {s}")
