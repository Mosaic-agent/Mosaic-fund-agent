"""
src/importer/fetchers/imf_weo_fetcher.py
─────────────────────────────────────────
Fetches IMF World Economic Outlook (WEO) projections via the
DataMapper public REST API — no auth required.

API base: https://www.imf.org/external/datamapper/api/v1/

Key indicators fetched for India (IND) and G4 peers (USA, CHN, JPN, DEU):
  NGDP_RPCH   — GDP volume growth (annual %)
  PCPIPCH     — Inflation, average CPI (annual %)
  GGXCNL_NGDP — Net govt lending/borrowing (% of GDP, fiscal balance proxy)
  LUR         — Unemployment rate (%)
  NGDPD       — GDP current prices (USD bn) — for size context

(Current account balance is covered by the World Bank WDI fetcher instead,
as BCA_NGDPDZ is not available via the DataMapper API.)

The WEO is published twice yearly (Apr, Oct). Projections for 2–3 years
ahead are included (is_forecast=1). Historical years (< current year) are
actuals (is_forecast=0).

Cadence: annual data points, updated twice a year when WEO is released.
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Any

import requests

log = logging.getLogger(__name__)

_DMR_BASE = "https://www.imf.org/external/datamapper/api/v1"
_TIMEOUT  = 30

_INDICATORS: list[tuple[str, str]] = [
    ("NGDP_RPCH",   "GDP growth rate (annual %)"),
    ("PCPIPCH",     "Inflation CPI (annual %)"),
    ("GGXCNL_NGDP", "Net govt lending/borrowing (% of GDP)"),
    ("LUR",         "Unemployment rate (%)"),
    ("NGDPD",       "GDP current prices (USD bn)"),
]

# IMF area codes (3-letter WEO codes)
_AREAS = ["IND", "USA", "CHN", "JPN", "DEU"]

_WEO_TO_ISO2: dict[str, str] = {
    "IND": "IN", "USA": "US", "CHN": "CN", "JPN": "JP", "DEU": "DE",
}


def fetch_imf_weo(from_year: int, to_year: int) -> list[dict[str, Any]]:
    """
    Fetch IMF WEO projections for tracked countries.

    Parameters
    ----------
    from_year, to_year : inclusive year range to return
        (the API always returns the full WEO horizon; we filter here)

    Returns list of dicts with keys:
        ref_year, country_code, indicator_code, indicator_name,
        value, source, is_forecast
    """
    current_year = date.today().year
    rows: list[dict[str, Any]] = []
    area_str = ",".join(_AREAS)

    for ind_code, ind_name in _INDICATORS:
        url = f"{_DMR_BASE}/{ind_code}/{area_str}"
        try:
            resp = requests.get(url, timeout=_TIMEOUT)
            resp.raise_for_status()
            payload = resp.json()
        except requests.RequestException as exc:
            log.warning("IMF DataMapper request failed for %s: %s", ind_code, exc)
            continue
        except ValueError as exc:
            log.warning("IMF DataMapper bad JSON for %s: %s", ind_code, exc)
            continue

        # Response: {"values": {"NGDP_RPCH": {"IND": {"2020": val, ...}, ...}}}
        try:
            country_map: dict[str, dict[str, Any]] = (
                payload["values"][ind_code]
            )
        except (KeyError, TypeError):
            log.warning("IMF DataMapper unexpected structure for %s", ind_code)
            continue

        for weo_code, year_map in country_map.items():
            iso2 = _WEO_TO_ISO2.get(weo_code, weo_code[:2])
            for year_str, value in year_map.items():
                if value is None:
                    continue
                try:
                    year = int(year_str)
                    val  = float(value)
                except (ValueError, TypeError):
                    continue
                if not (from_year <= year <= to_year):
                    continue
                rows.append({
                    "ref_year":       year,
                    "country_code":   iso2,
                    "indicator_code": ind_code,
                    "indicator_name": ind_name,
                    "value":          val,
                    "source":         "imf_weo",
                    "is_forecast":    1 if year >= current_year else 0,
                })

    rows.sort(key=lambda r: (r["ref_year"], r["country_code"], r["indicator_code"]))
    log.info(
        "IMF WEO: %d rows for IND+peers (%d–%d)", len(rows), from_year, to_year
    )
    return rows
