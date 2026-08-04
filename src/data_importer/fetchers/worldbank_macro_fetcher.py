"""
src/importer/fetchers/worldbank_macro_fetcher.py
─────────────────────────────────────────────────
Fetches annual macro indicators for India (and key peers) from the
World Bank WDI REST API — no auth required, JSON format.

Primary indicators tracked for India (IN):
  NY.GDP.MKTP.KD.ZG  — GDP growth rate (annual %)
  FP.CPI.TOTL.ZG     — Inflation, CPI (annual %)
  BN.CAB.XOKA.GD.ZS  — Current account balance (% of GDP)
  GC.DOD.TOTL.GD.ZS  — Central govt debt (% of GDP)
  NY.GNS.ICTR.ZS     — Gross savings (% of GDP)
  BX.KLT.DINV.WD.GD.ZS — FDI net inflows (% of GDP)
  SL.UEM.TOTL.ZS     — Unemployment (% of total labour force)
  NE.EXP.GNFS.ZS     — Exports of goods & services (% of GDP)

Peer countries also fetched (for relative macro positioning):
  US, CN, JP, DE  — G4 economies
  Cadence: annual, ~12-month lag (2024 data available in late 2025).
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Any

import requests

log = logging.getLogger(__name__)

_WB_BASE = "https://api.worldbank.org/v2"
_TIMEOUT = 30

# Indicators to fetch — (code, human-readable name)
_INDICATORS: list[tuple[str, str]] = [
    ("NY.GDP.MKTP.KD.ZG",  "GDP growth rate (annual %)"),
    ("FP.CPI.TOTL.ZG",     "Inflation CPI (annual %)"),
    ("BN.CAB.XOKA.GD.ZS",  "Current account balance (% of GDP)"),
    ("GC.DOD.TOTL.GD.ZS",  "Central govt debt (% of GDP)"),
    ("NY.GNS.ICTR.ZS",     "Gross savings (% of GDP)"),
    ("BX.KLT.DINV.WD.GD.ZS", "FDI net inflows (% of GDP)"),
    ("SL.UEM.TOTL.ZS",     "Unemployment rate (%)"),
    ("NE.EXP.GNFS.ZS",     "Exports of goods & services (% of GDP)"),
]

# Countries: India first, then G4 peers for relative context
_COUNTRIES = ["IN", "US", "CN", "JP", "DE"]


def _country_name(iso2: str) -> str:
    return {
        "IN": "India", "US": "United States", "CN": "China",
        "JP": "Japan", "DE": "Germany",
    }.get(iso2, iso2)


def fetch_worldbank_macro(from_year: int, to_year: int) -> list[dict[str, Any]]:
    """
    Fetch World Bank macro indicators for tracked countries.

    Parameters
    ----------
    from_year, to_year : inclusive year range

    Returns list of dicts with keys:
        ref_year, country_code, indicator_code, indicator_name,
        value, source, is_forecast
    """
    rows: list[dict[str, Any]] = []
    country_str = ";".join(_COUNTRIES)

    for ind_code, ind_name in _INDICATORS:
        url = (
            f"{_WB_BASE}/country/{country_str}/indicator/{ind_code}"
            f"?format=json&date={from_year}:{to_year}&per_page=1000&mrv=30"
        )
        try:
            resp = requests.get(url, timeout=_TIMEOUT)
            resp.raise_for_status()
            payload = resp.json()
        except requests.RequestException as exc:
            log.warning("World Bank API request failed for %s: %s", ind_code, exc)
            continue
        except (ValueError, KeyError) as exc:
            log.warning("World Bank API bad response for %s: %s", ind_code, exc)
            continue

        # Payload is [metadata_dict, [data_points...]]
        if not isinstance(payload, list) or len(payload) < 2:
            log.warning("World Bank API unexpected structure for %s", ind_code)
            continue

        data_points = payload[1]
        if not data_points:
            log.debug("World Bank: no data for %s %d-%d", ind_code, from_year, to_year)
            continue

        for point in data_points:
            if point.get("value") is None:
                continue
            try:
                year = int(point["date"])
                iso2 = point["countryiso3code"][:2] if len(point.get("countryiso3code", "")) >= 2 else point["country"]["id"]
            except (KeyError, TypeError, ValueError):
                continue
            rows.append({
                "ref_year":       year,
                "country_code":   iso2,
                "indicator_code": ind_code,
                "indicator_name": ind_name,
                "value":          float(point["value"]),
                "source":         "world_bank",
                "is_forecast":    0,
            })

    rows.sort(key=lambda r: (r["ref_year"], r["country_code"], r["indicator_code"]))
    log.info(
        "World Bank macro: %d rows for IN+peers (%d–%d)", len(rows), from_year, to_year
    )
    return rows
