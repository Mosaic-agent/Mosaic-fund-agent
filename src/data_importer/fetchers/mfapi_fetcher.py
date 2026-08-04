"""
src/importer/fetchers/mfapi_fetcher.py
───────────────────────────────────────
Fetches daily NAV data from MFAPI.in for Indian ETFs and mutual funds.

MFAPI.in is a free REST API over AMFI's official NAV data:
  GET https://api.mfapi.in/mf/{scheme_code}

The response is a JSON object with a `data` list where each entry is:
  {"date": "22-02-2026", "nav": "127.3126"}

Date format from API: DD-MM-YYYY
"""

from __future__ import annotations

import logging
import time
from datetime import date
from typing import Any

import requests

logger = logging.getLogger(__name__)

_MFAPI_BASE = "https://api.mfapi.in/mf"
_REQUEST_DELAY = 0.35   # seconds — polite delay between requests (free API)
_TIMEOUT = 15           # seconds


def _parse_mfapi_date(date_str: str) -> date | None:
    """Parse DD-MM-YYYY string to a date object."""
    try:
        day, month, year = date_str.strip().split("-")
        return date(int(year), int(month), int(day))
    except (ValueError, AttributeError):
        return None


def fetch_nav(
    nse_symbol: str,
    scheme_code: str,
    from_date: date,
    to_date: date,
) -> list[dict[str, Any]]:
    """
    Fetch daily NAV records for a single scheme from MFAPI.in.

    Returns a list of dicts with keys:
        symbol, scheme_code, nav_date (date), nav (float)

    Only rows within [from_date, to_date] are returned.
    Returns an empty list on any error.
    """
    url = f"{_MFAPI_BASE}/{scheme_code}"
    try:
        resp = requests.get(url, timeout=_TIMEOUT)
        resp.raise_for_status()
        payload = resp.json()
    except Exception as exc:
        logger.warning("MFAPI fetch failed for %s (code=%s): %s", nse_symbol, scheme_code, exc)
        return []

    data_entries = payload.get("data", [])
    rows: list[dict[str, Any]] = []
    for entry in data_entries:
        nav_date = _parse_mfapi_date(entry.get("date", ""))
        if nav_date is None:
            continue
        if not (from_date <= nav_date <= to_date):
            continue
        try:
            nav_val = float(entry["nav"])
        except (KeyError, TypeError, ValueError):
            continue
        rows.append({
            "symbol":      nse_symbol,
            "scheme_code": scheme_code,
            "nav_date":    nav_date,
            "nav":         nav_val,
        })

    logger.debug(
        "MFAPI: %s (code=%s) → %d rows (%s→%s)",
        nse_symbol, scheme_code, len(rows), from_date, to_date,
    )
    return rows


def fetch_all_nav(
    scheme_codes: dict[str, str],   # {nse_symbol: scheme_code}
    from_date: date,
    to_date: date,
) -> list[dict[str, Any]]:
    """
    Fetch NAV for all symbols in scheme_codes with polite rate limiting.

    Returns the combined list of row dicts.
    """
    all_rows: list[dict[str, Any]] = []
    for i, (nse_symbol, scheme_code) in enumerate(scheme_codes.items()):
        rows = fetch_nav(nse_symbol, scheme_code, from_date, to_date)
        all_rows.extend(rows)
        if i < len(scheme_codes) - 1:
            time.sleep(_REQUEST_DELAY)
    return all_rows
