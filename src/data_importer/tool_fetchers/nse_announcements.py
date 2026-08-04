"""
src/tools/nse_announcements.py
────────────────────────────────
Fetches official NSE corporate announcements/disclosures — board meeting
outcomes, results, litigation, credit rating changes, M&A, management
changes. This is the ground-truth regulatory disclosure feed for "why did
this stock move", far more authoritative than aggregated news for
company-specific attribution.

Endpoint: https://www.nseindia.com/api/corporate-announcements
No API key required — NSE requires only a warmed-up session (cookies) and a
browser-like User-Agent, same pattern as nse_corporate_actions_fetcher.py.
"""

from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta
from typing import Any

import httpx
from langchain_core.tools import tool

logger = logging.getLogger(__name__)

_NSE_ANNOUNCEMENTS_URL = "https://www.nseindia.com/api/corporate-announcements"
_NSE_WARMUP = "https://www.nseindia.com/"
_NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
    "Connection": "keep-alive",
}
_TIMEOUT = 15

# Routine/administrative announcement categories — compliance noise, not
# market-moving events. Excluded by default so they don't dilute correlation
# attribution or clutter the research-agent tool output.
_ROUTINE_CATEGORIES = {
    "copy of newspaper publication",
    "certificate under sebi (depositories and participants) regulations, 2018",
    "trading window",
    "record date",
    "analysts/institutional investor meet/con. call updates",
}


def fetch_corporate_announcements(
    symbol: str, from_date: date, to_date: date, include_routine: bool = False
) -> list[dict[str, Any]]:
    """
    Fetch official NSE corporate announcements for a symbol within a date range.

    Returns dicts with: published_at (ISO), category, title, description, url, symbol.
    Routine/administrative categories are dropped by default — pass
    include_routine=True for an unfiltered audit view.
    """
    symbol_upper = symbol.strip().upper()
    rows: list[dict[str, Any]] = []
    data = None

    try:
        with httpx.Client(headers=_NSE_HEADERS, follow_redirects=True, timeout=_TIMEOUT) as client:
            # Warm-up: obtain NSE session cookies (required — NSE blocks cookie-less requests)
            client.get(_NSE_WARMUP, timeout=10)
            time.sleep(0.8)

            resp = client.get(
                _NSE_ANNOUNCEMENTS_URL,
                params={
                    "index": "equities",
                    "symbol": symbol_upper,
                    "from_date": from_date.strftime("%d-%m-%Y"),
                    "to_date": to_date.strftime("%d-%m-%Y"),
                },
                timeout=_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()
    except Exception as exc:
        logger.warning("NSE announcements fetch failed for %s: %s", symbol_upper, exc)
        return rows

    if not isinstance(data, list):
        return rows

    for item in data:
        category = str(item.get("desc") or "").strip()
        if not include_routine and category.lower() in _ROUTINE_CATEGORIES:
            continue

        sort_date = str(item.get("sort_date") or "")
        try:
            published_at = datetime.strptime(sort_date, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue

        rows.append({
            "published_at": published_at.isoformat(),
            "category": category or "General Updates",
            "title": f"{item.get('sm_name', symbol_upper)}: {category or 'General Updates'}",
            "description": str(item.get("attchmntText") or "").strip(),
            "url": item.get("attchmntFile") or "",
            "symbol": symbol_upper,
        })

    logger.info(
        "NSE announcements: %d material events fetched for %s (%s to %s)",
        len(rows), symbol_upper, from_date, to_date,
    )
    return rows


# ── LangChain Tool ────────────────────────────────────────────────────────────

@tool
def get_nse_announcements(input_str: str) -> dict[str, Any]:
    """
    Fetch official NSE corporate announcements/disclosures for an Indian
    stock over the last 365 days — board meeting outcomes, results,
    litigation, credit rating changes, M&A, management changes.

    This is the ground-truth regulatory disclosure feed, more authoritative
    than aggregated news for "why did this stock move" questions.

    Input format: "SYMBOL" (e.g. "ITC")

    Returns a list of material announcements with date, category, and description.
    """
    symbol = input_str.strip().upper()
    to_dt = date.today()
    from_dt = to_dt - timedelta(days=365)
    rows = fetch_corporate_announcements(symbol, from_dt, to_dt)

    if not rows:
        return {
            "symbol": symbol,
            "source": "NSE Corporate Announcements",
            "announcements": [],
            "note": "No material announcements found in the last 365 days.",
        }

    return {
        "symbol": symbol,
        "source": "NSE Corporate Announcements",
        "announcements": [
            {
                "published_at": r["published_at"],
                "category": r["category"],
                "description": r["description"],
                "url": r["url"],
            }
            for r in rows
        ],
        "count": len(rows),
    }
