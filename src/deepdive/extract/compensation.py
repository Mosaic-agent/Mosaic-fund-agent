"""
src/deepdive/extract/compensation.py
──────────────────────────────────────
Executive compensation data via sec-api.io ExecCompApi.

ExecCompApi.get_data(ticker) returns a list of NEO (Named Executive Officer)
compensation rows with fields:
  name, position, year, salary, bonus, stockAwards, optionAwards,
  nonEquityIncentiveCompensation, changeInPensionValueAndDeferredEarnings,
  otherCompensation, total

All values in USD (not millions). We keep raw USD for this module.

Cache-first: writes exec_comp.json on first call; reads from disk on re-run.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


def get_exec_comp(
    ticker: str,
    cache_path: Path,
    api_key: str,
    years: int = 3,
) -> list[dict[str, Any]]:
    """
    Fetch Named Executive Officer compensation data via ExecCompApi.

    Args:
        ticker:     US ticker symbol e.g. "ADSK"
        cache_path: Path to write exec_comp.json
        api_key:    sec-api.io API key
        years:      Number of most-recent fiscal years to include

    Returns:
        list of dicts with keys:
            name, position, year, salary, bonus, stockAwards, optionAwards,
            nonEquityIncentiveCompensation, otherCompensation, total
        Sorted by year desc, then total desc.
        Empty list on failure.

    Cache:
        cache_path (e.g. cache/ADSK/date/exec_comp.json)
    """
    if cache_path.exists():
        log.debug("compensation: cache hit %s", cache_path.name)
        return json.loads(cache_path.read_text())

    try:
        from sec_api import ExecCompApi  # noqa: PLC0415
        api = ExecCompApi(api_key=api_key)
        rows: list[dict] = api.get_data(ticker)
    except Exception as exc:
        log.warning("ExecCompApi.get_data(%s) failed: %s", ticker, exc)
        return []

    if not rows:
        log.warning("ExecCompApi returned no data for %s", ticker)
        return []

    # Keep only the most recent N years
    all_years = sorted({int(r.get("year", 0)) for r in rows if r.get("year")}, reverse=True)
    target_years = set(all_years[:years])

    filtered = [
        {
            "name": r.get("name", ""),
            "position": r.get("position", ""),
            "year": r.get("year"),
            "salary": r.get("salary"),
            "bonus": r.get("bonus"),
            "stockAwards": r.get("stockAwards"),
            "optionAwards": r.get("optionAwards"),
            "nonEquityIncentiveCompensation": r.get("nonEquityIncentiveCompensation"),
            "otherCompensation": r.get("otherCompensation"),
            "total": r.get("total"),
        }
        for r in rows
        if r.get("year") in target_years
    ]

    # Sort by year desc, total desc
    filtered.sort(key=lambda x: (-int(x.get("year") or 0), -(x.get("total") or 0)))

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(filtered, indent=2))
    log.info(
        "compensation: cached %d NEO rows (%d years) for %s → %s",
        len(filtered), len(target_years), ticker, cache_path.name,
    )
    return filtered


def summarise_exec_comp(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Collapse multi-year NEO data into one summary row per executive (most recent year).

    Returns:
        list of {name, position, year, total_usd_m, stock_pct} sorted by total desc.
        total_usd_m = total / 1_000_000, rounded to 2dp.
        stock_pct   = stockAwards / total * 100, rounded to 1dp.
    """
    # Latest year per name
    latest: dict[str, dict] = {}
    for r in rows:
        name = r.get("name", "")
        yr = int(r.get("year") or 0)
        if name not in latest or yr > int(latest[name].get("year") or 0):
            latest[name] = r

    summaries = []
    for r in latest.values():
        total = r.get("total") or 0
        stock = r.get("stockAwards") or 0
        summaries.append({
            "name": r.get("name", ""),
            "position": r.get("position", ""),
            "year": r.get("year"),
            "total_usd_m": round(total / 1_000_000, 2),
            "stock_pct": round(stock / total * 100, 1) if total else None,
        })

    summaries.sort(key=lambda x: -(x.get("total_usd_m") or 0))
    return summaries
