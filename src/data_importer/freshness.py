"""
src/importer/freshness.py
─────────────────────────
Automatic Freshness Guard for ClickHouse Market Data.

Checks max(trade_date) in ClickHouse across all categories:
(etfs, stocks, fii_dii, indices, commodities, fx_rates, us_stocks).

If any category is stale (older than max_age_days business days),
it automatically triggers ImportDataCommand(categories=[cat]).execute().
"""
from __future__ import annotations

import logging
import time
from datetime import date, timedelta
from typing import Sequence

logger = logging.getLogger(__name__)

ALL_CATEGORIES = (
    "etfs",
    "stocks",
    "fii_dii",
    "indices",
    "commodities",
    "fx_rates",
    "us_stocks",
)

_last_freshness_check: float = 0.0
_FRESHNESS_CHECK_TTL: float = 300.0  # 5 minutes TTL between full global checks


def check_and_auto_import_stale_categories(
    categories: Sequence[str],
    max_age_days: int = 1,
    force: bool = False,
) -> list[str]:
    """
    Check ClickHouse max(trade_date) for specified categories.
    Triggers automatic delta import for any category that is stale (> max_age_days old).

    Returns:
        List of categories that were auto-imported.
    """
    if not categories:
        return []

    from src.db.pool import query_df
    auto_imported = []

    today = date.today()
    # Calculate cutoff date excluding weekends
    cutoff = today - timedelta(days=max_age_days)
    if today.weekday() == 0:  # Monday -> cutoff Friday (3 days prior)
        cutoff = today - timedelta(days=3)
    elif today.weekday() == 6: # Sunday -> cutoff Friday
        cutoff = today - timedelta(days=2)

    for cat in categories:
        try:
            if cat == "fii_dii":
                df = query_df("SELECT max(trade_date) as max_date FROM market_data.fii_dii_flows FINAL")
            else:
                df = query_df(
                    f"SELECT max(trade_date) as max_date FROM market_data.daily_prices FINAL WHERE category = '{cat}'"
                )

            max_dt = None
            if not df.empty and df.iloc[0]["max_date"] is not None:
                val = df.iloc[0]["max_date"]
                max_dt = val.date() if hasattr(val, "date") else val

            if max_dt is None or max_dt < cutoff or force:
                logger.info(
                    "Auto-freshness guard: category '%s' max_date (%s) is older than cutoff (%s) — auto-importing...",
                    cat, max_dt, cutoff
                )
                from src.commands.import_cmd import ImportDataCommand
                cmd = ImportDataCommand(categories=[cat], lookback_days=30, full_reimport=False)
                cmd.execute()
                auto_imported.append(cat)
        except Exception as exc:
            logger.warning("Auto-freshness guard check failed for category '%s': %s", cat, exc)

    return auto_imported


def check_global_freshness(max_age_days: int = 1, force: bool = False) -> list[str]:
    """
    Audit all registered market categories and auto-import any stale categories.
    Uses a 5-minute TTL to avoid redundant checks during rapid tool calls.
    """
    global _last_freshness_check
    now = time.time()
    if not force and (now - _last_freshness_check) < _FRESHNESS_CHECK_TTL:
        return []

    _last_freshness_check = now
    return check_and_auto_import_stale_categories(ALL_CATEGORIES, max_age_days=max_age_days, force=force)
