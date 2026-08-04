"""Persist the preferred stock/ETF import source in ClickHouse."""

from __future__ import annotations

import logging

log = logging.getLogger(__name__)

PREFERENCE_KEY = "market_import_data_source"
TTL_HOURS = 24
VALID_SOURCES = {"shoonya", "nse", "yfinance"}

# Canonical alias map — single source of truth for all modules that accept
# a data_source string (cli.py, skills_tools.py, parallel_importer, etc.)
SOURCE_ALIASES: dict[str, str] = {
    "1": "shoonya",
    "shoonya": "shoonya",
    "2": "nse",
    "nse": "nse",
    "nselib": "nse",
    "3": "yfinance",
    "yf": "yfinance",
    "yahoo": "yfinance",
    "yfinance": "yfinance",
}

_DDL = """
CREATE TABLE IF NOT EXISTS market_data.agent_preferences (
    preference_key String,
    value          String,
    updated_at     DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY preference_key
"""


def normalize_data_source(value: str) -> str:
    return SOURCE_ALIASES.get(value.strip().lower(), "")


def get_saved_data_source() -> str:
    """Return the saved source when it is less than 24 hours old."""
    try:
        from src.db.pool import execute, query_df

        execute(_DDL)
        rows = query_df(
            f"""
            SELECT value
            FROM market_data.agent_preferences FINAL
            WHERE preference_key = '{PREFERENCE_KEY}'
              AND updated_at >= now() - INTERVAL {TTL_HOURS} HOUR
            ORDER BY updated_at DESC
            LIMIT 1
            """
        )
        if rows.empty:
            return ""
        return normalize_data_source(str(rows.iloc[0]["value"]))
    except Exception as exc:
        log.warning("Could not read import data-source preference: %s", exc)
        return ""


def save_data_source(source: str) -> bool:
    """Save a validated source and refresh its 24-hour TTL."""
    normalized = normalize_data_source(source)
    if not normalized:
        raise ValueError("data source must be shoonya, nse, or yfinance")

    try:
        from src.db.pool import acquire, execute

        execute(_DDL)
        with acquire() as client:
            client.insert(
                "market_data.agent_preferences",
                [[PREFERENCE_KEY, normalized]],
                column_names=["preference_key", "value"],
            )
        return True
    except Exception as exc:
        log.warning("Could not save import data-source preference: %s", exc)
        return False


def resolve_data_source(source: str = "") -> tuple[str, bool]:
    """
    Resolve an explicit or cached source.

    Returns ``(source, reused_saved_choice)``. An explicit source refreshes the
    persisted TTL.
    """
    if source.strip():
        normalized = normalize_data_source(source)
        if not normalized:
            raise ValueError("data source must be shoonya, nse, or yfinance")
        save_data_source(normalized)
        return normalized, False

    saved = get_saved_data_source()
    return saved, bool(saved)
