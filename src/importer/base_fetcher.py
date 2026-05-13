"""
src/importer/base_fetcher.py
─────────────────────────────
Fetcher ABC — Adapter pattern for data ingestion.

Every external data source implements this interface. The orchestrator
(MarketDataRepository.run_fetcher) handles watermarks, connection
lifecycle, dry-run, and summary logging — the fetcher only knows how
to fetch and where to write.

Implementing a new source
─────────────────────────
    class MyFetcher(Fetcher):
        source_name  = "my_source"    # watermark key
        symbol_key   = "MY_SYMBOL"    # watermark symbol

        def fetch(self, from_date, to_date) -> list[dict]:
            ...

        def insert(self, rows, ch) -> int:
            return ch.insert_my_table(rows)

    # Register in FETCHER_REGISTRY:
    FETCHER_REGISTRY["my_category"] = MyFetcher()

That's it. The orchestrator loop picks it up automatically.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
from typing import Any


class Fetcher(ABC):
    """
    Adapter interface between an external data source and ClickHouse.

    Attributes
    ----------
    source_name   : watermark source key  (e.g. "yfinance", "nse_fii_dii")
    symbol_key    : watermark symbol      (e.g. "MARKET", or per-symbol override)
    description   : human-readable label  (shown in CLI progress output)
    overlap_days  : re-fetch window to catch late corrections  (default 3)
    """

    source_name:  str
    symbol_key:   str
    description:  str = ""
    overlap_days: int = 3

    @abstractmethod
    def fetch(self, from_date: date, to_date: date) -> list[dict[str, Any]]:
        """
        Pull rows from the external source.

        Parameters
        ----------
        from_date : inclusive start date
        to_date   : inclusive end date

        Returns empty list if source is unavailable — never raises.
        """

    @abstractmethod
    def insert(self, rows: list[dict[str, Any]], ch) -> int:
        """
        Write rows to ClickHouse via the provided ClickHouseImporter.

        Returns the number of rows written.
        """

    def validate(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Optional: filter/clean rows before insert.
        Default implementation passes through unchanged.
        """
        return rows

    def max_date(self, rows: list[dict[str, Any]]) -> date:
        """
        Extract the latest date from rows for watermark update.
        Override if the date field has a non-standard name.
        """
        for key in ("trade_date", "report_date", "nav_date", "as_of_month", "snapshot_at"):
            dates = [r[key] for r in rows if r.get(key) is not None]
            if dates:
                latest = max(dates)
                return latest if isinstance(latest, date) else latest.date()
        raise ValueError(f"{self.__class__.__name__}: cannot determine max date from rows")

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(source={self.source_name!r}, symbol={self.symbol_key!r})"
