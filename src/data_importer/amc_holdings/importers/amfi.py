"""
src/scripts/fund_imports/importers/amfi.py
───────────────────────────────────────────
AMC-style fund importer for AMFI Category-Wise Monthly Flows & AUM.

Inherits from BaseFundImporter so it fits the standard AMC factory pattern
(src/scripts/fund_imports/factory.py) and can be run via:
    python src/main.py import --category amfi
    python src/main.py import --category amfi --month 2026-06
    python src/main.py import --category amfi --fresh 6
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

import httpx

from src.data_importer.amc_holdings.base import BaseFundImporter
from src.data_importer.fetchers.amfi_flows_fetcher import fetch_amfi_category_flows

logger = logging.getLogger(__name__)


class AmfiImporter(BaseFundImporter):
    """Importer for AMFI Category-Wise Monthly Flows & AUM."""

    REQUEST_DELAY = 0.1

    def __init__(self, full_reimport: bool = False, **kwargs) -> None:
        super().__init__(**kwargs)
        self._full_reimport = full_reimport

    def fund_name(self) -> str:
        return "AMFI Category-Wise Monthly Flows & AUM"

    def fetch_sources(self) -> list[Any]:
        # Sources are monthly target dates
        from src.data_importer.fetchers.amfi_flows_fetcher import _month_dates
        months_back = 120 if self._full_reimport else 24
        dates = _month_dates(months_back)
        # Each source is (first_of_month_date,)
        return [(d,) for d in dates]

    def parse_source(self, source: Any, http: httpx.Client) -> list[dict]:
        month_date = self.source_month(source)
        if not month_date:
            return []
        # Fetch flows for the target month window
        rows = fetch_amfi_category_flows(months_back=24)
        return [r for r in rows if r["report_month"] == month_date]

    def table_name(self) -> str:
        return "market_data.amfi_category_flows"

    def column_names(self) -> list[str]:
        return [
            "report_month",
            "category_name",
            "subcategory_group",
            "gross_purchase_cr",
            "gross_redemption_cr",
            "net_flow_cr",
            "closing_aum_cr",
            "flow_pct_of_aum",
        ]

    def watermark_source(self) -> str:
        return "amfi_category_flows"

    def watermark_rows(self, all_rows: list[dict]) -> list[tuple[str, date]]:
        if not all_rows:
            return []
        latest_date = max(r["report_month"] for r in all_rows)
        return [("INDUSTRY", latest_date)]
