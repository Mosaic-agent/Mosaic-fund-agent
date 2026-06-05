from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any
import httpx

from src.scripts.fund_imports.base import BaseFundImporter
from src.scripts.dsp.import_all_dsp_equity import process_month, ZIP_FILES, BASE_URL as MEDIA_BASE
from src.scripts.dsp.import_latest_dsp import discover_latest_zip

logger = logging.getLogger(__name__)


class DspImporter(BaseFundImporter):
    """
    DSP Mutual Fund holdings importer.
    Supports delta sync (latest month via auto-discovery) and full re-import.
    """

    def __init__(self, full_reimport: bool = False) -> None:
        super().__init__()
        self.full_reimport = full_reimport

    def fund_name(self) -> str:
        return "DSP Mutual Fund"

    def table_name(self) -> str:
        return "market_data.mf_holdings"

    def column_names(self) -> list[str]:
        return [
            "scheme_code",
            "fund_name",
            "as_of_month",
            "isin",
            "security_name",
            "asset_type",
            "market_value_cr",
            "pct_of_nav",
            "imported_at",
        ]

    def watermark_source(self) -> str:
        return "mf_holdings"

    def fetch_sources(self) -> list[tuple[str, str]]:
        """
        Return the list of (as_of_date_str, zip_url) to process.
        """
        if self.full_reimport:
            # Return all historical zip files
            return [(as_of, MEDIA_BASE + suffix) for as_of, suffix in ZIP_FILES]

        # Otherwise, try to discover the latest month
        discovered = discover_latest_zip()
        if discovered:
            return [discovered]

        # Fallback to the last hardcoded entry if scraping fails
        as_of, suffix = ZIP_FILES[-1]
        return [(as_of, MEDIA_BASE + suffix)]

    def filter_sources(self, sources: list[tuple[str, str]], client) -> list[tuple[str, str]]:
        """
        Remove months that are already imported by checking watermark.
        """
        if self.full_reimport:
            return sources

        # For DSP, check the watermark for DSP_MULTI_ASSET
        try:
            rows = client.query(
                "SELECT max(last_date) FROM market_data.import_watermarks "
                "WHERE source = 'mf_holdings' AND symbol = 'DSP_MULTI_ASSET'"
            ).result_rows
            if rows and rows[0][0]:
                last_date = rows[0][0]
                filtered = []
                for as_of_str, url in sources:
                    dt = datetime.strptime(as_of_str, "%Y-%m-%d").date()
                    if dt > last_date:
                        filtered.append((as_of_str, url))
                return filtered
        except Exception as exc:
            logger.warning("Failed to query DSP watermark: %s", exc)

        return sources

    def parse_source(self, source: tuple[str, str], http: httpx.Client) -> list[dict]:
        as_of_str, url = source
        # Call the existing process_month function
        raw_rows = process_month(as_of_str, url)

        # Convert as_of_month string to date object for insertion compatibility
        parsed_rows = []
        for r in raw_rows:
            parsed_r = dict(r)
            parsed_r["as_of_month"] = datetime.strptime(r["as_of_month"], "%Y-%m-%d").date()
            parsed_rows.append(parsed_r)

        return parsed_rows
