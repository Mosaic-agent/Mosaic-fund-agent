from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any
import httpx

from src.data_importer.amc_holdings.base import BaseFundImporter
from src.data_importer.amc_downloaders.dsp_holdings.import_all_dsp_equity import process_month, ZIP_FILES, BASE_URL as MEDIA_BASE
from src.data_importer.amc_downloaders.dsp_holdings.import_latest_dsp import discover_latest_zip

logger = logging.getLogger(__name__)


class DspImporter(BaseFundImporter):
    """
    DSP Mutual Fund holdings importer.
    Supports delta sync (latest month via auto-discovery) and full re-import.
    """

    def __init__(self, full_reimport: bool = False, from_year: int = 2020, target_month: date | None = None, freshness_months: int = 0) -> None:
        super().__init__(target_month=target_month, freshness_months=freshness_months)
        self.full_reimport = full_reimport
        self.from_year = from_year

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
        if self.full_reimport or self._target_month or self._freshness_months > 0:
            # Return all historical zip files plus any discovered new month
            sources = [(as_of, MEDIA_BASE + suffix) for as_of, suffix in ZIP_FILES]
            discovered = discover_latest_zip()
            if discovered and discovered[0] not in [s[0] for s in sources]:
                sources.append(discovered)
            return sources

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

            # Belt-and-suspenders: reject rows where pct_of_nav is clearly a
            # rogue total/AUM value that slipped past the per-sheet guard.
            # A diversified fund cannot have a single holding > 50% of NAV.
            pct = parsed_r.get("pct_of_nav", 0.0)
            if pct > 50.0:
                logger.warning(
                    "Skipping row '%s' (%s): pct_of_nav=%.2f exceeds 50%% guard",
                    parsed_r.get("security_name"), as_of_str, pct,
                )
                continue

            parsed_rows.append(parsed_r)

        # Sanity-check the parsed batch: warn per-fund if total weight is implausible.
        # (The full batch spans all fund sheets from the ZIP, each independently ~100%.)
        if parsed_rows:
            from collections import defaultdict
            per_fund: dict[str, float] = defaultdict(float)
            per_fund_n: dict[str, int] = defaultdict(int)
            for r in parsed_rows:
                fn = r.get("fund_name", "UNKNOWN")
                per_fund[fn] += r.get("pct_of_nav", 0.0)
                per_fund_n[fn] += 1
            bad = {fn: t for fn, t in per_fund.items() if t > 150.0}
            if bad:
                for fn, total in bad.items():
                    logger.warning(
                        "DSP %s [%s]: pct_total=%.1f%% — check source data",
                        as_of_str, fn, total,
                    )
                    self._console.print(
                        f"  [yellow]⚠ {as_of_str} [{fn}]: pct_total={total:.1f}%% — data quality warning[/yellow]"
                    )
            else:
                worst = max(per_fund.values(), default=0.0)
                self._console.print(
                    f"  [green]✓[/green] {as_of_str}: {len(parsed_rows)} rows across "
                    f"{len(per_fund)} funds — max pct_total={worst:.1f}%"
                )

        return parsed_rows
