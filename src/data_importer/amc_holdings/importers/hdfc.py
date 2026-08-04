"""
HDFC Asset Management Company (HDFC AMC) holdings importer via Morningstar API.

Supports delta sync, watermarking, and historical from-year configuration (default 2020).
"""

from __future__ import annotations

import os
import sys

sys.path.append(os.getcwd())

import argparse
import logging
from datetime import date, datetime
from typing import Any

import httpx

from src.data_importer.amc_holdings.base import BaseFundImporter, classify_asset

logger = logging.getLogger(__name__)

# ── Morningstar API ───────────────────────────────────────────────────────────

_SAL_BASE = "https://api-global.morningstar.com/sal-service/v1"
_API_KEY = "lstzFDEOhfFNMLikKa0am9mgEKLBl49T"

_MS_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "apikey": _API_KEY,
}

_MS_PARAMS = {
    "clientId": "MDC",
    "version": "4.71.0",
    "premiumNum": "10000",
    "freeNum": "10000",
}

# ── Fund catalogue ────────────────────────────────────────────────────────────
# (amfi_scheme_code, fund_name, isin, morningstar_sec_id)

HDFC_FUNDS: list[tuple[str, str, str, str]] = [
    ("119062", "HDFC_SMALL_CAP",            "INF179KC1BT1", "F00000PD0L"),
    ("119063", "HDFC_MIDCAP_OPPORTUNITIES", "INF179K01840", "F00000PD0M"),
    ("118989", "HDFC_FLEXI_CAP",             "INF179K01AY2", "F00000PD0B"),
    ("119027", "HDFC_TOP_100",               "INF179KC1BS3", "F00000PD0C"),
    ("119018", "HDFC_LARGE_AND_MID_CAP",     "INF179KC1BR5", "F00000PD0A"),
    ("118968", "HDFC_BALANCED_ADVANTAGE",   "INF179K01AZ9", "F00000PD0D"),
    ("119033", "HDFC_INFRASTRUCTURE",       "INF179K01BL0", "F00000PD0E"),
    ("148674", "HDFC_BANKING_FINANCIAL",     "INF179KC1EX2", "F00000PD0N"),
    ("119056", "HDFC_ELSS_TAX_SAVER",        "INF179K01BE5", "F00000PD0O"),
    ("151740", "HDFC_PHARMA_HEALTHCARE",     "INF179KC1FC6", "F00000PD0P"),
    ("151745", "HDFC_TECHNOLOGY",            "INF179KC1FD4", "F00000PD0Q"),
    ("151125", "HDFC_BUSINESS_CYCLE",        "INF179KC1EY0", "F00000PD0R"),
]

_COLUMNS = [
    "scheme_code", "fund_name", "as_of_month",
    "isin", "security_name", "asset_type",
    "market_value_cr", "pct_of_nav", "imported_at",
]


class HdfcImporter(BaseFundImporter):
    REQUEST_DELAY = 1.5

    def __init__(
        self,
        full_reimport: bool = False,
        from_year: int = 2020,
        target_month: date | None = None,
        freshness_months: int = 0,
    ) -> None:
        super().__init__(target_month=target_month, freshness_months=freshness_months)
        self.full_reimport = full_reimport
        self.from_year = from_year

    def fund_name(self) -> str:
        return "HDFC Asset Management Company"

    def table_name(self) -> str:
        return "market_data.mf_holdings"

    def column_names(self) -> list[str]:
        return _COLUMNS

    def watermark_source(self) -> str:
        return "mf_holdings"

    def fetch_sources(self) -> list[Any]:
        return list(HDFC_FUNDS)

    def filter_sources(self, sources: list[Any], client) -> list[Any]:
        if self.full_reimport:
            return sources

        try:
            filtered = []
            for src in sources:
                scheme_code, fund_name, isin, sec_id = src
                rows = client.query(
                    "SELECT max(last_date) FROM market_data.import_watermarks "
                    f"WHERE source = 'mf_holdings' AND symbol = '{fund_name}'"
                ).result_rows
                if rows and rows[0][0]:
                    last_date = rows[0][0]
                    if last_date.year >= date.today().year and last_date.month >= date.today().month:
                        continue
                filtered.append(src)
            return filtered
        except Exception as exc:
            logger.warning("Failed to query HDFC watermark: %s", exc)

        return sources

    def parse_source(self, source: Any, http: httpx.Client) -> list[dict]:
        scheme_code, fund_name, isin, sec_id = source
        as_of_month = date.today().replace(day=1)
        url = f"{_SAL_BASE}/fund/portfolio/holding/v2/{sec_id}/data"

        try:
            with httpx.Client(timeout=30, follow_redirects=True) as ms:
                resp = ms.get(url, headers=_MS_HEADERS, params=_MS_PARAMS)
                resp.raise_for_status()
                data = resp.json()
        except httpx.HTTPStatusError as exc:
            logger.warning(
                "Morningstar %d for %s: %s", exc.response.status_code, fund_name, exc
            )
            return []
        except Exception as exc:
            logger.warning("Failed to fetch %s: %s", fund_name, exc)
            return []

        rows: list[dict] = []
        imported_at = datetime.now()

        for page_key in ("equityHoldingPage", "boldHoldingPage", "otherHoldingPage"):
            page_data = data.get(page_key)
            if not page_data:
                continue
            for h in page_data.get("holdingList", []):
                security_name = str(h.get("securityName") or "Unknown")
                try:
                    pct_of_nav = float(h.get("weighting") or 0.0)
                except (TypeError, ValueError):
                    pct_of_nav = 0.0
                type_id = str(h.get("holdingTypeId") or h.get("holdingType") or "")
                asset_type = classify_asset(type_id, security_name)
                holding_isin = str(h.get("isin") or h.get("secId") or "")
                try:
                    market_value_cr = round(float(h.get("marketValue") or 0.0) / 1e7, 4)
                except (TypeError, ValueError):
                    market_value_cr = 0.0
                rows.append({
                    "scheme_code":     scheme_code,
                    "fund_name":       fund_name,
                    "as_of_month":     as_of_month,
                    "isin":            holding_isin or security_name[:20],
                    "security_name":   security_name,
                    "asset_type":      asset_type,
                    "market_value_cr": market_value_cr,
                    "pct_of_nav":      pct_of_nav,
                    "imported_at":     imported_at,
                })

        pct_sum = sum(r["pct_of_nav"] for r in rows)
        color = "yellow" if pct_sum > 100 else "green"
        self._console.print(
            f"  [{color}]→ {fund_name}: {len(rows)} holdings, "
            f"pct_sum={pct_sum:.1f}% (month={as_of_month})[/{color}]"
        )
        return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="HDFC Asset Management Company Importer")
    parser.add_argument("--from-year", type=int, default=2020, help="Earliest year to import (default: 2020)")
    parser.add_argument("--full", action="store_true", help="Reimport all months, ignoring watermarks")
    parser.add_argument("--dry-run", action="store_true", help="Parse and print counts; skip DB insert")
    parser.add_argument("--test", action="store_true", help="Process first source only")
    args = parser.parse_args()

    importer = HdfcImporter(full_reimport=args.full, from_year=args.from_year)
    importer.run(dry_run=args.dry_run, test=args.test)


if __name__ == "__main__":
    main()
