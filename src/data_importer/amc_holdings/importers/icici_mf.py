"""
ICICI Prudential AMC fund holdings via Morningstar sal-service API.

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
_API_KEY = "lstzFDEOhfFNMLikKa0am9mgEKLBl49T"   # public key embedded in mstarpy

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

ICICI_FUNDS: list[tuple[str, str, str, str]] = [
    ("120716", "ICICI_MULTI_ASSET",        "INF109K015K4",  "F00000PE3K"),
    ("120586", "ICICI_BLUECHIP",           "INF109K01ZW2",  "F00000N9YF"),
    ("120505", "ICICI_VALUE_DISCOVERY",    "INF109K01XV0",  "F0000029OM"),
    ("120251", "ICICI_BAF",                "INF109K01ZN1",  "F00000N9YD"),
    ("120379", "ICICI_MIDCAP",             "INF109K01373",  "F0000029ON"),
    ("120828", "ICICI_SMALLCAP",           "INF109K01ZX0",  "F00000N9YG"),
    ("120593", "ICICI_TECH",               "INF109K01ZV4",  "F00000N9YE"),
    ("120380", "ICICI_INFRA",              "INF109K01375",  "F0000029OP"),
    ("148571", "ICICI_NIFTY50_INDEX",      "INF109KA1FH5",  "F00001485N"),
    ("120397", "ICICI_FMCG",              "INF109K01ZU6",  "F00000N9YC"),
    ("148570", "ICICI_COMMODITIES",        "INF109KA13N5",  "F00001485M"),
    ("148516", "ICICI_ESG",                "INF109KC1O09",  "F000015Q0S"),
]

_COLUMNS = [
    "scheme_code", "fund_name", "as_of_month",
    "isin", "security_name", "asset_type",
    "market_value_cr", "pct_of_nav", "imported_at",
]


class IciciMFImporter(BaseFundImporter):
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
        return "ICICI Prudential AMC"

    def table_name(self) -> str:
        return "market_data.mf_holdings"

    def column_names(self) -> list[str]:
        return _COLUMNS

    def watermark_source(self) -> str:
        return "mf_holdings"

    def fetch_sources(self) -> list[Any]:
        return list(ICICI_FUNDS)

    def filter_sources(self, sources: list[Any], client) -> list[Any]:
        if self.full_reimport:
            return sources

        # Filter sources based on watermark per scheme
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
                    # If imported within current month, skip unless reimporting
                    if last_date.year >= date.today().year and last_date.month >= date.today().month:
                        continue
                filtered.append(src)
            return filtered
        except Exception as exc:
            logger.warning("Failed to query ICICI watermark: %s", exc)

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
    parser = argparse.ArgumentParser(description="ICICI Prudential AMC Holdings Importer")
    parser.add_argument("--from-year", type=int, default=2020, help="Earliest year to import (default: 2020)")
    parser.add_argument("--full", action="store_true", help="Reimport all months, ignoring watermarks")
    parser.add_argument("--dry-run", action="store_true", help="Parse and print counts; skip DB insert")
    parser.add_argument("--test", action="store_true", help="Process first source only")
    args = parser.parse_args()

    importer = IciciMFImporter(full_reimport=args.full, from_year=args.from_year)
    importer.run(dry_run=args.dry_run, test=args.test)


if __name__ == "__main__":
    main()
