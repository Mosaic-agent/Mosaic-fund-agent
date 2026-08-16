"""
src/data_importer/amc_holdings/importers/invesco.py
────────────────────────────────────────────────────
Invesco Mutual Fund (AMC) monthly portfolio holdings importer.

Discovers monthly portfolio disclosures across all active Invesco schemes
(Equity, Hybrid, Fixed Income, Fund of Funds, ETF, Index Funds) by querying Invesco's
Sitefinity backend API endpoints (https://www.invescomutualfund.com/api/CompleteMonthlyHoldings),
downloads monthly Excel workbooks, parses instrument holdings, classifies asset types,
converts values from Rs. Lakhs to Crores, scales percentages, and stores rows into
market_data.mf_holdings with delta sync and watermarking.
"""

from __future__ import annotations

import calendar
import io
import logging
import re
import xml.etree.ElementTree as ET
from datetime import date, datetime
from typing import Any

import httpx
import pandas as pd

from src.data_importer.amc_holdings.base import (
    COMMON_HEADERS,
    BaseFundImporter,
    classify_asset,
)

logger = logging.getLogger(__name__)

API_CLASSIFICATIONS_URL = "https://www.invescomutualfund.com/api/ClassificationCompleteMonthlyHoldings"
API_HOLDINGS_URL = "https://www.invescomutualfund.com/api/CompleteMonthlyHoldings"
_TIMEOUT = 30.0

_ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{10}$")

# Static / canonical scheme mapping by scheme name or sheet code
SCHEME_MAP: dict[str, tuple[str, str]] = {
    # Equity
    "Invesco India Smallcap Fund": ("INVESCO_SMALL_CAP", "Invesco India Smallcap Fund"),
    "Invesco India Flexi Cap Fund": ("INVESCO_FLEXI_CAP", "Invesco India Flexi Cap Fund"),
    "Invesco India Midcap Fund": ("INVESCO_MID_CAP", "Invesco India Midcap Fund"),
    "Invesco India Large & Mid Cap Fund": ("INVESCO_LARGE_AND_MID_CAP", "Invesco India Large & Mid Cap Fund"),
    "Invesco India Largecap Fund": ("INVESCO_LARGE_CAP", "Invesco India Largecap Fund"),
    "Invesco India Contra Fund": ("INVESCO_CONTRA", "Invesco India Contra Fund"),
    "Invesco India Multicap Fund": ("INVESCO_MULTICAP", "Invesco India Multicap Fund"),
    "Invesco India Focused Fund": ("INVESCO_FOCUSED", "Invesco India Focused Fund"),
    "Invesco India Financial Services Fund": ("INVESCO_FINANCIAL_SERVICES", "Invesco India Financial Services Fund"),
    "Invesco India Infrastructure Fund": ("INVESCO_INFRASTRUCTURE", "Invesco India Infrastructure Fund"),
    "Invesco India Manufacturing Fund": ("INVESCO_MANUFACTURING", "Invesco India Manufacturing Fund"),
    "Invesco India Technology Fund": ("INVESCO_TECHNOLOGY", "Invesco India Technology Fund"),
    "Invesco India Consumption Fund": ("INVESCO_CONSUMPTION", "Invesco India Consumption Fund"),
    "Invesco India Business Cycle Fund": ("INVESCO_BUSINESS_CYCLE", "Invesco India Business Cycle Fund"),
    "Invesco India ELSS Tax Saver Fund": ("INVESCO_ELSS_TAX_SAVER", "Invesco India ELSS Tax Saver Fund"),
    "Invesco India ESG Integration Strategy Fund": ("INVESCO_ESG", "Invesco India ESG Integration Strategy Fund"),
    "Invesco India PSU Equity Fund": ("INVESCO_PSU_EQUITY", "Invesco India PSU Equity Fund"),

    # Hybrid
    "Invesco India Balanced Advantage Fund": ("INVESCO_BALANCED_ADVANTAGE", "Invesco India Balanced Advantage Fund"),
    "Invesco India Multi Asset Allocation Fund": ("INVESCO_MULTI_ASSET_ALLOCATION", "Invesco India Multi Asset Allocation Fund"),
    "Invesco India Arbitrage Fund": ("INVESCO_ARBITRAGE", "Invesco India Arbitrage Fund"),
    "Invesco India Equity Savings Fund": ("INVESCO_EQUITY_SAVINGS", "Invesco India Equity Savings Fund"),
    "Invesco India Aggressive Hybrid Fund": ("INVESCO_AGGRESSIVE_HYBRID", "Invesco India Aggressive Hybrid Fund"),

    # Debt / Cash
    "Invesco India Liquid Fund": ("INVESCO_LIQUID", "Invesco India Liquid Fund"),
    "Invesco India Overnight Fund": ("INVESCO_OVERNIGHT", "Invesco India Overnight Fund"),
    "Invesco India Money Market Fund": ("INVESCO_MONEY_MARKET", "Invesco India Money Market Fund"),
    "Invesco India Ultra Short Duration Fund": ("INVESCO_ULTRA_SHORT_DURATION", "Invesco India Ultra Short Duration Fund"),
    "Invesco India Low Duration Fund": ("INVESCO_LOW_DURATION", "Invesco India Low Duration Fund"),
    "Invesco India Short Duration Fund": ("INVESCO_SHORT_DURATION", "Invesco India Short Duration Fund"),
    "Invesco India Medium Duration Fund": ("INVESCO_MEDIUM_DURATION", "Invesco India Medium Duration Fund"),
    "Invesco India Corporate Bond Fund": ("INVESCO_CORPORATE_BOND", "Invesco India Corporate Bond Fund"),
    "Invesco India Banking and PSU Fund": ("INVESCO_BANKING_AND_PSU", "Invesco India Banking and PSU Fund"),
    "Invesco India Credit Risk Fund": ("INVESCO_CREDIT_RISK", "Invesco India Credit Risk Fund"),
    "Invesco India Gilt Fund": ("INVESCO_GILT", "Invesco India Gilt Fund"),

    # ETF / Index
    "Invesco India Nifty 50 Exchange Traded Fund": ("INVESCO_NIFTY_50_ETF", "Invesco India Nifty 50 ETF"),
    "Invesco India Gold Exchange Traded Fund": ("INVESCO_GOLD_ETF", "Invesco India Gold ETF"),
    "Invesco India BSE Sensex Index Fund": ("INVESCO_SENSEX_INDEX", "Invesco India BSE Sensex Index Fund"),
    "Invesco India Nifty Bank Index Fund": ("INVESCO_NIFTY_BANK_INDEX", "Invesco India Nifty Bank Index Fund"),
}

_COLUMNS = [
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

_MONTHS = [
    ("Jan", 1), ("Feb", 2), ("Mar", 3), ("Apr", 4),
    ("May", 5), ("Jun", 6), ("Jul", 7), ("Aug", 8),
    ("Sep", 9), ("Oct", 10), ("Nov", 11), ("Dec", 12),
]


def _normalise_fund_identity(fund_title: str, sheet_name: str = "") -> tuple[str, str]:
    """Resolve (scheme_code, fund_name) from Invesco fund name or sheet name."""
    clean_title = re.sub(r"\s+", " ", fund_title).strip()
    if clean_title in SCHEME_MAP:
        return SCHEME_MAP[clean_title]

    for known_title, (code, name) in SCHEME_MAP.items():
        if known_title.lower() in clean_title.lower() or clean_title.lower() in known_title.lower():
            return (code, name)

    # Fallback to generated code
    code = clean_title.upper()
    code = re.sub(r"^INVESCO\s+INDIA\s+", "", code)
    code = re.sub(r"\s+FUND$", "", code)
    code = re.sub(r"[^A-Z0-9_]", "_", code)
    code = re.sub(r"_+", "_", code).strip("_")
    return (f"INVESCO_{code}", clean_title)


class InvescoImporter(BaseFundImporter):
    """
    Invesco Mutual Fund monthly portfolio importer.
    Discovers disclosures via Invesco's API and parses Excel workbooks.
    """

    REQUEST_DELAY = 0.4

    def __init__(
        self,
        full_reimport: bool = False,
        from_year: int = 2023,
        target_month: date | None = None,
        freshness_months: int = 0,
        **kwargs: Any,
    ) -> None:
        super().__init__(target_month=target_month, freshness_months=freshness_months)
        self.full_reimport = full_reimport
        self.from_year = from_year
        self._latest_imported_date: date | None = None

    def fund_name(self) -> str:
        return "Invesco Mutual Fund"

    def table_name(self) -> str:
        return "market_data.mf_holdings"

    def column_names(self) -> list[str]:
        return _COLUMNS

    def watermark_source(self) -> str:
        return "mf_holdings"

    def fetch_sources(self) -> list[tuple[date, str, str, str]]:
        """
        Query Invesco Sitefinity API across categories and years.
        Returns list of (as_of_date, scheme_title, filename, download_url).
        """
        sources: list[tuple[date, str, str, str]] = []
        headers = dict(COMMON_HEADERS)
        headers["Accept"] = "application/json, text/plain, */*"

        current_year = datetime.now().year
        years = list(range(self.from_year, current_year + 1))

        categories = [
            "equity",
            "hybrid",
            "fixed-income",
            "fund-of-funds",
            "exchange-traded-fund",
            "index-funds",
        ]

        with httpx.Client(headers=headers, timeout=_TIMEOUT, follow_redirects=True) as http:
            for yr in years:
                for cat in categories:
                    url = f"{API_HOLDINGS_URL}?year={yr}&classification={cat}"
                    try:
                        resp = http.get(url)
                        if resp.status_code != 200:
                            continue

                        items: list[dict] = []
                        ct = resp.headers.get("content-type", "")
                        if "json" in ct:
                            try:
                                items = resp.json()
                            except Exception:
                                items = []
                        elif "xml" in ct or resp.text.strip().startswith("<"):
                            try:
                                root = ET.fromstring(resp.text)
                                for elem in root:
                                    d: dict[str, str] = {}
                                    for child in elem:
                                        tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                                        d[tag] = child.text.strip() if child.text else ""
                                    items.append(d)
                            except Exception:
                                items = []

                        if not isinstance(items, list):
                            continue

                        for item in items:
                            scheme_title = str(item.get("Name", "")).strip()
                            if not scheme_title:
                                continue

                            for mon_prefix, mon_num in _MONTHS:
                                file_url = str(item.get(f"{mon_prefix}Url", "")).strip()
                                if not file_url or not (file_url.endswith(".xlsx") or file_url.endswith(".xls") or ".xlsx?" in file_url or ".xls?" in file_url):
                                    continue

                                _, last_day = calendar.monthrange(yr, mon_num)
                                as_of_date = date(yr, mon_num, last_day)

                                fname = file_url.split("/")[-1].split("?")[0]
                                sources.append((as_of_date, scheme_title, fname, file_url))

                    except Exception as exc:
                        logger.debug("Failed fetching Invesco sources for year %s, cat %s: %s", yr, cat, exc)

        # Deduplicate sources by (date, scheme_title, filename)
        seen: set[tuple[date, str, str]] = set()
        deduped: list[tuple[date, str, str, str]] = []
        for as_of, title, fn, u in sorted(sources, key=lambda x: (x[0], x[1])):
            key = (as_of, title, fn)
            if key not in seen:
                seen.add(key)
                deduped.append((as_of, title, fn, u))

        self._console.print(f"[dim]Invesco: discovered {len(deduped)} monthly portfolio source(s).[/dim]")
        return deduped

    def filter_sources(
        self, sources: list[tuple[date, str, str, str]], client: Any
    ) -> list[tuple[date, str, str, str]]:
        """Filter out months already imported into ClickHouse, unless full_reimport is active."""
        if self.full_reimport:
            return sources

        last_date: date | None = None
        try:
            rows = client.query(
                "SELECT max(last_date) FROM market_data.import_watermarks "
                "WHERE source = 'mf_holdings' AND symbol = 'INVESCO_MONTHLY'"
            ).result_rows
            if rows and rows[0][0]:
                last_date = rows[0][0]
        except Exception as exc:
            logger.warning("Failed to query Invesco watermark: %s", exc)

        if last_date is None:
            return sources

        filtered = [s for s in sources if s[0] > last_date]
        skipped = len(sources) - len(filtered)
        if skipped:
            self._console.print(
                f"[dim]Delta sync: {skipped} Invesco file(s) already in DB (watermark {last_date}), "
                f"{len(filtered)} to fetch.[/dim]"
            )
        return filtered

    def parse_source(
        self, source: tuple[date, str, str, str], http: httpx.Client
    ) -> list[dict]:
        """
        Download and parse an Invesco monthly Excel workbook.
        """
        as_of_date, scheme_title, fname, url = source
        headers = dict(COMMON_HEADERS)
        headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"

        try:
            resp = http.get(url, headers=headers, timeout=_TIMEOUT)
            resp.raise_for_status()
        except Exception as exc:
            self._console.print(f"  [red]Download failed ({scheme_title}, {url}): {exc}[/red]")
            return []

        # Load Excel workbook with engine fallback
        xl: pd.ExcelFile | None = None
        for engine in ("openpyxl", "xlrd"):
            try:
                xl = pd.ExcelFile(io.BytesIO(resp.content), engine=engine)
                break
            except Exception:
                continue

        if xl is None:
            self._console.print(f"  [red]Cannot parse Invesco workbook '{fname}' ({scheme_title})[/red]")
            return []

        all_holdings: list[dict] = []
        imported_at = datetime.now()

        scheme_code, fund_name = _normalise_fund_identity(scheme_title)

        for sheet in xl.sheet_names:
            try:
                df = xl.parse(sheet, header=None)
            except Exception as exc:
                logger.debug("Failed parsing sheet %s in %s: %s", sheet, fname, exc)
                continue

            if df.shape[0] < 5 or df.shape[1] < 4:
                continue

            # Locate column header row dynamically
            header_row_idx: int | None = None
            for r in range(min(15, len(df))):
                r_vals = [str(x).strip().lower() for x in df.iloc[r].tolist() if pd.notna(x)]
                if any("isin" in x for x in r_vals) and any("instrument" in x or "name" in x or "issuer" in x or "security" in x for x in r_vals):
                    header_row_idx = r
                    break

            if header_row_idx is None:
                continue

            headers_list = [str(x).strip() if pd.notna(x) else "" for x in df.iloc[header_row_idx].tolist()]

            isin_col: int | None = None
            name_col: int | None = None
            ind_col: int | None = None
            mv_col: int | None = None
            pct_col: int | None = None

            for c_idx, h in enumerate(headers_list):
                hl = h.lower()
                if "isin" in hl:
                    isin_col = c_idx
                elif "instrument" in hl or "name" in hl or "issuer" in hl or "security" in hl:
                    if name_col is None:
                        name_col = c_idx
                elif "rating" in hl or "industry" in hl:
                    ind_col = c_idx
                elif "market value" in hl or "market/fair" in hl or "market" in hl:
                    if mv_col is None:
                        mv_col = c_idx
                elif "aum" in hl or "net assets" in hl or "nav" in hl:
                    pct_col = c_idx
                elif "%" in hl and pct_col is None and "ytm" not in hl and "ytc" not in hl:
                    pct_col = c_idx

            # Standard layout fallback if not detected
            if isin_col is None:
                isin_col = 2
            if name_col is None:
                name_col = 1
            if ind_col is None:
                ind_col = 3
            if mv_col is None:
                mv_col = 5
            if pct_col is None:
                pct_col = 6

            sheet_raw_rows: list[tuple[str, str, str, float, float]] = []
            for r_idx in range(header_row_idx + 1, len(df)):
                row = df.iloc[r_idx].tolist()
                if len(row) <= max([c for c in (isin_col, name_col, ind_col, mv_col, pct_col) if c is not None]):
                    continue

                isin_val = str(row[isin_col]).strip() if isin_col is not None else ""
                if not _ISIN_RE.match(isin_val):
                    continue

                name_val = str(row[name_col]).strip() if name_col is not None and pd.notna(row[name_col]) else ""
                ind_val = str(row[ind_col]).strip() if ind_col is not None and pd.notna(row[ind_col]) else ""

                try:
                    mv_raw = float(row[mv_col]) if mv_col is not None else 0.0
                except (TypeError, ValueError):
                    mv_raw = 0.0

                try:
                    pct_raw = float(row[pct_col]) if pct_col is not None else 0.0
                except (TypeError, ValueError):
                    pct_raw = 0.0

                sheet_raw_rows.append((isin_val, name_val, ind_val, mv_raw, pct_raw))

            if not sheet_raw_rows:
                continue

            # Check percentage scaling (fraction 0-1 vs percentage 0-100)
            valid_pcts = [r[4] for r in sheet_raw_rows if r[4] == r[4] and r[4] != 0]
            raw_sum = sum(valid_pcts)
            pct_scale = 100.0 if raw_sum <= 5.0 else 1.0

            sheet_holdings: list[dict] = []
            for isin_val, name_val, ind_val, mv_raw, pct_raw in sheet_raw_rows:
                pct_val = round(pct_raw * pct_scale, 4)
                if pct_val > 50.0:
                    logger.warning(
                        "Skipping anomalous row '%s' (%s, %s): pct_of_nav=%.2f%% > 50%%",
                        name_val,
                        scheme_code,
                        as_of_date,
                        pct_val,
                    )
                    continue

                sheet_holdings.append({
                    "scheme_code": scheme_code,
                    "fund_name": fund_name,
                    "as_of_month": as_of_date,
                    "isin": isin_val,
                    "security_name": name_val,
                    "asset_type": classify_asset(name_val, ind_val),
                    "market_value_cr": round(mv_raw / 100.0, 4),  # Rs Lakhs -> Rs Cr
                    "pct_of_nav": pct_val,
                    "imported_at": imported_at,
                })

            if sheet_holdings:
                pct_total = sum(h["pct_of_nav"] for h in sheet_holdings)
                color = "green" if pct_total <= 105.0 else "yellow"
                self._console.print(
                    f"  [{color}]{fund_name}: {len(sheet_holdings)} holdings, "
                    f"pct_total={pct_total:.1f}% ({as_of_date})[/{color}]"
                )
                all_holdings.extend(sheet_holdings)
                # Found the detailed holdings sheet, break
                break

        if self._latest_imported_date is None or as_of_date > self._latest_imported_date:
            self._latest_imported_date = as_of_date

        return all_holdings

    def watermark_rows(self, all_rows: list[dict]) -> list[tuple[str, date]]:
        if self._latest_imported_date:
            return [("INVESCO_MONTHLY", self._latest_imported_date)]
        return []
