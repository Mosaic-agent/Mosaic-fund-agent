"""
src/data_importer/amc_holdings/importers/mirae.py
─────────────────────────────────────────────────
Mirae Asset Mutual Fund (AMC) monthly portfolio holdings importer.

Discovers monthly portfolio disclosures across all active Mirae Asset schemes
(Large Cap, Large & Midcap / Emerging Bluechip, Flexi Cap, Small Cap, Midcap,
Multi Asset Allocation, Focused, ELSS Tax Saver, Banking & PSU, Dynamic Bond,
Sectoral / Thematic, ETFs & FoFs) by querying the AjaxService portal API:
https://www.miraeassetmf.co.in/AjaxService/GetDownloadsData
with pagination, downloads monthly Excel workbooks, parses instrument holdings,
classifies asset types, converts values from Rs. Lakhs to Crores, scales
percentages, and stores rows into market_data.mf_holdings with delta sync and watermarking.
"""

from __future__ import annotations

import calendar
import io
import logging
import re
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

API_URL = "https://www.miraeassetmf.co.in/AjaxService/GetDownloadsData"
BASE_URL = "https://www.miraeassetmf.co.in"
_TIMEOUT = 30.0

_ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{10}$")

MONTH_NAMES = {
    "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
    "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
    "aug": 8, "august": 8, "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12,
}

# Static / canonical scheme mapping by scheme name
SCHEME_MAP: dict[str, tuple[str, str]] = {
    # Equity
    "Mirae Asset Large Cap Fund": ("MIRAE_LARGE_CAP", "Mirae Asset Large Cap Fund"),
    "Mirae Asset Large & Midcap Fund": ("MIRAE_LARGE_AND_MID_CAP", "Mirae Asset Large & Midcap Fund"),
    "Mirae Asset Emerging Bluechip Fund": ("MIRAE_LARGE_AND_MID_CAP", "Mirae Asset Large & Midcap Fund"),
    "Mirae Asset Flexi Cap Fund": ("MIRAE_FLEXI_CAP", "Mirae Asset Flexi Cap Fund"),
    "Mirae Asset Midcap Fund": ("MIRAE_MIDCAP", "Mirae Asset Midcap Fund"),
    "Mirae Asset Small Cap Fund": ("MIRAE_SMALL_CAP", "Mirae Asset Small Cap Fund"),
    "Mirae Asset Multicap Fund": ("MIRAE_MULTICAP", "Mirae Asset Multicap Fund"),
    "Mirae Asset Focused Fund": ("MIRAE_FOCUSED", "Mirae Asset Focused Fund"),
    "Mirae Asset ELSS Tax Saver Fund": ("MIRAE_ELSS_TAX_SAVER", "Mirae Asset ELSS Tax Saver Fund"),
    "Mirae Asset Great Consumer Fund": ("MIRAE_GREAT_CONSUMER", "Mirae Asset Great Consumer Fund"),
    "Mirae Asset Healthcare Fund": ("MIRAE_HEALTHCARE", "Mirae Asset Healthcare Fund"),
    "Mirae Asset Banking and Financial Services Fund": ("MIRAE_BANKING_AND_FINANCIAL_SERVICES", "Mirae Asset Banking and Financial Services Fund"),
    "Mirae Asset Infrastructure Fund": ("MIRAE_INFRASTRUCTURE", "Mirae Asset Infrastructure Fund"),
    "Mirae Asset Manufacturing Fund": ("MIRAE_MANUFACTURING", "Mirae Asset Manufacturing Fund"),

    # Hybrid
    "Mirae Asset Multi Asset Allocation Fund": ("MIRAE_MULTI_ASSET_ALLOCATION", "Mirae Asset Multi Asset Allocation Fund"),
    "Mirae Asset Balanced Advantage Fund": ("MIRAE_BALANCED_ADVANTAGE", "Mirae Asset Balanced Advantage Fund"),
    "Mirae Asset Hybrid Equity Fund": ("MIRAE_HYBRID_EQUITY", "Mirae Asset Hybrid Equity Fund"),
    "Mirae Asset Equity Savings Fund": ("MIRAE_EQUITY_SAVINGS", "Mirae Asset Equity Savings Fund"),

    # Debt / Cash
    "Mirae Asset Banking and PSU Fund": ("MIRAE_BANKING_AND_PSU", "Mirae Asset Banking and PSU Fund"),
    "Mirae Asset Dynamic Bond Fund": ("MIRAE_DYNAMIC_BOND", "Mirae Asset Dynamic Bond Fund"),
    "Mirae Asset Corporate Bond Fund": ("MIRAE_CORPORATE_BOND", "Mirae Asset Corporate Bond Fund"),
    "Mirae Asset Short Duration Fund": ("MIRAE_SHORT_DURATION", "Mirae Asset Short Duration Fund"),
    "Mirae Asset Low Duration Fund": ("MIRAE_LOW_DURATION", "Mirae Asset Low Duration Fund"),
    "Mirae Asset Ultra Short Duration Fund": ("MIRAE_ULTRA_SHORT_DURATION", "Mirae Asset Ultra Short Duration Fund"),
    "Mirae Asset Money Market Fund": ("MIRAE_MONEY_MARKET", "Mirae Asset Money Market Fund"),
    "Mirae Asset Liquid Fund": ("MIRAE_LIQUID", "Mirae Asset Liquid Fund"),
    "Mirae Asset Overnight Fund": ("MIRAE_OVERNIGHT", "Mirae Asset Overnight Fund"),
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


def _parse_disclosure_date(title: str, pub_date_raw: str = "") -> date | None:
    """Parse date from disclosure title or Sitefinity timestamp."""
    m = re.search(r"as on\s+(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]+)\s+(\d{4})", title, re.IGNORECASE)
    if m:
        day, mon_str, year = int(m.group(1)), m.group(2).lower(), int(m.group(3))
        if mon_str in MONTH_NAMES:
            month = MONTH_NAMES[mon_str]
            _, last_day = calendar.monthrange(year, month)
            return date(year, month, min(day, last_day))

    m2 = re.search(r"\b([A-Za-z]+)[-\s]+(\d{4})\b", title, re.IGNORECASE)
    if m2:
        mon_str, year = m2.group(1).lower(), int(m2.group(2))
        if mon_str in MONTH_NAMES:
            month = MONTH_NAMES[mon_str]
            _, last_day = calendar.monthrange(year, month)
            return date(year, month, last_day)

    if pub_date_raw and "/Date(" in pub_date_raw:
        try:
            ts_ms = int(re.search(r"\d+", pub_date_raw).group(0))
            dt = datetime.fromtimestamp(ts_ms / 1000.0)
            _, last_day = calendar.monthrange(dt.year, dt.month)
            return date(dt.year, dt.month, last_day)
        except Exception:
            pass

    return None


def _normalise_fund_identity(title: str, fname: str = "", sheet_header: str = "") -> tuple[str, str]:
    """Resolve (scheme_code, fund_name) from Mirae Asset title, filename, or sheet header."""
    fund_name = ""
    if " for " in title:
        fund_name = title.split(" for ", 1)[1].strip()
    elif " - " in title:
        fund_name = title.split(" - ", 1)[1].strip()
    else:
        fund_name = title

    combined = f"{fund_name} {title} {fname} {sheet_header}"

    for known_title, (code, name) in SCHEME_MAP.items():
        if known_title.lower() in combined.lower():
            return (code, name)

    # Fallback to generated code
    code = fund_name.upper()
    code = re.sub(r"^MIRAE\s+ASSET\s+", "", code)
    code = re.sub(r"\s+FUND$", "", code)
    code = re.sub(r"[^A-Z0-9_]", "_", code)
    code = re.sub(r"_+", "_", code).strip("_")
    return (f"MIRAE_{code}", fund_name or title)


class MiraeImporter(BaseFundImporter):
    """
    Mirae Asset Mutual Fund monthly portfolio importer.
    Queries AjaxService/GetDownloadsData and parses monthly Excel workbooks.
    """

    REQUEST_DELAY = 0.3

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
        return "Mirae Asset Mutual Fund"

    def table_name(self) -> str:
        return "market_data.mf_holdings"

    def column_names(self) -> list[str]:
        return _COLUMNS

    def watermark_source(self) -> str:
        return "mf_holdings"

    def fetch_sources(self) -> list[tuple[date, str, str, str]]:
        """
        Query Mirae Asset AjaxService API with pagination.
        Returns list of (as_of_date, scheme_title, filename, download_url).
        """
        sources: list[tuple[date, str, str, str]] = []
        headers = dict(COMMON_HEADERS)
        headers.update({
            "Content-Type": "application/json;charset=utf-8",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
        })

        pgno = 1
        pgsize = 100
        total_items = None

        with httpx.Client(headers=headers, timeout=_TIMEOUT, follow_redirects=True) as http:
            while True:
                payload = {
                    "request": {
                        "modulename": "portfolio_tab1",
                        "pgno": pgno,
                        "pgsize": pgsize,
                    }
                }
                try:
                    resp = http.post(API_URL, json=payload)
                    if resp.status_code != 200:
                        break

                    data = resp.json()
                    if data.get("ReturnCode") != "0":
                        break

                    if total_items is None:
                        total_items = int(data.get("DataCount", 0))

                    items = data.get("Data", [])
                    if not items:
                        break

                    reached_historical_limit = False
                    for item in items:
                        title = str(item.get("Title", "")).strip()
                        rel_url = str(item.get("URL", "")).strip()
                        pub_date_raw = str(item.get("PublishDate", "")).strip()

                        if not rel_url or not any(rel_url.lower().endswith(ext) for ext in [".xlsx", ".xls"]):
                            continue

                        as_of_date = _parse_disclosure_date(title, pub_date_raw)
                        if as_of_date is None:
                            continue

                        if as_of_date.year < self.from_year:
                            reached_historical_limit = True
                            continue

                        fname = rel_url.split("/")[-1].split("?")[0]
                        full_url = rel_url if rel_url.startswith("http") else f"{BASE_URL}{rel_url}"
                        sources.append((as_of_date, title, fname, full_url))

                    if reached_historical_limit and not self.full_reimport:
                        break

                    if len(items) < pgsize or (total_items and pgno * pgsize >= total_items):
                        break

                    pgno += 1

                except Exception as exc:
                    logger.debug("Error querying Mirae Asset API page %d: %s", pgno, exc)
                    break

        # Deduplicate sources by (date, download_url)
        by_key: dict[tuple[date, str], tuple[date, str, str, str]] = {}
        for as_of, title, fn, u in sources:
            key = (as_of, u)
            if key not in by_key:
                by_key[key] = (as_of, title, fn, u)

        deduped = sorted(by_key.values(), key=lambda x: (x[0], x[2]))
        self._console.print(f"[dim]Mirae Asset: discovered {len(deduped)} monthly portfolio source(s).[/dim]")
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
                "WHERE source = 'mf_holdings' AND symbol = 'MIRAE_ASSET_MONTHLY'"
            ).result_rows
            if rows and rows[0][0]:
                last_date = rows[0][0]
        except Exception as exc:
            logger.warning("Failed to query Mirae Asset watermark: %s", exc)

        if last_date is None:
            return sources

        filtered = [s for s in sources if s[0] > last_date]
        skipped = len(sources) - len(filtered)
        if skipped:
            self._console.print(
                f"[dim]Delta sync: {skipped} Mirae Asset file(s) already in DB (watermark {last_date}), "
                f"{len(filtered)} to fetch.[/dim]"
            )
        return filtered

    def parse_source(
        self, source: tuple[date, str, str, str], http: httpx.Client
    ) -> list[dict]:
        """
        Download and parse a Mirae Asset monthly Excel workbook.
        """
        as_of_date, scheme_title, fname, url = source
        headers = dict(COMMON_HEADERS)

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
            self._console.print(f"  [red]Cannot parse Mirae Asset workbook '{fname}' ({scheme_title})[/red]")
            return []

        all_holdings: list[dict] = []
        imported_at = datetime.now()

        for sheet in xl.sheet_names:
            try:
                df = xl.parse(sheet, header=None)
            except Exception as exc:
                logger.debug("Failed parsing sheet %s in %s: %s", sheet, fname, exc)
                continue

            if df.shape[0] < 5 or df.shape[1] < 4:
                continue

            # Extract scheme header from top rows if present
            sheet_hdr = ""
            for r in range(min(5, len(df))):
                row_txt = " ".join([str(x) for x in df.iloc[r].tolist() if pd.notna(x)])
                if "MIRAE" in row_txt.upper() or "FUND" in row_txt.upper():
                    sheet_hdr = row_txt
                    break

            scheme_code, fund_name = _normalise_fund_identity(scheme_title, fname, sheet_hdr)

            # Locate column header row dynamically
            header_row_idx: int | None = None
            for r in range(min(15, len(df))):
                r_vals = [str(x).strip().lower() for x in df.iloc[r].tolist() if pd.notna(x)]
                if any("isin" in x for x in r_vals) and any(
                    "instrument" in x or "name" in x or "issuer" in x or "security" in x for x in r_vals
                ):
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
                elif "market" in hl or "value" in hl or "lakh" in hl or "lacs" in hl:
                    if mv_col is None:
                        mv_col = c_idx
                elif "net assets" in hl or "%" in hl or "nav" in hl:
                    if pct_col is None and "ytm" not in hl and "yield" not in hl:
                        pct_col = c_idx

            # Standard layout fallback
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

            # Scaling: if all non-zero pct values are <= 1.0 (e.g. 0.0304 for 3.04%), scale by 100
            valid_pcts = [r[4] for r in sheet_raw_rows if r[4] == r[4] and r[4] > 0]
            pct_scale = 100.0 if (valid_pcts and all(p <= 1.0 for p in valid_pcts)) else 1.0

            sheet_holdings: list[dict] = []
            for isin_val, name_val, ind_val, mv_raw, pct_raw in sheet_raw_rows:
                pct_val = round(pct_raw * pct_scale, 4)
                if pct_val > 105.0:
                    logger.warning(
                        "Skipping anomalous row '%s' (%s, %s): pct_of_nav=%.2f%% > 105%%",
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
                break

        if self._latest_imported_date is None or as_of_date > self._latest_imported_date:
            self._latest_imported_date = as_of_date

        return all_holdings

    def watermark_rows(self, all_rows: list[dict]) -> list[tuple[str, date]]:
        if self._latest_imported_date:
            return [("MIRAE_ASSET_MONTHLY", self._latest_imported_date)]
        return []
