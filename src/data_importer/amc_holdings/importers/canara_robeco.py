"""
src/data_importer/amc_holdings/importers/canara_robeco.py
──────────────────────────────────────────────────────────
Canara Robeco Mutual Fund (AMC) monthly portfolio holdings importer.

Discovers monthly portfolio disclosures across all active Canara Robeco schemes
(Small Cap, Flexi Cap, Large & Mid Cap, Mid Cap, Multi Cap, Value, Manufacturing,
Infrastructure, Corporate Bond, Income, Ultra Short Term, Overnight, etc.) by querying
the monthly portfolio disclosure portal:
https://www.canararobeco.com/documents/statutory-disclosures/scheme-dashboard/scheme-monthly-portfolio/
with year/month query filters, downloads monthly Excel workbooks, parses instrument
holdings, classifies asset types, converts values from Rs. Lacs to Crores, scales
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
from bs4 import BeautifulSoup

from src.data_importer.amc_holdings.base import (
    COMMON_HEADERS,
    BaseFundImporter,
    classify_asset,
)

logger = logging.getLogger(__name__)

DISCLOSURE_URL = (
    "https://www.canararobeco.com/documents/statutory-disclosures/scheme-dashboard/scheme-monthly-portfolio/"
)
_TIMEOUT = 30.0

_ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{10}$")

# Static / canonical scheme mapping by scheme name
SCHEME_MAP: dict[str, tuple[str, str]] = {
    # Equity
    "Canara Robeco Small Cap Fund": ("CANARA_SMALL_CAP", "Canara Robeco Small Cap Fund"),
    "Canara Robeco Flexi Cap Fund": ("CANARA_FLEXI_CAP", "Canara Robeco Flexi Cap Fund"),
    "Canara Robeco Large and Mid Cap Fund": ("CANARA_LARGE_AND_MID_CAP", "Canara Robeco Large and Mid Cap Fund"),
    "Canara Robeco Emerging Equities": ("CANARA_LARGE_AND_MID_CAP", "Canara Robeco Large and Mid Cap Fund"),
    "Canara Robeco Large Cap Fund": ("CANARA_LARGE_CAP", "Canara Robeco Large Cap Fund"),
    "Canara Robeco Bluechip Equity Fund": ("CANARA_LARGE_CAP", "Canara Robeco Large Cap Fund"),
    "Canara Robeco Mid Cap Fund": ("CANARA_MID_CAP", "Canara Robeco Mid Cap Fund"),
    "Canara Robeco Multi Cap Fund": ("CANARA_MULTICAP", "Canara Robeco Multi Cap Fund"),
    "Canara Robeco Focused Fund": ("CANARA_FOCUSED", "Canara Robeco Focused Fund"),
    "Canara Robeco Value Fund": ("CANARA_VALUE", "Canara Robeco Value Fund"),
    "Canara Robeco Infrastructure": ("CANARA_INFRASTRUCTURE", "Canara Robeco Infrastructure Fund"),
    "Canara Robeco Manufacturing Fund": ("CANARA_MANUFACTURING", "Canara Robeco Manufacturing Fund"),
    "Canara Robeco ELSS Tax Saver Fund": ("CANARA_ELSS_TAX_SAVER", "Canara Robeco ELSS Tax Saver Fund"),
    "Canara Robeco Banking and Financial Services Fund": ("CANARA_BANKING_AND_FINANCIAL_SERVICES", "Canara Robeco Banking and Financial Services Fund"),
    "Canara Robeco Consumption Fund": ("CANARA_CONSUMPTION", "Canara Robeco Consumption Fund"),

    # Hybrid
    "Canara Robeco Multi Asset Allocation Fund": ("CANARA_MULTI_ASSET_ALLOCATION", "Canara Robeco Multi Asset Allocation Fund"),
    "Canara Robeco Balanced Advantage Fund": ("CANARA_BALANCED_ADVANTAGE", "Canara Robeco Balanced Advantage Fund"),
    "Canara Robeco Equity Hybrid Fund": ("CANARA_EQUITY_HYBRID", "Canara Robeco Equity Hybrid Fund"),
    "Canara Robeco Conservative Hybrid Fund": ("CANARA_CONSERVATIVE_HYBRID", "Canara Robeco Conservative Hybrid Fund"),

    # Debt / Cash
    "Canara Robeco Income Fund": ("CANARA_INCOME", "Canara Robeco Income Fund"),
    "Canara Robeco Corporate Bond Fund": ("CANARA_CORPORATE_BOND", "Canara Robeco Corporate Bond Fund"),
    "Canara Robeco Banking and PSU Debt Fund": ("CANARA_BANKING_AND_PSU", "Canara Robeco Banking and PSU Debt Fund"),
    "Canara Robeco Dynamic Bond Fund": ("CANARA_DYNAMIC_BOND", "Canara Robeco Dynamic Bond Fund"),
    "Canara Robeco Gilt Fund": ("CANARA_GILT", "Canara Robeco Gilt Fund"),
    "Canara Robeco Liquid Fund": ("CANARA_LIQUID", "Canara Robeco Liquid Fund"),
    "Canara Robeco Overnight Fund": ("CANARA_OVERNIGHT", "Canara Robeco Overnight Fund"),
    "Canara Robeco Savings Fund": ("CANARA_SAVINGS", "Canara Robeco Savings Fund"),
    "Canara Robeco Short Duration Fund": ("CANARA_SHORT_DURATION", "Canara Robeco Short Duration Fund"),
    "Canara Robeco Ultra Short Term Fund": ("CANARA_ULTRA_SHORT_TERM", "Canara Robeco Ultra Short Term Fund"),
}

PREFIX_MAP: dict[str, tuple[str, str]] = {
    "SC": ("CANARA_SMALL_CAP", "Canara Robeco Small Cap Fund"),
    "VF": ("CANARA_VALUE", "Canara Robeco Value Fund"),
    "MF": ("CANARA_MULTICAP", "Canara Robeco Multi Cap Fund"),
    "MD": ("CANARA_MID_CAP", "Canara Robeco Mid Cap Fund"),
    "MN": ("CANARA_MANUFACTURING", "Canara Robeco Manufacturing Fund"),
    "MI": ("CANARA_CONSERVATIVE_HYBRID", "Canara Robeco Conservative Hybrid Fund"),
    "IF": ("CANARA_INCOME", "Canara Robeco Income Fund"),
    "TA": ("CANARA_ULTRA_SHORT_TERM", "Canara Robeco Ultra Short Term Fund"),
    "OF": ("CANARA_OVERNIGHT", "Canara Robeco Overnight Fund"),
    "MO": ("CANARA_CORPORATE_BOND", "Canara Robeco Corporate Bond Fund"),
    "IN": ("CANARA_INFRASTRUCTURE", "Canara Robeco Infrastructure Fund"),
    "CRCHF": ("CANARA_CONSERVATIVE_HYBRID", "Canara Robeco Conservative Hybrid Fund"),
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


def _normalise_fund_identity(title: str, fname: str = "", sheet_header: str = "") -> tuple[str, str]:
    """Resolve (scheme_code, fund_name) from Canara Robeco title, filename, or sheet header."""
    title_clean = re.sub(r"[–—]", "-", title or "").strip()
    fname_clean = re.sub(r"[–—]", "-", fname or "").strip()
    hdr_clean = re.sub(r"[–—]", "-", sheet_header or "").strip()

    combined = f"{title_clean} {fname_clean} {hdr_clean}"

    for known_title, (code, name) in SCHEME_MAP.items():
        if known_title.lower() in combined.lower():
            return (code, name)

    for pfx, (code, name) in PREFIX_MAP.items():
        if (
            re.search(rf"\b{pfx}\b", title_clean, re.IGNORECASE)
            or title_clean.upper().startswith(f"{pfx}-")
            or title_clean.upper().startswith(f"{pfx} ")
            or fname_clean.upper().startswith(f"{pfx}-")
            or f"_{pfx}_" in fname_clean.upper()
            or f"-{pfx}-" in fname_clean.upper()
        ):
            return (code, name)

    # Fallback to generated code
    code = title_clean.upper()
    code = re.sub(r"^CANARA\s+ROBECO\s+", "", code)
    code = re.sub(r"\s+FUND$", "", code)
    code = re.sub(r"[^A-Z0-9_]", "_", code)
    code = re.sub(r"_+", "_", code).strip("_")
    return (f"CANARA_{code}", title_clean or fname_clean)


class CanaraRobecoImporter(BaseFundImporter):
    """
    Canara Robeco Mutual Fund monthly portfolio importer.
    Discovers disclosures via the statutory disclosure portal and parses Excel workbooks.
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
        return "Canara Robeco Mutual Fund"

    def table_name(self) -> str:
        return "market_data.mf_holdings"

    def column_names(self) -> list[str]:
        return _COLUMNS

    def watermark_source(self) -> str:
        return "mf_holdings"

    def fetch_sources(self) -> list[tuple[date, str, str, str]]:
        """
        Query Canara Robeco statutory disclosures portal with year & month filters.
        Returns list of (as_of_date, scheme_title, filename, download_url).
        """
        sources: list[tuple[date, str, str, str]] = []
        headers = dict(COMMON_HEADERS)
        headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"

        current_year = datetime.now().year
        current_month = datetime.now().month
        years = list(range(self.from_year, current_year + 1))

        with httpx.Client(headers=headers, timeout=_TIMEOUT, follow_redirects=True) as http:
            for yr in years:
                max_m = current_month if yr == current_year else 12
                for m in range(1, max_m + 1):
                    m_str = f"{m:02d}"
                    url = f"{DISCLOSURE_URL}?filteryear={yr}&filtermonth={m_str}"
                    try:
                        resp = http.get(url)
                        if resp.status_code != 200:
                            continue

                        soup = BeautifulSoup(resp.text, "html.parser")
                        for a in soup.find_all("a", href=True):
                            href = a["href"].strip()
                            if not any(
                                href.lower().endswith(ext) or f"{ext}?" in href.lower()
                                for ext in [".xlsx", ".xls"]
                            ):
                                continue

                            text = a.get_text(" ", strip=True)
                            if text.lower() == "download" or not text:
                                parent = a.find_parent(["div", "tr", "li", "p"])
                                title = parent.get_text(" ", strip=True) if parent else href.split("/")[-1]
                                title = re.sub(r"\bdownload\b", "", title, flags=re.IGNORECASE).strip()
                            else:
                                title = text

                            fname = href.split("/")[-1].split("?")[0]
                            _, last_day = calendar.monthrange(yr, m)
                            as_of_date = date(yr, m, last_day)
                            sources.append((as_of_date, title, fname, href))

                    except Exception as exc:
                        logger.debug("Failed fetching Canara Robeco sources for %s-%s: %s", yr, m_str, exc)

        # Deduplicate sources by (date, download_url), preferring non-Download title
        by_key: dict[tuple[date, str], tuple[date, str, str, str]] = {}
        for as_of, title, fn, u in sources:
            key = (as_of, u)
            if key not in by_key or (by_key[key][1].lower() == "download" and title.lower() != "download"):
                by_key[key] = (as_of, title, fn, u)

        deduped = sorted(by_key.values(), key=lambda x: (x[0], x[2]))
        self._console.print(f"[dim]Canara Robeco: discovered {len(deduped)} monthly portfolio source(s).[/dim]")
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
                "WHERE source = 'mf_holdings' AND symbol = 'CANARA_ROBECO_MONTHLY'"
            ).result_rows
            if rows and rows[0][0]:
                last_date = rows[0][0]
        except Exception as exc:
            logger.warning("Failed to query Canara Robeco watermark: %s", exc)

        if last_date is None:
            return sources

        filtered = [s for s in sources if s[0] > last_date]
        skipped = len(sources) - len(filtered)
        if skipped:
            self._console.print(
                f"[dim]Delta sync: {skipped} Canara Robeco file(s) already in DB (watermark {last_date}), "
                f"{len(filtered)} to fetch.[/dim]"
            )
        return filtered

    def parse_source(
        self, source: tuple[date, str, str, str], http: httpx.Client
    ) -> list[dict]:
        """
        Download and parse a Canara Robeco monthly Excel workbook.
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
            self._console.print(f"  [red]Cannot parse Canara Robeco workbook '{fname}' ({scheme_title})[/red]")
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

            # Extract possible scheme header from top rows
            sheet_hdr = ""
            for r in range(min(4, len(df))):
                row_txt = " ".join([str(x) for x in df.iloc[r].tolist() if pd.notna(x)])
                if "CANARA" in row_txt.upper() or "FUND" in row_txt.upper():
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
                elif "market value" in hl or "market/fair" in hl or "market" in hl or "lacs" in hl:
                    if mv_col is None:
                        mv_col = c_idx
                elif "aum" in hl or "net assets" in hl or "nav" in hl or "%" in hl:
                    if pct_col is None and "yield" not in hl and "ytm" not in hl and "ytc" not in hl:
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

            # Check percentage scaling: if all individual values are <= 1.0 (e.g. 0.0249 for 2.49%), scale by 100
            valid_pcts = [r[4] for r in sheet_raw_rows if r[4] == r[4] and r[4] > 0]
            pct_scale = 100.0 if (valid_pcts and all(p <= 1.0 for p in valid_pcts)) else 1.0

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
                    "market_value_cr": round(mv_raw / 100.0, 4),  # Rs Lacs -> Rs Cr
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
            return [("CANARA_ROBECO_MONTHLY", self._latest_imported_date)]
        return []
