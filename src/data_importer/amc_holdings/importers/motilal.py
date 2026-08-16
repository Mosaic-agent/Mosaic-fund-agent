"""
src/data_importer/amc_holdings/importers/motilal.py
───────────────────────────────────────────────────
Motilal Oswal Mutual Fund portfolio holdings importer.

Discovers monthly portfolio disclosures via Motilal Oswal AEM search API:
    GET https://www.motilaloswalmf.com/content/aem-cloud-dept-backend-motilal-oswal/api/search-documents.json?type=mf

Each monthly disclosure is a consolidated Excel workbook covering ~87 schemes:
  - Sheet 'Index': lists scheme mapping (Sr No., Fund Name, Fund Code e.g. YO46)
  - Individual Sheets (e.g. YO46, YO07, YO08, YO01, YO03, etc.):
      Row 5: MONTHLY PORTFOLIO STATEMENT AS ON [MONTH] [DD], [YYYY]
      Row 7: Scheme Name
      Row 9: Header columns:
             Col 0: Sr. No.
             Col 1: Name of the Instrument
             Col 2: ISIN
             Col 3: Industry*
             Col 4: Quantity
             Col 5: Market/Fair Value (Rs. in Lakhs) -> scaled to Crores (/ 100.0)
             Col 6: % to Net Assets -> percentage
             (Cols 9-10 are sector summary tables and ignored)

Classification:
  - asset_type derived via classify_asset(security_name, industry)
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

from src.data_importer.amc_holdings.base import BaseFundImporter, classify_asset

logger = logging.getLogger(__name__)

_DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
}

_BASE_URL = "https://www.motilaloswalmf.com"
_DOCS_API_URL = "https://www.motilaloswalmf.com/content/aem-cloud-dept-backend-motilal-oswal/api/search-documents.json?type=mf"

_MONTH_LOOKUP = {
    "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
    "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6,
    "jul": 7, "july": 7, "aug": 8, "august": 8, "sep": 9, "september": 9,
    "oct": 10, "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12
}


def _parse_disclosure_date(title: str, url: str = "") -> date | None:
    """Extract month-end disclosure date from document title or URL path."""
    text = f"{title} {url}".strip()

    # 1. Match DD-MM-YYYY or DD.MM.YYYY
    m = re.search(r'(\d{1,2})[-_.](\d{1,2})[-_.](\d{4})', text)
    if m:
        d, mon, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 2010 <= y <= 2030 and 1 <= mon <= 12:
            last_day = calendar.monthrange(y, mon)[1]
            return date(y, mon, last_day)

    # 2. Match Month YYYY (e.g. "july 2026", "june-2025", "february-26")
    m = re.search(
        r'\b(january|february|march|april|may|june|july|august|september|october|november|december|'
        r'jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b[_\s-]*(\d{2,4})',
        text,
        re.IGNORECASE,
    )
    if m:
        mon_str = m.group(1).lower()
        yr_str = m.group(2)
        mon = _MONTH_LOOKUP.get(mon_str)
        y = int(yr_str)
        if y < 100:
            y += 2000
        if mon and 2010 <= y <= 2030:
            last_day = calendar.monthrange(y, mon)[1]
            return date(y, mon, last_day)

    return None


def _parse_sheet_date(text: str) -> date | None:
    """Extract disclosure date from sheet banner (e.g. 'MONTHLY PORTFOLIO STATEMENT AS ON JULY 31, 2026')."""
    if not text:
        return None
    m = re.search(r'(?:as on|as at|statement as on)\s*([a-zA-Z]+)\s+(\d{1,2}),?\s+(\d{4})', text, re.IGNORECASE)
    if m:
        mon_str, day_str, yr_str = m.group(1), m.group(2), m.group(3)
        try:
            return datetime.strptime(f"{mon_str[:3]} {day_str} {yr_str}", "%b %d %Y").date()
        except Exception:
            pass
    return None


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


def _clean_scheme_name(raw_name: str) -> str:
    """Normalize Motilal Oswal scheme name."""
    name = str(raw_name).split("\n")[0].strip()
    name = re.sub(r'\(Formerly known as.*?\)', '', name, flags=re.IGNORECASE).strip()
    name = re.sub(r'\s+', ' ', name).strip()
    if not name.lower().startswith("motilal oswal"):
        name = f"Motilal Oswal {name}"
    return name


def _clean_scheme_identity(sheet_code: str, raw_name: str) -> tuple[str, str]:
    """Derive canonical (scheme_code, fund_name) for a Motilal Oswal scheme."""
    cleaned_name = _clean_scheme_name(raw_name)
    slug = cleaned_name.upper().replace("MOTILAL OSWAL", "").strip()
    slug = re.sub(r'[^A-Z0-9]+', '_', slug).strip('_')
    scheme_code = f"MOTILAL_{slug}" if slug else f"MOTILAL_{sheet_code.upper()}"
    return scheme_code, cleaned_name


class MotilalOswalImporter(BaseFundImporter):
    """
    Motilal Oswal Mutual Fund portfolio holdings importer.

    Fetches month-end consolidated workbooks published under Motilal Oswal
    statutory downloads.
    """

    AMC_NAME = "Motilal Oswal Mutual Fund"

    def __init__(
        self,
        from_year: int = 2017,
        full_reimport: bool = False,
        target_month: date | None = None,
        timeout: float = 60.0,
    ) -> None:
        super().__init__()
        self.from_year = from_year
        self.full_reimport = full_reimport
        self._target_month = target_month
        self.timeout = timeout

    def fund_name(self) -> str:
        return self.AMC_NAME

    def table_name(self) -> str:
        return "market_data.mf_holdings"

    def column_names(self) -> list[str]:
        return _COLUMNS

    def watermark_source(self) -> str:
        return "mf_holdings"

    # ── Source Discovery ───────────────────────────────────────────────────────

    def fetch_sources(self) -> list[tuple[date, str, str, str]]:
        """
        Discover monthly portfolio files via Motilal Oswal AEM search API.

        Returns:
            list of (as_of_date, title, filename, download_url) sorted descending by date.
        """
        sources: list[tuple[date, str, str, str]] = []
        seen_urls: set[str] = set()

        try:
            with httpx.Client(headers=_DEFAULT_HEADERS, timeout=self.timeout, follow_redirects=True) as http:
                resp = http.get(_DOCS_API_URL)
                if resp.status_code != 200:
                    logger.error("Failed to fetch Motilal documents API: HTTP %s", resp.status_code)
                    return []

                results = resp.json().get("results", [])
                logger.info("Motilal Oswal API returned %d total document entries.", len(results))

                for r in results:
                    cat = str(r.get("category", "")).lower()
                    title = str(r.get("title", "")).strip()
                    path = str(r.get("path", "")).strip()

                    if not path or not any(path.lower().endswith(ext) for ext in [".xlsx", ".xls"]):
                        continue

                    # Filter out non-portfolio or interim files
                    t_low = title.lower()
                    p_low = path.lower()
                    if any(k in t_low or k in p_low for k in ["performance", "fortnightly", "half yearly", "riskometer", "unclaimed", "debt-money-market"]):
                        continue

                    if cat != "month end portfolio" and not (
                        "month-end-portfolio" in p_low and any(k in t_low for k in ["scheme portfolio", "monthly portfolio", "motilal portfolio", "factsheet"])
                    ):
                        continue

                    as_of = _parse_disclosure_date(title, path)
                    if not as_of:
                        continue

                    if as_of.year < self.from_year:
                        continue

                    if self._target_month and (as_of.year != self._target_month.year or as_of.month != self._target_month.month):
                        continue

                    download_url = f"{_BASE_URL}{path}" if path.startswith("/") else path
                    if download_url in seen_urls:
                        continue

                    seen_urls.add(download_url)
                    fname = path.split("/")[-1]
                    sources.append((as_of, title, fname, download_url))

        except Exception as e:
            logger.error("Error discovering Motilal Oswal portfolio sources: %s", e)

        # Sort descending by date
        sources.sort(key=lambda s: s[0], reverse=True)

        if not self.full_reimport and not self._target_month and sources:
            # By default delta-sync latest month disclosures
            latest_month = sources[0][0]
            sources = [s for s in sources if s[0] == latest_month]

        # Group by month and keep preferred consolidated file per month
        by_month: dict[date, list[tuple[date, str, str, str]]] = {}
        for s in sources:
            by_month.setdefault(s[0], []).append(s)

        filtered_sources: list[tuple[date, str, str, str]] = []
        for m_date, m_sources in by_month.items():
            # Prefer consolidated files over single-scheme files if multiple exist
            cons = [
                s for s in m_sources
                if any(k in s[1].lower() or k in s[2].lower() for k in ["monthly portfolio", "scheme portfolio details", "motilal portfolio", "factsheet"])
            ]
            filtered_sources.extend(cons[:1] if cons else m_sources[:1])

        sources = filtered_sources
        sources.sort(key=lambda s: s[0], reverse=True)

        logger.info("Found %d Motilal Oswal disclosure sources to process.", len(sources))
        return sources

    # ── Source Parsing ─────────────────────────────────────────────────────────

    def parse_source(
        self,
        source: tuple[date, str, str, str],
        http: httpx.Client | None = None,
    ) -> list[dict[str, Any]]:
        """
        Download and parse a Motilal Oswal monthly portfolio workbook into ClickHouse rows.
        """
        as_of_date, title, filename, url = source
        logger.info("Downloading Motilal Oswal portfolio: %s (%s)", title, url)

        try:
            if http is not None:
                resp = http.get(url)
                if resp.status_code != 200:
                    logger.error("Failed downloading %s: HTTP %s", url, resp.status_code)
                    return []
                content = resp.content
            else:
                with httpx.Client(headers=_DEFAULT_HEADERS, timeout=self.timeout, follow_redirects=True) as client:
                    resp = client.get(url)
                    if resp.status_code != 200:
                        logger.error("Failed downloading %s: HTTP %s", url, resp.status_code)
                        return []
                    content = resp.content

            engine = "openpyxl" if filename.lower().endswith((".xlsx", ".xlsm")) else "xlrd"
            excel = pd.ExcelFile(io.BytesIO(content), engine=engine)

            rows: list[dict[str, Any]] = []

            # 1. Check for Index sheet scheme code mapping
            scheme_code_map: dict[str, str] = {}
            if "Index" in excel.sheet_names:
                try:
                    idx_df = excel.parse("Index", header=None)
                    for _, row in idx_df.iterrows():
                        vals = [str(x).strip() for x in row.values if pd.notna(x)]
                        if len(vals) >= 2:
                            for i in range(len(vals) - 1):
                                code = vals[i + 1]
                                name = vals[i]
                                if re.match(r'^(?:YO|MO)\d+$', code, re.IGNORECASE):
                                    scheme_code_map[code.upper()] = name
                except Exception as e:
                    logger.debug("Failed parsing Index sheet: %s", e)

            # 2. Iterate through scheme sheets
            for sheet_name in excel.sheet_names:
                if sheet_name.lower() in ("index", "summary", "instructions", "disclaimer", "cover"):
                    continue

                try:
                    df = excel.parse(sheet_name, header=None)
                    if len(df) < 10:
                        continue

                    parsed_rows = self._parse_scheme_sheet(df, sheet_name, scheme_code_map, as_of_date)
                    rows.extend(parsed_rows)
                except Exception as e:
                    logger.warning("Error parsing sheet '%s' in %s: %s", sheet_name, filename, e)

            logger.info("  %s (%s): %d holdings parsed", title, as_of_date, len(rows))
            return rows

        except Exception as e:
            logger.error("Failed parsing Motilal Oswal source %s: %s", title, e, exc_info=True)
            return []

    # ── Sheet Parser ───────────────────────────────────────────────────────────

    def _parse_scheme_sheet(
        self,
        df: pd.DataFrame,
        sheet_name: str,
        scheme_code_map: dict[str, str],
        fallback_date: date,
    ) -> list[dict[str, Any]]:
        """Parse a single scheme sheet from the consolidated workbook."""
        # 1. Scheme name
        scheme_name = scheme_code_map.get(sheet_name.upper())
        if not scheme_name:
            for r_i in range(min(9, len(df))):
                row_str = " ".join([str(x).strip() for x in df.iloc[r_i].values if pd.notna(x)])
                if "motilal oswal" in row_str.lower():
                    scheme_name = df.iloc[r_i, 0] if pd.notna(df.iloc[r_i, 0]) else row_str
                    break
        if not scheme_name:
            scheme_name = f"Motilal Oswal {sheet_name}"

        scheme_code, scheme_name = _clean_scheme_identity(sheet_name, scheme_name)

        # 2. Disclosure date
        as_of_date = fallback_date
        for r_i in range(min(8, len(df))):
            for cell in df.iloc[r_i].values:
                if pd.notna(cell):
                    d = _parse_sheet_date(str(cell))
                    if d:
                        as_of_date = d
                        break
            if as_of_date != fallback_date:
                break

        # 3. Locate header row
        header_row = None
        for r_i in range(min(15, len(df))):
            row_vals = [str(x).strip().lower() for x in df.iloc[r_i].values if pd.notna(x)]
            if any("isin" in x for x in row_vals) and any("instrument" in x or "security" in x for x in row_vals):
                header_row = r_i
                break

        if header_row is None:
            # Fallback scan for ISIN header
            for r_i in range(min(15, len(df))):
                row_vals = [str(x).strip().lower() for x in df.iloc[r_i].values if pd.notna(x)]
                if any("isin" in x for x in row_vals):
                    header_row = r_i
                    break

        if header_row is None:
            return []

        h_df = df.iloc[header_row + 1:].copy()
        imported_at = datetime.utcnow()
        rows: list[dict[str, Any]] = []

        for _, row in h_df.iterrows():
            sec_name = str(row.iloc[1]).strip() if pd.notna(row.iloc[1]) else ""
            isin = str(row.iloc[2]).strip() if len(row) > 2 and pd.notna(row.iloc[2]) else ""
            industry = str(row.iloc[3]).strip() if len(row) > 3 and pd.notna(row.iloc[3]) else ""
            qty_raw = row.iloc[4] if len(row) > 4 else None
            mv_lakhs_raw = row.iloc[5] if len(row) > 5 else None
            pct_raw = row.iloc[6] if len(row) > 6 else None

            # Skip header, section labels, or footer totals
            if not sec_name or sec_name.lower() in (
                "total", "sub total", "sub-total", "grand total", "net assets", "nil", "nan", "none"
            ):
                continue
            if any(sec_name.lower().startswith(p) for p in [
                "(a)", "(b)", "(c)", "(d)", "(e)", "(f)",
                "listed /", "unlisted", "equity &", "debt instruments", "money market",
                "cash &", "mutual fund units", "commercial paper", "certificate of deposit",
                "treasury bill", "government bond", "alternative investment fund"
            ]):
                continue

            # Parse Market Value (given in Rs. in Lakhs -> scale to Crores)
            try:
                mv_val = float(str(mv_lakhs_raw).replace(",", "").strip())
                if pd.isna(mv_val):
                    mv_cr = 0.0
                else:
                    mv_cr = round(mv_val / 100.0, 4)
            except Exception:
                mv_cr = 0.0

            # Parse % of Net Assets
            try:
                pct_val = float(str(pct_raw).replace("%", "").replace(",", "").strip())
                if pd.isna(pct_val):
                    pct_of_nav = 0.0
                elif 0.0 < pct_val <= 1.0 and pct_val != 1.0:
                    pct_of_nav = round(pct_val * 100.0, 4)
                else:
                    pct_of_nav = round(pct_val, 4)
            except Exception:
                pct_of_nav = 0.0

            # Reject empty rows without financial values or valid ISIN
            if mv_cr <= 0 and pct_of_nav <= 0 and not isin:
                continue

            # Clean ISIN
            if isin.lower() in ("nan", "nil", "-", "none", "n.a.", "na") or len(isin) < 6:
                isin = ""

            asset_type = classify_asset(sec_name, industry)

            rows.append({
                "scheme_code": scheme_code,
                "fund_name": scheme_name,
                "as_of_month": as_of_date,
                "isin": isin,
                "security_name": sec_name,
                "asset_type": asset_type,
                "market_value_cr": mv_cr,
                "pct_of_nav": pct_of_nav,
                "imported_at": imported_at,
            })

        return rows
