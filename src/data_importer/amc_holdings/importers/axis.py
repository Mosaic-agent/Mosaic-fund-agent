"""
src/data_importer/amc_holdings/importers/axis.py
─────────────────────────────────────────────────
Axis Mutual Fund portfolio holdings importer.

Discovers monthly portfolio disclosures via Axis MF CMS service API:
    POST https://www.axismf.com/cms/token
    POST https://www.axismf.com/cms/get-scheme-documents
Payload:
    {"sdType": "yearMonthSchemeDocs", "sdID": "sdMonthSchemePortfolio"}

Each monthly disclosure is an Excel workbook (multi-sheet consolidated file
covering all 80+ schemes, or single-scheme workbooks):
  - Row 0: [Acronym, Fund Name, ...]
  - Row 2: [..., 'Monthly Portfolio Statement as on July 31, 2026', ...]
  - Row 3: Header columns (Name of the Instrument, ISIN, Industry, Quantity, Market Value in Lakhs, % of Net Assets)
  - Columns:
      Col 1: security_name
      Col 2: isin
      Col 3: industry / rating
      Col 4: quantity
      Col 5: market_value (Rs. in Lakhs -> scaled to Crores via / 100.0)
      Col 6: pct_of_nav (decimal fraction e.g. 0.057 -> scaled to 5.70%)

Classification:
  - asset_type derived via classify_asset(security_name, isin, industry)
"""

from __future__ import annotations

import io
import logging
import re
import urllib.parse
from datetime import date, datetime
from typing import Any

import httpx
import pandas as pd

from src.data_importer.amc_holdings.base import BaseFundImporter, classify_asset

logger = logging.getLogger(__name__)

_DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json",
}

_CMS_TOKEN_URL = "https://www.axismf.com/cms/token"
_CMS_SCHEME_DOCS_URL = "https://www.axismf.com/cms/get-scheme-documents"

# ── Scheme Mapping ────────────────────────────────────────────────────────────

SCHEME_MAP: dict[str, tuple[str, str]] = {
    "Axis Small Cap Fund": ("AXIS_SMALL_CAP", "Axis Small Cap Fund"),
    "Axis Midcap Fund": ("AXIS_MID_CAP", "Axis Mid Cap Fund"),
    "Axis Mid Cap Fund": ("AXIS_MID_CAP", "Axis Mid Cap Fund"),
    "Axis Multi Asset Allocation Fund": ("AXIS_MULTI_ASSET_ALLOCATION", "Axis Multi Asset Allocation Fund"),
    "Axis Multi-Asset Active FoF": ("AXIS_MULTI_ASSET_ACTIVE_FOF", "Axis Multi-Asset Active FoF"),
    "Axis Flexi Cap Fund": ("AXIS_FLEXI_CAP", "Axis Flexi Cap Fund"),
    "Axis Large Cap Fund": ("AXIS_LARGE_CAP", "Axis Large Cap Fund"),
    "Axis Bluechip Fund": ("AXIS_LARGE_CAP", "Axis Bluechip Fund"),
    "Axis Large & Mid Cap Fund": ("AXIS_LARGE_AND_MID_CAP", "Axis Large & Mid Cap Fund"),
    "Axis Large & Mid cap Fund": ("AXIS_LARGE_AND_MID_CAP", "Axis Large & Mid Cap Fund"),
    "Axis Focused Fund": ("AXIS_FOCUSED", "Axis Focused Fund"),
    "Axis Focused 25 Fund": ("AXIS_FOCUSED", "Axis Focused Fund"),
    "Axis ELSS Tax Saver Fund": ("AXIS_ELSS_TAX_SAVER", "Axis ELSS Tax Saver Fund"),
    "Axis Long Term Equity Fund": ("AXIS_ELSS_TAX_SAVER", "Axis Long Term Equity Fund"),
    "Axis Balanced Advantage Fund": ("AXIS_BALANCED_ADVANTAGE", "Axis Balanced Advantage Fund"),
    "Axis Aggressive Hybrid Fund": ("AXIS_AGGRESSIVE_HYBRID", "Axis Aggressive Hybrid Fund"),
    "Axis Equity Savings Fund": ("AXIS_EQUITY_SAVINGS", "Axis Equity Savings Fund"),
    "Axis Quant Fund": ("AXIS_QUANT", "Axis Quant Fund"),
    "Axis Value Fund": ("AXIS_VALUE", "Axis Value Fund"),
    "Axis Multicap Fund": ("AXIS_MULTICAP", "Axis Multicap Fund"),
    "Axis India Manufacturing Fund": ("AXIS_INDIA_MANUFACTURING", "Axis India Manufacturing Fund"),
    "Axis Consumption Fund": ("AXIS_CONSUMPTION", "Axis Consumption Fund"),
    "AXIS CONSUMPTION FUND": ("AXIS_CONSUMPTION", "Axis Consumption Fund"),
    "Axis Innovation Fund": ("AXIS_INNOVATION", "Axis Innovation Fund"),
    "Axis Services Opportunities Fund": ("AXIS_SERVICES_OPPORTUNITIES", "Axis Services Opportunities Fund"),
    "Axis Business Cycles Fund": ("AXIS_BUSINESS_CYCLES", "Axis Business Cycles Fund"),
    "Axis ESG Integration Strategy Fund": ("AXIS_ESG_INTEGRATION", "Axis ESG Integration Strategy Fund"),
    "Axis Special Situations Fund": ("AXIS_SPECIAL_SITUATIONS", "Axis Special Situations Fund"),
    "Axis Gold ETF": ("AXIS_GOLD_ETF", "Axis Gold ETF"),
    "Axis Gold Fund": ("AXIS_GOLD_FUND", "Axis Gold Fund"),
    "Axis Gold and Silver Passive FoF": ("AXIS_GOLD_AND_SILVER_PASSIVE_FOF", "Axis Gold and Silver Passive FoF"),
    "Axis Silver ETF": ("AXIS_SILVER_ETF", "Axis Silver ETF"),
    "Axis Silver Fund of Fund": ("AXIS_SILVER_FOF", "Axis Silver Fund of Fund"),
    "AXIS SILVER FUND OF FUND": ("AXIS_SILVER_FOF", "Axis Silver Fund of Fund"),
    "Axis NIFTY 50 ETF": ("AXIS_NIFTY_50_ETF", "Axis NIFTY 50 ETF"),
    "Axis NIFTY Bank ETF": ("AXIS_NIFTY_BANK_ETF", "Axis NIFTY Bank ETF"),
    "Axis NIFTY IT ETF": ("AXIS_NIFTY_IT_ETF", "Axis NIFTY IT ETF"),
    "Axis NIFTY Healthcare ETF": ("AXIS_NIFTY_HEALTHCARE_ETF", "Axis NIFTY Healthcare ETF"),
    "Axis Nifty 50 Index Fund": ("AXIS_NIFTY_50_INDEX", "Axis Nifty 50 Index Fund"),
    "Axis Nifty 500 Index Fund": ("AXIS_NIFTY_500_INDEX", "Axis Nifty 500 Index Fund"),
    "Axis Nifty 100 Index Fund": ("AXIS_NIFTY_100_INDEX", "Axis Nifty 100 Index Fund"),
    "Axis Nifty Next 50 Index Fund": ("AXIS_NIFTY_NEXT_50_INDEX", "Axis Nifty Next 50 Index Fund"),
    "Axis Nifty Smallcap 50 Index Fund": ("AXIS_NIFTY_SMALLCAP_50_INDEX", "Axis Nifty Smallcap 50 Index Fund"),
    "AXIS NIFTY SMALLCAP 50 INDEX FUND": ("AXIS_NIFTY_SMALLCAP_50_INDEX", "Axis Nifty Smallcap 50 Index Fund"),
    "Axis Nifty Midcap 50 Index Fund": ("AXIS_NIFTY_MIDCAP_50_INDEX", "Axis Nifty Midcap 50 Index Fund"),
    "AXIS NIFTY MIDCAP 50 INDEX FUND": ("AXIS_NIFTY_MIDCAP_50_INDEX", "Axis Nifty Midcap 50 Index Fund"),
    "Axis Arbitrage Fund": ("AXIS_ARBITRAGE", "Axis Arbitrage Fund"),
    "Axis Liquid Fund": ("AXIS_LIQUID", "Axis Liquid Fund"),
    "Axis Treasury Advantage Fund": ("AXIS_TREASURY_ADVANTAGE", "Axis Treasury Advantage Fund"),
    "Axis Corporate Bond Fund": ("AXIS_CORPORATE_BOND", "Axis Corporate Bond Fund"),
    "Axis Banking & PSU Debt Fund": ("AXIS_BANKING_AND_PSU_DEBT", "Axis Banking & PSU Debt Fund"),
    "Axis Short Duration Fund": ("AXIS_SHORT_DURATION", "Axis Short Duration Fund"),
    "Axis Ultra Short Duration Fund": ("AXIS_ULTRA_SHORT_DURATION", "Axis Ultra Short Duration Fund"),
    "Axis Money Market Fund": ("AXIS_MONEY_MARKET", "Axis Money Market Fund"),
    "Axis Dynamic Bond Fund": ("AXIS_DYNAMIC_BOND", "Axis Dynamic Bond Fund"),
    "Axis Strategic Bond Fund": ("AXIS_STRATEGIC_BOND", "Axis Strategic Bond Fund"),
    "Axis Children’s Fund": ("AXIS_CHILDRENS_FUND", "Axis Children's Fund"),
    "Axis Children's Fund": ("AXIS_CHILDRENS_FUND", "Axis Children's Fund"),
    "Axis Retirement Fund - Aggressive Plan": ("AXIS_RETIREMENT_AGGRESSIVE", "Axis Retirement Fund - Aggressive Plan"),
    "Axis Retirement Fund - Dynamic Plan": ("AXIS_RETIREMENT_DYNAMIC", "Axis Retirement Fund - Dynamic Plan"),
    "Axis Retirement Fund - Conservative Plan": ("AXIS_RETIREMENT_CONSERVATIVE", "Axis Retirement Fund - Conservative Plan"),
}

MONTH_MAP: dict[str, int] = {
    "jan": 1, "january": 1,
    "feb": 2, "february": 2,
    "mar": 3, "march": 3,
    "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,
    "jul": 7, "july": 7,
    "aug": 8, "august": 8,
    "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}


def _last_day_of_month(year: int, month: int) -> date:
    if month == 12:
        return date(year, 12, 31)
    import calendar
    _, last_day = calendar.monthrange(year, month)
    return date(year, month, last_day)


def _parse_disclosure_date(text: str) -> date | None:
    """
    Extract disclosure month-end date from title, e.g.:
      'Monthly Portfolio Statement as on July 31, 2026'
      'Monthly Portfolio-31 07 26'
      'Monthly Portfolio - Axis Small Cap Fund - 31 July 2026'
      'Monthly_Portfolio_31072026_8a12978eff.xlsx'
      'monthly_20portfolio-31_2003_2026.xlsx'
    """
    if not text:
        return None

    cleaned = text.replace("_2520", " ").replace("%2520", " ").replace("_20", " ").replace("%20", " ")
    cleaned = urllib.parse.unquote(cleaned).replace("_", " ")
    cleaned = re.sub(r"[–—]", "-", cleaned)

    parts = [p.strip() for p in cleaned.split("-") if p.strip()]
    candidates = []
    if len(parts) > 1:
        candidates.append(parts[-1])
        if len(parts) > 2:
            candidates.append(parts[-2])
    candidates.append(cleaned)

    for cand in candidates:
        # 1. Pattern: 31 July 2026 or 31May2026 or 31st July 2026
        m = re.search(r"(\d{1,2})(?:st|nd|rd|th)?\s*([A-Za-z]+)[,\s]*(20\d{2})", cand)
        if m:
            day, mon_str, year = int(m.group(1)), m.group(2).lower(), int(m.group(3))
            if 2010 <= year <= 2027:
                mon = MONTH_MAP.get(mon_str)
                if mon:
                    return _last_day_of_month(year, mon)

        # 2. Pattern: July 31, 2026 or July 31 2026
        m = re.search(r"([A-Za-z]+)\s*(\d{1,2})[,\s]*(20\d{2})", cand)
        if m:
            mon_str, day, year = m.group(1).lower(), int(m.group(2)), int(m.group(3))
            if 2010 <= year <= 2027:
                mon = MONTH_MAP.get(mon_str)
                if mon:
                    return _last_day_of_month(year, mon)

        # 3. Pattern: 31 July 26 or 31May26 or 31st July 26
        m = re.search(r"(\d{1,2})(?:st|nd|rd|th)?\s*([A-Za-z]+)[,\s]*(\d{2})(?:\D|$)", cand)
        if m:
            day, mon_str, yr_short = int(m.group(1)), m.group(2).lower(), int(m.group(3))
            year = yr_short + 2000
            if 2010 <= year <= 2027:
                mon = MONTH_MAP.get(mon_str)
                if mon:
                    return _last_day_of_month(year, mon)

        # 4. Pattern: DD MM YYYY (e.g. 31 07 2026 or 31072026)
        m = re.search(r"(?:^|\D)(?:31|30|28|29)\s*(0[1-9]|1[0-2])\s*(20\d{2})(?:\D|$)", cand)
        if m:
            mon = int(m.group(1))
            year = int(m.group(2))
            if 2010 <= year <= 2027:
                return _last_day_of_month(year, mon)

        # 5. Pattern: DD MM YY (e.g. 31 07 26 or 31_07_26)
        m = re.search(r"(?:^|\D)(?:31|30|28|29)\s*(0[1-9]|1[0-2])\s*(\d{2})(?:\D|$)", cand)
        if m:
            mon = int(m.group(1))
            year = int(m.group(2)) + 2000
            if 2010 <= year <= 2027:
                return _last_day_of_month(year, mon)

        # 6. Pattern: [Month] [20XX] (e.g. July 2026 or Jul 2026)
        m = re.search(r"([A-Za-z]+)[,\s-]+(20\d{2})", cand)
        if m:
            mon_str, year = m.group(1).lower(), int(m.group(2))
            if 2010 <= year <= 2027:
                mon = MONTH_MAP.get(mon_str)
                if mon:
                    return _last_day_of_month(year, mon)

    return None


def _normalise_fund_identity(title: str, default_sheet: str = "") -> tuple[str, str]:
    """Normalize fund name to canonical (scheme_code, fund_name)."""
    t = re.sub(r"\s+", " ", title).strip()
    if not t and default_sheet:
        t = default_sheet

    # Direct map
    for k, (code, name) in SCHEME_MAP.items():
        if k.lower() == t.lower():
            return code, name

    # Substring match
    for k, (code, name) in SCHEME_MAP.items():
        if k.lower() in t.lower() or t.lower() in k.lower():
            return code, name

    # Fallback clean
    clean = re.sub(r"[^A-Za-z0-9]+", "_", t).strip("_").upper()
    if not clean.startswith("AXIS"):
        clean = "AXIS_" + clean
    return clean, t


# ── Importer Class ────────────────────────────────────────────────────────────

class AxisImporter(BaseFundImporter):
    """
    Axis Mutual Fund monthly portfolio holdings importer.
    Discovers Excel files from Axis MF CMS API and parses both consolidated
    workbooks and single-scheme disclosures.
    """

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
        return "Axis Mutual Fund"

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

    def watermark_rows(self, rows: list[dict[str, Any]]) -> list[tuple[str, date]]:
        """Return distinct (watermark_id, as_of_month) pairs for imported rows."""
        seen: set[tuple[str, date]] = set()
        for r in rows:
            m = r.get("as_of_month")
            if isinstance(m, date):
                seen.add(("AXIS_MF_MONTHLY", m))
        return list(seen)

    def fetch_sources(self) -> list[tuple[date, str, str, str]]:
        """
        Query Axis MF CMS API to discover monthly portfolio disclosures.
        Returns: list of (as_of_month, doc_title, filename, download_url).
        """
        sources: list[tuple[date, str, str, str]] = []
        try:
            with httpx.Client(headers=_DEFAULT_HEADERS, timeout=45.0, follow_redirects=True) as http:
                # 1. Fetch Bearer token
                tok_resp = http.post(_CMS_TOKEN_URL, json={})
                tok_data = tok_resp.json()
                token = tok_data.get("data", {}).get("token") or tok_data.get("token")
                if not token:
                    logger.error("Failed to obtain CMS token from Axis MF.")
                    return []

                h = dict(_DEFAULT_HEADERS, Authorization=token)

                # 2. Fetch document list for sdMonthSchemePortfolio
                docs_resp = http.post(
                    _CMS_SCHEME_DOCS_URL,
                    headers=h,
                    json={
                        "sdType": "yearMonthSchemeDocs",
                        "sdID": "sdMonthSchemePortfolio",
                    },
                )
                if docs_resp.status_code != 200:
                    logger.error("Failed to fetch Axis documents: HTTP %d", docs_resp.status_code)
                    return []

                docs_data = docs_resp.json().get("data", {})
                doc_list = docs_data.get("documentList", [])
                logger.info("Axis CMS returned %d total document entries.", len(doc_list))

                seen_urls: set[str] = set()

                for d in doc_list:
                    url = d.get("docuementURL") or ""
                    title = d.get("documentName") or ""
                    if not url or url in seen_urls:
                        continue

                    # Ensure only monthly portfolio files are selected
                    t_low = title.lower()
                    u_low = url.lower()
                    if not ("monthly" in t_low and "portfolio" in t_low) and not ("monthly" in u_low and "portfolio" in u_low):
                        continue

                    # Skip weekly/fortnightly/adhoc/daily files
                    if any(k in t_low or k in u_low for k in ["weekly", "fortnightly", "adhoc", "daily", "debt quants", "debt_quants", "quants"]):
                        continue

                    as_of = _parse_disclosure_date(title) or _parse_disclosure_date(url)
                    if not as_of:
                        continue

                    if as_of.year < self.from_year:
                        continue

                    if self._target_month and (as_of.year != self._target_month.year or as_of.month != self._target_month.month):
                        continue

                    seen_urls.add(url)
                    fname = url.split("/")[-1]
                    sources.append((as_of, title, fname, url))

        except Exception as e:
            logger.error("Error discovering Axis portfolio sources: %s", e)

        # Sort descending by date
        sources.sort(key=lambda s: s[0], reverse=True)

        if not self.full_reimport and not self._target_month and sources:
            # By default delta-sync latest month disclosures (consolidated or all schemes for latest month)
            latest_month = sources[0][0]
            sources = [s for s in sources if s[0] == latest_month]

        # Deduplicate per month: prefer consolidated workbook if present
        by_month: dict[date, list[tuple[date, str, str, str]]] = {}
        for s in sources:
            by_month.setdefault(s[0], []).append(s)

        filtered_sources: list[tuple[date, str, str, str]] = []
        for m_date, m_sources in by_month.items():
            cons = [
                s for s in m_sources
                if re.search(r"monthly[_\s-]?portfolio[_\s-]?\d+", s[1], re.IGNORECASE)
                or re.search(r"monthly[_\s-]?portfolio[_\s-]?\d+", s[2], re.IGNORECASE)
            ]
            if cons:
                filtered_sources.extend(cons)
            else:
                filtered_sources.extend(m_sources)

        sources = filtered_sources
        sources.sort(key=lambda s: s[0], reverse=True)

        logger.info("Found %d Axis MF disclosure sources to process.", len(sources))
        return sources

    def parse_source(
        self,
        source: tuple[date, str, str, str],
        http: httpx.Client,
    ) -> list[dict[str, Any]]:
        """
        Download and parse an Axis MF Excel workbook (multi-sheet or single-sheet).
        """
        as_of_date, title, filename, url = source
        logger.info("Downloading Axis portfolio: %s (%s)", title, url)

        try:
            resp = http.get(url, timeout=120.0, follow_redirects=True)
            if resp.status_code != 200 or len(resp.content) < 500:
                logger.warning("Failed download for %s: HTTP %d (%d bytes)", url, resp.status_code, len(resp.content))
                return []
            content = resp.content
        except Exception as e:
            logger.error("Download error for %s: %s", url, e)
            return []

        # Parse Excel workbook with openpyxl or xlrd
        excel_file = io.BytesIO(content)
        try:
            excel = pd.ExcelFile(excel_file, engine="openpyxl")
        except Exception:
            excel_file.seek(0)
            try:
                excel = pd.ExcelFile(excel_file, engine="xlrd")
            except Exception as e:
                logger.error("Failed to read Excel workbook for %s: %s", filename, e)
                return []

        imported_at = datetime.utcnow()
        all_rows: list[dict[str, Any]] = []

        for sheet_name in excel.sheet_names:
            if sheet_name.lower() in ["index", "summary", "instructions", "sheet1"]:
                if len(excel.sheet_names) > 1 and sheet_name.lower() in ["index", "summary", "instructions"]:
                    continue

            try:
                df = excel.parse(sheet_name, header=None)
            except Exception as e:
                logger.debug("Failed parsing sheet %s: %s", sheet_name, e)
                continue

            if df.empty or df.shape[0] < 2 or df.shape[1] < 4:
                continue

            # Identify fund title and date
            fund_raw_name = str(df.iloc[0, 1]).strip() if pd.notna(df.iloc[0, 1]) else sheet_name
            if fund_raw_name.lower() in ["nan", "none", ""]:
                fund_raw_name = title or sheet_name

            # Check if row 2 has date text
            sheet_date = as_of_date
            if df.shape[0] > 2 and pd.notna(df.iloc[2, 1]):
                extracted_date = _parse_disclosure_date(str(df.iloc[2, 1]))
                if extracted_date:
                    sheet_date = extracted_date

            scheme_code, canonical_fund_name = _normalise_fund_identity(fund_raw_name, default_sheet=sheet_name)

            # Find header row
            header_row_idx = 3
            name_col = 1
            isin_col = 2
            ind_col = 3
            mv_col = 5
            pct_col = 6
            is_debt_format = False

            for r_i in range(min(12, len(df))):
                row_vals = [str(x).strip().lower() for x in df.iloc[r_i] if pd.notna(x)]
                row_str = " ".join(row_vals)

                if "scheme code" in row_str and "security name" in row_str and df.shape[1] >= 10:
                    # 22-column debt format (e.g. AXISTAA_12_Aug2026.xls)
                    is_debt_format = True
                    header_row_idx = r_i
                    name_col = 3
                    isin_col = 2
                    ind_col = 14
                    mv_col = 6  # Market Value in Rs
                    pct_col = 7
                    if r_i + 1 < len(df) and pd.notna(df.iloc[r_i + 1, 1]):
                        debt_scheme_name = str(df.iloc[r_i + 1, 1]).strip()
                        if debt_scheme_name and debt_scheme_name.lower() not in ["nan", "none"]:
                            scheme_code, canonical_fund_name = _normalise_fund_identity(debt_scheme_name, default_sheet=sheet_name)
                    break
                elif "name of the instrument" in row_str or "security name" in row_str:
                    header_row_idx = r_i
                    # Match exact columns
                    for c_i, c_val in enumerate(df.iloc[r_i]):
                        c_str = str(c_val).lower() if pd.notna(c_val) else ""
                        if "name of the instrument" in c_str or "security name" in c_str:
                            name_col = c_i
                        elif "isin" in c_str:
                            isin_col = c_i
                        elif "industry" in c_str or "rating" in c_str or "sector" in c_str:
                            ind_col = c_i
                        elif "market" in c_str or "fair value" in c_str or "value" in c_str:
                            mv_col = c_i
                        elif "%" in c_str or "nav" in c_str or "assets" in c_str:
                            pct_col = c_i
                    break

            # Parse holding rows
            raw_holdings = []
            valid_pcts = []

            for idx in range(header_row_idx + 1, len(df)):
                name = str(df.iloc[idx, name_col]).strip() if df.shape[1] > name_col and pd.notna(df.iloc[idx, name_col]) else ""
                isin = str(df.iloc[idx, isin_col]).strip() if df.shape[1] > isin_col and pd.notna(df.iloc[idx, isin_col]) else ""
                ind = str(df.iloc[idx, ind_col]).strip() if df.shape[1] > ind_col and pd.notna(df.iloc[idx, ind_col]) else ""
                mv = df.iloc[idx, mv_col] if df.shape[1] > mv_col else None
                pct = df.iloc[idx, pct_col] if df.shape[1] > pct_col else None

                if not name or name.lower() in [
                    "nan", "none", "sub total", "total", "grand total",
                    "equity & equity related", "others", "debt instruments",
                    "money market instruments", "net current assets", "cash & other receivables",
                ]:
                    continue

                if name.startswith("(") and ("listed" in name.lower() or "unlisted" in name.lower()):
                    continue

                try:
                    mv_f = float(str(mv).replace(",", "").strip()) if pd.notna(mv) else 0.0
                    pct_f = float(str(pct).replace(",", "").strip()) if pd.notna(pct) else 0.0
                except (ValueError, TypeError):
                    continue

                if isin in ["nan", "None", "-", "N.A.", "NA"]:
                    isin = ""

                raw_holdings.append((name, isin, ind, mv_f, pct_f))
                if pct_f > 0:
                    valid_pcts.append(pct_f)

            if not raw_holdings:
                continue

            # Determine percentage scale: if >=50% of valid numbers are <= 1.0, scale by 100
            if valid_pcts:
                fractional_count = sum(1 for p in valid_pcts if p <= 1.0)
                pct_scale = 100.0 if fractional_count / len(valid_pcts) >= 0.5 else 1.0
            else:
                pct_scale = 1.0

            for name, isin, ind, mv_f, pct_f in raw_holdings:
                if is_debt_format:
                    mv_cr = round(mv_f / 10000000.0, 4)  # Rs -> Crores
                else:
                    mv_cr = round(mv_f / 100.0, 4)  # Rs. Lakhs -> Crores

                pct_val = round(pct_f * pct_scale, 4)

                # Skip header/total outliers
                if pct_val > 105.0:
                    continue

                # Asset classification
                asset_type = classify_asset(name, ind)

                all_rows.append({
                    "scheme_code": scheme_code,
                    "fund_name": canonical_fund_name,
                    "as_of_month": sheet_date,
                    "isin": isin if isin else "",
                    "security_name": name,
                    "asset_type": asset_type,
                    "market_value_cr": mv_cr,
                    "pct_of_nav": pct_val,
                    "imported_at": imported_at,
                })

        logger.info("  %s (%s): %d holdings parsed", title, as_of_date, len(all_rows))
        return all_rows
