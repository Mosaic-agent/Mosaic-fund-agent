from __future__ import annotations

import logging
import io
import re
import calendar
from datetime import date, datetime
from typing import Any
import httpx
import pandas as pd
from bs4 import BeautifulSoup

from src.data_importer.amc_holdings.base import BaseFundImporter, classify_asset

logger = logging.getLogger(__name__)

BASE_URL = "https://quantmutual.com"
DISCLOSURES_URL = "https://quantmutual.com/statutorydisclosures.aspx/displaydisclouser"

_ISIN_RE = re.compile(r'^[A-Z]{2}[A-Z0-9]{10}$')

SCHEME_CODES = {
    "qMAF": "120821",    # quant Multi Asset Allocation Fund
    "qActive": "120827",  # quant Multi Cap Fund
    "qSCF": "120812",    # quant Small Cap Fund
    "qFLEXI": "120828",  # quant Flexi Cap Fund
    "qVF": "120863",     # quant Value Fund
    "qIF": "120819",     # quant Infrastructure Fund
    "qTP": "120843",     # quant ELSS Tax Saver Fund
    "qMAB": "120845",    # quant Arbitrage Fund
    "qLF": "120815",     # quant Liquid Fund
    "qMGG": "120813",    # quant Gilt Fund
    "qMON": "120816",    # quant Overnight Fund
    "qMDA": "120833",    # quant Dynamic Asset Allocation Fund
    "qMHC": "120835",    # quant Healthcare Fund
    "qMQM": "120839",    # quant Quantamental Fund
    "qMBC": "120838",    # quant Business Cycle Fund
    "qESG": "120841",    # quant ESG Integration Strategy Fund
    "qFF": "120842",     # quant Focused Fund
    "qMTK": "120846",    # quant Teck Fund
    "qMCO": "120848",    # quant Commodities Fund
    "qMBS": "120853",    # quant BFSI Fund
    "qMCN": "120854",    # quant Consumption Fund
    "qL&MF": "120857",   # quant Large & Mid Cap Fund
    "qMMF": "120858",    # quant Manufacturing Fund
    "qMMO": "120859",    # quant Momentum Fund
    "qAF": "120844",     # quant Aggressive Hybrid Fund
    "qMCF": "120847",    # quant Mid Cap Fund
    "qMLC": "120849",    # quant Large Cap Fund
    "qMPU": "120852",    # quant PSU Fund
    "qMES": "120840",    # quant Equity Savings Fund
}

FUND_NAMES = {
    "qMAF": "QUANT_MULTI_ASSET",
    "qActive": "QUANT_ACTIVE",
    "qSCF": "QUANT_SMALL_CAP",
    "qFLEXI": "QUANT_FLEXI_CAP",
    "qVF": "QUANT_VALUE",
    "qIF": "QUANT_INFRASTRUCTURE",
    "qTP": "QUANT_ELSS_TAX_SAVER",
    "qMAB": "QUANT_ARBITRAGE",
    "qLF": "QUANT_LIQUID",
    "qMGG": "QUANT_GILT",
    "qMON": "QUANT_OVERNIGHT",
    "qMDA": "QUANT_DYNAMIC_ASSET_ALLOCATION",
    "qMHC": "QUANT_HEALTHCARE",
    "qMQM": "QUANT_QUANTAMENTAL",
    "qMBC": "QUANT_BUSINESS_CYCLE",
    "qESG": "QUANT_ESG",
    "qFF": "QUANT_FOCUSED",
    "qMTK": "QUANT_TECK",
    "qMCO": "QUANT_COMMODITIES",
    "qMBS": "QUANT_BFSI",
    "qMCN": "QUANT_CONSUMPTION",
    "qL&MF": "QUANT_LARGE_AND_MID_CAP",
    "qMMF": "QUANT_MANUFACTURING",
    "qMMO": "QUANT_MOMENTUM",
    "qAF": "QUANT_AGGRESSIVE_HYBRID",
    "qMCF": "QUANT_MID_CAP",
    "qMLC": "QUANT_LARGE_CAP",
    "qMPU": "QUANT_PSU",
    "qMES": "QUANT_EQUITY_SAVINGS",
}


def _parse_month_year(text: str) -> date | None:
    """Parse text like 'June 2026' or 'May 2026' or 'Feb 26' into last day of that month."""
    t = text.lower().strip()
    months = {
        "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
        "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6, "jul": 7, "july": 7,
        "aug": 8, "august": 8, "sep": 9, "september": 9, "oct": 10, "october": 10,
        "nov": 11, "november": 11, "dec": 12, "december": 12
    }
    
    # Try to find month keyword
    month_val = None
    for kw, val in months.items():
        if kw in t:
            month_val = val
            break
            
    if not month_val:
        return None
        
    # Extract year
    match = re.search(r"\b(\d{2,4})\b", t)
    if not match:
        return None
        
    year_val = int(match.group(1))
    if year_val < 100:
        year_val += 2000
        
    last_day = calendar.monthrange(year_val, month_val)[1]
    return date(year_val, month_val, last_day)


class QuantImporter(BaseFundImporter):
    """
    Quant Mutual Fund holdings importer.
    Loads dynamic list of monthly Excel sheets from quantmutual.com.
    """

    def __init__(self, full_reimport: bool = False, target_month: date | None = None, freshness_months: int = 0, **kwargs) -> None:
        super().__init__(target_month=target_month, freshness_months=freshness_months)
        self.full_reimport = full_reimport

    def fund_name(self) -> str:
        return "Quant Mutual Fund"

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
        Request monthly list from statutory disclosures API and return
        confirmed (as_of_date_str, xlsx_url) pairs in chronological order.
        """
        self._console.print("[dim]Fetching monthly portfolio disclosure links from Quant AMC...[/dim]")
        
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
            )
        }
        
        sources: list[tuple[str, str]] = []
        current_year = datetime.now().year
        for year in range(2023, current_year + 1):
            payload = {"id": str(year), "cat": "MONTHLY PORTFOLIO"}
            try:
                resp = httpx.post(DISCLOSURES_URL, json=payload, headers=headers, timeout=20)
                resp.raise_for_status()
                data = resp.json()
                html_content = data.get("d", "")
                
                if not html_content:
                    continue
                    
                soup = BeautifulSoup(html_content, "html.parser")
                for a_tag in soup.find_all("a"):
                    href = a_tag.get("href", "")
                    text = a_tag.get_text()
                    
                    if href.lower().endswith((".xlsx", ".xls")):
                        # Full download URL
                        url = href if href.startswith("http") else BASE_URL + href
                        dt = _parse_month_year(text)
                        if dt:
                            sources.append((dt.strftime("%Y-%m-%d"), url))
            except Exception as exc:
                logger.error("Failed to fetch Quant disclosures for year %d: %s", year, exc)
                self._console.print(f"[red]Error fetching Quant portfolio list for year {year}: {exc}[/red]")
            
        # Return sorted chronologically
        return sorted(sources, key=lambda x: x[0])

    def filter_sources(self, sources: list[tuple[str, str]], client) -> list[tuple[str, str]]:
        """
        Remove months that are already imported by checking watermark.
        """
        if self.full_reimport:
            return sources

        try:
            rows = client.query(
                "SELECT max(last_date) FROM market_data.import_watermarks "
                "WHERE source = 'mf_holdings' AND symbol = 'QUANT_MULTI_ASSET'"
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
            logger.warning("Failed to query Quant watermark: %s", exc)

        return sources

    def parse_source(self, source: tuple[str, str], http: httpx.Client) -> list[dict]:
        as_of_str, url = source
        self._console.print(f"[dim]Downloading Quant holdings file: {url}…[/dim]")
        
        try:
            resp = http.get(url, timeout=45)
            resp.raise_for_status()
        except Exception as exc:
            self._console.print(f"  [red]Download failed: {exc}[/red]")
            return []

        try:
            xl = pd.ExcelFile(io.BytesIO(resp.content))
        except Exception as exc:
            self._console.print(f"  [red]Cannot parse Excel: {exc}[/red]")
            return []

        as_of_date = datetime.strptime(as_of_str, "%Y-%m-%d").date()
        imported_at = datetime.now()
        
        all_holdings: list[dict] = []
        
        for sheet in xl.sheet_names:
            # We only parse sheets that are mapped to known fund names
            if sheet not in SCHEME_CODES:
                continue
                
            try:
                df = xl.parse(sheet, header=None)
            except Exception:
                continue
                
            if df.shape[0] < 8 or df.shape[1] < 5:
                continue

            # Extract fund name from first few rows
            fund_name_raw = None
            for r_idx in range(0, 5):
                row_vals = [str(x).strip() for x in df.iloc[r_idx].tolist() if str(x) != 'nan' and str(x) != '']
                for val in row_vals:
                    if "quant" in val.lower() and "statement" not in val.lower() and "as on" not in val.lower() and "mutual fund" not in val.lower():
                        fund_name_raw = val
                        break
                if fund_name_raw:
                    break
                    
            fund_name = FUND_NAMES.get(sheet, fund_name_raw or sheet)
            scheme_code = SCHEME_CODES.get(sheet, sheet)
            
            # Locate headers row to find columns dynamically
            headers_row = None
            headers_idx = 6  # fallback default
            for r_idx in range(4, 10):
                row_vals = [str(x).lower().strip() for x in df.iloc[r_idx].tolist()]
                if any("isin" in val for val in row_vals) or any("instrument" in val for val in row_vals):
                    headers_row = row_vals
                    headers_idx = r_idx
                    break
                    
            # Set default column indexes
            isin_col = 1
            name_col = 2
            industry_col = 3
            mv_col = 6
            pct_col = 7
            
            if headers_row:
                for idx, val in enumerate(headers_row):
                    if "isin" in val:
                        isin_col = idx
                    elif "instrument" in val or "name of" in val:
                        name_col = idx
                    elif "industry" in val:
                        industry_col = idx
                    elif "market value" in val or "lakhs" in val:
                        mv_col = idx
                    elif "nav" in val or "% to" in val or "percentage" in val:
                        pct_col = idx
            
            sheet_holdings = []
            for idx, row in df.iterrows():
                if idx <= headers_idx:
                    continue
                vals = row.tolist()
                if len(vals) <= max(isin_col, name_col, mv_col, pct_col):
                    continue
                    
                isin = str(vals[isin_col]).strip()
                if not _ISIN_RE.match(isin):
                    continue
                    
                name = str(vals[name_col]).strip()
                industry = str(vals[industry_col]).strip() if len(vals) > industry_col and str(vals[industry_col]) != 'nan' else ""
                
                try:
                    mv_lakhs = float(vals[mv_col])
                except (TypeError, ValueError):
                    mv_lakhs = 0.0
                    
                try:
                    pct = float(vals[pct_col])
                except (TypeError, ValueError):
                    pct = 0.0
                    
                # Belt-and-suspenders guard for rogue totals
                if pct > 50.0 and "overnight" not in fund_name.lower() and "liquid" not in fund_name.lower():
                    continue
                    
                # Filter out NaN/null values in pct or ISIN
                if pd.isna(pct) or pct == 0.0 or not isin:
                    continue
                    
                sheet_holdings.append({
                    "scheme_code": scheme_code,
                    "fund_name": fund_name,
                    "as_of_month": as_of_date,
                    "isin": isin,
                    "security_name": name,
                    "asset_type": classify_asset(name, industry),
                    "market_value_cr": round(mv_lakhs / 100, 4),  # Convert lakhs to crores
                    "pct_of_nav": round(pct, 4),
                    "imported_at": imported_at,
                })
                
            if sheet_holdings:
                pct_sum = sum(h["pct_of_nav"] for h in sheet_holdings)
                self._console.print(
                    f"  [green]✓[/green]  {fund_name} ({sheet}): {len(sheet_holdings)} holdings, "
                    f"pct_sum={pct_sum:.1f}% ({as_of_str})"
                )
                all_holdings.extend(sheet_holdings)
                
        return all_holdings
