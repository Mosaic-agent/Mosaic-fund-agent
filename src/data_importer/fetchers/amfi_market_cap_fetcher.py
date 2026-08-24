"""
src/data_importer/fetchers/amfi_market_cap_fetcher.py
─────────────────────────────────────────────────────
Fetches official AMFI Semi-Annual Average Market Capitalization rankings
and SEBI cap categorization from the AMFI portal.

Source:
    https://portal.amfiindia.com/spages/AverageMarketCapitalization30Jun2026.xlsx

Each row parsed contains:
    period_end_date     (date)   End of 6-month calculation period
    rank                (int)    Official statutory rank (1 to 5400+)
    company_name        (str)    Company name
    isin                (str)    ISIN
    bse_symbol          (str)    BSE symbol/scrip
    bse_avg_mcap_cr     (float)  BSE 6-month average market cap in Rs. Cr
    nse_symbol          (str)    NSE symbol
    nse_avg_mcap_cr     (float)  NSE 6-month average market cap in Rs. Cr
    msei_symbol         (str)    MSEI symbol
    msei_avg_mcap_cr    (float)  MSEI 6-month average market cap in Rs. Cr
    avg_mcap_cr         (float)  Average of all exchanges in Rs. Cr
    cap_category        (str)    Large Cap | Mid Cap | Small Cap
"""

from __future__ import annotations

import io
import logging
import re
from datetime import date, datetime
from typing import Any

import pandas as pd
import requests

logger = logging.getLogger(__name__)

# Known historical & recent AMFI Excel URLs
_AMFI_BASE_URL = "https://portal.amfiindia.com/spages/"
_KNOWN_FILES: list[tuple[date, str]] = [
    (date(2026, 6, 30), "AverageMarketCapitalization30Jun2026.xlsx"),
    (date(2025, 12, 31), "AverageMarketCapitalization31Dec2025.xlsx"),
    (date(2025, 6, 30), "AverageMarketCapitalization30Jun2025.xlsx"),
    (date(2024, 12, 31), "AverageMarketCapitalization31Dec2024.xlsx"),
    (date(2024, 6, 30), "AverageMarketCapitalization30Jun2024.xlsx"),
]


def _safe_float(val: Any) -> float:
    """Parse numeric values safely, handling commas, strings, and dashes."""
    if pd.isna(val) or val is None:
        return 0.0
    if isinstance(val, (int, float)):
        return float(val) if not pd.isna(val) else 0.0
    val_str = str(val).replace(",", "").replace("-", "").strip()
    try:
        return float(val_str) if val_str else 0.0
    except (ValueError, TypeError):
        return 0.0


def _safe_int(val: Any) -> int:
    """Parse integer rank safely."""
    if pd.isna(val) or val is None:
        return 0
    try:
        return int(float(str(val).replace(",", "").strip()))
    except (ValueError, TypeError):
        return 0


def _clean_str(val: Any) -> str:
    """Clean string values."""
    if pd.isna(val) or val is None:
        return ""
    s = str(val).strip()
    return "" if s.lower() in ("nan", "none", "null", "-", "--") else s


def fetch_amfi_market_cap(
    period_end: date | str | None = None,
    url: str | None = None,
) -> list[dict[str, Any]]:
    """
    Download and parse the AMFI Semi-Annual Average Market Capitalization Excel sheet.

    Parameters:
        period_end: Target period end date (e.g. '2026-06-30' or date(2026, 6, 30))
        url: Optional direct URL to the Excel file

    Returns:
        List of dict records ready for ClickHouse insertion.
    """
    target_date = date(2026, 6, 30)
    if isinstance(period_end, str):
        target_date = datetime.strptime(period_end, "%Y-%m-%d").date()
    elif isinstance(period_end, date):
        target_date = period_end

    target_url = url
    if not target_url:
        # Find matching filename or default to latest
        for d, fname in _KNOWN_FILES:
            if d == target_date:
                target_url = f"{_AMFI_BASE_URL}{fname}"
                break
        if not target_url:
            target_url = f"{_AMFI_BASE_URL}AverageMarketCapitalization{target_date.strftime('%d%b%Y')}.xlsx"

    logger.info("Fetching AMFI Average Market Cap data from %s (Period: %s)", target_url, target_date)
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet, application/octet-stream, */*",
    }

    try:
        resp = requests.get(target_url, headers=headers, timeout=30)
        if resp.status_code != 200:
            logger.warning("AMFI request failed with status %d for %s", resp.status_code, target_url)
            return []
        excel_bytes = io.BytesIO(resp.content)
    except Exception as exc:
        logger.error("Failed to download AMFI Excel file from %s: %s", target_url, exc)
        return []

    try:
        # Header is usually row 1 (second line)
        df = pd.read_excel(excel_bytes, header=1)
    except Exception as exc:
        logger.error("Failed to parse AMFI Excel file: %s", exc)
        return []

    if df.empty:
        logger.warning("Parsed AMFI Excel sheet is empty")
        return []

    # Map column headers dynamically
    col_sr = next((c for c in df.columns if "Sr" in str(c) or "No" in str(c)), "Sr. No.")
    col_name = next((c for c in df.columns if "Company" in str(c) or "Name" in str(c)), "Company name")
    col_isin = next((c for c in df.columns if "ISIN" in str(c)), "ISIN")
    col_bse_sym = next((c for c in df.columns if "BSE" in str(c) and "Symbol" in str(c)), "BSE Symbol")
    col_bse_mcap = next((c for c in df.columns if "BSE" in str(c) and "Avg" in str(c)), "BSE 6 month Avg Total Market Cap in (Rs. Crs.)")
    col_nse_sym = next((c for c in df.columns if "NSE" in str(c) and "Symbol" in str(c)), "NSE Symbol")
    col_nse_mcap = next((c for c in df.columns if "NSE" in str(c) and "Avg" in str(c)), "NSE 6 month Avg Total Market Cap (Rs. Crs.)")
    col_msei_sym = next((c for c in df.columns if "MSEI" in str(c) and "Symbol" in str(c)), "MSEI Symbol")
    col_msei_mcap = next((c for c in df.columns if "MSEI" in str(c) and "Avg" in str(c)), "MSEI 6 month Avg Total Market Cap in (Rs Crs.)")
    col_avg_mcap = next((c for c in df.columns if "Average of All" in str(c) or "All Exchanges" in str(c)), "Average of All Exchanges (Rs. Cr.)")
    col_cat = next((c for c in df.columns if "Categorization" in str(c) or "Category" in str(c)), "Categorization as per SEBI Circular dated Oct 6, 2017")

    records: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        rank = _safe_int(row.get(col_sr))
        cname = _clean_str(row.get(col_name))
        isin = _clean_str(row.get(col_isin))
        if not cname and not isin:
            continue

        raw_cat = _clean_str(row.get(col_cat))
        # Normalise category
        if "large" in raw_cat.lower() or (rank > 0 and rank <= 100):
            cat = "Large Cap"
        elif "mid" in raw_cat.lower() or (rank > 100 and rank <= 250):
            cat = "Mid Cap"
        else:
            cat = "Small Cap"

        avg_mcap = _safe_float(row.get(col_avg_mcap))
        bse_mcap = _safe_float(row.get(col_bse_mcap))
        nse_mcap = _safe_float(row.get(col_nse_mcap))
        msei_mcap = _safe_float(row.get(col_msei_mcap))

        records.append({
            "period_end_date": target_date,
            "rank": rank,
            "company_name": cname,
            "isin": isin,
            "bse_symbol": _clean_str(row.get(col_bse_sym)),
            "bse_avg_mcap_cr": bse_mcap,
            "nse_symbol": _clean_str(row.get(col_nse_sym)),
            "nse_avg_mcap_cr": nse_mcap,
            "msei_symbol": _clean_str(row.get(col_msei_sym)),
            "msei_avg_mcap_cr": msei_mcap,
            "avg_mcap_cr": avg_mcap,
            "cap_category": cat,
        })

    logger.info("Successfully parsed %d AMFI market cap records for %s", len(records), target_date)
    return records
