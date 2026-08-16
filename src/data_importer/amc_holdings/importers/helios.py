"""
src/data_importer/amc_holdings/importers/helios.py
───────────────────────────────────────────────────
Helios Mutual Fund (AMC) monthly portfolio holdings importer.

Scrapes downloads page from https://www.heliosmf.in/downloads,
downloads monthly XLS/XLSX workbooks across all active schemes (Small Cap, Flexi Cap,
Mid Cap, Large & Mid Cap, Balanced Advantage, Financial Services, Arbitrage, Overnight),
parses instrument holdings, classifies asset types, converts values from Rs. Lakhs to
Crores, scales percentages, and stores rows into market_data.mf_holdings with delta sync
and watermarking.
"""

from __future__ import annotations

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

DOWNLOADS_URL = "https://www.heliosmf.in/downloads"
BASE_URL = "https://www.heliosmf.in"
_TIMEOUT = 30.0

_ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{10}$")

# Static / canonical scheme mapping by sheet code or slug
SCHEME_MAP: dict[str, tuple[str, str]] = {
    "HFCF":  ("HELIOS_FLEXI_CAP", "Helios Flexi Cap Fund"),
    "HSCF":  ("HELIOS_SMALL_CAP", "Helios Small Cap Fund"),
    "HMCF":  ("HELIOS_MID_CAP", "Helios Mid Cap Fund"),
    "HLMCF": ("HELIOS_LARGE_AND_MID_CAP", "Helios Large & Mid Cap Fund"),
    "HLMC":  ("HELIOS_LARGE_AND_MID_CAP", "Helios Large & Mid Cap Fund"),
    "HBAF":  ("HELIOS_BALANCED_ADVANTAGE", "Helios Balanced Advantage Fund"),
    "HFSF":  ("HELIOS_FINANCIAL_SERVICES", "Helios Financial Services Fund"),
    "HARF":  ("HELIOS_ARBITRAGE", "Helios Arbitrage Fund"),
    "HOF":   ("HELIOS_OVERNIGHT", "Helios Overnight Fund"),
    "HONF":  ("HELIOS_OVERNIGHT", "Helios Overnight Fund"),
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


def _get_helios_headers() -> dict[str, str]:
    """Helios web server requires Accept header to avoid HTTP 406 on file downloads."""
    headers = dict(COMMON_HEADERS)
    headers["Accept"] = (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"
    )
    headers["Accept-Language"] = "en-US,en;q=0.9"
    return headers


def _parse_disclosure_date(title_str: str, fname_str: str) -> date | None:
    """Parse date from disclosure link text or URL filename."""
    text = f"{title_str} {fname_str}".lower()
    # Strip ordinal suffixes ONLY from numbers (e.g. 31st -> 31, preserving 'August')
    clean = re.sub(r"(\d{1,2})(st|nd|rd|th)", r"\1", text, flags=re.I)

    m = re.search(
        r"(january|february|march|april|may|june|july|august|september|october|november|december)\s+(\d{1,2}),?\s+(\d{4})",
        clean,
        re.I,
    )
    if m:
        mon, d, y = m.groups()
        try:
            return datetime.strptime(f"{y}-{mon}-{d}", "%Y-%B-%d").date()
        except ValueError:
            pass

    m2 = re.search(
        r"(\d{1,2})[\s\-_\.](january|february|march|april|may|june|july|august|september|october|november|december|\d{1,2})[\s\-_\.](\d{4})",
        clean,
        re.I,
    )
    if m2:
        d, mon, y = m2.groups()
        try:
            if mon.isdigit():
                return datetime(int(y), int(mon), int(d)).date()
            return datetime.strptime(f"{y}-{mon}-{d}", "%Y-%B-%d").date()
        except ValueError:
            pass

    m3 = re.search(
        r"(january|february|march|april|may|june|july|august|september|october|november|december)[\s\-_\.](\d{4})",
        clean,
        re.I,
    )
    if m3:
        mon, y = m3.groups()
        try:
            import calendar
            dt = datetime.strptime(f"{y}-{mon}-01", "%Y-%B-%d").date()
            _, last_day = calendar.monthrange(dt.year, dt.month)
            return date(dt.year, dt.month, last_day)
        except ValueError:
            pass

    return None


def _normalise_fund_identity(sheet_name: str, fname_str: str = "", text_snippet: str = "") -> tuple[str, str]:
    """Resolve (scheme_code, fund_name) from sheet name, filename, or text snippet."""
    key = sheet_name.strip().upper()
    if key in SCHEME_MAP:
        return SCHEME_MAP[key]

    comb = f"{sheet_name} {fname_str} {text_snippet}".upper()
    if "SMALL" in comb:
        return SCHEME_MAP["HSCF"]
    if "FLEXI" in comb:
        return SCHEME_MAP["HFCF"]
    if "MID" in comb and "LARGE" not in comb:
        return SCHEME_MAP["HMCF"]
    if "LARGE" in comb or "LMC" in comb:
        return SCHEME_MAP["HLMCF"]
    if "BALANCED" in comb or "ADVANTAGE" in comb or "BAF" in comb:
        return SCHEME_MAP["HBAF"]
    if "FINANCIAL" in comb or "BANKING" in comb or "FSF" in comb:
        return SCHEME_MAP["HFSF"]
    if "ARBITRAGE" in comb:
        return SCHEME_MAP["HARF"]
    if "OVERNIGHT" in comb:
        return SCHEME_MAP["HOF"]

    clean_code = re.sub(r"[^A-Z0-9_]", "", key.replace(" ", "_"))
    if not clean_code.startswith("HELIOS_"):
        clean_code = f"HELIOS_{clean_code}"
    return (clean_code, f"Helios {sheet_name.strip()}")


class HeliosImporter(BaseFundImporter):
    """
    Helios Mutual Fund monthly portfolio importer.
    Supports auto-discovery, delta sync, watermarking, and historical re-imports.
    """

    REQUEST_DELAY = 0.5

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
        return "Helios Mutual Fund"

    def table_name(self) -> str:
        return "market_data.mf_holdings"

    def column_names(self) -> list[str]:
        return _COLUMNS

    def watermark_source(self) -> str:
        return "mf_holdings"

    def fetch_sources(self) -> list[tuple[date, str, str]]:
        """
        Scrape downloads page to discover all monthly portfolio workbooks.
        Returns list of (as_of_date, filename, full_download_url).
        """
        headers = _get_helios_headers()
        sources: list[tuple[date, str, str]] = []

        with httpx.Client(headers=headers, timeout=_TIMEOUT, follow_redirects=True) as http:
            try:
                resp = http.get(DOWNLOADS_URL)
                resp.raise_for_status()
                html = resp.text
            except Exception as exc:
                self._console.print(f"[red]Helios: failed to load downloads page: {exc}[/red]")
                return []

            soup = BeautifulSoup(html, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                text = a.get_text(" ", strip=True)
                lower_href = href.lower()

                if not (".xlsx" in lower_href or ".xls" in lower_href):
                    continue

                if "monthly-portfolio" not in lower_href and "monthly_portfolio" not in lower_href:
                    continue

                as_of = _parse_disclosure_date(text, href)
                if as_of is None:
                    continue

                if as_of.year < self.from_year:
                    continue

                fname = href.split("/")[-1]
                full_url = href if href.startswith("http") else BASE_URL + href
                sources.append((as_of, fname, full_url))

        # Deduplicate sources by date and filename
        seen: set[tuple[date, str]] = set()
        deduped: list[tuple[date, str, str]] = []
        for as_of, fn, u in sorted(sources, key=lambda x: (x[0], x[1])):
            key = (as_of, fn)
            if key not in seen:
                seen.add(key)
                deduped.append((as_of, fn, u))

        self._console.print(f"[dim]Helios: discovered {len(deduped)} monthly portfolio file(s).[/dim]")
        return deduped

    def filter_sources(
        self, sources: list[tuple[date, str, str]], client: Any
    ) -> list[tuple[date, str, str]]:
        """Filter out months already imported into ClickHouse, unless full_reimport is active."""
        if self.full_reimport:
            return sources

        last_date: date | None = None
        try:
            rows = client.query(
                "SELECT max(last_date) FROM market_data.import_watermarks "
                "WHERE source = 'mf_holdings' AND symbol = 'HELIOS_MONTHLY'"
            ).result_rows
            if rows and rows[0][0]:
                last_date = rows[0][0]
        except Exception as exc:
            logger.warning("Failed to query Helios watermark: %s", exc)

        if last_date is None:
            return sources

        filtered = [s for s in sources if s[0] > last_date]
        skipped = len(sources) - len(filtered)
        if skipped:
            self._console.print(
                f"[dim]Delta sync: {skipped} Helios file(s) already in DB (watermark {last_date}), "
                f"{len(filtered)} to fetch.[/dim]"
            )
        return filtered

    def parse_source(
        self, source: tuple[date, str, str], http: httpx.Client
    ) -> list[dict]:
        """
        Download and parse a Helios monthly Excel workbook.
        """
        as_of_date, fname, url = source
        headers = _get_helios_headers()

        try:
            resp = http.get(url, headers=headers, timeout=_TIMEOUT)
            resp.raise_for_status()
        except Exception as exc:
            self._console.print(f"  [red]Download failed ({url}): {exc}[/red]")
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
            self._console.print(f"  [red]Cannot parse Helios workbook '{fname}'[/red]")
            return []

        all_holdings: list[dict] = []
        imported_at = datetime.now()

        for sheet in xl.sheet_names:
            if sheet.strip().lower() in ("index", "summary"):
                continue

            try:
                df = xl.parse(sheet, header=None)
            except Exception as exc:
                logger.debug("Failed parsing sheet %s: %s", sheet, exc)
                continue

            if df.shape[0] < 5 or df.shape[1] < 5:
                continue

            row0_str = str(df.iloc[0, 1]) if df.shape[1] > 1 and pd.notna(df.iloc[0, 1]) else ""
            row2_str = str(df.iloc[2, 2]) if df.shape[0] > 2 and df.shape[1] > 2 and pd.notna(df.iloc[2, 2]) else ""
            scheme_code, fund_name = _normalise_fund_identity(sheet, fname, f"{row0_str} {row2_str}")

            # Locate column header row dynamically
            header_row_idx: int | None = None
            for r in range(min(15, len(df))):
                r_vals = [str(x).strip().lower() for x in df.iloc[r].tolist() if pd.notna(x)]
                if any("isin" in x for x in r_vals) and any("instrument" in x or "name" in x or "issuer" in x for x in r_vals):
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
                elif "instrument" in hl or "name" in hl or "issuer" in hl:
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
                isin_col = 3
            if name_col is None:
                name_col = 2
            if ind_col is None:
                ind_col = 4
            if mv_col is None:
                mv_col = 6
            if pct_col is None:
                pct_col = 7

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
                    f"  [{color}]{fund_name} ({sheet}): {len(sheet_holdings)} holdings, "
                    f"pct_total={pct_total:.1f}% ({as_of_date})[/{color}]"
                )
                all_holdings.extend(sheet_holdings)

        if self._latest_imported_date is None or as_of_date > self._latest_imported_date:
            self._latest_imported_date = as_of_date

        return all_holdings

    def watermark_rows(self, all_rows: list[dict]) -> list[tuple[str, date]]:
        if self._latest_imported_date:
            return [("HELIOS_MONTHLY", self._latest_imported_date)]
        return []
