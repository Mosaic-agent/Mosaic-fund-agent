"""
src/data_importer/amc_holdings/importers/abakkus.py
────────────────────────────────────────────────────
Abakkus Mutual Fund (AMC) monthly portfolio holdings importer.

Scrapes statutory disclosures from https://www.abakkusmf.com/statutory-disclosures.html,
downloads monthly XLS/XLSX workbooks across all active schemes (Flexi Cap, Small Cap,
Liquid, Large & Mid Cap), parses instrument holdings, classifies asset types, converts
values to Crores, and stores rows into market_data.mf_holdings with delta sync and watermarking.
"""

from __future__ import annotations

import io
import json
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

DISCLOSURES_URL = "https://www.abakkusmf.com/statutory-disclosures.html"
BASE_URL = "https://www.abakkusmf.com"
_TIMEOUT = 30.0

_ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{10}$")

# Static / canonical scheme mapping
SCHEME_MAP: dict[str, tuple[str, str]] = {
    "ABAFC":  ("ABAKKUS_FLEXI_CAP", "Abakkus Flexi Cap Fund"),
    "ABASC":  ("ABAKKUS_SMALL_CAP", "Abakkus Small Cap Fund"),
    "ABALI":  ("ABAKKUS_LIQUID", "Abakkus Liquid Fund"),
    "ABALMC": ("ABAKKUS_LARGE_AND_MID_CAP", "Abakkus Large & Mid Cap Fund"),
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


def _parse_disclosure_date(title_str: str, fname_str: str) -> date | None:
    """Parse date from disclosure title or filename."""
    clean = re.sub(r"(st|nd|rd|th)", "", title_str)
    m = re.search(
        r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),?\s+(\d{4})",
        clean,
        re.I,
    )
    if m:
        month_name, day, year = m.groups()
        try:
            return datetime.strptime(f"{year}-{month_name}-{day}", "%Y-%B-%d").date()
        except ValueError:
            pass

    m2 = re.search(
        r"(\d{1,2})[\s\-_\.](January|February|March|April|May|June|July|August|September|October|November|December|\d{1,2})[\s\-_\.](\d{4})",
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

    # Try filename fallback e.g. Abakkus_Mutual_Fund_31.05.2026.xls
    m3 = re.search(r"(\d{1,2})[\._\-](\d{1,2})[\._\-](\d{4})", fname_str)
    if m3:
        d, mon, y = m3.groups()
        try:
            return datetime(int(y), int(mon), int(d)).date()
        except ValueError:
            pass

    return None


def _normalise_fund_identity(sheet_name: str, row0_text: str = "", row1_text: str = "") -> tuple[str, str]:
    """Resolve (scheme_code, fund_name) from sheet name or text snippet."""
    key = sheet_name.strip().upper()
    if key in SCHEME_MAP:
        return SCHEME_MAP[key]

    comb = f"{sheet_name} {row0_text} {row1_text}".upper()
    if "FLEXI" in comb:
        return SCHEME_MAP["ABAFC"]
    if "SMALL" in comb:
        return SCHEME_MAP["ABASC"]
    if "LIQUID" in comb:
        return SCHEME_MAP["ABALI"]
    if "LARGE" in comb or "MID" in comb:
        return SCHEME_MAP["ABALMC"]

    norm_code = re.sub(r"[^A-Z0-9_]", "", key.replace(" ", "_"))
    if not norm_code.startswith("ABAKKUS_"):
        norm_code = f"ABAKKUS_{norm_code}"
    return (norm_code, f"Abakkus {sheet_name.strip()}")


class AbakkusImporter(BaseFundImporter):
    """
    Abakkus Mutual Fund monthly portfolio importer.
    Supports auto-discovery, delta sync, watermarking, and historical re-imports.
    """

    REQUEST_DELAY = 1.0

    def __init__(
        self,
        full_reimport: bool = False,
        from_year: int = 2024,
        target_month: date | None = None,
        freshness_months: int = 0,
        **kwargs: Any,
    ) -> None:
        super().__init__(target_month=target_month, freshness_months=freshness_months)
        self.full_reimport = full_reimport
        self.from_year = from_year
        self._latest_imported_date: date | None = None

    def fund_name(self) -> str:
        return "Abakkus Mutual Fund"

    def table_name(self) -> str:
        return "market_data.mf_holdings"

    def column_names(self) -> list[str]:
        return _COLUMNS

    def watermark_source(self) -> str:
        return "mf_holdings"

    def fetch_sources(self) -> list[tuple[date, str, str]]:
        """
        Scrape statutory disclosures page to discover all monthly portfolio workbooks.
        Returns list of (as_of_date, filename, full_download_url).
        """
        sources: list[tuple[date, str, str]] = []
        with httpx.Client(headers=COMMON_HEADERS, timeout=_TIMEOUT, follow_redirects=True) as http:
            try:
                resp = http.get(DISCLOSURES_URL)
                resp.raise_for_status()
                html = resp.text
            except Exception as exc:
                self._console.print(f"[red]Abakkus: failed to load disclosures page: {exc}[/red]")
                return []

            m = re.search(r"const verticals = (\[.*?\]);\s*\n", html, re.DOTALL)
            if not m:
                self._console.print("[red]Abakkus: could not extract verticals JSON from page[/red]")
                return []

            try:
                verticals_data = json.loads(m.group(1))
            except Exception as exc:
                self._console.print(f"[red]Abakkus: failed to parse verticals JSON: {exc}[/red]")
                return []

            for v in verticals_data:
                v_title = v.get("title", "")
                if "monthly portfolio" not in v_title.lower():
                    continue

                for sec in v.get("sections", []):
                    for sub in sec.get("subSections", []):
                        for item in sub.get("items", []):
                            title = item.get("title", "")
                            media = item.get("downloadMedia") or {}
                            url = media.get("url") or ""
                            fname = media.get("name") or ""
                            ext = media.get("ext") or ""

                            if ext.lower() not in [".xls", ".xlsx"]:
                                continue

                            as_of = _parse_disclosure_date(title, fname)
                            if as_of is None:
                                continue

                            if as_of.year < self.from_year:
                                continue

                            full_url = url if url.startswith("http") else BASE_URL + url
                            sources.append((as_of, fname, full_url))

        # Deduplicate sources by date and filename
        seen: set[tuple[date, str]] = set()
        deduped: list[tuple[date, str, str]] = []
        for as_of, fn, u in sorted(sources, key=lambda x: x[0]):
            key = (as_of, fn)
            if key not in seen:
                seen.add(key)
                deduped.append((as_of, fn, u))

        self._console.print(f"[dim]Abakkus: discovered {len(deduped)} monthly portfolio file(s).[/dim]")
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
                "WHERE source = 'mf_holdings' AND symbol = 'ABAKKUS_MONTHLY'"
            ).result_rows
            if rows and rows[0][0]:
                last_date = rows[0][0]
        except Exception as exc:
            logger.warning("Failed to query Abakkus watermark: %s", exc)

        if last_date is None:
            return sources

        filtered = [s for s in sources if s[0] > last_date]
        skipped = len(sources) - len(filtered)
        if skipped:
            self._console.print(
                f"[dim]Delta sync: {skipped} Abakkus file(s) already in DB (watermark {last_date}), "
                f"{len(filtered)} to fetch.[/dim]"
            )
        return filtered

    def parse_source(
        self, source: tuple[date, str, str], http: httpx.Client
    ) -> list[dict]:
        """
        Download and parse an Abakkus monthly Excel workbook across all scheme sheets.
        """
        as_of_date, fname, url = source
        try:
            resp = http.get(url, timeout=_TIMEOUT)
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
            self._console.print(f"  [red]Cannot parse Abakkus workbook '{fname}'[/red]")
            return []

        all_holdings: list[dict] = []
        imported_at = datetime.now()

        for sheet in xl.sheet_names:
            if sheet.strip().lower() == "index":
                continue

            try:
                df = xl.parse(sheet, header=None)
            except Exception as exc:
                logger.debug("Failed parsing sheet %s: %s", sheet, exc)
                continue

            if df.shape[0] < 5 or df.shape[1] < 6:
                continue

            row0_str = str(df.iloc[0, 1]) if df.shape[1] > 1 and pd.notna(df.iloc[0, 1]) else ""
            row1_str = str(df.iloc[1, 1]) if df.shape[0] > 1 and df.shape[1] > 1 and pd.notna(df.iloc[1, 1]) else ""
            scheme_code, fund_name = _normalise_fund_identity(sheet, row0_str, row1_str)

            # Locate column header row dynamically
            header_row_idx: int | None = None
            for r in range(min(15, len(df))):
                r_vals = [str(x).strip().lower() for x in df.iloc[r].tolist() if pd.notna(x)]
                if any("isin" in x for x in r_vals) and any("instrument" in x or "name" in x for x in r_vals):
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
                elif "instrument" in hl or "name" in hl:
                    if name_col is None:
                        name_col = c_idx
                elif "industry" in hl or "rating" in hl:
                    ind_col = c_idx
                elif "market" in hl or "fair value" in hl:
                    mv_col = c_idx
                elif "%" in hl or "net assets" in hl or "nav" in hl:
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
                if len(row) <= max(isin_col, name_col, ind_col, mv_col, pct_col):
                    continue

                isin_val = str(row[isin_col]).strip()
                if not _ISIN_RE.match(isin_val):
                    continue

                name_val = str(row[name_col]).strip() if pd.notna(row[name_col]) else ""
                ind_val = str(row[ind_col]).strip() if pd.notna(row[ind_col]) else ""

                try:
                    mv_raw = float(row[mv_col])
                except (TypeError, ValueError):
                    mv_raw = 0.0

                try:
                    pct_raw = float(row[pct_col])
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
            return [("ABAKKUS_MONTHLY", self._latest_imported_date)]
        return []
