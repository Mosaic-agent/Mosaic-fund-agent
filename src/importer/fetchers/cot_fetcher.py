"""
src/importer/fetchers/cot_fetcher.py
─────────────────────────────────────
Fetches CFTC Disaggregated Commitment of Traders (COT) report for Gold (COMEX).

Primary source:  https://publicreporting.cftc.gov  (Socrata Open Data API — no auth)
Supplementary:   https://www.cftc.gov/dea/newcot/f_disagg.txt  (current-year live file)
                 https://www.cftc.gov/files/dea/history/fut_disagg_txt_{year}.zip (annual archives)

Dataset: Disaggregated Futures-Only COT, CFTC commodity code 088 = GOLD (COMEX)
Cadence: Weekly report, released every Friday ~15:30 ET for the prior Tuesday.

Note: The Socrata API dataset (kh3c-gbw2) has a known lag of several months.
When Socrata is stale (< today − 60 days), the fetcher automatically supplements
with CFTC annual ZIP archives and the live current-year TXT file.

Key signal interpretation:
  mm_net > 0  → hedge funds net long  → bullish momentum (crash risk at extremes)
  mm_net < 0  → hedge funds net short → bearish (potential short-squeeze fuel)
  mm_net_pct_oi > +25% → crowded long → high reversal risk
"""
from __future__ import annotations

import io
import logging
import zipfile
from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd
import requests

_SOCRATA_URL  = "https://publicreporting.cftc.gov/resource/kh3c-gbw2.json"
_CFTC_CUR_URL = "https://www.cftc.gov/dea/newcot/f_disagg.txt"
_CFTC_ZIP_URL = "https://www.cftc.gov/files/dea/history/fut_disagg_txt_{year}.zip"
_HEADERS      = {"Accept": "application/json"}
_UA_HEADERS   = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"}
_TIMEOUT      = 30

# SOCRATA_STALE_DAYS: if Socrata's latest row is older than this, also pull direct files
_SOCRATA_STALE_DAYS = 60

log = logging.getLogger(__name__)


# ── helpers ───────────────────────────────────────────────────────────────────

def _row_from_dict(raw: dict) -> Optional[dict]:
    """Build a cot_gold schema row from a Socrata API result dict."""
    try:
        mm_long  = int(raw.get("m_money_positions_long_all",  0) or 0)
        mm_short = int(raw.get("m_money_positions_short_all", 0) or 0)
        mm_spr   = int(raw.get("m_money_positions_spread",    0) or 0)
        cl_long  = int(raw.get("prod_merc_positions_long",    0) or 0)
        cl_short = int(raw.get("prod_merc_positions_short",   0) or 0)
        oi       = int(raw.get("open_interest_all",           0) or 0)
        return {
            "report_date":   datetime.strptime(
                raw["report_date_as_yyyy_mm_dd"][:10], "%Y-%m-%d"
            ).date(),
            "mm_long":       mm_long,
            "mm_short":      mm_short,
            "mm_spread":     mm_spr,
            "mm_net":        mm_long - mm_short,
            "comm_long":     cl_long,
            "comm_short":    cl_short,
            "comm_net":      cl_long - cl_short,
            "open_interest": oi,
            "source":        "cftc_disaggregated",
        }
    except (KeyError, ValueError):
        return None


def _row_from_df_row(r: "pd.Series") -> Optional[dict]:
    """Build a cot_gold schema row from a parsed CFTC CSV/TXT DataFrame row."""
    try:
        mm_long  = int(r["M_Money_Positions_Long_All"]  or 0)
        mm_short = int(r["M_Money_Positions_Short_All"] or 0)
        mm_spr   = int(r["M_Money_Positions_Spread_All"] or 0)
        cl_long  = int(r["Prod_Merc_Positions_Long_All"] or 0)
        cl_short = int(r["Prod_Merc_Positions_Short_All"] or 0)
        oi       = int(r["Open_Interest_All"]            or 0)
        return {
            "report_date":   datetime.strptime(str(r["Report_Date_as_YYYY-MM-DD"])[:10], "%Y-%m-%d").date(),
            "mm_long":       mm_long,
            "mm_short":      mm_short,
            "mm_spread":     mm_spr,
            "mm_net":        mm_long - mm_short,
            "comm_long":     cl_long,
            "comm_short":    cl_short,
            "comm_net":      cl_long - cl_short,
            "open_interest": oi,
            "source":        "cftc_direct",
        }
    except (KeyError, ValueError, TypeError):
        return None


def _parse_cftc_gold_df(df: "pd.DataFrame") -> list[dict]:
    """Filter a CFTC CSV DataFrame to GOLD (COMEX) rows — excludes MICRO GOLD."""
    # Anchor to start-of-string so "MICRO GOLD - COMMODITY ..." is excluded
    gold = df[df["Market_and_Exchange_Names"].str.match(r"^GOLD - COMMODITY", na=False)]
    rows = []
    for _, r in gold.iterrows():
        row = _row_from_df_row(r)
        if row:
            rows.append(row)
    return rows


def _fetch_cftc_direct(years: list[int]) -> list[dict]:
    """
    Fetch CFTC disaggregated COT data for the given years directly from CFTC.gov.

    For the current year: downloads the live f_disagg.txt (updated weekly).
    For past years: downloads the annual ZIP archive.

    Returns list of cot_gold schema dicts for GOLD (COMEX) only.
    """
    current_year = date.today().year
    rows: list[dict] = []

    for year in years:
        try:
            if year == current_year:
                r = requests.get(_CFTC_CUR_URL, headers=_UA_HEADERS, timeout=_TIMEOUT)
                r.raise_for_status()
                df = pd.read_csv(io.StringIO(r.text), header=None, low_memory=False)
                # The live TXT has no header — apply column names from historical schema
                df = _name_headerless_df(df)
            else:
                zip_url = _CFTC_ZIP_URL.format(year=year)
                r = requests.get(zip_url, headers=_UA_HEADERS, timeout=60)
                r.raise_for_status()
                zf = zipfile.ZipFile(io.BytesIO(r.content))
                with zf.open(zf.namelist()[0]) as f:
                    df = pd.read_csv(f, low_memory=False)

            year_rows = _parse_cftc_gold_df(df)
            log.info("CFTC direct (%d): %d gold rows", year, len(year_rows))
            rows.extend(year_rows)
        except Exception as exc:
            log.warning("CFTC direct fetch failed for year %d: %s", year, exc)

    return rows


# CFTC annual ZIPs include a header row; the live TXT does not.
# Column order is identical — 191 columns (indices 0–190).
_CFTC_COLS = [
    "Market_and_Exchange_Names", "As_of_Date_In_Form_YYMMDD", "Report_Date_as_YYYY-MM-DD",
    "CFTC_Contract_Market_Code", "CFTC_Market_Code", "CFTC_Region_Code",
    "CFTC_Commodity_Code", "Open_Interest_All",
    "Prod_Merc_Positions_Long_All", "Prod_Merc_Positions_Short_All",
    "Swap_Positions_Long_All", "Swap__Positions_Short_All", "Swap__Positions_Spread_All",
    "M_Money_Positions_Long_All", "M_Money_Positions_Short_All", "M_Money_Positions_Spread_All",
    "Other_Rept_Positions_Long_All", "Other_Rept_Positions_Short_All", "Other_Rept_Positions_Spread_All",
    "Tot_Rept_Positions_Long_All", "Tot_Rept_Positions_Short_All",
    "NonRept_Positions_Long_All", "NonRept_Positions_Short_All",
]  # First 23 columns — all we need; rest are ignored


def _name_headerless_df(df: "pd.DataFrame") -> "pd.DataFrame":
    """Assign column names to the headerless live TXT DataFrame."""
    n_need = len(_CFTC_COLS)
    rename = {i: _CFTC_COLS[i] for i in range(min(n_need, len(df.columns)))}
    return df.rename(columns=rename)


# ── public API ────────────────────────────────────────────────────────────────

def fetch_cot_gold(
    from_date: Optional[date] = None,
    limit: int = 500,
) -> list[dict]:
    """
    Fetch Disaggregated COT rows for COMEX Gold.

    Strategy:
      1. Query Socrata API (https://publicreporting.cftc.gov) — fast, well-indexed.
      2. If Socrata's latest row is stale (> SOCRATA_STALE_DAYS old), automatically
         supplement with CFTC direct files (annual ZIPs + live current-year TXT).
      3. Merge, deduplicate by report_date, return sorted ascending.

    Parameters
    ----------
    from_date : fetch rows on or after this date (None → most recent `limit` rows)
    limit     : max rows returned from Socrata (direct files are not limited)

    Returns
    -------
    List of dicts matching the cot_gold ClickHouse schema.
    """
    # ── 1. Socrata ────────────────────────────────────────────────────────────
    where_clause = "cftc_commodity_code='088'"
    if from_date:
        where_clause += f" AND report_date_as_yyyy_mm_dd >= '{from_date.isoformat()}'"

    socrata_rows: list[dict] = []
    socrata_max: Optional[date] = None
    try:
        resp = requests.get(
            _SOCRATA_URL,
            params={
                "$where":  where_clause,
                "$order":  "report_date_as_yyyy_mm_dd DESC",
                "$limit":  str(limit),
                "$select": ",".join([
                    "report_date_as_yyyy_mm_dd",
                    "m_money_positions_long_all", "m_money_positions_short_all",
                    "m_money_positions_spread",
                    "prod_merc_positions_long", "prod_merc_positions_short",
                    "open_interest_all",
                ]),
            },
            headers=_HEADERS,
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        for raw in resp.json():
            row = _row_from_dict(raw)
            if row:
                socrata_rows.append(row)
        if socrata_rows:
            socrata_max = max(r["report_date"] for r in socrata_rows)
            log.info("Socrata COT: %d rows, latest %s", len(socrata_rows), socrata_max)
    except requests.RequestException as exc:
        log.warning("Socrata COT fetch failed: %s", exc)

    # ── 2. Supplement with CFTC direct files if Socrata is stale ─────────────
    stale_threshold = date.today() - timedelta(days=_SOCRATA_STALE_DAYS)
    direct_rows: list[dict] = []

    if socrata_max is None or socrata_max < stale_threshold:
        # Determine which years we need from direct files
        gap_start = (socrata_max.year if socrata_max else (from_date.year if from_date else 2017))
        gap_start = max(gap_start, socrata_max.year if socrata_max else gap_start)
        years_needed = list(range(gap_start, date.today().year + 1))
        log.info("Socrata is stale (latest: %s). Fetching CFTC direct for years: %s",
                 socrata_max, years_needed)
        direct_rows = _fetch_cftc_direct(years_needed)

    # ── 3. Merge & deduplicate ────────────────────────────────────────────────
    all_rows = socrata_rows + direct_rows

    # Keep one row per date: prefer cftc_direct (more fresh) over socrata
    by_date: dict[date, dict] = {}
    for row in all_rows:
        d = row["report_date"]
        if d not in by_date or row["source"] == "cftc_direct":
            by_date[d] = row

    # Apply from_date filter (direct files give full-year data regardless)
    result = sorted(by_date.values(), key=lambda r: r["report_date"])
    if from_date:
        result = [r for r in result if r["report_date"] >= from_date]

    log.info("CFTC COT: %d rows total for Gold (Socrata %d + direct %d, deduped)",
             len(result), len(socrata_rows), len(direct_rows))
    return result
