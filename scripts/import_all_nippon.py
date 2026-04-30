"""
scripts/import_all_nippon.py
─────────────────────────────
Import Nippon India (formerly Reliance MF) monthly portfolio holdings
into market_data.mf_holdings, modelled after import_all_dsp_equity.py.

Source:
  https://mf.nipponindiaim.com/investor-service/downloads/
  factsheet-portfolio-and-other-disclosures

File format (each monthly XLS, multi-sheet):
  Sheet "Index"  → maps short code → full fund name (used to build scheme map)
  Other sheets   → one per fund
    row 0: col0=scheme_code  col1=full fund name  col7="Index"
    row 1: col1="Monthly Portfolio Statement as on <date>"
    row 3: column headers (ISIN, Name, Industry, Qty, Market Value, % to NAV)
    row 4+: data rows (col0=sec_code, col1=ISIN, col2=name, col3=industry,
                        col5=market value in Lacs, col6=% to NAV as 0-1 proportion)

Market value: Rs. in Lacs  →  Crores: divide by 100
% to NAV   : stored as 0–1 (e.g. 0.0358 = 3.58%) → multiply by 100

Usage
─────
    PYTHONPATH=. python scripts/import_all_nippon.py
    PYTHONPATH=. python scripts/import_all_nippon.py --dry-run
    PYTHONPATH=. python scripts/import_all_nippon.py --test       # first file, no DB
    PYTHONPATH=. python scripts/import_all_nippon.py --from-year 2020
"""

from __future__ import annotations

import argparse
import io
import os
import re
import sys
import time
from datetime import datetime, date
from typing import Optional

import httpx
import pandas as pd
import clickhouse_connect
from rich.console import Console
from rich.progress import Progress

sys.path.append(os.getcwd())
from config.settings import settings

console = Console()

BASE_URL = "https://mf.nipponindiaim.com"
DISCLOSURES_PAGE = (
    "https://mf.nipponindiaim.com/investor-service/downloads/"
    "factsheet-portfolio-and-other-disclosures"
)
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    ),
}
_REQUEST_DELAY: float = 1.0
_ISIN_RE = re.compile(r'^[A-Z]{2}[A-Z0-9]{10}$')

# ── Hardcoded monthly portfolio file list (Jan 2017 → Apr 2026) ───────────────
# (as_of_date, url_path)  — date is the last calendar day of the month

XLS_FILES = [
    # 2017 (Reliance MF era)
    ("2017-01-31", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-31.01.2017.xls"),
    ("2017-02-28", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-28-02-2017.xlsx"),
    ("2017-03-31", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-31-03-2017.xls"),
    ("2017-04-30", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-30-04-2017.xls"),
    ("2017-05-31", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-31.05.2017.xls"),
    ("2017-06-30", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-30-06-2017.xls"),
    ("2017-07-31", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-31.07.2017.xls"),
    ("2017-08-31", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-31-08-2017.xls"),
    ("2017-09-30", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-30.09.2017.xls"),
    ("2017-10-31", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-31.10.2017.xls"),
    ("2017-11-30", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-30.11.2017.xls"),
    ("2017-12-31", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-31.12.2017.xls"),
    # 2018
    ("2018-01-31", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-31.01.2018.xls"),
    ("2018-02-28", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-28.02.2018.xls"),
    ("2018-03-31", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-31.03.2018.xls"),
    ("2018-04-30", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-30.04.2018.xls"),
    ("2018-05-31", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-31.05.2018.xls"),
    ("2018-06-30", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-30.06.2018.xls"),
    ("2018-07-31", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-31.07.2018.xls"),
    ("2018-08-31", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-31.08.2018.xls"),
    ("2018-09-30", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-30.09.2018.xls"),
    ("2018-10-31", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-31-10-2018.xls"),
    ("2018-11-30", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-30.11.2018.xls"),
    ("2018-12-31", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-31-12-2018.xls"),
    # 2019 (Nippon brand takeover mid-year)
    ("2019-01-31", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-31-01-2019.xls"),
    ("2019-02-28", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-28-02-2019.xls"),
    ("2019-03-31", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-31-03-2019.xls"),
    ("2019-04-30", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-30-04-2019.xls"),
    ("2019-05-31", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-31-05-2019.xls"),
    ("2019-06-30", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-30-06-2019.xls"),
    ("2019-07-31", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-31-07-2019.xls"),
    ("2019-08-31", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-31-08-2019.xls"),
    ("2019-09-30", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-30-09-2019.xls"),
    ("2019-10-31", "/InvestorServices/FactsheetsDocuments/Reliance-Monthly-Portfolios-31-10-2019.xls"),
    ("2019-11-30", "/InvestorServices/FactsheetsDocuments/NipponIndia-Monthly-Portfolios-30-11-2019.xls"),
    ("2019-12-31", "/InvestorServices/FactsheetsDocuments/NipponIndia-Monthly-Portfolios-31-12-2019.xls"),
    # 2020
    ("2020-01-31", "/InvestorServices/FactsheetsDocuments/NipponIndia-Monthly-Portfolios-31-01-2020.xls"),
    ("2020-02-29", "/InvestorServices/FactsheetsDocuments/NipponIndia-Monthly-Portfolios-29-02-2020.xls"),
    ("2020-03-31", "/InvestorServices/FactsheetsDocuments/NipponIndia-Monthly-Portfolios-31-03-2020.xls"),
    ("2020-04-30", "/InvestorServices/FactsheetsDocuments/NipponIndia-Monthly-Portfolios-30-04-2020.xls"),
    ("2020-05-31", "/InvestorServices/FactsheetsDocuments/NipponIndia-Monthly-Portfolios-May-2020.xls"),
    ("2020-06-30", "/InvestorServices/FactsheetsDocuments/NipponIndia-Monthly-Portfolios-June-2020.xls"),
    ("2020-07-31", "/InvestorServices/FactsheetsDocuments/NIMF-Monthly-Portfolio-July-2020.xls"),
    ("2020-08-31", "/InvestorServices/FactsheetsDocuments/NIMF-Monthly-Portfolio-Aug-2020.xls"),
    ("2020-09-30", "/InvestorServices/FactsheetsDocuments/NIMF-Monthly-Portfolio-Report-Sep-20.xls"),
    ("2020-10-31", "/InvestorServices/FactsheetsDocuments/NIMF-Monthly-PORTFOLIO_REPORT-Oct-20.xls"),
    ("2021-01-31", "/InvestorServices/FactsheetsDocuments/NIMF-Monthly-Portfolio-Jan-2021.xls"),
    ("2021-04-30", "/InvestorServices/FactsheetsDocuments/Monthly-Portfolio-as-on-30-04-2021-with-Riskometer.xls"),
    ("2021-05-31", "/InvestorServices/FactsheetsDocuments/Monthly-portfolio-May-2021-With-riskometer.xls"),
    ("2021-10-31", "/InvestorServices/FactsheetsDocuments/Monthly-portfolio-Oct-21.xls"),
    ("2021-11-30", "/InvestorServices/FactsheetsDocuments/Monthly-Portfolio-as-on-30-11-2021-with-Riskometer.xlsx"),
    ("2021-12-31", "/InvestorServices/FactsheetsDocuments/NIMF-Monthly-Portfolio-Dec-21-With-Riskometer.xls"),
    # 2022
    ("2022-03-31", "/InvestorServices/FactsheetsDocuments/Monthly-Portfolio-Mar-2022.xls"),
    ("2022-05-31", "/InvestorServices/FactsheetsDocuments/NIMF-Monthly-Portfolio-May-2022.xls"),
    ("2022-06-30", "/InvestorServices/FactsheetsDocuments/NIMF-Monthly-Portfolio-30062022.xls"),
    ("2022-07-31", "/InvestorServices/FactsheetsDocuments/MONTHLY-PORTFOLIO-31st-JULY-2022.xls"),
    ("2022-08-31", "/InvestorServices/FactsheetsDocuments/MONTHLY-PORTFOLIO-AUGUST-2022.xls"),
    ("2022-09-30", "/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-SEP-2022.xls"),
    ("2022-10-31", "/InvestorServices/FactsheetsDocuments/MONTHLY-PORTFOLIO-Oct-22.xls"),
    ("2022-11-30", "/InvestorServices/FactsheetsDocuments/MONTHLY-PORTFOLIO-NOV-2022.xls"),
    ("2022-12-31", "/InvestorServices/FactsheetsDocuments/MONTHLY-PORTFOLIO-DEC-22.xls"),
    # 2023
    ("2023-01-31", "/InvestorServices/FactsheetsDocuments/MONTHLY-PORTFOLIO-JAN-23.xls"),
    ("2023-02-28", "/InvestorServices/FactsheetsDocuments/MONTHLY-PORTFOLIO-FEB-23.xls"),
    ("2023-03-31", "/InvestorServices/FactsheetsDocuments/MONTHLY-PORTFOLIO-MAR-23.xls"),
    ("2023-04-30", "/InvestorServices/FactsheetsDocuments/MONTHLY-PORTFOLIO-APR-23.xls"),
    ("2023-05-31", "/InvestorServices/FactsheetsDocuments/MONTHLY-PORTFOLIO-MAY-2023.xls"),
    ("2023-06-30", "/InvestorServices/FactsheetsDocuments/NIMF_MONTHLY_PORTFOLIO_June-2023.xls"),
    ("2023-07-31", "/InvestorServices/FactsheetsDocuments/MONTHLY-PORTFOLIO-JULY-2023.xls"),
    ("2023-08-31", "/InvestorServices/FactsheetsDocuments/MONTHLY-PORTFOLIO-AUGUST-2023.xls"),
    ("2023-09-30", "/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-Sep-23.xls"),
    ("2023-10-31", "/InvestorServices/FactsheetsDocuments/MONTHLY-PORTFOLIO-OCTOBER-2023.xls"),
    ("2023-11-30", "/InvestorServices/FactsheetsDocuments/MONTHLY-PORTFOLIO-NOV-23.xls"),
    ("2023-12-31", "/InvestorServices/FactsheetsDocuments/MONTHLY-PORTFOLIO-DEC-23.xls"),
    # 2024
    ("2024-01-31", "/InvestorServices/FactsheetsDocuments/MONTHLY-PORTFOLIO-JAN-2024.xls"),
    ("2024-02-29", "/InvestorServices/FactsheetsDocuments/MONTHLY-PORTFOLIO-REPORT-FEB-24.xls"),
    ("2024-03-31", "/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-REPORT-March-24.xls"),
    ("2024-04-30", "/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-APRIL-2024.xls"),
    # File deleted from Nippon server; archived copy from Wayback Machine (20240704)
    ("2024-05-31", "https://web.archive.org/web/20240704135859/https://mf.nipponindiaim.com/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-May-24.xlsx"),
    ("2024-06-30", "/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-June-24.xls"),
    ("2024-07-31", "/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-31-July-24.xls"),
    ("2024-08-31", "/InvestorServices/FactsheetsDocuments/NIMF_MONTHLY_PORTFOLIO_31-Aug-24.xls"),
    ("2024-09-30", "/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-30-Sep-24.xls"),
    ("2024-10-31", "/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-31-Oct-24.xls"),
    ("2024-11-30", "/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-30-Nov-2024.xls"),
    ("2024-12-31", "/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-31-Dec-24.xls"),
    # 2025
    ("2025-01-31", "/InvestorServices/FactsheetsDocuments/NIMF_MONTHLY_PORTFOLIO_31-Jan-25.xls"),
    ("2025-02-28", "/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-28-Feb-25.xls"),
    ("2025-03-31", "/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-31-Mar-25.xls"),
    ("2025-04-30", "/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-30-April-25.xls"),
    ("2025-05-31", "/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-31-May-25.xls"),
    ("2025-06-30", "/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-30-June-25.xls"),
    ("2025-07-31", "/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-31-July-25.xls"),
    ("2025-08-31", "/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-31-Aug-25.xls"),
    ("2025-09-30", "/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-30-Sep-25.xls"),
    ("2025-10-31", "/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-31-Oct-25.xls"),
    ("2025-11-30", "/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-Nov-25.xls"),
    ("2025-12-31", "/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-31-Dec-25.xls"),
    # 2026
    ("2026-01-31", "/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-31-Jan-26.xls"),
    ("2026-02-28", "/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-28-Feb-26.xls"),
    ("2026-03-31", "/InvestorServices/FactsheetsDocuments/NIMF-MONTHLY-PORTFOLIO-31-Mar-26.xls"),
]


# ── Fund name normalisation ───────────────────────────────────────────────────

def _normalise_fund_name(raw: str) -> str:
    """Convert full fund name to a short stable identifier."""
    # Strip parenthetical category description and newlines
    name = re.sub(r'\s*[\(\n].*$', '', raw, flags=re.DOTALL).strip()
    # Replace special chars
    name = re.sub(r'[&/\\]', '_AND_', name)
    name = re.sub(r'[^A-Za-z0-9_ ]', '', name)
    name = re.sub(r'\s+', '_', name).upper()
    name = re.sub(r'_+', '_', name).strip('_')
    # Prefix
    if not (name.startswith('NIPPON') or name.startswith('RELIANCE') or name.startswith('NIMF')):
        name = 'NIPPON_' + name
    return name[:80]   # cap length


# ── Asset classification ──────────────────────────────────────────────────────

def classify_asset(name: str, industry: str) -> str:
    combined = f"{name} {industry}".lower()
    if any(k in combined for k in ('gold', 'silver', 'precious')):
        return 'gold'
    if any(k in combined for k in ('bond', 'debt', 'gilt', 'g-sec', 'sdl',
                                    'treasury', 'ncd', 'debenture', 'goi',
                                    'tbill', 'commercial paper', 'trep', 'repo',
                                    'certificate of deposit', 'cblo', 'fixed income')):
        return 'bond'
    if any(k in combined for k in ('cash', 'liquid', 'overnight', 'money market')):
        return 'cash'
    if any(k in combined for k in ('equity', 'nifty', 'sensex', 'cap', 'etf',
                                    ' ltd', ' limited', 'bank', 'finance')):
        return 'equity'
    return 'other'


# ── Process one monthly file ──────────────────────────────────────────────────

def process_month(as_of_str: str, path: str, client: httpx.Client) -> list[dict]:
    url = path if path.startswith('http') else BASE_URL + path
    try:
        resp = client.get(url)
        resp.raise_for_status()
    except Exception as exc:
        console.print(f'  [red]Download failed: {exc}[/red]')
        return []

    engine = 'xlrd' if path.lower().endswith('.xls') else 'openpyxl'
    try:
        xl = pd.ExcelFile(io.BytesIO(resp.content), engine=engine)
    except Exception:
        # Many post-2022 files are xlsx despite .xls extension — try the other engine
        alt_engine = 'openpyxl' if engine == 'xlrd' else 'xlrd'
        try:
            xl = pd.ExcelFile(io.BytesIO(resp.content), engine=alt_engine)
        except Exception as exc:
            console.print(f'  [red]Cannot parse Excel: {exc}[/red]')
            return []

    as_of_date = datetime.strptime(as_of_str, '%Y-%m-%d').date()
    sheets = [s for s in xl.sheet_names if s.lower() != 'index']
    all_holdings: list[dict] = []
    imported_at = datetime.now()

    for sheet in sheets:
        try:
            df = xl.parse(sheet, header=None)
        except Exception:
            continue
        if df.shape[0] < 5 or df.shape[1] < 7:
            continue

        row0 = df.iloc[0].tolist()
        scheme_code = str(row0[0]).strip() if str(row0[0]) != 'nan' else sheet
        fund_name_raw = str(row0[1]).strip() if len(row0) > 1 else sheet
        fund_name = _normalise_fund_name(fund_name_raw)

        # Collect data rows: identify by ISIN pattern in col 1
        raw_rows: list[tuple] = []
        for _, row in df.iterrows():
            vals = row.tolist()
            if len(vals) < 7:
                continue
            isin = str(vals[1]).strip()
            if not _ISIN_RE.match(isin):
                continue
            name = str(vals[2]).strip()
            industry = str(vals[3]).strip()
            try:
                mv_lacs = float(vals[5])
            except (TypeError, ValueError):
                mv_lacs = 0.0
            try:
                pct_raw = float(vals[6])
            except (TypeError, ValueError):
                pct_raw = 0.0
            raw_rows.append((isin, name, industry, mv_lacs, pct_raw))

        if not raw_rows:
            continue

        # Detect % scale: if all values < 2 they are 0-1 proportions → ×100
        valid_pcts = [r[4] for r in raw_rows if r[4] == r[4] and r[4] != 0]
        max_pct = max(valid_pcts) if valid_pcts else 0.0
        pct_scale = 100.0 if max_pct <= 2.0 else 1.0

        sheet_holdings: list[dict] = []
        for isin, name, industry, mv_lacs, pct_raw in raw_rows:
            sheet_holdings.append({
                'scheme_code':     scheme_code,
                'fund_name':       fund_name,
                'as_of_month':     as_of_date,
                'isin':            isin,
                'security_name':   name,
                'asset_type':      classify_asset(name, industry),
                'market_value_cr': round(mv_lacs / 100, 4),   # Lacs → Crores
                'pct_of_nav':      round(pct_raw * pct_scale, 4),
                'imported_at':     imported_at,
            })

        pct_sum = sum(h['pct_of_nav'] for h in sheet_holdings)
        color = 'yellow' if pct_sum > 105 else 'green'
        console.print(
            f'  [{color}]  {fund_name}: {len(sheet_holdings)} holdings, '
            f'pct_sum={pct_sum:.1f}% ({as_of_str})[/{color}]'
        )
        all_holdings.extend(sheet_holdings)

    return all_holdings


# ── Main import ───────────────────────────────────────────────────────────────

def _already_imported_months(db_client) -> set[date]:
    """Return set of as_of_month dates already in mf_holdings for Nippon/Reliance funds."""
    try:
        result = db_client.query(
            "SELECT DISTINCT as_of_month FROM market_data.mf_holdings "
            "WHERE fund_name LIKE 'NIPPON%' OR fund_name LIKE 'RELIANCE%' OR fund_name LIKE 'NIMF%'"
        )
        return {row[0] for row in result.result_rows}
    except Exception:
        return set()


def run_import(
    months: list[tuple] | None = None,
    from_year: int = 2017,
    dry_run: bool = False,
    full_reimport: bool = False,
) -> None:
    candidates = months or [(d, p) for d, p in XLS_FILES
                            if int(d[:4]) >= from_year]

    db_client = None
    if not dry_run:
        db_client = clickhouse_connect.get_client(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            username=settings.clickhouse_user,
            password=settings.clickhouse_password,
            database=settings.clickhouse_database,
        )

    # Delta sync: skip months already in ClickHouse unless full reimport requested
    if db_client and not full_reimport:
        done = _already_imported_months(db_client)
        if done:
            targets = [(d, p) for d, p in candidates
                       if datetime.strptime(d, '%Y-%m-%d').date() not in done]
            console.print(
                f'[dim]Delta sync: {len(done)} months already in DB, '
                f'{len(candidates) - len(targets)} skipped, '
                f'{len(targets)} to fetch.[/dim]'
            )
        else:
            targets = candidates
    else:
        targets = candidates

    if not targets:
        console.print('[bold green]✓ All months already imported — nothing to do.[/bold green]')
        if db_client:
            db_client.close()
        return

    console.print(
        f'[bold cyan]Nippon India import — {len(targets)} monthly files '
        f'(from {from_year})[/bold cyan]'
    )

    all_holdings: list[dict] = []
    failed_months: list[str] = []

    with httpx.Client(headers=_HEADERS, timeout=60, follow_redirects=True) as http:
        with Progress() as progress:
            task = progress.add_task(
                '[cyan]Importing Nippon India monthly portfolios...',
                total=len(targets),
            )
            for i, (as_of, path) in enumerate(targets):
                progress.console.print(f'Processing [bold]{as_of}[/bold] → {path.split("/")[-1]}')
                holdings = process_month(as_of, path, http)
                if holdings:
                    all_holdings.extend(holdings)
                else:
                    failed_months.append(as_of)
                if i < len(targets) - 1:
                    time.sleep(_REQUEST_DELAY)
                progress.advance(task)

    if failed_months:
        console.print(
            f'[yellow]Warning: {len(failed_months)} months failed: {failed_months}[/yellow]'
        )

    if dry_run:
        funds  = len({h['fund_name'] for h in all_holdings})
        months_imported = len({h['as_of_month'] for h in all_holdings})
        console.print(
            f'[bold blue]DRY RUN: {len(all_holdings):,} holdings, '
            f'{funds} funds, {months_imported} months — nothing inserted.[/bold blue]'
        )
        return

    if not all_holdings:
        console.print('[red]✗ No holdings to insert.[/red]')
        if db_client:
            db_client.close()
        return

    rows = [
        (
            h['scheme_code'], h['fund_name'],
            h['as_of_month'],
            h['isin'], h['security_name'], h['asset_type'],
            h['market_value_cr'], h['pct_of_nav'], h['imported_at'],
        )
        for h in all_holdings
    ]
    db_client.insert(
        'market_data.mf_holdings',
        rows,
        column_names=[
            'scheme_code', 'fund_name', 'as_of_month', 'isin',
            'security_name', 'asset_type', 'market_value_cr',
            'pct_of_nav', 'imported_at',
        ],
    )
    console.print(
        f'[bold green]✓ Inserted {len(rows):,} holdings for '
        f'{len({h["fund_name"] for h in all_holdings})} funds across '
        f'{len({h["as_of_month"] for h in all_holdings})} months.[/bold green]'
    )

    # Watermarks: latest date per fund
    fund_latest: dict[str, date] = {}
    for h in all_holdings:
        fn = h['fund_name']
        if fn not in fund_latest or h['as_of_month'] > fund_latest[fn]:
            fund_latest[fn] = h['as_of_month']

    wm_rows = [['mf_holdings', fn, d] for fn, d in fund_latest.items()]
    db_client.insert(
        'market_data.import_watermarks',
        wm_rows,
        column_names=['source', 'symbol', 'last_date'],
    )
    console.print(f'[dim]Watermarks set for {len(wm_rows)} funds.[/dim]')
    db_client.close()


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Import Nippon India AMC monthly portfolio holdings (2017–present)'
    )
    parser.add_argument('--from-year', type=int, default=2017,
                        help='Earliest year to import (default: 2017)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Download and parse but skip DB insert')
    parser.add_argument('--full', action='store_true',
                        help='Re-import all months, ignoring watermarks')
    parser.add_argument('--test', action='store_true',
                        help='Process first file only, no DB insert')
    args = parser.parse_args()

    if args.test:
        run_import(months=XLS_FILES[:1], dry_run=True)
    elif args.dry_run:
        run_import(from_year=args.from_year, dry_run=True)
    else:
        run_import(from_year=args.from_year, full_reimport=args.full)
