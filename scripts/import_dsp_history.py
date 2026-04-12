
import os
import sys
import zipfile
import shutil
import argparse
import pandas as pd
import requests
import clickhouse_connect
from datetime import datetime
from pathlib import Path
from rich.console import Console
from rich.progress import Progress

# Add src to path
sys.path.append(os.getcwd())
from config.settings import settings

console = Console()

# Configuration
SCHEME_CODE = "152056"
FUND_NAME = "DSP_MULTI_ASSET"
BASE_URL = "https://www.dspim.com/media/pages/mandatory-disclosures/portfolio-disclosures/"

# Stable identifiers for commodities that have no real ISIN
COMMODITY_ISIN = {
    'gold etcd#': 'GOLD_ETCD_DSP',
    'silver etcd#': 'SILVER_ETCD_DSP',
    'copper etcd#': 'COPPER_ETCD_DSP',
}

# (as_of_date, url_suffix) — Sep 2023 through Mar 2026 (31 months)
ZIP_FILES = [
    ("2023-09-30", "ef8b385bd2-1757771557/monthend-portfolio-september-2023.zip"),
    ("2023-10-31", "c4ca90394b-1757771557/monthend-portfolio-october-2023.zip"),
    ("2023-11-30", "1ee0bbaa37-1757771557/monthend-portfolio-november-2023.zip"),
    ("2023-12-31", "6755b783ec-1757771557/monthend-portfolio-december-31-2023.zip"),
    ("2024-01-31", "6fd07c04dc-1757771557/monthend-portfolio-january-2024.zip"),
    ("2024-02-29", "2ebdbd8d27-1757771557/monthend-portfolio-february-2024.zip"),
    ("2024-03-31", "1bfaab3f7d-1757771557/monthend-portfolio-march-31-2024.zip"),
    ("2024-04-30", "376234d7a6-1757771557/monthend-portfolio-april-2024.zip"),
    ("2024-05-31", "4a937682c8-1757771557/monthend-portfolio-may-2024.zip"),
    ("2024-06-30", "901b3e3f5a-1757771557/monthend-portfolio-june-2024.zip"),
    ("2024-07-31", "1352cabccd-1757771557/monthend-portfolio-july-2024.zip"),
    ("2024-08-31", "0bb3a4390d-1757771557/monthend-portfolio-august-2024.zip"),
    ("2024-09-30", "d22c22489a-1757771557/monthend-portfolio-september-30-2024.zip"),
    ("2024-10-31", "fb740025a4-1757771557/monthend-portfolio-october-31-2024.zip"),
    ("2024-11-30", "9ae79acb70-1757771557/monthend-portfolio-november-30-2024.zip"),
    ("2024-12-31", "b43e0a72c2-1757771557/monthend-portfolio-december-31-2024.zip"),
    ("2025-01-31", "758c5da9c1-1757771557/monthend-portfolio-january-31-2025.zip"),
    ("2025-02-28", "f715dc48e9-1757771557/monthend-portfolio-february-28-2025.zip"),
    ("2025-03-31", "339326760c-1757771557/monthend-portfolio-march-31-2025.zip"),
    ("2025-04-30", "d68a67cdea-1757771557/monthend-portfolio-april-30-2025.zip"),
    ("2025-05-31", "79859b96a0-1757771557/monthend-portfolio-may-2025.zip"),
    ("2025-06-30", "8f0e90fd0c-1757771557/monthend-portfolio-june-2025.zip"),
    ("2025-07-31", "b68e3ec871-1757771557/monthend-portfolio-july-2025.zip"),
    ("2025-08-31", "6eda8470d5-1757771557/monthend-portfolio-august-2025.zip"),
    ("2025-09-30", "754f55d76e-1760032442/monthend-portfolio-september-30-2025.zip"),
    ("2025-10-31", "d155b953f0-1765445657/monthend-portfolio-october-2025.zip"),
    ("2025-11-30", "0e5f7b1d70-1765381448/monthend-portfolio-november-2025.zip"),
    ("2025-12-31", "b3e426eed3-1768654090/monthend-portfolio-december-31-2025.zip"),
    ("2026-01-31", "fd9bf9ce01-1770662546/monthend-portfolio-january-31-2026.zip"),
    ("2026-02-28", "1c747fb85f-1773156944/monthend-portfolio-february-28-2026.zip"),
    ("2026-03-31", "b1bcdfd489-1775749401/monthend-portfolio-31march2026.zip"),
]


def classify_asset(name, sector):
    name = str(name).lower()
    sector = str(sector).lower()
    if any(k in name for k in ['gold', 'silver']): return 'gold'
    if any(k in sector for k in ['gold', 'silver']): return 'gold'
    if any(k in sector for k in ['debt', 'g-sec', 'sdl', 'treasury']): return 'bond'
    if any(k in name for k in ['cash', 'liquid', 'treps', 'repo']): return 'cash'
    if any(k in name for k in ['equity', 'limited', 'ltd', 'inc', 'corp']): return 'equity'
    return 'other'


def download_and_extract(url, temp_dir):
    """Download zip, extract to temp_dir, return the xlsx that contains a 'Multi Asset' sheet.

    DSP changed the zip's Excel file name between 2023 and later years, so we scan
    all extracted xlsx files instead of relying on a fixed filename pattern.
    """
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        zip_path = temp_dir / "temp.zip"
        zip_path.write_bytes(r.content)

        with zipfile.ZipFile(zip_path, 'r') as zf:
            zf.extractall(temp_dir)

        for path in sorted(temp_dir.rglob("*.xlsx")):
            try:
                xl = pd.ExcelFile(path)
                if any('Multi Asset' in s for s in xl.sheet_names):
                    return path
            except Exception:
                continue
    except Exception as e:
        console.print(f"[red]Error downloading {url}: {e}[/red]")
    return None


def process_month(as_of_str, url):
    temp_dir = Path("temp_backfill")
    # Clean slate — rmtree handles nested dirs safely
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    temp_dir.mkdir()

    full_url = BASE_URL + url
    xl_path = download_and_extract(full_url, temp_dir)
    if not xl_path:
        return []

    try:
        xl = pd.ExcelFile(xl_path)
        sheet_name = next((s for s in xl.sheet_names if 'Multi Asset' in s), None)
        if not sheet_name:
            console.print(f"[yellow]  Sheet 'Multi Asset' not found in {xl_path.name}[/yellow]")
            return []

        df = pd.read_excel(xl_path, sheet_name=sheet_name, header=None)

        # Locate header row: look for "Name of Instrument" in column B (index 1)
        data_start_row = 6  # safe fallback for older formats
        for idx, row in df.iterrows():
            if str(row.iloc[1]).strip() == "Name of Instrument":
                data_start_row = idx + 1
                break

        # --- Pass 1: collect raw numeric rows ---
        raw_rows = []
        for i in range(data_start_row, len(df)):
            row = df.iloc[i]
            try:
                mv_lakhs = float(row.iloc[5])
                pct_raw = float(row.iloc[6])
            except (ValueError, TypeError):
                continue
            isin = str(row.iloc[2]).strip()
            name = str(row.iloc[1]).strip()
            sector = str(row.iloc[3]).strip()
            raw_rows.append((isin, name, sector, mv_lakhs, pct_raw))

        if not raw_rows:
            console.print(f"[yellow]  No numeric rows found for {as_of_str}[/yellow]")
            return []

        # --- Detect pct scale once per sheet ---
        # If any value > 1 the column is already in percentage form; else it's decimal.
        # Guard: if all values are 0 (empty/corrupt sheet), default to percentage form.
        max_pct = max(r[4] for r in raw_rows)
        pct_scale = 1.0 if max_pct > 1.0 else (100.0 if max_pct > 0.0 else 1.0)

        # --- Pass 2: build holdings ---
        holdings = []
        imported_at = datetime.now()
        for isin, name, sector, mv_lakhs, pct_raw in raw_rows:
            is_valid_isin = len(isin) == 12
            name_lower = name.lower()
            is_commodity = name_lower in COMMODITY_ISIN

            if not (is_valid_isin or is_commodity):
                continue

            holdings.append({
                "scheme_code": SCHEME_CODE,
                "fund_name": FUND_NAME,
                "as_of_month": as_of_str,
                "isin": isin if is_valid_isin else COMMODITY_ISIN[name_lower],
                "security_name": name,
                "asset_type": classify_asset(name, sector),
                "market_value_cr": round(mv_lakhs / 100, 4),   # Lakhs → Crores
                "pct_of_nav": round(pct_raw * pct_scale, 4),
                "imported_at": imported_at,
            })

        pct_sum = sum(h["pct_of_nav"] for h in holdings)
        pct_color = "yellow" if pct_sum > 100 else "green"
        pct_note = " ⚠ >100% (derivative margin rows included)" if pct_sum > 100 else ""
        console.print(
            f"  [{pct_color}]→ {len(holdings)} holdings, pct_sum={pct_sum:.1f}%"
            f"{pct_note} (scale={pct_scale}, month={as_of_str})[/{pct_color}]"
        )
        return holdings

    except Exception as e:
        console.print(f"[red]Error parsing {xl_path}: {e}[/red]")
        return []


def run_import(months=None, dry_run=False):
    """
    months: list of (as_of_str, url) tuples to import; defaults to all ZIP_FILES.
    dry_run: parse and print but do not insert into ClickHouse.
    """
    targets = months or ZIP_FILES
    client = None
    if not dry_run:
        client = clickhouse_connect.get_client(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            username=settings.clickhouse_user,
            password=settings.clickhouse_password,
            database=settings.clickhouse_database,
        )

    all_holdings = []
    failed_months = []

    with Progress() as progress:
        task = progress.add_task("[cyan]Importing DSP Multi Asset History...", total=len(targets))
        for as_of, url in targets:
            progress.console.print(f"Processing [bold]{as_of}[/bold]...")
            month_holdings = process_month(as_of, url)
            if month_holdings:
                all_holdings.extend(month_holdings)
            else:
                failed_months.append(as_of)
            progress.advance(task)

    if failed_months:
        console.print(f"[yellow]Warning: {len(failed_months)} months returned no data: {failed_months}[/yellow]")

    if dry_run:
        console.print(f"[bold blue]DRY RUN: {len(all_holdings)} holdings parsed across {len(targets)} months — nothing inserted.[/bold blue]")
        return

    if all_holdings:
        rows = [
            (
                h['scheme_code'], h['fund_name'],
                datetime.strptime(h['as_of_month'], '%Y-%m-%d').date(),
                h['isin'], h['security_name'], h['asset_type'],
                h['market_value_cr'], h['pct_of_nav'], h['imported_at'],
            )
            for h in all_holdings
        ]
        client.insert(
            'market_data.mf_holdings', rows,
            column_names=['scheme_code', 'fund_name', 'as_of_month', 'isin', 'security_name',
                          'asset_type', 'market_value_cr', 'pct_of_nav', 'imported_at'],
        )
        console.print(
            f"[bold green]✓ Imported {len(rows)} holdings across "
            f"{len(targets) - len(failed_months)}/{len(targets)} months.[/bold green]"
        )
        # Write per-fund watermark so the CLI Morningstar path can identify
        # which months have already been backfilled from the DSP source.
        last_date = max(
            datetime.strptime(h['as_of_month'], '%Y-%m-%d').date()
            for h in all_holdings
        )
        client.insert(
            'market_data.import_watermarks',
            [['mf_holdings', FUND_NAME, last_date]],
            column_names=['source', 'symbol', 'last_date'],
        )
        console.print(f"[dim]Watermark set: mf_holdings/{FUND_NAME} → {last_date}[/dim]")
    else:
        console.print("[red]✗ No holdings found to import.[/red]")

    client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Import DSP Multi Asset historical holdings")
    parser.add_argument("--test", action="store_true", help="Run only the first month (no DB insert)")
    parser.add_argument("--dry-run", action="store_true", help="Parse all months but skip DB insert")
    args = parser.parse_args()

    if args.test:
        run_import(months=ZIP_FILES[:1], dry_run=True)
    elif args.dry_run:
        run_import(dry_run=True)
    else:
        run_import()
