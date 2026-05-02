
import os
import sys
import zipfile
import shutil
import argparse
import pandas as pd
import numpy as np
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
FUNDS_TO_IMPORT = [
    ("152056", "DSP_MULTI_ASSET", "Multi Asset"),
    ("154167", "DSP_MULTI_ASSET_OMNI_FOF", "Multi Asset Omni Fund of Funds"),
]
BASE_URL = "https://www.dspim.com/media/pages/mandatory-disclosures/portfolio-disclosures/"

# Stable identifiers for commodities that have no real ISIN
COMMODITY_ISIN = {
    'gold etcd#': 'GOLD_ETCD_DSP',
    'silver etcd#': 'SILVER_ETCD_DSP',
    'copper etcd#': 'COPPER_ETCD_DSP',
    'gold': 'GOLD_ETCD_DSP',
    'silver': 'SILVER_ETCD_DSP',
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
    if 'gold' in name or 'gold' in sector: return 'gold'
    if 'silver' in name or 'silver' in sector: return 'silver'
    
    # Bond/Debt identification
    if any(k in sector for k in ['debt', 'g-sec', 'sdl', 'treasury', 'sovereign', 'bonds']): return 'bond'
    if any(k in name for k in ['gilt', 'short term', 'debt']): return 'bond'
    
    # Cash identification
    if any(k in name for k in ['cash', 'liquid', 'treps', 'repo', 'receivables', 'payables']): return 'cash'
    
    # Equity identification (including common fund types)
    if any(k in name for k in ['equity', 'limited', 'ltd', 'inc', 'corp', 'cap', 'nifty', 'index', 'etf', 'fmcg', 'healthcare', 'bank', 'flexicap']): return 'equity'
    if any(k in sector for k in ['equity', 'mixed']): return 'equity'
    
    # Fallback for funds in FoF
    if 'fund' in name:
        if any(k in name for k in ['term', 'gilt', 'bond', 'income']): return 'bond'
        return 'equity'
        
    return 'other'


def download_and_extract(url, temp_dir):
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
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    temp_dir.mkdir()

    full_url = BASE_URL + url
    xl_path = download_and_extract(full_url, temp_dir)
    if not xl_path:
        return []

    all_month_holdings = []
    try:
        xl = pd.ExcelFile(xl_path)
        
        for scheme_code, fund_name, sheet_search in FUNDS_TO_IMPORT:
            # Match sheet name
            sheet_name = next((s for s in xl.sheet_names if sheet_search == s or (sheet_search in s and len(s) < len(sheet_search) + 5)), None)
            if not sheet_name:
                continue

            df = pd.read_excel(xl_path, sheet_name=sheet_name, header=None)

            # Find start of data
            data_start_row = 0
            for idx, row in df.iterrows():
                if "Name of Instrument" in str(row.iloc[1]):
                    data_start_row = idx + 1
                    break
            
            if data_start_row == 0: continue

            raw_rows = []
            for i in range(data_start_row, len(df)):
                row = df.iloc[i]
                try:
                    name = str(row.iloc[1]).strip()
                    if not name or name == "nan" or "Total" in name: continue
                    
                    mv_lakhs = float(row.iloc[5])
                    pct_raw = float(row.iloc[6])
                    isin = str(row.iloc[2]).strip()
                    sector = str(row.iloc[3]).strip()
                    raw_rows.append((isin, name, sector, mv_lakhs, pct_raw))
                except:
                    continue

            if not raw_rows:
                continue

            # Robust scale detection: if median is < 0.05, it's almost certainly decimal (0.01 = 1%)
            # because even a small fund rarely has a median holding of 0.05% (0.0005)
            vals = [r[4] for r in raw_rows if not np.isnan(r[4])]
            if not vals: continue
            
            max_val = np.nanmax(vals)
            # If max value is <= 1.0, it's decimal. If > 1.0, it's percentage.
            pct_scale = 100.0 if max_val <= 1.01 else 1.0

            imported_at = datetime.now()
            for isin, name, sector, mv_lakhs, pct_raw in raw_rows:
                if np.isnan(pct_raw): continue
                
                name_lower = name.lower()
                is_valid_isin = len(isin) == 12
                is_commodity = any(k in name_lower for k in ['gold', 'silver', 'copper'])
                is_special = any(k in name_lower for k in ['treps', 'receivables', 'payables'])

                if not (is_valid_isin or is_commodity or is_special):
                    continue

                # Map commodity ISINs
                final_isin = isin
                if not is_valid_isin:
                    if 'gold' in name_lower: final_isin = 'GOLD_ETCD_DSP'
                    elif 'silver' in name_lower: final_isin = 'SILVER_ETCD_DSP'
                    elif 'copper' in name_lower: final_isin = 'COPPER_ETCD_DSP'
                    else: final_isin = name[:20]

                all_month_holdings.append({
                    "scheme_code": scheme_code,
                    "fund_name": fund_name,
                    "as_of_month": as_of_str,
                    "isin": final_isin,
                    "security_name": name,
                    "asset_type": classify_asset(name, sector),
                    "market_value_cr": round(mv_lakhs / 100, 4),
                    "pct_of_nav": round(pct_raw * pct_scale, 4),
                    "imported_at": imported_at,
                })

            pct_sum = sum(h["pct_of_nav"] for h in all_month_holdings if h["fund_name"] == fund_name)
            console.print(f"  [green]→ {fund_name}: {len([h for h in all_month_holdings if h['fund_name'] == fund_name])} holdings, pct_sum={pct_sum:.1f}%[/green]")
            
        return all_month_holdings

    except Exception as e:
        console.print(f"[red]Error parsing {xl_path}: {e}[/red]")
        return []


def run_import(months=None, dry_run=False):
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
        console.print(f"[bold green]✓ Imported {len(rows)} holdings across {len(targets) - len(failed_months)}/{len(targets)} months.[/bold green]")
        
        watermark_rows = []
        for _, fund_name, _ in FUNDS_TO_IMPORT:
            fund_holdings = [h for h in all_holdings if h['fund_name'] == fund_name]
            if fund_holdings:
                last_date = max(datetime.strptime(h['as_of_month'], '%Y-%m-%d').date() for h in fund_holdings)
                watermark_rows.append(['mf_holdings', fund_name, last_date])
        
        if watermark_rows:
            client.insert('market_data.import_watermarks', watermark_rows, column_names=['source', 'symbol', 'last_date'])
            for _, fn, ld in watermark_rows:
                console.print(f"[dim]Watermark set: mf_holdings/{fn} → {ld}[/dim]")
    else:
        console.print("[red]✗ No holdings found to import.[/red]")
    if client: client.close()


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
