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

DSP_SCHEME_MAP = {
    'Aggressive Hybrid': ('119019', 'DSP_AGGRESSIVE_HYBRID'),
    'Flexi Cap': ('119076', 'DSP_FLEXI_CAP'),
    'Large Cap': ('119250', 'DSP_LARGE_CAP'),
    'Large & Mid Cap': ('119218', 'DSP_LARGE_AND_MID_CAP'),
    'TIGER': ('119247', 'DSP_TIGER'),
    'MIDCAP': ('119071', 'DSP_MID_CAP'),
    'TAX': ('119242', 'DSP_ELSS_TAX_SAVER'),
    'SMALLCAP': ('119212', 'DSP_SMALL_CAP'),
    'World Gold Mining FOF': ('119277', 'DSP_WORLD_GOLD_MINING_FOF'),
    'NRNEF': ('119028', 'DSP_NRNEF'),
    'Global Clean Energy FOF': ('119275', 'DSP_GLOBAL_CLEAN_ENERGY_FOF'),
    'Focused': ('119096', 'DSP_FOCUSED'),
    'World Mining FOF': ('119279', 'DSP_WORLD_MINING_FOF'),
    'US Specific Equity FoF': ('119252', 'DSP_US_EQUITY_FOF'),
    'DAAF': ('126393', 'DSP_DYNAMIC_ASSET_ALLOCATION'),
    'ESF': ('136567', 'DSP_EQUITY_SAVINGS'),
    'EQUALNIFTY50': ('141877', 'DSP_NIFTY_50_EQUAL_WEIGHT_INDEX'),
    'ARBITRAGE': ('142283', 'DSP_ARBITRAGE'),
    'HEALTHCARE': ('145454', 'DSP_HEALTHCARE'),
    'NIFTY50INDEX': ('146376', 'DSP_NIFTY_50_INDEX'),
    'NIFTYNEXT50INDEX': ('146381', 'DSP_NIFTY_NEXT_50_INDEX'),
    'QUANT': ('147306', 'DSP_QUANT'),
    'VALUE': ('148595', 'DSP_VALUE'),
    'Nifty 50 Equal ETF': ('149286', 'DSP_NIFTY_50_EQUAL_WEIGHT_ETF'),
    'Nifty 50 ETF': ('149392', 'DSP_NIFTY_50_ETF'),
    'Midcap 150 Quality 50 ETF': ('149403', 'DSP_NIFTY_MIDCAP_150_QUALITY_50_ETF'),
    'Global Innovation': ('149816', 'DSP_GLOBAL_INNOVATION_FOF'),
    'NIFTY MIDCAP 150 Q50': ('150428', 'DSP_NIFTY_MIDCAP_150_QUALITY_50_INDEX'),
    'SILVER ETF': ('150523', 'DSP_SILVER_ETF'),
    'Nifty Bank ETF': ('151262', 'DSP_NIFTY_BANK_ETF'),
    'GOLD ETF': ('151737', 'DSP_GOLD_ETF'),
    'Nifty IT ETF': ('151820', 'DSP_NIFTY_IT_ETF'),
    'BSE Sensex ETF': ('151886', 'DSP_BSE_SENSEX_ETF'),
    'Nifty PSU Bank ETF': ('151888', 'DSP_NIFTY_PSU_BANK_ETF'),
    'Nifty Private Bank ETF': ('151887', 'DSP_NIFTY_PRIVATE_BANK_ETF'),
    'Multi Asset': ('152056', 'DSP_MULTI_ASSET'),
    'GOLD ETF FOF': ('152183', 'DSP_GOLD_ETF_FOF'),
    'Banking and Financial Services': ('152206', 'DSP_BANKING_FINANCIAL_SERVICES'),
    'Nifty Smallcap250 Quality 50': ('152243', 'DSP_NIFTY_SMALLCAP_250_QUALITY_50_INDEX'),
    'Multicap Fund': ('152310', 'DSP_MULTICAP'),
    'Healthcare ETF': ('152306', 'DSP_HEALTHCARE_ETF'),
    'Nifty Bank Index': ('152654', 'DSP_NIFTY_BANK_INDEX'),
    'Nifty Top 10 Equal': ('152814', 'DSP_NIFTY_TOP_10_EQUAL_INDEX'),
    'Nifty Top 10 Equal ETF': ('152812', 'DSP_NIFTY_TOP_10_EQUAL_ETF'),
    'Business Cycle Fund': ('153121', 'DSP_BUSINESS_CYCLE'),
    'Sensex Next 30 ETF': ('153228', 'DSP_SENSEX_NEXT_30_ETF'),
    'Sensex Next 30 Index': ('153219', 'DSP_SENSEX_NEXT_30_INDEX'),
    'Nifty Pvt Bank Index': ('153348', 'DSP_NIFTY_PRIVATE_BANK_INDEX'),
    'Silver ETF FOF': ('153487', 'DSP_SILVER_ETF_FOF'),
    'Nifty Healthcare Index': ('153594', 'DSP_NIFTY_HEALTHCARE_INDEX'),
    'Nifty IT Index': ('153590', 'DSP_NIFTY_IT_INDEX'),
    'Nifty500 Flexicap Qlty30': ('153801', 'DSP_NIFTY_500_FLEXICAP_QUALITY_30_INDEX'),
    'Nifty500 Flexicap Qlty30 ETF': ('153874', 'DSP_NIFTY_500_FLEXICAP_QUALITY_30_ETF'),
    'MSCI India ETF': ('153975', 'DSP_MSCI_INDIA_ETF'),
    'Nifty Midcap 150 Index': ('154014', 'DSP_NIFTY_MIDCAP_150_INDEX'),
    'Nifty Smallcap 250 ETF': ('154024', 'DSP_NIFTY_SMALLCAP_250_ETF'),
    'Nifty Smallcap 250 Index': ('154018', 'DSP_NIFTY_SMALLCAP_250_INDEX'),
    'Nifty Midcap 150 ETF': ('154025', 'DSP_NIFTY_MIDCAP_150_ETF'),
    'Nifty 500 Index': ('154088', 'DSP_NIFTY_500_INDEX'),
    'Nifty Next 50 ETF': ('154087', 'DSP_NIFTY_NEXT_50_ETF'),
    'BSE Top 10 Banks ETF': ('154256', 'DSP_BSE_TOP_10_BANKS_ETF'),
    'Multi Asset Omni Fund of Funds': ('154167', 'DSP_MULTI_ASSET_OMNI_FOF'),
}

def classify_asset(name, sector):
    name = str(name).lower()
    sector = str(sector).lower()
    combined = f"{name} {sector}"
    if any(k in combined for k in ['gold', 'silver', 'precious metal']): return 'gold'
    if any(k in combined for k in ['debt', 'gilt', 'short term', 'treasury', 'g-sec', 'sdl', 'ncd', 'debenture', 'bond', 'fixed income', 'goi']): return 'bond'
    if any(k in combined for k in ['cash', 'liquid', 'treps', 'repo', 'overnight']): return 'cash'
    if any(k in combined for k in ['equity', 'nifty', 'sensex', 'cap', 'growth', 'healthcare', 'it', 'fmcg', 'bank', 'psu', 'midcap', 'smallcap', 'top 10', 'flexicap', 'limited', 'ltd', 'inc', 'corp', ' sa', 'ord', 'adr', 'units']): return 'equity'
    if any(k in combined for k in ['etf', 'fund']): return 'equity'
    return 'other'


def download_and_extract(url, temp_dir):
    """Download zip, extract to temp_dir, return the xlsx that contains DSP equity portfolios.
    We just return the first parsed xlsx since it typically contains all the sheets.
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
                if any(s in DSP_SCHEME_MAP for s in xl.sheet_names):
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

    try:
        xl = pd.ExcelFile(xl_path)
        all_holdings = []
        
        for sheet_name in xl.sheet_names:
            if sheet_name not in DSP_SCHEME_MAP:
                continue
                
            scheme_code, fund_name = DSP_SCHEME_MAP[sheet_name]
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
                name = str(row.iloc[1]).strip()

                # Skip summary/total rows
                if not name or name.lower() in ('nan', 'none', 'total', 'sub total', 'grand total', 'subtotal') or name.startswith('Total '):
                    continue

                try:
                    if pd.isna(row.iloc[5]) or pd.isna(row.iloc[6]):
                        continue
                    mv_lakhs = float(row.iloc[5])
                    pct_raw = float(row.iloc[6])
                except (ValueError, TypeError):
                    continue
                isin = str(row.iloc[2]).strip()
                sector = str(row.iloc[3]).strip()
                raw_rows.append((isin, name, sector, mv_lakhs, pct_raw))
            if not raw_rows:
                console.print(f"[yellow]  No numeric rows found for {fund_name} in {as_of_str}[/yellow]")
                continue

            # --- Detect pct scale once per sheet ---
            valid_pcts = [r[4] for r in raw_rows if not pd.isna(r[4])]
            max_pct = max(valid_pcts) if valid_pcts else 0.0
            pct_scale = 1.0 if max_pct > 1.0 else (100.0 if max_pct > 0.0 else 1.0)
            
            # --- Pass 2: build holdings ---
            imported_at = datetime.now()
            sheet_holdings = []
            for isin, name, sector, mv_lakhs, pct_raw in raw_rows:
                is_valid_isin = len(isin) == 12
                name_lower = name.lower()
                is_commodity = name_lower in COMMODITY_ISIN
                
                # Determine final ISIN for the record
                if is_valid_isin:
                    final_isin = isin
                elif is_commodity:
                    final_isin = COMMODITY_ISIN[name_lower]
                elif name:
                    # Generic placeholder for cash/arbitrage/others without ISIN
                    final_isin = f"PH_{name.upper().replace(' ', '_')[:20]}"
                else:
                    continue

                sheet_holdings.append({
                    "scheme_code": scheme_code,
                    "fund_name": fund_name,
                    "as_of_month": as_of_str,
                    "isin": final_isin,
                    "security_name": name,
                    "asset_type": classify_asset(name, sector),
                    "market_value_cr": round(mv_lakhs / 100, 4),   # Lakhs → Crores
                    "pct_of_nav": round(pct_raw * pct_scale, 4),
                    "imported_at": imported_at,
                })

            pct_sum = sum(h["pct_of_nav"] for h in sheet_holdings)
            pct_color = "yellow" if pct_sum > 100 else "green"
            pct_note = " ⚠ >100%" if pct_sum > 100 else ""
            console.print(
                f"  [{pct_color}]→ {fund_name}: {len(sheet_holdings)} holdings, pct_sum={pct_sum:.1f}%"
                f"{pct_note} (month={as_of_str})[/{pct_color}]"
            )
            all_holdings.extend(sheet_holdings)

        return all_holdings

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
        task = progress.add_task("[cyan]Importing All DSP Equity History...", total=len(targets))
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
        # Find latest date per fund_name
        fund_latest_dates = {}
        for h in all_holdings:
            d = datetime.strptime(h['as_of_month'], '%Y-%m-%d').date()
            if h['fund_name'] not in fund_latest_dates or d > fund_latest_dates[h['fund_name']]:
                fund_latest_dates[h['fund_name']] = d
                
        watermark_rows = [
            ['mf_holdings', fund, d] for fund, d in fund_latest_dates.items()
        ]
        
        client.insert(
            'market_data.import_watermarks',
            watermark_rows,
            column_names=['source', 'symbol', 'last_date'],
        )
        console.print(f"[dim]Watermark set for {len(watermark_rows)} funds.[/dim]")
    else:
        console.print("[red]✗ No holdings found to import.[/red]")

    if client:
        client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Import All DSP Equity historical holdings")
    parser.add_argument("--test", action="store_true", help="Run only the first month (no DB insert)")
    parser.add_argument("--dry-run", action="store_true", help="Parse all months but skip DB insert")
    args = parser.parse_args()

    if args.test:
        run_import(months=ZIP_FILES[:1], dry_run=True)
    elif args.dry_run:
        run_import(dry_run=True)
    else:
        run_import()