import os
import sys
import zipfile
import shutil
import argparse
import re
import calendar
import pandas as pd
import requests
from bs4 import BeautifulSoup
import clickhouse_connect
from datetime import datetime, date
from pathlib import Path
from rich.console import Console
from rich.progress import Progress
from calendar import monthrange

# Add src to path
sys.path.append(os.getcwd())
from config.settings import settings

console = Console()

BASE_URL = "https://www.dspim.com/mandatory-disclosures/portfolio-disclosures"

COMMODITY_ISIN = {
    'gold etcd#': 'GOLD_ETCD_DSP',
    'silver etcd#': 'SILVER_ETCD_DSP',
    'copper etcd#': 'COPPER_ETCD_DSP',
}

# The Top 10 target funds
TOP_10_FUNDS = {
    '119212': 'DSP_SMALL_CAP',
    '119071': 'DSP_MID_CAP',
    '119019': 'DSP_AGGRESSIVE_HYBRID',
    '119242': 'DSP_ELSS_TAX_SAVER',
    '119218': 'DSP_LARGE_AND_MID_CAP',
    '119076': 'DSP_FLEXI_CAP',
    '119028': 'DSP_NRNEF',
    '119247': 'DSP_TIGER',
    '126393': 'DSP_DYNAMIC_ASSET_ALLOCATION',
    '119250': 'DSP_LARGE_CAP',
}

# Flexible sheet name matcher
SHEET_MAP = {
    'DSP_SMALL_CAP': ['SMALLCAP', 'Small Cap', 'DSP Small Cap'],
    'DSP_MID_CAP': ['MIDCAP', 'Mid Cap', 'DSP Midcap'],
    'DSP_AGGRESSIVE_HYBRID': ['Aggressive Hybrid', 'DSP Aggressive Hybrid', 'Equity & Bond', 'EQUITY&BOND'],
    'DSP_ELSS_TAX_SAVER': ['TAX', 'Tax Saver', 'DSP ELSS Tax Saver'],
    'DSP_LARGE_AND_MID_CAP': ['Large & Mid Cap', 'DSP Large & Mid Cap', 'Equity Opportunities', 'EQUITYOPPOR'],
    'DSP_FLEXI_CAP': ['Flexi Cap', 'DSP Flexi Cap', 'Equity Fund'],
    'DSP_NRNEF': ['NRNEF', 'Natural Resources', 'DSP NRNEF'],
    'DSP_TIGER': ['TIGER', 'T.I.G.E.R', 'DSP TIGER'],
    'DSP_DYNAMIC_ASSET_ALLOCATION': ['DAAF', 'Dynamic Asset Allocation', 'DSP DAAF'],
    'DSP_LARGE_CAP': ['Large Cap', 'Top 100', 'DSP Large Cap', 'TOP100'],
}

def classify_asset(name, sector):
    name = str(name).lower()
    sector = str(sector).lower()
    if any(k in name for k in ['gold', 'silver']): return 'gold'
    if any(k in sector for k in ['gold', 'silver']): return 'gold'
    if any(k in sector for k in ['debt', 'g-sec', 'sdl', 'treasury']): return 'bond'
    if any(k in name for k in ['cash', 'liquid', 'treps', 'repo']): return 'cash'
    if any(k in name for k in ['equity', 'limited', 'ltd', 'inc', 'corp']): return 'equity'
    return 'other'

def get_month_end_date(year, month_name):
    # Convert month string to month number
    try:
        month_abbr = month_name[:3].capitalize()
        month_num = list(calendar.month_abbr).index(month_abbr)
        _, last_day = monthrange(year, month_num)
        return date(year, month_num, last_day).strftime('%Y-%m-%d')
    except ValueError:
        return None

def scrape_zip_links(start_year=2020):
    """Scrape DSP website for month-end portfolio ZIP links back to start_year."""
    console.print(f"[cyan]Scraping DSP website for links since {start_year}...[/cyan]")
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        r = requests.get(BASE_URL, headers=headers, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, 'html.parser')
        
        all_links = []
        for a in soup.find_all('a', href=True):
            href = a['href']
            if '.zip' in href.lower() and 'monthend' in href.lower():
                # Extract year and month from URL
                match = re.search(r'(january|february|march|april|may|june|july|august|september|october|november|december).*?(\d{4})', href.lower())
                if match:
                    month_name = match.group(1)
                    year = int(match.group(2))
                    
                    if year >= start_year:
                        as_of_date = get_month_end_date(year, month_name)
                        if as_of_date:
                            # Avoid duplicates
                            if not any(l[0] == as_of_date for l in all_links):
                                all_links.append((as_of_date, href))
        
        # Sort chronologically
        all_links.sort(key=lambda x: x[0])
        console.print(f"[green]Found {len(all_links)} month-end portfolios since {start_year}.[/green]")
        return all_links
    except Exception as e:
        console.print(f"[red]Error scraping DSP website: {e}[/red]")
        return []

def download_and_extract(url, temp_dir):
    try:
        if not url.startswith('http'):
            url = 'https://www.dspim.com' + url
            
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        zip_path = temp_dir / "temp.zip"
        zip_path.write_bytes(r.content)

        with zipfile.ZipFile(zip_path, 'r') as zf:
            zf.extractall(temp_dir)

        # Look for the main equity file (usually contains 'ISIN' and 'Equity' or 'Portfolio')
        for path in sorted(temp_dir.rglob("*.xlsx")):
            name = path.name.lower()
            if 'debt' not in name and ('isin' in name or 'portfolio' in name):
                return path
    except Exception as e:
        console.print(f"[red]Error downloading/extracting {url}: {e}[/red]")
    return None

def process_month(as_of_str, url):
    temp_dir = Path("temp_backfill")
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    temp_dir.mkdir()

    xl_path = download_and_extract(url, temp_dir)
    if not xl_path:
        return []

    try:
        xl = pd.ExcelFile(xl_path)
        all_holdings = []
        found_funds = set()
        
        # Invert the mapping for easier lookup: alias -> (scheme_code, fund_name)
        lookup_map = {}
        for scheme_code, fund_name in TOP_10_FUNDS.items():
            for alias in SHEET_MAP[fund_name]:
                lookup_map[alias.lower()] = (scheme_code, fund_name)

        for sheet_name in xl.sheet_names:
            sheet_lower = sheet_name.lower().strip()
            
            # Find matching fund
            matched_fund = None
            for alias, data in lookup_map.items():
                if alias in sheet_lower:
                    matched_fund = data
                    break
            
            if not matched_fund:
                continue
                
            scheme_code, fund_name = matched_fund
            if fund_name in found_funds:
                continue # Already processed this fund (avoid duplicates if multiple sheets match)
                
            df = pd.read_excel(xl_path, sheet_name=sheet_name, header=None)

            # Locate header row: look for "Name of Instrument" or "ISIN"
            data_start_row = 6
            isin_col_idx = 2
            name_col_idx = 1
            sector_col_idx = 3
            mv_col_idx = 5
            pct_col_idx = 6
            
            header_found = False
            for idx, row in df.iterrows():
                row_str = " ".join(str(x).lower() for x in row.values)
                if "instrument" in row_str or "isin" in row_str:
                    data_start_row = idx + 1
                    header_found = True
                    # Dynamically find columns
                    for c_idx, val in enumerate(row.values):
                        v = str(val).lower().strip()
                        if 'isin' in v: isin_col_idx = c_idx
                        elif 'instrument' in v or 'company' in v: name_col_idx = c_idx
                        elif 'rating' in v or 'industry' in v or 'sector' in v: sector_col_idx = c_idx
                        elif 'lakhs' in v or 'market value' in v: mv_col_idx = c_idx
                        elif '% to net' in v or 'net asset' in v or '% of nav' in v or 'percent' in v: pct_col_idx = c_idx
                    break
            
            if not header_found:
                continue

            # Pass 1: collect numeric rows
            raw_rows = []
            for i in range(data_start_row, len(df)):
                if i >= len(df): break
                row = df.iloc[i]
                try:
                    if pd.isna(row.iloc[mv_col_idx]) or pd.isna(row.iloc[pct_col_idx]):
                        continue
                    mv_lakhs = float(row.iloc[mv_col_idx])
                    pct_raw = float(row.iloc[pct_col_idx])
                except (ValueError, TypeError, IndexError):
                    continue
                    
                isin = str(row.iloc[isin_col_idx]).strip() if isin_col_idx < len(row) else ""
                name = str(row.iloc[name_col_idx]).strip() if name_col_idx < len(row) else ""
                sector = str(row.iloc[sector_col_idx]).strip() if sector_col_idx < len(row) else ""
                raw_rows.append((isin, name, sector, mv_lakhs, pct_raw))

            if not raw_rows:
                continue

            # Detect pct scale
            valid_pcts = [r[4] for r in raw_rows if not pd.isna(r[4])]
            max_pct = max(valid_pcts) if valid_pcts else 0.0
            pct_scale = 1.0 if max_pct > 1.0 else (100.0 if max_pct > 0.0 else 1.0)

            # Pass 2: build holdings
            imported_at = datetime.now()
            sheet_holdings = []
            for isin, name, sector, mv_lakhs, pct_raw in raw_rows:
                is_valid_isin = len(isin) == 12
                name_lower = name.lower()
                is_commodity = name_lower in COMMODITY_ISIN

                if not (is_valid_isin or is_commodity):
                    continue

                sheet_holdings.append({
                    "scheme_code": scheme_code,
                    "fund_name": fund_name,
                    "as_of_month": as_of_str,
                    "isin": isin if is_valid_isin else COMMODITY_ISIN[name_lower],
                    "security_name": name,
                    "asset_type": classify_asset(name, sector),
                    "market_value_cr": round(mv_lakhs / 100, 4),
                    "pct_of_nav": round(pct_raw * pct_scale, 4),
                    "imported_at": imported_at,
                })

            if sheet_holdings:
                found_funds.add(fund_name)
                all_holdings.extend(sheet_holdings)

        # Print summary for this month
        if found_funds:
            console.print(f"  [green]→ Parsed {len(found_funds)}/10 top funds ({len(all_holdings)} holdings)[/green]")
        else:
            console.print(f"  [yellow]→ No top 10 funds found in {as_of_str}[/yellow]")

        return all_holdings

    except Exception as e:
        console.print(f"[red]Error parsing {xl_path}: {e}[/red]")
        return []

def run_import(start_year=2020, limit=None, dry_run=False):
    links = scrape_zip_links(start_year)
    if not links:
        console.print("[red]No links found. Exiting.[/red]")
        return

    if limit:
        links = links[:limit]

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
    
    with Progress() as progress:
        task = progress.add_task(f"[cyan]Importing Top 10 DSP Funds since {start_year}...", total=len(links))
        for as_of, url in links:
            progress.console.print(f"Processing [bold]{as_of}[/bold]...")
            month_holdings = process_month(as_of, url)
            if month_holdings:
                all_holdings.extend(month_holdings)
            progress.advance(task)

    if dry_run:
        console.print(f"[bold blue]DRY RUN: {len(all_holdings)} holdings parsed across {len(links)} months — nothing inserted.[/bold blue]")
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
        
        # Delete existing data for these funds to prevent duplicates if formats changed slightly
        fund_names = tuple(TOP_10_FUNDS.values())
        min_date = links[0][0]
        max_date = links[-1][0]
        
        console.print(f"[yellow]Cleaning existing data for Top 10 funds between {min_date} and {max_date}...[/yellow]")
        client.command(f"""
            ALTER TABLE market_data.mf_holdings 
            DELETE WHERE fund_name IN {fund_names} 
            AND as_of_month >= '{min_date}' AND as_of_month <= '{max_date}'
        """)
        
        console.print("[cyan]Inserting new data...[/cyan]")
        client.insert(
            'market_data.mf_holdings', rows,
            column_names=['scheme_code', 'fund_name', 'as_of_month', 'isin', 'security_name',
                          'asset_type', 'market_value_cr', 'pct_of_nav', 'imported_at'],
        )
        console.print(f"[bold green]✓ Imported {len(rows)} holdings.[/bold green]")
        
        # Update watermarks
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
    parser = argparse.ArgumentParser(description="Import DSP Top 10 Funds since 2020")
    parser.add_argument("--dry-run", action="store_true", help="Parse but skip DB insert")
    parser.add_argument("--limit", type=int, help="Limit number of months to process")
    args = parser.parse_args()

    run_import(start_year=2020, limit=args.limit, dry_run=args.dry_run)