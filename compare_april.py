
import os
import sys
import pandas as pd
import clickhouse_connect
from datetime import date
from rich.console import Console
from rich.table import Table

# Add src to path
sys.path.append(os.getcwd())
from config.settings import settings
from src.importer.fetchers.mf_holdings_fetcher import fetch_holdings

console = Console()

def run_april_comparison():
    # 1. Fetch current data for DSP from Morningstar
    console.print("[cyan]Fetching latest April data from Morningstar for DSP...[/cyan]")
    # ('AMFI', 'Name', 'ISIN')
    dsp_watchlist = [
        ('152056', 'DSP_MULTI_ASSET',   'INF740KA1TE9'),
        ('154167', 'DSP_MULTI_ASSET_OMNI_FOF', 'INF740KA1YE9')
    ]
    ms_rows = fetch_holdings(dsp_watchlist, date(2026, 4, 1))
    df_dsp = pd.DataFrame(ms_rows)
    
    # 2. Fetch April data for other funds from ClickHouse
    client = clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_database
    )
    df_others = client.query_df("""
        SELECT fund_name, asset_type, pct_of_nav
        FROM market_data.mf_holdings FINAL
        WHERE as_of_month = '2026-04-01'
          AND fund_name IN ('ICICI_MULTI_ASSET', 'QUANT_MULTI_ASSET', 'BAJAJ_MULTI_ASSET')
    """)
    client.close()

    # 3. Combine Data
    # Get allocation per fund per asset class
    results = {}
    
    # Process others (ClickHouse)
    for fn in df_others['fund_name'].unique():
        subset = df_others[df_others['fund_name'] == fn]
        alloc = subset.groupby('asset_type')['pct_of_nav'].sum().to_dict()
        results[fn] = alloc

    # Process DSP (Morningstar)
    if not df_dsp.empty:
        for fn in df_dsp['fund_name'].unique():
            subset = df_dsp[df_dsp['fund_name'] == fn]
            alloc = subset.groupby('asset_type')['pct_of_nav'].sum().to_dict()
            results[fn] = alloc

    # 4. Display Comparison
    table = Table(title="Multi-Asset Fund Allocation Comparison — April 2026", header_style="bold magenta")
    table.add_column("Asset Class", style="dim")
    
    funds = sorted(results.keys())
    for f in funds:
        table.add_column(f.replace('_MULTI_ASSET', '').replace('_', ' '), justify="right")

    asset_types = set()
    for f in results:
        asset_types.update(results[f].keys())
    
    for t in sorted(asset_types):
        row = [t.upper()]
        for f in funds:
            val = results[f].get(t, 0.0)
            row.append(f"{val:.2f}%")
        table.add_row(*row)
    
    total_row = ["TOTAL"]
    for f in funds:
        total_row.append(f"{sum(results[f].values()):.2f}%")
    table.add_row(*total_row, style="bold")

    console.print(table)
    
    if 'DSP_MULTI_ASSET_OMNI_FOF' not in results:
        console.print("[yellow]Note: DSP Omni FoF still has no data in Morningstar.[/yellow]")

if __name__ == "__main__":
    run_april_comparison()
