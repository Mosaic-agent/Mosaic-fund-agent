import os
import sys
import pandas as pd
from datetime import datetime
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

# Add src to path
sys.path.append(os.getcwd())

try:
    from config.settings import settings
    import clickhouse_connect
except ImportError as e:
    print(f"Error importing project modules: {e}")
    sys.exit(1)

console = Console()

def run_comparison():
    client = clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_database
    )
    
    # Query for the latest month's allocation
    query = """
    SELECT
        fund_name,
        as_of_month,
        asset_type,
        sum(pct_of_nav) AS total_pct
    FROM market_data.mf_holdings FINAL
    WHERE fund_name IN ('DSP_MULTI_ASSET', 'DSP_MULTI_ASSET_OMNI_FOF')
    GROUP BY fund_name, as_of_month, asset_type
    ORDER BY as_of_month DESC, fund_name, asset_type
    """
    df = client.query_df(query)
    client.close()
    
    if df.empty:
        console.print("[red]No holding data found in ClickHouse.[/red]")
        return
        
    # Get the latest month where BOTH funds have data
    months_omni = set(df[df['fund_name'] == 'DSP_MULTI_ASSET_OMNI_FOF']['as_of_month'])
    months_std = set(df[df['fund_name'] == 'DSP_MULTI_ASSET']['as_of_month'])
    common_months = months_omni.intersection(months_std)
    
    if not common_months:
        console.print("[red]No common months found for comparison.[/red]")
        return
        
    latest_month = max(common_months)
    df_latest = df[df['as_of_month'] == latest_month]
    
    # Pivot for comparison
    pivot_df = df_latest.pivot(index='asset_type', columns='fund_name', values='total_pct').fillna(0)
    
    # Add a 'Total' row
    pivot_df.loc['TOTAL'] = pivot_df.sum()
    
    # Calculate difference
    if 'DSP_MULTI_ASSET' in pivot_df.columns and 'DSP_MULTI_ASSET_OMNI_FOF' in pivot_df.columns:
        pivot_df['Difference (Omni - Standard)'] = pivot_df['DSP_MULTI_ASSET_OMNI_FOF'] - pivot_df['DSP_MULTI_ASSET']

    console.print(Panel(f"[bold cyan]DSP Multi Asset Fund Comparison[/bold cyan]\n[dim]Snapshot as of: {latest_month}[/dim]", expand=False))
    
    table = Table(show_header=True, header_style="bold magenta", box=box.ROUNDED)
    table.add_column("Asset Class", style="dim")
    table.add_column("DSP Multi Asset (Standard)", justify="right")
    table.add_column("DSP Multi Asset Omni FoF", justify="right")
    table.add_column("Difference", justify="right")
    
    for asset_type, row in pivot_df.iterrows():
        style = "bold" if asset_type == "TOTAL" else ""
        diff = row.get('Difference (Omni - Standard)', 0)
        diff_str = f"{diff:+.2f}%"
        diff_color = "green" if diff > 0 else "red" if diff < 0 else "white"
        
        table.add_row(
            asset_type.upper(),
            f"{row.get('DSP_MULTI_ASSET', 0):.2f}%",
            f"{row.get('DSP_MULTI_ASSET_OMNI_FOF', 0):.2f}%",
            f"[{diff_color}]{diff_str}[/{diff_color}]",
            style=style
        )
        
    console.print(table)
    
    # Qualitative Comparison Section
    console.print("\n[bold]Key Differences & Strategy Insights:[/bold]")
    console.print("• [bold]Structure:[/bold] Standard is a [cyan]Direct Asset Fund[/cyan]; Omni is a [cyan]Fund of Funds[/cyan].")
    console.print("• [bold]Taxation:[/bold] Both are typically taxed as equity if domestic equity exposure > 65% (Omni FoF also enjoys 12.5% LTCG after 2 years).")
    console.print("• [bold]Strategy:[/bold] Omni uses the [bold]Netra Quant Framework[/bold] for dynamic shifts; Standard is more fund-manager driven.")
    console.print("• [bold]Allocation Notes:[/bold] Negative gold in Standard indicates [yellow]hedging/derivatives[/yellow]. Omni has [green]higher bond allocation[/green] providing more stability.")

if __name__ == "__main__":
    from rich import box
    run_comparison()
