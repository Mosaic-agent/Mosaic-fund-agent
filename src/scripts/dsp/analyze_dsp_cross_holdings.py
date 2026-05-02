
import os
import sys
import pandas as pd
import clickhouse_connect

# Add src to path
sys.path.append(os.getcwd())
from config.settings import settings
from rich.console import Console
from rich.table import Table

console = Console()

def analyze():
    client = clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_database
    )

    # 1. Stocks by Overlap (count of schemes)
    query_overlap = """
    SELECT 
        security_name,
        count(DISTINCT fund_name) as fund_count,
        round(sum(pct_of_nav), 2) as total_weight
    FROM market_data.mf_holdings FINAL
    WHERE as_of_month = '2026-03-31'
      AND asset_type = 'equity'
      AND fund_name LIKE 'DSP_%'
      AND security_name NOT LIKE '%Mutual Fund%'
      AND security_name NOT LIKE '%ETF%'
    GROUP BY security_name
    ORDER BY fund_count DESC, total_weight DESC
    LIMIT 15
    """
    df_overlap = client.query_df(query_overlap)

    # 2. Stocks by Aggregate Intensity (sum of weights)
    query_intensity = """
    SELECT 
        security_name,
        round(sum(pct_of_nav), 2) as total_weight,
        count(DISTINCT fund_name) as fund_count
    FROM market_data.mf_holdings FINAL
    WHERE as_of_month = '2026-03-31'
      AND asset_type = 'equity'
      AND fund_name LIKE 'DSP_%'
      AND security_name NOT LIKE '%Mutual Fund%'
      AND security_name NOT LIKE '%ETF%'
    GROUP BY security_name
    ORDER BY total_weight DESC
    LIMIT 15
    """
    df_intensity = client.query_df(query_intensity)

    # 3. Fund-specific concentration (most common stocks in top funds)
    query_top_funds = """
    SELECT fund_name, security_name, pct_of_nav
    FROM market_data.mf_holdings FINAL
    WHERE as_of_month = '2026-03-31'
      AND fund_name IN ('DSP_FLEXI_CAP', 'DSP_TOP_100', 'DSP_MID_CAP', 'DSP_SMALL_CAP')
      AND asset_type = 'equity'
    ORDER BY fund_name, pct_of_nav DESC
    """
    # Note: 'DSP_TOP_100' is 'DSP_LARGE_CAP' in my registry
    
    console.print("\n[bold cyan]DSP Cross-Holding Analysis: March 2026 Snapshot[/bold cyan]")
    
    table_overlap = Table(title="Most Common Stocks (By Number of Schemes)", header_style="bold magenta")
    table_overlap.add_column("Security Name", style="dim")
    table_overlap.add_column("Fund Count", justify="right")
    table_overlap.add_column("Agg. Weight (%)", justify="right")
    
    for _, r in df_overlap.iterrows():
        table_overlap.add_row(r['security_name'], str(r['fund_count']), f"{r['total_weight']}%")
        
    console.print(table_overlap)

    table_intensity = Table(title="Highest Intensity Stocks (By Aggregate Weight)", header_style="bold green")
    table_intensity.add_column("Security Name", style="dim")
    table_intensity.add_column("Agg. Weight (%)", justify="right")
    table_intensity.add_column("Fund Count", justify="right")
    
    for _, r in df_intensity.iterrows():
        table_intensity.add_row(r['security_name'], f"{r['total_weight']}%", str(r['fund_count']))
        
    console.print(table_intensity)

    client.close()

if __name__ == "__main__":
    analyze()
