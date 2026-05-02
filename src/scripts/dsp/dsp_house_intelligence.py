
import os
import sys
import pandas as pd
import clickhouse_connect
from config.settings import settings
from rich.console import Console
from rich.table import Table

# Add src to path
sys.path.append(os.getcwd())

console = Console()

def run_house_intelligence():
    client = clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_database
    )

    # --- 1. Identify Nifty 50 ---
    # We use the Nifty 50 Index Fund as a proxy for the current list
    nifty50_df = client.query_df("SELECT security_name FROM market_data.mf_holdings FINAL WHERE fund_name = 'DSP_NIFTY_50_INDEX' AND as_of_month = '2026-03-31'")
    nifty50_list = set(nifty50_df['security_name'].tolist())

    # --- 2. House Conviction Score ---
    query_conviction = """
    SELECT 
        security_name,
        count(DISTINCT fund_name) as fund_count,
        round(sum(pct_of_nav), 2) as agg_weight,
        round(count(DISTINCT fund_name) * sum(pct_of_nav), 0) as conviction_score
    FROM market_data.mf_holdings FINAL
    WHERE as_of_month = '2026-03-31'
      AND asset_type = 'equity'
      AND fund_name LIKE 'DSP_%'
      AND security_name NOT LIKE '%ETF%'
      AND security_name NOT LIKE '%Mutual Fund%'
    GROUP BY security_name
    ORDER BY conviction_score DESC
    LIMIT 10
    """
    df_conviction = client.query_df(query_conviction)

    # --- 3. Mid-Cap Cluster Scan ---
    # Held by 3-7 schemes, NOT in Nifty 50
    query_midcap = """
    SELECT 
        security_name,
        count(DISTINCT fund_name) as fund_count,
        round(sum(pct_of_nav), 2) as agg_weight
    FROM market_data.mf_holdings FINAL
    WHERE as_of_month = '2026-03-31'
      AND asset_type = 'equity'
      AND fund_name LIKE 'DSP_%'
      AND security_name NOT LIKE '%ETF%'
      AND security_name NOT LIKE '%Mutual Fund%'
    GROUP BY security_name
    HAVING fund_count BETWEEN 3 AND 8
    ORDER BY agg_weight DESC
    LIMIT 10
    """
    df_midcap = client.query_df(query_midcap)
    # Filter out Nifty 50 from midcap list
    df_midcap = df_midcap[~df_midcap['security_name'].isin(nifty50_list)]

    # --- 4. Turnover Synchronization (House Pivot) ---
    query_pivot = """
    WITH cur AS (
        SELECT fund_name, security_name, pct_of_nav as p_cur
        FROM market_data.mf_holdings FINAL
        WHERE as_of_month = '2026-03-31' AND fund_name LIKE 'DSP_%'
    ),
    prev AS (
        SELECT fund_name, security_name, pct_of_nav as p_prev
        FROM market_data.mf_holdings FINAL
        WHERE as_of_month = '2026-02-28' AND fund_name LIKE 'DSP_%'
    )
    SELECT 
        security_name,
        sum(p_cur - p_prev) as house_drift,
        countIf(p_cur > p_prev + 0.1) as add_count,
        countIf(p_cur < p_prev - 0.1) as trim_count
    FROM cur 
    FULL OUTER JOIN prev ON cur.fund_name = prev.fund_name AND cur.security_name = prev.security_name
    WHERE security_name NOT LIKE '%ETF%'
    GROUP BY security_name
    HAVING abs(house_drift) > 2 OR add_count > 10 OR trim_count > 10
    ORDER BY abs(house_drift) DESC
    LIMIT 10
    """
    df_pivot = client.query_df(query_pivot)

    # --- Display Results ---
    console.print("\n[bold cyan]DSP House Intelligence Report: March 2026[/bold cyan]")
    
    t1 = Table(title="Top Conviction (Sch × Weight)", header_style="bold magenta")
    t1.add_column("Security", style="dim")
    t1.add_column("Score", justify="right")
    t1.add_column("Schemes", justify="right")
    for _, r in df_conviction.iterrows():
        t1.add_row(r['security_name'][:25], str(int(r['conviction_score'])), str(r['fund_count']))
    console.print(t1)

    t2 = Table(title="Mid-Cap Cluster (Emerging Favorites)", header_style="bold yellow")
    t2.add_column("Security", style="dim")
    t2.add_column("Agg. Weight", justify="right")
    t2.add_column("Schemes", justify="right")
    for _, r in df_midcap.iterrows():
        t2.add_row(r['security_name'][:25], f"{r['agg_weight']}%", str(r['fund_count']))
    console.print(t2)

    t3 = Table(title="House-Wide Pivots (Synchronized Drift)", header_style="bold green")
    t3.add_column("Security", style="dim")
    t3.add_column("House Δ", justify="right")
    t3.add_column("Add#", style="green")
    t3.add_column("Trim#", style="red")
    for _, r in df_pivot.iterrows():
        t3.add_row(r['security_name'][:25], f"{r['house_drift']:+.2f}%", str(r['add_count']), str(r['trim_count']))
    console.print(t3)

    client.close()

if __name__ == "__main__":
    run_house_intelligence()
