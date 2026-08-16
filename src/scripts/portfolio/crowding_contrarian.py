#!/usr/bin/env python3
import argparse
import os
import sys
import pandas as pd
from rich.console import Console
from rich.table import Table
from rich import box
from rich.panel import Panel

# Ensure src is in the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
from src.db.pool import get_pool

def main():
    parser = argparse.ArgumentParser(description="Crowding Risk & Contrarian Alpha Signal")
    parser.add_argument("--min-crowd", type=int, default=5, help="Minimum number of funds holding the stock to be considered crowded")
    parser.add_argument("--top", type=int, default=20, help="Top N stocks to display")
    args = parser.parse_args()

    console = Console()
    pool = get_pool()

    # Query latest two months
    query_months = """
    SELECT DISTINCT as_of_month 
    FROM market_data.mf_holdings FINAL 
    ORDER BY as_of_month DESC 
    LIMIT 2
    """
    df_months = pool.query_df(query_months)
    if len(df_months) < 2:
        console.print("[red]Not enough data for MoM comparison[/red]")
        return
    
    curr_month = str(df_months.iloc[0]['as_of_month'])[:10]
    prev_month = str(df_months.iloc[1]['as_of_month'])[:10]

    query_holdings = f"""
    SELECT as_of_month, isin, security_name, fund_name, asset_type, pct_of_nav, scheme_code
    FROM market_data.mf_holdings FINAL
    WHERE as_of_month IN (toDate('{curr_month}'), toDate('{prev_month}'))
    AND asset_type = 'equity'
    AND security_name NOT ILIKE '%Gold%'
    AND security_name NOT ILIKE '%Silver%'
    AND security_name NOT ILIKE '%ETF%'
    AND security_name NOT ILIKE '%Liquid%'
    AND security_name NOT ILIKE '%Treasury%'
    AND security_name NOT ILIKE '%TREPS%'
    AND security_name NOT ILIKE '%Repo%'
    AND security_name NOT ILIKE '%Future%'
    AND security_name NOT ILIKE '%Option%'
    """
    df_holdings = pool.query_df(query_holdings)
    
    if df_holdings.empty:
        console.print("[red]No equity holdings found.[/red]")
        return
    
    df_curr = df_holdings[df_holdings['as_of_month'] == curr_month]
    df_prev = df_holdings[df_holdings['as_of_month'] == prev_month]

    # Aggregate
    agg_curr = df_curr.groupby(['isin', 'security_name']).agg(
        num_funds_curr=('fund_name', 'nunique'),
        agg_weight_curr=('pct_of_nav', 'sum'),
        funds_holding_curr=('fund_name', list)
    ).reset_index()

    agg_prev = df_prev.groupby(['isin', 'security_name']).agg(
        num_funds_prev=('fund_name', 'nunique'),
        agg_weight_prev=('pct_of_nav', 'sum'),
        funds_holding_prev=('fund_name', list)
    ).reset_index()

    df_merged = pd.merge(agg_curr, agg_prev, on=['isin', 'security_name'], how='outer')
    df_merged['num_funds_curr'] = df_merged['num_funds_curr'].fillna(0)
    df_merged['agg_weight_curr'] = df_merged['agg_weight_curr'].fillna(0)
    df_merged['num_funds_prev'] = df_merged['num_funds_prev'].fillna(0)
    df_merged['agg_weight_prev'] = df_merged['agg_weight_prev'].fillna(0)
    
    df_merged['delta_funds'] = df_merged['num_funds_curr'] - df_merged['num_funds_prev']
    df_merged['delta_weight'] = df_merged['agg_weight_curr'] - df_merged['agg_weight_prev']
    
    # 1. Crowding Signal
    df_crowding = df_merged[(df_merged['num_funds_curr'] >= args.min_crowd) & (df_merged['delta_weight'] > 0)].copy()
    df_crowding['crowd_score'] = df_crowding['num_funds_curr'] * df_crowding['agg_weight_curr']
    df_crowding = df_crowding.sort_values(by='crowd_score', ascending=False).head(args.top)

    def get_risk_level(row):
        if row['num_funds_curr'] >= 8 and row['delta_weight'] > 0:
            return "[red]CROWDED[/red]"
        elif row['num_funds_curr'] >= 6:
            return "[yellow]ELEVATED[/yellow]"
        return "[green]MODERATE[/green]"

    df_crowding['Risk Level'] = df_crowding.apply(get_risk_level, axis=1)

    # 2. Contrarian Signal
    contrarian_data = []
    for isin in df_merged['isin'].unique():
        prev_funds = df_prev[df_prev['isin'] == isin]
        curr_funds = df_curr[df_curr['isin'] == isin]
        
        prev_holdings = dict(zip(prev_funds['fund_name'], prev_funds['pct_of_nav']))
        curr_holdings = dict(zip(curr_funds['fund_name'], curr_funds['pct_of_nav']))
        
        exiting_funds = []
        for fund, prev_w in prev_holdings.items():
            curr_w = curr_holdings.get(fund, 0)
            if curr_w == 0 or (curr_w < prev_w * 0.5):
                exiting_funds.append(fund)
                
        adding_funds = []
        for fund, curr_w in curr_holdings.items():
            prev_w = prev_holdings.get(fund, 0)
            if curr_w > prev_w:
                delta = curr_w - prev_w
                is_conviction = "DSP" in fund or "QUANT" in fund.upper()
                adding_funds.append({"fund": fund, "delta": delta, "conviction": is_conviction})
        
        if len(exiting_funds) >= 3 and len(adding_funds) >= 1:
            adding_funds.sort(key=lambda x: (x['conviction'], x['delta']), reverse=True)
            key_adder = adding_funds[0]
            sec_name = df_merged[df_merged['isin'] == isin]['security_name'].iloc[0]
            score = len(exiting_funds) * key_adder['delta']
            
            contrarian_data.append({
                "Security Name": sec_name,
                "# AMCs Exiting": len(exiting_funds),
                "# AMCs Adding": len(adding_funds),
                "Key Adder": key_adder['fund'],
                "Adder Δ Weight": key_adder['delta'],
                "Conviction Score": score
            })

    df_contrarian = pd.DataFrame(contrarian_data)
    if not df_contrarian.empty:
        df_contrarian = df_contrarian.sort_values(by="Conviction Score", ascending=False).head(args.top)

    # 3. Fresh Institutional Entries
    df_fresh = df_merged[(df_merged['num_funds_curr'] >= 2) & (df_merged['num_funds_prev'] == 0)].copy()
    df_fresh = df_fresh.sort_values(by='num_funds_curr', ascending=False).head(args.top)

    # OUTPUT
    console.print(Panel("🚨 Crowding Risk & 🔍 Contrarian Alpha Scanner", style="bold cyan"))
    
    # Table 1
    t1 = Table(title="Crowding Risk", box=box.ROUNDED)
    t1.add_column("Security Name", style="cyan")
    t1.add_column("# AMCs Holding", justify="right")
    t1.add_column("Aggregate Weight %", justify="right")
    t1.add_column("MoM Δ Weight", justify="right")
    t1.add_column("MoM Δ Funds", justify="right")
    t1.add_column("Risk Level")
    
    for _, row in df_crowding.iterrows():
        t1.add_row(
            str(row['security_name']),
            str(int(row['num_funds_curr'])),
            f"{row['agg_weight_curr']:.2f}%",
            f"{row['delta_weight']:+.2f}%",
            f"{int(row['delta_funds']):+d}",
            row['Risk Level']
        )
    console.print(t1)

    # Table 2
    t2 = Table(title="Contrarian Alpha", box=box.ROUNDED)
    t2.add_column("Security Name", style="cyan")
    t2.add_column("# AMCs Exiting", justify="right")
    t2.add_column("# AMCs Adding", justify="right")
    t2.add_column("Key Adder", style="magenta")
    t2.add_column("Adder Δ Weight", justify="right")
    t2.add_column("Conviction Score", justify="right")
    
    if not df_contrarian.empty:
        for _, row in df_contrarian.iterrows():
            t2.add_row(
                str(row['Security Name']),
                str(row['# AMCs Exiting']),
                str(row['# AMCs Adding']),
                str(row['Key Adder']),
                f"{row['Adder Δ Weight']:+.2f}%",
                f"{row['Conviction Score']:.2f}"
            )
    else:
        t2.add_row("No signals found", "-", "-", "-", "-", "-")
    console.print(t2)

    # Table 3
    t3 = Table(title="Fresh Institutional Entries", box=box.ROUNDED)
    t3.add_column("Security Name", style="cyan")
    t3.add_column("# AMCs Holding Now", justify="right")
    t3.add_column("Aggregate Weight %", justify="right")
    
    if not df_fresh.empty:
        for _, row in df_fresh.iterrows():
            t3.add_row(
                str(row['security_name']),
                str(int(row['num_funds_curr'])),
                f"{row['agg_weight_curr']:.2f}%"
            )
    else:
        t3.add_row("No fresh entries found", "-", "-")
    console.print(t3)

if __name__ == "__main__":
    main()
