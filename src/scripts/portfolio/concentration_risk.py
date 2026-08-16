#!/usr/bin/env python3
import argparse
import os
import sys
from rich.console import Console
from rich.table import Table
from rich import box
from rich.panel import Panel

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
from src.db.pool import get_pool

from src.tools.mf_multi_asset import get_all_multi_asset_funds

def analyze_fund(pool, fund_filter=None, fund_name=None, scheme_code=None):
    """Return equity concentration metrics for one fund selection.

    ``fund_filter`` is used by the curated multi-asset roster.  CLI selectors
    are passed as ClickHouse parameters so a fund name cannot alter the query.
    """
    parameters = {}
    if fund_filter:
        where_clause = f"({fund_filter})"
    elif fund_name:
        where_clause = "fund_name ILIKE {fund_pattern:String}"
        parameters["fund_pattern"] = f"%{fund_name}%"
    elif scheme_code:
        where_clause = "scheme_code = {scheme_code:String}"
        parameters["scheme_code"] = scheme_code
    elif not fund_filter:
        raise ValueError("A fund filter, fund name, or scheme code is required")

    query = f"""
    SELECT
        isin,
        security_name,
        sum(pct_of_nav) AS pct_of_nav,
        any(fund_name) AS selected_fund_name
    FROM market_data.mf_holdings FINAL
    WHERE {where_clause}
      AND as_of_month = (
          SELECT max(as_of_month)
          FROM market_data.mf_holdings FINAL
          WHERE {where_clause}
      )
      AND lower(asset_type) = 'equity'
    GROUP BY isin, security_name
    """
    df = pool.query_df(query, parameters=parameters)
    if df.empty:
        return None

    # Get actual fund name from data
    actual_fund_name = df['selected_fund_name'].iloc[0]

    total_equity = df['pct_of_nav'].sum()
    if total_equity == 0:
        return None

    df['equity_weight'] = df['pct_of_nav'] / total_equity

    hhi = (df['equity_weight'] ** 2).sum() * 10000

    if hhi < 1500:
        rating = "[green]Diversified[/green]"
    elif hhi <= 2500:
        rating = "[yellow]Moderate concentration[/yellow]"
    else:
        rating = "[red]Highly concentrated[/red]"

    eff_stocks = 1 / (hhi / 10000) if hhi > 0 else 0

    df_sorted = df.sort_values(by='equity_weight', ascending=False)

    top3 = df_sorted['equity_weight'].head(3).sum() * 100
    top5 = df_sorted['equity_weight'].head(5).sum() * 100
    top10 = df_sorted['equity_weight'].head(10).sum() * 100
    top20 = df_sorted['equity_weight'].head(20).sum() * 100

    alerts = df_sorted[df_sorted['equity_weight'] > 0.08].copy()

    def classify_sector(name):
        name_up = str(name).upper()
        if any(x in name_up for x in ['BANK', 'FINANCIAL', 'INSURANCE', 'HDFC', 'ICICI', 'SBI', 'KOTAK', 'BAJAJ FIN']):
            return 'BFSI'
        if any(x in name_up for x in ['INFOSYS', 'TCS', 'TECH', 'WIPRO', 'HCL', 'SOFTWARE', 'LTIMINDTREE']):
            return 'IT'
        if any(x in name_up for x in ['PHARMA', 'HEALTHCARE', 'HOSPITAL', 'SUN PHARMA', 'DR. REDDY', 'CIPLA', 'APOLLO']):
            return 'Pharma'
        if any(x in name_up for x in ['AUTO', 'MARUTI', 'TATA MOTORS', 'MAHINDRA', 'HERO', 'BAJAJ AUTO', 'TVS']):
            return 'Auto'
        if any(x in name_up for x in ['AIRTEL', 'JIO', 'VODAFONE', 'TELECOM', 'BHARTI']):
            return 'Telecom'
        if any(x in name_up for x in ['RELIANCE', 'OIL', 'NATURAL GAS', 'PETRO', 'ONGC', 'BPCL']):
            return 'Energy'
        return 'Other'

    df_sorted['Sector'] = df_sorted['security_name'].apply(classify_sector)
    sector_weights = df_sorted.groupby('Sector')['equity_weight'].sum().reset_index()
    sector_hhi = (sector_weights['equity_weight'] ** 2).sum() * 10000

    return {
        "fund_name": actual_fund_name,
        "hhi": hhi,
        "rating": rating,
        "eff_stocks": eff_stocks,
        "top3": top3,
        "top5": top5,
        "top10": top10,
        "top20": top20,
        "max_single": df_sorted['equity_weight'].max() * 100 if not df_sorted.empty else 0,
        "top_holdings": df_sorted.head(3)[["security_name", "equity_weight"]].copy(),
        "alerts": alerts,
        "sector_weights": sector_weights,
        "sector_hhi": sector_hhi
    }

def build_parser():
    parser = argparse.ArgumentParser(description="Concentration Risk & HHI Monitor")
    selector = parser.add_mutually_exclusive_group(required=True)
    selector.add_argument("--fund", type=str, help="Fund name to analyze")
    selector.add_argument("--scheme", type=str, help="Scheme code to analyze")
    selector.add_argument("--all", action="store_true", help="Scan all multi-asset funds")
    return parser


def main():
    args = build_parser().parse_args()

    console = Console()
    pool = get_pool()

    console.print(Panel("📊 Concentration Risk Dashboard", style="bold blue"))

    if args.all:
        funds = get_all_multi_asset_funds()
        results = []
        for fund in funds:
            if not isinstance(fund, dict) or not fund.get("filter"):
                continue
            res = analyze_fund(pool, fund_filter=fund["filter"])
            if res:
                results.append(res)

        if results:
            t = Table(title="Cross-Fund Comparison", box=box.ROUNDED)
            t.add_column("Fund Name", style="cyan")
            t.add_column("HHI", justify="right")
            t.add_column("Top 3 %", justify="right")
            t.add_column("Top 10 %", justify="right")
            t.add_column("Max Single Stock", justify="right")
            t.add_column("Rating")

            for res in sorted(results, key=lambda x: x['hhi'], reverse=True):
                t.add_row(
                    res['fund_name'],
                    f"{res['hhi']:.1f}",
                    f"{res['top3']:.1f}%",
                    f"{res['top10']:.1f}%",
                    f"{res['max_single']:.1f}%",
                    res['rating']
                )
            console.print(t)
        else:
            console.print("[yellow]No multi-asset funds returned results.[/yellow]")
    else:
        res = analyze_fund(pool, fund_name=args.fund, scheme_code=args.scheme)
        if not res:
            console.print("[red]No data found for the specified fund/scheme.[/red]")
            return

        t1 = Table(title=f"Overall Metrics: {res['fund_name']}", box=box.ROUNDED)
        t1.add_column("Metric", style="cyan")
        t1.add_column("Value", justify="right")

        t1.add_row("HHI", f"{res['hhi']:.1f}")
        t1.add_row("HHI Rating", res['rating'])
        t1.add_row("Effective # Stocks", f"{res['eff_stocks']:.1f}")
        t1.add_row("Top 3 %", f"{res['top3']:.1f}%")
        t1.add_row("Top 5 %", f"{res['top5']:.1f}%")
        t1.add_row("Top 10 %", f"{res['top10']:.1f}%")
        t1.add_row("Top 20 %", f"{res['top20']:.1f}%")
        console.print(t1)

        t_top = Table(title="Top 3 Equity Holdings", box=box.ROUNDED)
        t_top.add_column("Security Name", style="cyan")
        t_top.add_column("Equity Weight", justify="right")
        for _, row in res["top_holdings"].iterrows():
            t_top.add_row(row["security_name"], f"{row['equity_weight'] * 100:.2f}%")
        console.print(t_top)
        
        t2 = Table(title="Single-Stock Ceiling Alerts", box=box.ROUNDED)
        t2.add_column("Security Name", style="cyan")
        t2.add_column("Equity Weight", justify="right")
        t2.add_column("Status")

        alerts = res['alerts']
        if alerts.empty:
            t2.add_row("None", "-", "[green]OK[/green]")
        else:
            for _, row in alerts.iterrows():
                weight = row['equity_weight'] * 100
                status = "[red]CRITICAL[/red]" if weight > 9.5 else "[yellow]WARNING[/yellow]"
                t2.add_row(row['security_name'], f"{weight:.2f}%", status)
        console.print(t2)

        t3 = Table(title=f"Sector Concentration (HHI: {res['sector_hhi']:.1f})", box=box.ROUNDED)
        t3.add_column("Sector", style="cyan")
        t3.add_column("Weight", justify="right")

        for _, row in res['sector_weights'].sort_values('equity_weight', ascending=False).iterrows():
            t3.add_row(row['Sector'], f"{row['equity_weight']*100:.1f}%")
        console.print(t3)

if __name__ == "__main__":
    main()
