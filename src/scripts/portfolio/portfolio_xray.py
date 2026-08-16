import argparse
import sys
from collections import defaultdict
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import box

from src.db.pool import get_pool, get_client

def main():
    parser = argparse.ArgumentParser(description="Portfolio X-Ray Engine")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--schemes", help="Comma-separated list of scheme codes")
    group.add_argument("--fund-names", help="Comma-separated list of fund names (ILIKE match)")
    parser.add_argument("--amounts", help="Comma-separated list of investment amounts")
    
    args = parser.parse_args()
    
    console = Console()
    client = get_client()
    
    fund_list = []
    if args.schemes:
        schemes = [s.strip() for s in args.schemes.split(",")]
        for sc in schemes:
            q = f"SELECT DISTINCT fund_name FROM market_data.mf_holdings FINAL WHERE scheme_code = '{sc}' LIMIT 1"
            rows = client.query(q).result_rows
            if rows:
                fund_list.append((sc, rows[0][0]))
            else:
                console.print(f"[red]Could not find fund for scheme code: {sc}[/red]")
    else:
        names = [n.strip() for n in args.fund_names.split(",")]
        for n in names:
            q = f"SELECT scheme_code, fund_name FROM market_data.mf_holdings FINAL WHERE fund_name ILIKE '%{n}%' LIMIT 1"
            rows = client.query(q).result_rows
            if rows:
                fund_list.append((rows[0][0], rows[0][1]))
            else:
                console.print(f"[red]Could not find fund matching: {n}[/red]")
                
    if not fund_list:
        console.print("[red]No valid funds found. Exiting.[/red]")
        sys.exit(1)
        
    num_funds = len(fund_list)
    if args.amounts:
        amounts = [float(a.strip()) for a in args.amounts.split(",")]
        if len(amounts) != num_funds:
            console.print(f"[red]Number of amounts ({len(amounts)}) does not match number of valid funds ({num_funds}).[/red]")
            sys.exit(1)
    else:
        amounts = [100000.0] * num_funds
        
    total_amount = sum(amounts)
    
    # Store holdings
    consolidated_holdings = defaultdict(lambda: {'security_name': '', 'asset_type': '', 'weight': 0.0, 'value': 0.0, 'held_by': 0})
    fund_summaries = []
    asset_breakdown = defaultdict(float)
    
    for (sc, fn), amt in zip(fund_list, amounts):
        q = f"""
            SELECT isin, security_name, asset_type, pct_of_nav, market_value_cr, as_of_month 
            FROM market_data.mf_holdings FINAL 
            WHERE fund_name = '{fn}' 
              AND as_of_month = (SELECT max(as_of_month) FROM market_data.mf_holdings FINAL WHERE fund_name = '{fn}')
        """
        rows = client.query(q).result_rows
        
        if not rows:
            console.print(f"[yellow]No holdings found for {fn}[/yellow]")
            continue
            
        latest_month = rows[0][5]
        num_holdings = len(rows)
        fund_summaries.append((fn, sc, amt, latest_month, num_holdings))
        
        weight_multiplier = amt / total_amount
        
        for r in rows:
            isin, sec_name, asset_type, pct_nav, mv_cr, _ = r
            if not isin:
                isin = sec_name # fallback
                
            w = (pct_nav / 100.0) * weight_multiplier
            v = (pct_nav / 100.0) * amt
            
            h = consolidated_holdings[isin]
            h['security_name'] = sec_name
            h['asset_type'] = asset_type
            h['weight'] += w * 100.0
            h['value'] += v
            h['held_by'] += 1
            
            asset_breakdown[asset_type] += w * 100.0
            
    # Sort holdings
    sorted_holdings = sorted(consolidated_holdings.items(), key=lambda x: x[1]['weight'], reverse=True)
    
    # Concentration metrics (Equity only)
    equity_weights = [v['weight'] for k, v in sorted_holdings if v['asset_type'].lower() in ('equity',)]
    sum_equity = sum(equity_weights)
    if sum_equity > 0:
        normalized_equity = [w / sum_equity for w in equity_weights]
        hhi = sum(w**2 for w in normalized_equity) * 10000
        eff_stocks = 1 / sum(w**2 for w in normalized_equity) if sum(w**2 for w in normalized_equity) > 0 else 0
    else:
        hhi = 0
        eff_stocks = 0
        
    top_5 = sum(v['weight'] for k, v in sorted_holdings[:5])
    top_10 = sum(v['weight'] for k, v in sorted_holdings[:10])
    top_20 = sum(v['weight'] for k, v in sorted_holdings[:20])
    max_single = sorted_holdings[0][1]['weight'] if sorted_holdings else 0
    
    # Render Output
    console.print(Panel("[bold cyan]Portfolio X-Ray: Look-Through Stock Exposure[/bold cyan]"))
    
    # Table 1: Top 25
    t1 = Table(title="Top 25 Consolidated Holdings", box=box.SIMPLE)
    t1.add_column("Security Name")
    t1.add_column("Asset Type")
    t1.add_column("Combined Weight %", justify="right")
    t1.add_column("Approx Value ₹", justify="right")
    t1.add_column("Held By", justify="center")
    
    for k, v in sorted_holdings[:25]:
        t1.add_row(v['security_name'][:40], v['asset_type'], f"{v['weight']:.2f}%", f"₹{v['value']:,.0f}", f"{v['held_by']}/{num_funds}")
    console.print(t1)
    console.print()
    
    # Table 2: Asset Class Breakdown
    t2 = Table(title="Asset Class Breakdown", box=box.SIMPLE)
    for k in sorted(asset_breakdown.keys()):
        t2.add_column(k, justify="right")
    t2.add_row(*[f"{asset_breakdown[k]:.2f}%" for k in sorted(asset_breakdown.keys())])
    console.print(t2)
    console.print()
    
    # Table 3: Concentration
    t3 = Table(title="Concentration Metrics", box=box.SIMPLE)
    t3.add_column("Metric")
    t3.add_column("Value")
    t3.add_row("Effective Stocks (Equity)", f"{eff_stocks:.1f}")
    t3.add_row("HHI (Equity)", f"{hhi:.0f}")
    t3.add_row("Top 5 Concentration", f"{top_5:.2f}%")
    t3.add_row("Top 10 Concentration", f"{top_10:.2f}%")
    t3.add_row("Top 20 Concentration", f"{top_20:.2f}%")
    t3.add_row("Max Single Stock Weight", f"{max_single:.2f}%")
    console.print(t3)
    console.print()
    
    # Table 4: Per Fund
    t4 = Table(title="Per-Fund Summary", box=box.SIMPLE)
    t4.add_column("Fund Name")
    t4.add_column("Scheme Code")
    t4.add_column("User Amount", justify="right")
    t4.add_column("Latest Month")
    t4.add_column("Holdings", justify="right")
    
    for fn, sc, amt, lm, nh in fund_summaries:
        t4.add_row(fn[:40], str(sc), f"₹{amt:,.0f}", str(lm), str(nh))
    console.print(t4)

if __name__ == "__main__":
    main()
