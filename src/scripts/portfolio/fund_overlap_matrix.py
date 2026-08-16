import argparse
import sys
from collections import defaultdict
import itertools
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import box

from src.db.pool import get_pool, get_client

def main():
    parser = argparse.ArgumentParser(description="Pairwise Fund Overlap Calculator")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--funds", help="Comma-separated list of fund names")
    group.add_argument("--schemes", help="Comma-separated list of scheme codes")
    
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
                fund_list.append(rows[0][0])
            else:
                console.print(f"[red]Could not find fund for scheme code: {sc}[/red]")
    else:
        funds = [f.strip() for f in args.funds.split(",")]
        for f in funds:
            # We assume exact or highly similar names if provided directly
            fund_list.append(f)
            
    if len(fund_list) < 2:
        console.print("[red]Need at least 2 funds to compute overlap.[/red]")
        sys.exit(1)
        
    num_funds = len(fund_list)
    
    # dict of fund_name -> {isin: weight}
    fund_holdings = {}
    holding_details = {} # isin -> {'security_name': ...}
    
    for fn in fund_list:
        q = f"""
            SELECT isin, security_name, pct_of_nav, as_of_month 
            FROM market_data.mf_holdings FINAL 
            WHERE fund_name = '{fn}' 
              AND asset_type = 'equity'
              AND as_of_month = (SELECT max(as_of_month) FROM market_data.mf_holdings FINAL WHERE fund_name = '{fn}')
        """
        rows = client.query(q).result_rows
        
        if not rows:
            console.print(f"[yellow]No equity holdings found for {fn}[/yellow]")
            fund_holdings[fn] = {}
            continue
            
        weights = {}
        for r in rows:
            isin, sec_name, pct_nav, _ = r
            if not isin:
                isin = sec_name
            weights[isin] = pct_nav
            if isin not in holding_details:
                holding_details[isin] = {'security_name': sec_name}
                
        fund_holdings[fn] = weights
        
    # Pairwise overlap
    overlap_matrix = defaultdict(dict)
    shared_count_matrix = defaultdict(dict)
    
    for fn1, fn2 in itertools.product(fund_list, repeat=2):
        if fn1 == fn2:
            overlap_matrix[fn1][fn2] = 100.0
            shared_count_matrix[fn1][fn2] = len(fund_holdings[fn1])
            continue
            
        h1 = fund_holdings[fn1]
        h2 = fund_holdings[fn2]
        
        shared_isins = set(h1.keys()).intersection(set(h2.keys()))
        overlap_pct = sum(min(h1[isin], h2[isin]) for isin in shared_isins)
        
        overlap_matrix[fn1][fn2] = overlap_pct
        shared_count_matrix[fn1][fn2] = len(shared_isins)
        
    # Cannibalized stocks (held by > 1 fund)
    # isin -> list of weights
    stock_weights = defaultdict(list)
    for fn, holdings in fund_holdings.items():
        for isin, w in holdings.items():
            stock_weights[isin].append(w)
            
    cannibalized = []
    unique_holdings = defaultdict(list)
    
    for isin, weights in stock_weights.items():
        if len(weights) > 1:
            avg_w = sum(weights) / len(weights)
            cannibalized.append({
                'isin': isin,
                'name': holding_details[isin]['security_name'],
                'count': len(weights),
                'avg_w': avg_w,
                'max_w': max(weights),
                'min_w': min(weights),
                'score': len(weights) * avg_w
            })
        elif len(weights) == 1:
            # find which fund has it
            for fn, h in fund_holdings.items():
                if isin in h:
                    unique_holdings[fn].append({
                        'isin': isin,
                        'name': holding_details[isin]['security_name'],
                        'weight': weights[0]
                    })
                    break

    cannibalized.sort(key=lambda x: x['score'], reverse=True)
    
    # Output
    console.print(Panel("[bold cyan]Fund Overlap Matrix[/bold cyan]"))
    
    # Table 1: Overlap %
    t1 = Table(title="Pairwise Overlap Matrix (%)", box=box.SIMPLE)
    t1.add_column("Fund")
    for fn in fund_list:
        t1.add_column(fn[:20], justify="right")
        
    for fn1 in fund_list:
        row = [fn1[:20]]
        for fn2 in fund_list:
            row.append(f"{overlap_matrix[fn1][fn2]:.1f}%")
        t1.add_row(*row)
    console.print(t1)
    console.print()
    
    # Table 2: Shared counts
    t2 = Table(title="Shared Holdings Count Matrix", box=box.SIMPLE)
    t2.add_column("Fund")
    for fn in fund_list:
        t2.add_column(fn[:20], justify="right")
        
    for fn1 in fund_list:
        row = [fn1[:20]]
        for fn2 in fund_list:
            row.append(f"{shared_count_matrix[fn1][fn2]}")
        t2.add_row(*row)
    console.print(t2)
    console.print()
    
    # Table 3: Cannibalized
    t3 = Table(title="Most Cannibalized Holdings (Top 15)", box=box.SIMPLE)
    t3.add_column("Security Name")
    t3.add_column("# Funds Holding", justify="center")
    t3.add_column("Avg Weight %", justify="right")
    t3.add_column("Max Weight %", justify="right")
    t3.add_column("Min Weight %", justify="right")
    
    for c in cannibalized[:15]:
        t3.add_row(c['name'][:40], str(c['count']), f"{c['avg_w']:.2f}%", f"{c['max_w']:.2f}%", f"{c['min_w']:.2f}%")
    console.print(t3)
    console.print()
    
    # Table 4: Unique Differentiators
    t4 = Table(title="Unique Holdings (Top 5 per fund)", box=box.SIMPLE)
    t4.add_column("Fund")
    t4.add_column("Security Name")
    t4.add_column("Weight %", justify="right")
    
    for fn in fund_list:
        uh = sorted(unique_holdings[fn], key=lambda x: x['weight'], reverse=True)
        for i, u in enumerate(uh[:5]):
            if i == 0:
                t4.add_row(fn[:20], u['name'][:40], f"{u['weight']:.2f}%")
            else:
                t4.add_row("", u['name'][:40], f"{u['weight']:.2f}%")
    console.print(t4)
    
if __name__ == "__main__":
    main()
