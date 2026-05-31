"""
scripts/whale_tracker.py
────────────────────────
Monitors "Whale" (Institutional) moves in core 2026 macro themes:
  1. Commodities (Gold/Silver)
  2. Electrification & Nuclear (NTPC, L&T, BHEL, etc.)
  3. Energy (ONGC, IOC, etc.)

Tracks changes between the two most recent portfolio disclosures for all
multi-asset funds in ClickHouse (≥2 months of data required).
"""

import sys
import os
from datetime import datetime
from typing import Dict, List, Any

# Add project root to sys.path
sys.path.append(os.getcwd())

from config.settings import settings
import clickhouse_connect
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

# ── Configuration ─────────────────────────────────────────────────────────────

WHALE_FUNDS = [
    {
        'name': 'Nippon India Multi Asset',
        'query_filter': "scheme_code = 'RLMF806'",
    },
    {
        'name': 'Nippon India Multi Asset FoF',
        'query_filter': "scheme_code = 'RLMF811'",
    },
    {
        'name': 'DSP Multi Asset',
        'query_filter': "scheme_code = '152056'",
    },
    {
        'name': 'DSP Multi Asset Omni FoF',
        'query_filter': "scheme_code = '154167'",
    },
    {
        'name': 'Bajaj Multi Asset',
        'query_filter': "scheme_code = '152639'",
    },
    {
        'name': 'Quant Multi Asset',
        'query_filter': "scheme_code = '120821'",
    },
    {
        'name': 'ICICI Multi Asset',
        'query_filter': "fund_name = 'ICICI_MULTI_ASSET'",
    },
]

# Key themes to track
THEME_KEYWORDS = {
    '🥈 Silver': ['Silver', 'SILVERBEES'],
    '🥇 Gold': ['Gold', 'GOLDBEES'],
    '⚛️ Nuclear/Grid': ['NTPC', 'L&T', 'Larsen', 'BHEL', 'Bharat Heavy', 'Power Grid', 'POWERGRID'],
    '🛢️ Energy': ['ONGC', 'IOC', 'Coal India', 'BPCL', 'HPCL', 'GAIL', 'Adani Green', 'Premier Energies'],
    '🏗️ Infra': ['Larsen', 'L&T', 'Reliance', 'Adani Ports', 'NMDC', 'REC', 'PFC']
}

def get_fund_holdings(client, query_filter: str, as_of_month: str) -> Dict[str, float]:
    """Fetch security_name -> max(pct_of_nav) for a given fund filter and month."""
    query = f"""
    SELECT security_name, max(pct_of_nav)
    FROM market_data.mf_holdings 
    WHERE {query_filter} AND as_of_month = '{as_of_month}'
    GROUP BY security_name
    """
    res = client.query(query).result_rows
    return {row[0]: float(row[1]) for row in res}

def run_whale_tracker():
    console = Console()
    client = clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
    )

    console.print(Panel(
        "[bold cyan]🐋 Whale Tracker: Institutional Macro Moves[/bold cyan]\n"
        "[dim]Tracking weight shifts in Multi-Asset Funds (Silver, Gold, Nuclear, Energy)[/dim]",
        border_style="cyan"
    ))

    # Composite aggregations for the Institutional Conviction Index
    composite_latest = {}
    composite_prev = {}
    fund_ownership = {}

    for fund in WHALE_FUNDS:
        fund_name = fund['name']
        query_filter = fund['query_filter']
        # 1. Identify two most recent months for this fund
        months_query = f"SELECT DISTINCT as_of_month FROM market_data.mf_holdings WHERE {query_filter} ORDER BY as_of_month DESC LIMIT 2"
        months = [str(r[0]) for r in client.query(months_query).result_rows]

        if len(months) < 2:
            console.print(f"\n[yellow]⚠ {fund_name}: Insufficient historical data to track changes.[/yellow]")
            continue

        latest_m, prev_m = months[0], months[1]
        latest_h = get_fund_holdings(client, query_filter, latest_m)
        prev_h = get_fund_holdings(client, query_filter, prev_m)

        # Accumulate for Composite Index
        for sec, val in latest_h.items():
            composite_latest[sec] = composite_latest.get(sec, 0.0) + val
            if sec not in fund_ownership:
                fund_ownership[sec] = []
            fund_ownership[sec].append(fund_name)

        for sec, val in prev_h.items():
            composite_prev[sec] = composite_prev.get(sec, 0.0) + val

        table = Table(title=f"{fund_name} (Changes: {prev_m} → {latest_m})", show_header=True)
        table.add_column("Theme", style="dim")
        table.add_column("Security", style="magenta")
        table.add_column("Prev %", justify="right")
        table.add_column("Latest %", justify="right")
        table.add_column("Change", justify="right")

        has_changes = False
        all_securities = set(latest_h.keys()) | set(prev_h.keys())

        for sec in all_securities:
            # Check if security matches any theme
            theme_label = None
            for label, kws in THEME_KEYWORDS.items():
                if any(kw.lower() in sec.lower() for kw in kws):
                    theme_label = label
                    break
            
            if not theme_label:
                continue

            prev_val = prev_h.get(sec, 0.0)
            curr_val = latest_h.get(sec, 0.0)
            diff = curr_val - prev_val

            if abs(diff) > 0.01: # Filter out noise < 0.01%
                has_changes = True
                diff_str = f"{diff:+.2f}%"
                if diff > 0.5: style = "bold green"
                elif diff < -0.5: style = "bold red"
                elif diff > 0: style = "green"
                elif diff < 0: style = "red"
                else: style = "dim"

                table.add_row(
                    theme_label,
                    sec,
                    f"{prev_val:.2f}%",
                    f"{curr_val:.2f}%",
                    f"[{style}]{diff_str}[/{style}]"
                )

        if has_changes:
            console.print(table)
        else:
            console.print(f"[dim]  - No significant changes in tracked themes for {fund_name}.[/dim]")

    # ── 2. Composite Institutional Conviction Index ──────────────────────────────
    console.print("\n" + "=" * 80)
    console.print(Panel(
        "[bold green]🐳 COMPOSITE INSTITUTIONAL CONVICTION INDEX (ALL FUNDS)[/bold green]\n"
        "[dim]Aggregated multi-asset flows and cross-ownership conviction signals[/dim]",
        border_style="green"
    ))

    # 2a. Theme Aggregations
    theme_latest = {}
    theme_prev = {}

    for label, kws in THEME_KEYWORDS.items():
        theme_latest[label] = 0.0
        theme_prev[label] = 0.0
        
        # Calculate sum across all matching securities in composite sets
        for sec in composite_latest:
            if any(kw.lower() in sec.lower() for kw in kws):
                theme_latest[label] += composite_latest[sec]
        for sec in composite_prev:
            if any(kw.lower() in sec.lower() for kw in kws):
                theme_prev[label] += composite_prev[sec]

    theme_table = Table(title="Unified Macro Theme Allocations", show_header=True)
    theme_table.add_column("Macro Theme", style="bold cyan")
    theme_table.add_column("Combined Prev Weight", justify="right")
    theme_table.add_column("Combined Latest Weight", justify="right")
    theme_table.add_column("Net Flow Change", justify="right")

    for theme, curr_val in theme_latest.items():
        prev_val = theme_prev[theme]
        diff = curr_val - prev_val
        diff_str = f"{diff:+.2f}%"
        
        if diff > 0.5: style = "bold green"
        elif diff < -0.5: style = "bold red"
        elif diff > 0: style = "green"
        elif diff < 0: style = "red"
        else: style = "dim"

        theme_table.add_row(
            theme,
            f"{prev_val:.2f}%",
            f"{curr_val:.2f}%",
            f"[{style}]{diff_str}[/{style}]"
        )
    console.print(theme_table)

    # 2b. High Conviction Single-Name Equities (Cross-ownership >= 2 funds)
    # Exclude Gold/Silver ETFs, cash, or derivatives to focus on direct equities
    exclude_kws = THEME_KEYWORDS['🥈 Silver'] + THEME_KEYWORDS['🥇 Gold'] + ['cash', 'liquid', 'treasury', 'arbitrage', 'mutual fund', 'yield', 'margin', 'repo']
    
    equity_conviction = []
    for sec, funds in fund_ownership.items():
        # Check exclusion
        if any(kw.lower() in sec.lower() for kw in exclude_kws):
            continue
            
        num_funds = len(funds)
        if num_funds >= 2:
            prev_val = composite_prev.get(sec, 0.0)
            curr_val = composite_latest.get(sec, 0.0)
            diff = curr_val - prev_val
            
            # Conviction rating definition
            if num_funds >= 3 and diff > 0:
                rating = "🔥 CORE CONVICTION"
                r_style = "bold green"
            elif num_funds >= 2 and diff > 0.3:
                rating = "📈 TACTICAL ADD"
                r_style = "green"
            elif diff < -0.3:
                rating = "⚠️ TRIMMING"
                r_style = "bold red"
            else:
                rating = "HOLDING"
                r_style = "dim"
                
            equity_conviction.append({
                "security": sec,
                "num_funds": num_funds,
                "prev_val": prev_val,
                "curr_val": curr_val,
                "diff": diff,
                "rating": rating,
                "r_style": r_style
            })

    # Sort: most funds holding first, then highest current weight
    equity_conviction.sort(key=lambda x: (x["num_funds"], x["curr_val"]), reverse=True)

    equity_table = Table(title="High-Conviction Equity Cross-Ownership", show_header=True)
    equity_table.add_column("Security Name", style="magenta")
    equity_table.add_column("Funds Count", justify="center")
    equity_table.add_column("Combined Prev %", justify="right")
    equity_table.add_column("Combined Latest %", justify="right")
    equity_table.add_column("Net Change", justify="right")
    equity_table.add_column("Conviction Rating", justify="center")

    for item in equity_conviction[:15]: # Show top 15 high-conviction names
        diff = item["diff"]
        diff_str = f"{diff:+.2f}%"
        if diff > 0.3: d_style = "green"
        elif diff < -0.3: d_style = "red"
        else: d_style = "dim"

        equity_table.add_row(
            item["security"],
            str(item["num_funds"]),
            f"{item['prev_val']:.2f}%",
            f"{item['curr_val']:.2f}%",
            f"[{d_style}]{diff_str}[/{d_style}]",
            f"[{item['r_style']}]{item['rating']}[/{item['r_style']}]"
        )
        
    if equity_conviction:
        console.print(equity_table)
    else:
        console.print("[dim]  - No multi-fund cross-ownership detected in direct equities.[/dim]")

    client.close()

if __name__ == "__main__":
    run_whale_tracker()
