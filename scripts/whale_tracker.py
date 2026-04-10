"""
scripts/whale_tracker.py
────────────────────────
Monitors "Whale" (Institutional) moves in core 2026 macro themes:
  1. Commodities (Gold/Silver)
  2. Electrification & Nuclear (NTPC, L&T, BHEL, etc.)
  3. Energy (ONGC, IOC, etc.)

Tracks changes between the two most recent portfolio disclosures for:
  - Quant Multi Asset Fund (120821)
  - ICICI Multi Asset Fund (120334)
  - DSP Multi Asset Fund (152056)
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

WHALE_FUNDS = {
    '120821': 'Quant Multi Asset',
    '120334': 'ICICI Multi Asset',
    '152056': 'DSP Multi Asset'
}

# Key themes to track
THEME_KEYWORDS = {
    '🥈 Silver': ['Silver', 'SILVERBEES'],
    '🥇 Gold': ['Gold', 'GOLDBEES'],
    '⚛️ Nuclear/Grid': ['NTPC', 'L&T', 'Larsen', 'BHEL', 'Bharat Heavy', 'Power Grid', 'POWERGRID'],
    '🛢️ Energy': ['ONGC', 'IOC', 'Coal India', 'BPCL', 'HPCL', 'GAIL', 'Adani Green', 'Premier Energies'],
    '🏗️ Infra': ['Larsen', 'L&T', 'Reliance', 'Adani Ports', 'NMDC', 'REC', 'PFC']
}

def get_fund_holdings(client, scheme_code: str, as_of_month: str) -> Dict[str, float]:
    """Fetch security_name -> pct_of_nav for a given fund and month."""
    query = f"""
    SELECT security_name, pct_of_nav 
    FROM market_data.mf_holdings 
    WHERE scheme_code = '{scheme_code}' AND as_of_month = '{as_of_month}'
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

    for scheme_code, fund_name in WHALE_FUNDS.items():
        # 1. Identify two most recent months for this fund
        months_query = f"SELECT DISTINCT as_of_month FROM market_data.mf_holdings WHERE scheme_code = '{scheme_code}' ORDER BY as_of_month DESC LIMIT 2"
        months = [str(r[0]) for r in client.query(months_query).result_rows]

        if len(months) < 2:
            console.print(f"\n[yellow]⚠ {fund_name}: Insufficient historical data to track changes.[/yellow]")
            continue

        latest_m, prev_m = months[0], months[1]
        latest_h = get_fund_holdings(client, scheme_code, latest_m)
        prev_h = get_fund_holdings(client, scheme_code, prev_m)

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

    client.close()

if __name__ == "__main__":
    run_whale_tracker()
