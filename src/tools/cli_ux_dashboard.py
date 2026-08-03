"""
src/tools/cli_ux_dashboard.py
───────────────────────────────────
High-End Terminal UX & Executive Dashboard Generator for AGY & Mosaic CLI.

Provides visual CLI representations combining:
  1. Header Panel (AUM, AMC metadata, period lookback, active fund count)
  2. Dual-Divergence Sector Rotation Table (Micro-bar charts, colored deltas)
  3. Conviction Radial Tree (Accumulation vs Exit stock branches)
  4. Executive Action Callout Banner

Usage:
  from src.tools.cli_ux_dashboard import render_executive_amc_dashboard
  render_executive_amc_dashboard(amc_name="QUANT", lookback_months=12)
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional
import pandas as pd

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.tree import Tree
from rich.text import Text

from src.db.pool import get_pool
from src.tools.mf_sector_analyzer import classify_sector

logger = logging.getLogger(__name__)


def render_executive_amc_dashboard(amc_name: str = "QUANT", lookback_months: int = 12) -> None:
    """
    Render a high-end executive terminal CLI UX dashboard for any AMC.
    """
    console = Console()
    pool = get_pool()
    clean_amc = amc_name.upper().strip()

    amc_filter_sql = f"AND (fund_name LIKE '{clean_amc}%' OR fund_name LIKE 'RELIANCE%')" if clean_amc == "NIPPON" else f"AND fund_name LIKE '{clean_amc}%'"

    # 1. Fetch available disclosure months for the AMC
    months_df = pool.query_df(f"""
        SELECT DISTINCT as_of_month
        FROM market_data.mf_holdings FINAL
        WHERE fund_name NOT LIKE '%INDEX%' AND fund_name NOT LIKE '%ETF%' {amc_filter_sql}
        ORDER BY as_of_month DESC
    """)

    if len(months_df) < 2:
        console.print(f"[bold red]⚠️ Insufficient data for AMC {amc_name}.[/bold red]")
        return

    curr_m = months_df.iloc[0]['as_of_month'].strftime("%Y-%m-%d")
    idx = min(lookback_months - 1, len(months_df) - 1)
    prior_m = months_df.iloc[idx]['as_of_month'].strftime("%Y-%m-%d")

    # 2. Fetch current & prior month equity holdings
    df_curr = pool.query_df(f"""
        SELECT fund_name, security_name, market_value_cr, pct_of_nav
        FROM market_data.mf_holdings FINAL
        WHERE as_of_month = '{curr_m}'
          AND fund_name NOT LIKE '%INDEX%' AND fund_name NOT LIKE '%ETF%'
          AND lower(asset_type) = 'equity' {amc_filter_sql}
    """)

    df_prior = pool.query_df(f"""
        SELECT fund_name, security_name, market_value_cr, pct_of_nav
        FROM market_data.mf_holdings FINAL
        WHERE as_of_month = '{prior_m}'
          AND fund_name NOT LIKE '%INDEX%' AND fund_name NOT LIKE '%ETF%'
          AND lower(asset_type) = 'equity' {amc_filter_sql}
    """)

    if len(df_curr) == 0 or len(df_prior) == 0:
        console.print(f"[bold red]⚠️ Missing holdings data for '{curr_m}' vs '{prior_m}'.[/bold red]")
        return

    df_curr['sector'] = df_curr['security_name'].apply(classify_sector)
    df_prior['sector'] = df_prior['security_name'].apply(classify_sector)

    total_aum_curr = df_curr['market_value_cr'].sum()
    total_aum_prior = df_prior['market_value_cr'].sum()
    active_funds_cnt = df_curr['fund_name'].nunique()

    # Aggregate sectors
    sec_curr = df_curr.groupby('sector')['market_value_cr'].sum().reset_index()
    sec_curr['pct_curr'] = (sec_curr['market_value_cr'] / total_aum_curr) * 100

    sec_prior = df_prior.groupby('sector')['market_value_cr'].sum().reset_index()
    sec_prior['pct_prior'] = (sec_prior['market_value_cr'] / total_aum_prior) * 100

    merged_sec = pd.merge(sec_curr, sec_prior, on='sector', how='outer').fillna(0)
    merged_sec['mom_weight_change'] = merged_sec['pct_curr'] - merged_sec['pct_prior']
    merged_sec['mom_capital_change_cr'] = merged_sec['market_value_cr_x'] - merged_sec['market_value_cr_y']
    merged_sec = merged_sec.sort_values(by='mom_weight_change', ascending=False)

    # 3. Header Panel
    header = Text()
    header.append(f"🏛️ {clean_amc} MUTUAL FUND ", style="bold gold1")
    header.append("│ EXECUTIVE QUANT & SECTOR ROTATION DASHBOARD\n", style="bold white")
    header.append("Equity AUM: ", style="dim")
    header.append(f"₹{total_aum_curr:,.2f} Cr  ", style="bold cyan")
    header.append("Period: ", style="dim")
    header.append(f"{prior_m} ➔ {curr_m} ({lookback_months}M Lookback)  ", style="bold green")
    header.append("Active Funds: ", style="dim")
    header.append(f"{active_funds_cnt} Funds", style="bold yellow")

    console.print(Panel(header, border_style="gold1", title="[bold white]MOSAIC QUANT INTELLIGENCE[/bold white]", title_align="left"))

    # 4. Rotation Matrix Table
    table = Table(
        show_header=True,
        header_style="bold white on blue",
        border_style="dim cyan",
        expand=True
    )

    table.add_column("Sector / Theme", style="bold white", width=28)
    table.add_column("Prior Wt", justify="right", style="dim", width=9)
    table.add_column("Curr Wt", justify="right", style="bold white", width=9)
    table.add_column(f"{lookback_months}M Shift", justify="right", width=11)
    table.add_column("Divergence Bar", justify="center", width=20)
    table.add_column("Rotation Signal", justify="center", width=24)

    for _, r in merged_sec.iterrows():
        w_delta = r['mom_weight_change']
        prior_w = f"{r['pct_prior']:.2f}%"
        curr_w = f"{r['pct_curr']:.2f}%"
        shift_str = f"{w_delta:+.2f}%"

        # Generate Divergence Bar
        num_blocks = min(10, max(1, int(abs(w_delta))))
        if w_delta > 0:
            bar = f"▲ [green]{'█' * num_blocks}[/green]"
        elif w_delta < 0:
            bar = f"▼ [red]{'█' * num_blocks}[/red]"
        else:
            bar = "• [dim]─[/dim]"

        if w_delta >= 1.5:
            sig = "[bold green]🟢 AGGRESSIVE ROTATION IN[/bold green]"
        elif w_delta >= 0.4:
            sig = "[green]🟢 MODERATE ACCUMULATION[/green]"
        elif w_delta <= -1.5:
            sig = "[bold red]🔴 AGGRESSIVE ROTATION OUT[/bold red]"
        elif w_delta <= -0.4:
            sig = "[yellow]🔴 MODERATE TRIM[/yellow]"
        else:
            sig = "[dim]⏸️ NEUTRAL / STABLE[/dim]"

        shift_style = "bold bright_green" if w_delta > 0 else ("bold bright_red" if w_delta < 0 else "white")
        table.add_row(r['sector'], prior_w, curr_w, f"[{shift_style}]{shift_str}[/{shift_style}]", bar, sig)

    # 5. Stock Conviction Radial Tree
    stock_curr = df_curr.groupby('security_name').agg(val_curr=('market_value_cr', 'sum'), sector=('sector', 'first')).reset_index()
    stock_prior = df_prior.groupby('security_name').agg(val_prior=('market_value_cr', 'sum'), sector=('sector', 'first')).reset_index()
    stock_m = pd.merge(stock_curr, stock_prior, on=['security_name', 'sector'], how='outer').fillna(0)
    stock_m['stock_delta_cr'] = stock_m['val_curr'] - stock_m['val_prior']

    tree = Tree(f"[bold yellow]2. HIGH-CONVICTION STOCK NODES & FLOW BRANCHES ({clean_amc})[/bold yellow]")

    # Inflow Branch
    in_branch = tree.add("[bold green]🟢 TOP CAPITAL INFLOW BRANCHES[/bold green]")
    top_inflows = stock_m.sort_values(by='stock_delta_cr', ascending=False).head(4)
    for _, st in top_inflows.iterrows():
        if st['stock_delta_cr'] > 0:
            nav_w = (st['val_curr'] / total_aum_curr) * 100
            in_branch.add(f"[cyan]{st['security_name']}[/cyan] ➔ [bold white]{nav_w:.2f}% NAV[/bold white] ([dim]₹{st['val_curr']:,.1f} Cr[/dim]) | [bold green]+₹{st['stock_delta_cr']:,.1f} Cr Inflow[/bold green]")

    # Outflow Branch
    out_branch = tree.add("[bold red]🔴 TOP CAPITAL OUTFLOW BRANCHES[/bold red]")
    top_outflows = stock_m.sort_values(by='stock_delta_cr', ascending=True).head(4)
    for _, st in top_outflows.iterrows():
        if st['stock_delta_cr'] < 0:
            nav_w = (st['val_curr'] / total_aum_curr) * 100
            out_branch.add(f"[red]{st['security_name']}[/red] ➔ [bold white]{nav_w:.2f}% NAV[/bold white] | [bold red]-₹{abs(st['stock_delta_cr']):,.1f} Cr Outflow / Exit[/bold red]")

    console.print(Panel(tree, border_style="yellow"))


def render_master_amc_rotation_dashboard(amc_name: str = "QUANT", lookback_months: int = 12) -> None:
    """
    Render Master Executive AMC Dashboard combining:
      1. Top Half: Dynamic Horizontal Left-to-Right Sector Rotation Timeline Box Flow
      2. Bottom Half: Exhaustive Side-by-Side Green ADD & Red REMOVE Stock Shift Ledger
    """
    from rich.console import Console
    from rich.panel import Panel
    from src.tools.mf_time_journey import render_horizontal_box_journey
    from src.tools.mf_sector_rotation import render_exhaustive_shift_visual_dashboard

    console = Console()
    clean_amc = amc_name.upper().strip()

    console.print(f"\n[bold gold1]🏛️ MASTER EXECUTIVE ROTATION DASHBOARD: {clean_amc} AMC[/bold gold1]\n")

    # 1. TOP HALF: Left-to-Right Horizontal Sector Timeline
    render_horizontal_box_journey(amc_name=amc_name, num_milestones=4)

    console.print("\n" + "─" * 84 + "\n")

    # 2. BOTTOM HALF: Exhaustive Side-by-Side Stock Shifts
    render_exhaustive_shift_visual_dashboard(amc_name=amc_name, lookback_months=lookback_months, max_display_rows=15)


from langchain_core.tools import tool

@tool
def display_master_amc_dashboard(amc_name: str = "QUANT", lookback_months: int = 12) -> str:
    """
    Display the Master Executive AMC Sector & Stock Rotation Dashboard with Left-to-Right Timeline and Side-by-Side Shift Matrix.

    Parameters:
      amc_name: Target AMC ('QUANT', 'DSP', 'HDFC', 'NIPPON', 'BAJAJ', 'ICICI', 'SBI')
      lookback_months: Months to look back for shift audit (default: 12)
    """
    try:
        render_master_amc_rotation_dashboard(amc_name=amc_name, lookback_months=lookback_months)
        return f"Successfully rendered Master Executive Rotation Dashboard for {amc_name} AMC on terminal console."
    except Exception as exc:
        logger.error("Error displaying Master AMC Dashboard: %s", exc)
        return f"Error displaying Master AMC Dashboard: {exc}"
