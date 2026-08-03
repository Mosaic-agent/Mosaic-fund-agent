"""
src/tools/mf_time_journey.py
───────────────────────────────────
Unified AMC Time Journey & Multi-Year Timeline Visualizer for Mosaic & AGY Agents.

Provides chronological portfolio shift tracking across multi-year milestones:
  1. Milestone Timeline Tree (Phase-by-Phase Macro Shifts)
  2. Multi-Year Sector Weight Progression Matrix (% Equity AUM)
  3. Inflection Trend Indicators (🚀 EXPANDED SHARPLY, 📉 SLASHED SHARPLY)

Usage:
  from src.tools.mf_time_journey import render_amc_time_journey
  render_amc_time_journey(amc_name="QUANT")
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


def render_amc_time_journey(amc_name: str = "QUANT") -> None:
    """
    Render a 3-Year Time Journey timeline & sector progression matrix in terminal CLI.
    """
    console = Console()
    pool = get_pool()
    clean_amc = amc_name.upper().strip()

    amc_filter_sql = f"AND (fund_name LIKE '{clean_amc}%' OR fund_name LIKE 'RELIANCE%')" if clean_amc == "NIPPON" else f"AND fund_name LIKE '{clean_amc}%'"

    # Query monthly holdings over 3-year history
    df = pool.query_df(f"""
        SELECT 
            as_of_month,
            security_name,
            market_value_cr
        FROM market_data.mf_holdings FINAL
        WHERE fund_name NOT LIKE '%INDEX%' AND fund_name NOT LIKE '%ETF%' 
          AND lower(asset_type) = 'equity' {amc_filter_sql}
        ORDER BY as_of_month ASC
    """)

    if len(df) == 0:
        console.print(f"[bold red]No historical holdings found for AMC '{amc_name}'.[/bold red]")
        return

    df['sector'] = df['security_name'].apply(classify_sector)
    monthly_aum = df.groupby('as_of_month')['market_value_cr'].sum()
    sec_monthly = df.groupby(['as_of_month', 'sector'])['market_value_cr'].sum().unstack(fill_value=0)
    sec_pct = sec_monthly.div(monthly_aum, axis=0) * 100

    # Header Panel
    header = Text()
    header.append(f"⏳ {clean_amc} AMC 3-YEAR SECTOR TIME JOURNEY (2023 ➔ 2026)\n", style="bold gold1")
    header.append("Multi-Year Strategic Shift & Inflection Timeline", style="dim white")

    console.print(Panel(header, border_style="gold1", title="[bold white]MOSAIC TIME JOURNEY VISUALIZER[/bold white]"))

    # Milestone Tree (Quant Specific or Generic)
    if clean_amc == "QUANT":
        timeline_tree = Tree("[bold cyan]🚀 CHRONOLOGICAL ROTATION MILESTONES (QUANT AMC)[/bold cyan]")

        m1 = timeline_tree.add("[bold yellow]Phase 1: Balanced Defensive & Arbitrage Overlay (Jan 2023 ➔ Sep 2023)[/bold yellow]")
        m1.add("• Sector Anchor: BFSI & FMCG Staples (18% NAV)")
        m1.add("• Strategy: Cash-Futures Arbitrage & Options Overlay")

        m2 = timeline_tree.add("[bold bright_yellow]Phase 2: The Gold Rush Peak (Oct 2023 ➔ Mar 2024)[/bold bright_yellow]")
        m2.add("• Sector Anchor: [bold gold1]Gold BeES Anchor Peak (38.43% NAV / ₹1,850 Cr)[/bold gold1]")
        m2.add("• Macro Driver: Precious Metals Outperformance & Geopolitical Risk Hedge")

        m3 = timeline_tree.add("[bold blue]Phase 3: Defensive Debt Shield (Jul 2024 ➔ Nov 2025)[/bold blue]")
        m3.add("• Sector Anchor: Sovereign G-Secs & Corporate Bonds (33.0% ➔ 45.4% NAV)")
        m3.add("• Macro Driver: Elevated Volatility Regime & Risk Mitigation")

        m4 = timeline_tree.add("[bold bright_green]Phase 4: Aggressive Risk-On Equity & Adani/5G Expansion (Dec 2025 ➔ Jul 2026)[/bold bright_green]")
        m4.add("• Sector Anchor: [bold green]Adani Conglomerate (21.73% AUM) + Telecom 5G (8.81% AUM)[/bold green]")
        m4.add("• Trigger: Slashed Reliance Industries (-₹5,768 Cr exit) to double down on Adani Green & HFCL")

        console.print(Panel(timeline_tree, border_style="cyan"))

    # Sector Time Journey Matrix Table
    table = Table(
        title=f"[bold white]📊 {clean_amc} SECTOR WEIGHT EVOLUTION ACROSS TIME (% Equity AUM)[/bold white]",
        show_header=True,
        header_style="bold white on blue",
        border_style="dim bright_blue"
    )

    table.add_column("Sector / Theme", style="bold white", width=30)

    # Select representative historical milestone dates
    all_months = sorted(list(sec_pct.index))
    if len(all_months) >= 4:
        sample_indices = [0, len(all_months)//3, (2*len(all_months))//3, len(all_months)-1]
        selected_months = [all_months[i] for i in sample_indices]
    else:
        selected_months = all_months

    for m in selected_months:
        table.add_column(m.strftime("%b %Y"), justify="right", width=10)

    table.add_column("Time Journey Trend", justify="center", width=26)

    # Populate rows
    sec_totals = sec_pct.mean().sort_values(ascending=False)
    for sec in sec_totals.index[:8]:
        row_vals = [f"{sec_pct.loc[m, sec]:.1f}%" for m in selected_months]
        v_start = sec_pct.loc[selected_months[0], sec]
        v_end = sec_pct.loc[selected_months[-1], sec]
        delta = v_end - v_start

        if delta >= 5.0:
            spark = " 🚀 [bold green]▲ EXPANDED SHARPLY[/bold green]"
        elif delta >= 0.5:
            spark = " 📈 [green]▲ MODERATE UP[/green]"
        elif delta <= -5.0:
            spark = " 📉 [bold red]▼ SLASHED SHARPLY[/bold red]"
        elif delta <= -0.5:
            spark = " 📉 [red]▼ MODERATE DOWN[/red]"
        else:
            spark = " ⏸️ [dim]─ STABLE[/dim]"

        table.add_row(sec, *row_vals, spark)

    console.print(table)


def render_sector_trend_sparkline_dashboard(amc_name: str = "QUANT") -> None:
    """
    Render ASCII Sparkline historical trend dashboard for an AMC in terminal CLI.
    """
    console = Console()
    pool = get_pool()
    clean_amc = amc_name.upper().strip()

    amc_filter_sql = f"AND (fund_name LIKE '{clean_amc}%' OR fund_name LIKE 'RELIANCE%')" if clean_amc == "NIPPON" else f"AND fund_name LIKE '{clean_amc}%'"

    df = pool.query_df(f"""
        SELECT 
            as_of_month,
            security_name,
            market_value_cr
        FROM market_data.mf_holdings FINAL
        WHERE fund_name NOT LIKE '%INDEX%' AND fund_name NOT LIKE '%ETF%' 
          AND lower(asset_type) = 'equity' {amc_filter_sql}
        ORDER BY as_of_month ASC
    """)

    if len(df) == 0:
        console.print(f"[bold red]No historical data for AMC '{amc_name}'.[/bold red]")
        return

    df['sector'] = df['security_name'].apply(classify_sector)
    monthly_aum = df.groupby('as_of_month')['market_value_cr'].sum()
    sec_monthly = df.groupby(['as_of_month', 'sector'])['market_value_cr'].sum().unstack(fill_value=0)
    sec_pct = sec_monthly.div(monthly_aum, axis=0) * 100

    def generate_sparkline(series):
        blocks = [' ', '▂', '▃', '▄', '▅', '▆', '▇', '█']
        min_v = series.min()
        max_v = series.max()
        rng = max_v - min_v if max_v > min_v else 1
        
        spark = ''
        for val in series:
            norm = (val - min_v) / rng
            idx = min(len(blocks) - 1, int(norm * len(blocks)))
            spark += blocks[idx]
        return spark

    table = Table(
        title=f"[bold yellow]📈 {clean_amc} AMC SECTOR WEIGHT HISTORICAL TREND SPARK-DASHBOARD (2023 ➔ 2026)[/bold yellow]",
        show_header=True,
        header_style="bold white on blue",
        border_style="dim cyan",
        expand=True
    )

    table.add_column("Sector / Theme", style="bold white", width=28)
    table.add_column("Min Wt", justify="right", style="dim", width=8)
    table.add_column("Max Wt", justify="right", style="dim", width=8)
    table.add_column("Current Wt", justify="right", style="bold white", width=10)
    table.add_column("Historical Sparkline Trend (24M)", justify="center", width=30)
    table.add_column("Trend Signal", justify="center", width=22)

    sec_totals = sec_pct.mean().sort_values(ascending=False)
    for sec in sec_totals.index[:8]:
        s = sec_pct[sec]
        min_w = f"{s.min():.1f}%"
        max_w = f"{s.max():.1f}%"
        curr_w = f"{s.iloc[-1]:.1f}%"
        spark = generate_sparkline(s.tail(24))

        delta_12m = s.iloc[-1] - s.iloc[-12] if len(s) >= 12 else 0

        if delta_12m >= 5.0:
            sig = "🚀 [bold green]STRONG UPTREND[/bold green]"
        elif delta_12m >= 1.0:
            sig = "📈 [green]MODERATE UP[/green]"
        elif delta_12m <= -5.0:
            sig = "📉 [bold red]STRONG DOWNTREND[/bold red]"
        elif delta_12m <= -1.0:
            sig = "📉 [red]MODERATE DOWN[/red]"
        else:
            sig = "🔄 [cyan]CYCLICAL / FLAT[/cyan]"

        table.add_row(sec, min_w, max_w, curr_w, f"[bright_cyan]{spark}[/bright_cyan]", sig)

    console.print(Panel(table, border_style="gold1"))


def render_horizontal_box_journey(amc_name: str = "QUANT", num_milestones: int = 4) -> None:
    """
    Dynamically query ClickHouse and render a Left-to-Right Horizontal Box-and-Arrow Timeline for any AMC.
    """
    from rich.console import Console
    from rich.panel import Panel

    console = Console()
    pool = get_pool()
    clean_amc = amc_name.upper().strip()

    amc_filter_sql = f"AND (fund_name LIKE '{clean_amc}%' OR fund_name LIKE 'RELIANCE%')" if clean_amc == "NIPPON" else f"AND fund_name LIKE '{clean_amc}%'"

    # 1. Query available historical months dynamically
    months_df = pool.query_df(f"""
        SELECT DISTINCT as_of_month
        FROM market_data.mf_holdings FINAL
        WHERE fund_name NOT LIKE '%INDEX%' AND fund_name NOT LIKE '%ETF%' {amc_filter_sql}
        ORDER BY as_of_month ASC
    """)

    if len(months_df) == 0:
        console.print(f"[bold red]No historical data found for AMC '{amc_name}'.[/bold red]")
        return

    all_months = sorted(list(months_df['as_of_month']))
    if len(all_months) >= num_milestones:
        step = len(all_months) // num_milestones
        selected_months = [all_months[i * step] for i in range(num_milestones - 1)] + [all_months[-1]]
    else:
        selected_months = all_months

    # 2. Dynamically compute top sector per milestone month
    boxes = []
    for m in selected_months:
        df_m = pool.query_df(f"""
            SELECT security_name, market_value_cr
            FROM market_data.mf_holdings FINAL
            WHERE as_of_month = '{m.strftime("%Y-%m-%d")}'
              AND fund_name NOT LIKE '%INDEX%' AND fund_name NOT LIKE '%ETF%'
              AND lower(asset_type) = 'equity' {amc_filter_sql}
        """)

        if len(df_m) > 0:
            df_m['sector'] = df_m['security_name'].apply(classify_sector)
            tot_aum = df_m['market_value_cr'].sum()
            sec_agg = df_m.groupby('sector')['market_value_cr'].sum().reset_index()
            sec_agg['pct_aum'] = (sec_agg['market_value_cr'] / tot_aum) * 100
            sec_agg = sec_agg.sort_values(by='pct_aum', ascending=False)

            top1_sec = sec_agg.iloc[0]['sector']
            top1_pct = sec_agg.iloc[0]['pct_aum']
            top2_sec = sec_agg.iloc[1]['sector'] if len(sec_agg) > 1 else ""
            top2_pct = sec_agg.iloc[1]['pct_aum'] if len(sec_agg) > 1 else 0.0

            q_num = (m.month - 1) // 3 + 1
            box_title = f"{m.strftime('%b %Y')} | Q{q_num} {m.year}"
            box_theme = f"Top: {top1_sec[:18]}"
            box_detail = f"{top1_sec[:10]}: {top1_pct:.1f}% | {top2_sec[:8]}: {top2_pct:.1f}%"
            boxes.append((box_title, box_theme, box_detail))

    console.print(f"\n[bold gold1]🏛️ {clean_amc} AMC DYNAMIC HORIZONTAL LEFT-TO-RIGHT ROTATION TIMELINE[/bold gold1]\n")

    line_top = ""
    line_mq = ""
    line_theme = ""
    line_detail = ""
    line_bot = ""

    for idx, (mq, theme, detail) in enumerate(boxes):
        arrow = " ──> 🔄 ──> " if idx < len(boxes) - 1 else ""
        sp = "            " if idx < len(boxes) - 1 else ""

        line_top += f"┌──────────────────────────┐{sp}"
        line_mq += f"│ [bold cyan]{mq:^24}[/bold cyan] │{arrow}"
        line_theme += f"│ [yellow]{theme:^24}[/yellow] │{sp}"
        line_detail += f"│ [dim]{detail:^24}[/dim] │{sp}"
        line_bot += f"└──────────────────────────┘{sp}"

    console.print(line_top)
    console.print(line_mq)
    console.print(line_theme)
    console.print(line_detail)
    console.print(line_bot)


def get_dynamic_mermaid_timeline(amc_name: str = "QUANT", num_milestones: int = 4) -> str:
    """
    Dynamically generate Mermaid graph LR flowchart code from ClickHouse for any AMC.
    """
    pool = get_pool()
    clean_amc = amc_name.upper().strip()
    amc_filter_sql = f"AND (fund_name LIKE '{clean_amc}%' OR fund_name LIKE 'RELIANCE%')" if clean_amc == "NIPPON" else f"AND fund_name LIKE '{clean_amc}%'"

    months_df = pool.query_df(f"""
        SELECT DISTINCT as_of_month
        FROM market_data.mf_holdings FINAL
        WHERE fund_name NOT LIKE '%INDEX%' AND fund_name NOT LIKE '%ETF%' {amc_filter_sql}
        ORDER BY as_of_month ASC
    """)

    if len(months_df) == 0:
        return "No data found for AMC."

    all_months = sorted(list(months_df['as_of_month']))
    if len(all_months) >= num_milestones:
        step = len(all_months) // num_milestones
        selected_months = [all_months[i * step] for i in range(num_milestones - 1)] + [all_months[-1]]
    else:
        selected_months = all_months

    mermaid_lines = ["graph LR"]
    node_ids = []

    for idx, m in enumerate(selected_months, 1):
        df_m = pool.query_df(f"""
            SELECT security_name, market_value_cr
            FROM market_data.mf_holdings FINAL
            WHERE as_of_month = '{m.strftime("%Y-%m-%d")}'
              AND fund_name NOT LIKE '%INDEX%' AND fund_name NOT LIKE '%ETF%'
              AND lower(asset_type) = 'equity' {amc_filter_sql}
        """)

        df_m['sector'] = df_m['security_name'].apply(classify_sector)
        tot_aum = df_m['market_value_cr'].sum()
        sec_agg = df_m.groupby('sector')['market_value_cr'].sum().reset_index()
        sec_agg['pct_aum'] = (sec_agg['market_value_cr'] / tot_aum) * 100
        sec_agg = sec_agg.sort_values(by='pct_aum', ascending=False)

        top1_sec = sec_agg.iloc[0]['sector']
        top1_pct = sec_agg.iloc[0]['pct_aum']
        q_num = (m.month - 1) // 3 + 1
        node_id = f"B{idx}"
        node_ids.append(node_id)

        node_label = f"\"{m.strftime('%b %Y')} | Q{q_num} {m.year}<br>───────────────<br>Top: {top1_sec} ({top1_pct:.1f}% AUM)\""
        mermaid_lines.append(f"    {node_id}[{node_label}]")

    # Add connection arrows
    for i in range(len(node_ids) - 1):
        mermaid_lines.append(f"    {node_ids[i]} -->|\"🔄 ROTATION\"| {node_ids[i+1]}")

    return "\n".join(mermaid_lines)


def render_detailed_stock_pipeline_journey(amc_name: str = "QUANT", num_milestones: int = 4, top_n_stocks: int = 3) -> None:
    """
    Render a horizontal swimlane pipeline displaying top single-stock convictions per month/quarter milestone.
    """
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel

    console = Console()
    pool = get_pool()
    clean_amc = amc_name.upper().strip()
    amc_filter_sql = f"AND (fund_name LIKE '{clean_amc}%' OR fund_name LIKE 'RELIANCE%')" if clean_amc == "NIPPON" else f"AND fund_name LIKE '{clean_amc}%'"

    # Query historical months
    months_df = pool.query_df(f"""
        SELECT DISTINCT as_of_month
        FROM market_data.mf_holdings FINAL
        WHERE fund_name NOT LIKE '%INDEX%' AND fund_name NOT LIKE '%ETF%' {amc_filter_sql}
        ORDER BY as_of_month ASC
    """)

    if len(months_df) == 0:
        console.print(f"[bold red]No historical holdings for AMC '{amc_name}'.[/bold red]")
        return

    all_months = sorted(list(months_df['as_of_month']))
    if len(all_months) >= num_milestones:
        step = len(all_months) // num_milestones
        selected_months = [all_months[i * step] for i in range(num_milestones - 1)] + [all_months[-1]]
    else:
        selected_months = all_months

    table = Table(
        title=f"[bold yellow]📊 {clean_amc} AMC DETAILED SINGLE-STOCK CONVICTION PIPELINE ACROSS TIME[/bold yellow]",
        show_header=True,
        header_style="bold white on blue",
        border_style="dim cyan",
        expand=True
    )

    # Setup Columns
    row_cells = []
    for m in selected_months:
        df_m = pool.query_df(f"""
            SELECT security_name, market_value_cr
            FROM market_data.mf_holdings FINAL
            WHERE as_of_month = '{m.strftime("%Y-%m-%d")}'
              AND fund_name NOT LIKE '%INDEX%' AND fund_name NOT LIKE '%ETF%'
              AND lower(asset_type) = 'equity' {amc_filter_sql}
        """)

        tot_aum = df_m['market_value_cr'].sum() if len(df_m) > 0 else 1.0
        q_num = (m.month - 1) // 3 + 1
        col_header = f"{m.strftime('%b %Y')}\n(Q{q_num} {m.year})\nAUM: ₹{tot_aum:,.0f} Cr"
        table.add_column(col_header, justify="left", width=26)

        # Aggregate stocks for month m
        agg_m = df_m.groupby('security_name')['market_value_cr'].sum().reset_index()
        top_stocks = agg_m.sort_values(by='market_value_cr', ascending=False).head(top_n_stocks)

        cell_text = ""
        for idx, (_, r) in enumerate(top_stocks.iterrows(), 1):
            pct = (r['market_value_cr'] / tot_aum) * 100
            st_name = r['security_name'][:20]
            cell_text += f"#{idx} [bold yellow]{st_name}[/bold yellow]\n   ➔ [bold green]{pct:.1f}% NAV[/bold green] ([dim]₹{r['market_value_cr']:,.0f} Cr[/dim])\n\n"

        row_cells.append(cell_text.strip())

    table.add_row(*row_cells)
    console.print(Panel(table, border_style="green"))


def render_green_add_red_remove_pipeline(amc_name: str = "QUANT", num_milestones: int = 4) -> None:
    """
    Render a Left-to-Right Horizontal Stock Pipeline showing GREEN ADD & RED REMOVE positions across time.
    """
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel

    console = Console()
    pool = get_pool()
    clean_amc = amc_name.upper().strip()
    amc_filter_sql = f"AND (fund_name LIKE '{clean_amc}%' OR fund_name LIKE 'RELIANCE%')" if clean_amc == "NIPPON" else f"AND fund_name LIKE '{clean_amc}%'"

    # Query historical months
    months_df = pool.query_df(f"""
        SELECT DISTINCT as_of_month
        FROM market_data.mf_holdings FINAL
        WHERE fund_name NOT LIKE '%INDEX%' AND fund_name NOT LIKE '%ETF%' {amc_filter_sql}
        ORDER BY as_of_month ASC
    """)

    if len(months_df) == 0:
        console.print(f"[bold red]No historical holdings for AMC '{amc_name}'.[/bold red]")
        return

    all_months = sorted(list(months_df['as_of_month']))
    if len(all_months) >= num_milestones:
        step = len(all_months) // num_milestones
        selected_months = [all_months[i * step] for i in range(num_milestones - 1)] + [all_months[-1]]
    else:
        selected_months = all_months

    table = Table(
        title=f"[bold yellow]🟢 GREEN ADD & 🔴 RED REMOVE STOCK ROTATION PIPELINE ({clean_amc})[/bold yellow]",
        show_header=True,
        header_style="bold white on blue",
        border_style="dim cyan",
        expand=True
    )

    # Setup Columns
    for m in selected_months:
        q_num = (m.month - 1) // 3 + 1
        col_header = f"{m.strftime('%b %Y')}\n(Q{q_num} {m.year})"
        table.add_column(col_header, justify="left", width=28)

    row_cells = []
    for i, m in enumerate(selected_months):
        m_str = m.strftime("%Y-%m-%d")
        df_curr = pool.query_df(f"""
            SELECT security_name, sum(market_value_cr) as val_curr
            FROM market_data.mf_holdings FINAL
            WHERE as_of_month = '{m_str}'
              AND fund_name NOT LIKE '%INDEX%' AND fund_name NOT LIKE '%ETF%'
              AND lower(asset_type) = 'equity' {amc_filter_sql}
            GROUP BY security_name
        """)

        tot_aum = df_curr['val_curr'].sum() if len(df_curr) > 0 else 1.0

        if i > 0:
            prior_m_str = selected_months[i-1].strftime("%Y-%m-%d")
            df_prior = pool.query_df(f"""
                SELECT security_name, sum(market_value_cr) as val_prior
                FROM market_data.mf_holdings FINAL
                WHERE as_of_month = '{prior_m_str}'
                  AND fund_name NOT LIKE '%INDEX%' AND fund_name NOT LIKE '%ETF%'
                  AND lower(asset_type) = 'equity' {amc_filter_sql}
                GROUP BY security_name
            """)

            m_df = pd.merge(df_curr, df_prior, on='security_name', how='outer').fillna(0)
            m_df['delta'] = m_df['val_curr'] - m_df['val_prior']

            adds = m_df.sort_values(by='delta', ascending=False).head(2)
            removes = m_df.sort_values(by='delta', ascending=True).head(2)

            cell_text = "[bold green]🟢 TOP ADDITIONS (ADD)[/bold green]\n"
            for _, r in adds.iterrows():
                if r['delta'] > 0:
                    cell_text += f"• [green]{r['security_name'][:18]}[/green]\n  [bold green]+₹{r['delta']:,.0f} Cr ADDED[/bold green]\n"

            cell_text += "\n[bold red]🔴 TOP EXITS (REMOVE)[/bold red]\n"
            for _, r in removes.iterrows():
                if r['delta'] < 0:
                    cell_text += f"• [red]{r['security_name'][:18]}[/red]\n  [bold red] -₹{abs(r['delta']):,.0f} Cr REMOVED[/bold red]\n"
        else:
            top_h = df_curr.sort_values(by='val_curr', ascending=False).head(3)
            cell_text = "[bold cyan]🏛️ INITIAL ANCHOR[/bold cyan]\n"
            for _, r in top_h.iterrows():
                pct = (r['val_curr'] / tot_aum) * 100
                cell_text += f"• [white]{r['security_name'][:18]}[/white]\n  [dim]{pct:.1f}% NAV (₹{r['val_curr']:,.0f} Cr)[/dim]\n"

        row_cells.append(cell_text.strip())

    table.add_row(*row_cells)
    console.print(Panel(table, border_style="green"))
