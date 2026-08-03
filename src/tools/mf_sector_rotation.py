"""
src/tools/mf_sector_rotation.py
───────────────────────────────────
Unified AMC Sector Rotation Detection Engine for Mosaic & AGY Agents.

Detects MoM (Month-over-Month) capital & weight rotation across sectors for any AMC 
(DSP, Quant, HDFC, Nippon, Bajaj, ICICI, SBI) or across all AMCs combined.

Usage:
  from src.tools.mf_sector_rotation import detect_amc_sector_rotation, get_sector_rotation_report
  report = get_sector_rotation_report(amc_name="QUANT")
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional
import pandas as pd

from langchain_core.tools import tool
from src.db.pool import get_pool
from src.tools.mf_sector_analyzer import classify_sector

logger = logging.getLogger(__name__)


def get_sector_rotation_report(
    amc_name: str = "QUANT",
    month_current: str = "latest",
    month_prior: str = "auto",
    lookback_months: int = 1
) -> str:
    """
    Detect sector rotation, capital shifts, and stock drivers for an AMC across a specified lookback horizon.

    Parameters:
      amc_name: Target AMC ('QUANT', 'DSP', 'HDFC', 'NIPPON', 'BAJAJ', 'ICICI', 'SBI', or 'ALL')
      month_current: 'latest' or 'YYYY-MM-DD'
      month_prior: 'auto' or 'YYYY-MM-DD'
      lookback_months: Months to look back if month_prior='auto' (e.g. 1 for MoM, 12 for 1-Year YoY)
    """
    pool = get_pool()
    clean_amc = amc_name.upper().strip()

    amc_filter_sql = ""
    if clean_amc != "ALL":
        amc_filter_sql = f"AND (fund_name LIKE '{clean_amc}%' OR fund_name LIKE 'RELIANCE%')" if clean_amc == "NIPPON" else f"AND fund_name LIKE '{clean_amc}%'"

    # 1. Fetch available disclosure months for the AMC
    months_df = pool.query_df(f"""
        SELECT DISTINCT as_of_month
        FROM market_data.mf_holdings FINAL
        WHERE fund_name NOT LIKE '%INDEX%' AND fund_name NOT LIKE '%ETF%' {amc_filter_sql}
        ORDER BY as_of_month DESC
    """)

    if len(months_df) < 2:
        return f"⚠️ Insufficient historical months to compute sector rotation for AMC '{amc_name}'."

    if month_current == "latest":
        curr_m = months_df.iloc[0]['as_of_month'].strftime("%Y-%m-%d")
    else:
        curr_m = month_current

    if month_prior == "auto":
        # Pick the month lookback_months prior to curr_m
        prior_months = months_df[months_df['as_of_month'] < pd.to_datetime(curr_m)]
        if len(prior_months) == 0:
            prior_m = months_df.iloc[-1]['as_of_month'].strftime("%Y-%m-%d")
        else:
            idx = min(lookback_months - 1, len(prior_months) - 1)
            prior_m = prior_months.iloc[idx]['as_of_month'].strftime("%Y-%m-%d")
    else:
        prior_m = month_prior

    # 2. Fetch equity holdings for both months
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
        return f"⚠️ Missing equity holdings data for comparison between '{curr_m}' and '{prior_m}'."

    df_curr['sector'] = df_curr['security_name'].apply(classify_sector)
    df_prior['sector'] = df_prior['security_name'].apply(classify_sector)

    total_aum_curr = df_curr['market_value_cr'].sum()
    total_aum_prior = df_prior['market_value_cr'].sum()

    # Aggregate by sector for current and prior month
    sec_curr = df_curr.groupby('sector')['market_value_cr'].sum().reset_index()
    sec_curr['pct_curr'] = (sec_curr['market_value_cr'] / total_aum_curr) * 100
    sec_curr.rename(columns={'market_value_cr': 'val_curr'}, inplace=True)

    sec_prior = df_prior.groupby('sector')['market_value_cr'].sum().reset_index()
    sec_prior['pct_prior'] = (sec_prior['market_value_cr'] / total_aum_prior) * 100
    sec_prior.rename(columns={'market_value_cr': 'val_prior'}, inplace=True)

    merged_sec = pd.merge(sec_curr, sec_prior, on='sector', how='outer').fillna(0)
    merged_sec['mom_weight_change'] = merged_sec['pct_curr'] - merged_sec['pct_prior']
    merged_sec['mom_capital_change_cr'] = merged_sec['val_curr'] - merged_sec['val_prior']
    merged_sec = merged_sec.sort_values(by='mom_weight_change', ascending=False)

    # 3. Stock-level MoM delta calculation to identify drivers
    stock_curr = df_curr.groupby('security_name').agg(val_curr=('market_value_cr', 'sum'), sector=('sector', 'first')).reset_index()
    stock_prior = df_prior.groupby('security_name').agg(val_prior=('market_value_cr', 'sum'), sector=('sector', 'first')).reset_index()
    
    stock_merged = pd.merge(stock_curr, stock_prior, on=['security_name', 'sector'], how='outer').fillna(0)
    stock_merged['stock_delta_cr'] = stock_merged['val_curr'] - stock_merged['val_prior']

    # Build Markdown Report
    report_lines = [
        f"# 🔄 {clean_amc} AMC Sector Rotation Detection Report",
        f"**Comparison Period:** `{prior_m}` ➔ `{curr_m}`",
        f"**Total Equity AUM Shift:** ₹{total_aum_prior:,.2f} Cr ➔ ₹{total_aum_curr:,.2f} Cr (Net: ₹{total_aum_curr - total_aum_prior:+,.2f} Cr)\n",
        "### 📊 Sector Rotation Matrix (Sorted by MoM Weight Shift)\n",
        "| Sector / Industry | Prior Weight | Current Weight | MoM Weight Shift | MoM Capital Shift (₹ Cr) | Rotation Signal |",
        "| :--- | :---: | :---: | :---: | :---: | :---: |"
    ]

    for _, r in merged_sec.iterrows():
        w_delta = r['mom_weight_change']
        c_delta = r['mom_capital_change_cr']

        if w_delta >= 1.5:
            sig = "🟢 AGGRESSIVE ROTATION IN"
        elif w_delta >= 0.4:
            sig = "🟢 MODERATE ACCUMULATION"
        elif w_delta <= -1.5:
            sig = "🔴 AGGRESSIVE ROTATION OUT"
        elif w_delta <= -0.4:
            sig = "🔴 MODERATE TRIM"
        else:
            sig = "⏸️ NEUTRAL / STABLE"

        report_lines.append(
            f"| **{r['sector']}** | {r['pct_prior']:.2f}% | **{r['pct_curr']:.2f}%** | **{w_delta:+.2f}%** | ₹{c_delta:+,.2f} Cr | {sig} |"
        )

    # 4. Top Stock Drivers for Rotating-IN Sectors
    top_in_sectors = merged_sec[merged_sec['mom_weight_change'] > 0.4].head(3)
    if len(top_in_sectors) > 0:
        report_lines.append("\n### 🟢 Key Stock Additions Driving Sector Rotations IN\n")
        for _, s_row in top_in_sectors.iterrows():
            sec_name = s_row['sector']
            top_adds = stock_merged[stock_merged['sector'] == sec_name].sort_values(by='stock_delta_cr', ascending=False).head(3)
            add_strs = [f"**{sr['security_name']}** (+₹{sr['stock_delta_cr']:,.1f} Cr)" for _, sr in top_adds.iterrows() if sr['stock_delta_cr'] > 0]
            if add_strs:
                report_lines.append(f"📌 **{sec_name}** (+{s_row['mom_weight_change']:.2f}% AUM):")
                report_lines.append(f"   - Stock Inflows: {', '.join(add_strs)}")

    # 5. Top Stock Drivers for Rotating-OUT Sectors
    top_out_sectors = merged_sec[merged_sec['mom_weight_change'] < -0.4].tail(3)
    if len(top_out_sectors) > 0:
        report_lines.append("\n### 🔴 Key Stock Exits Driving Sector Rotations OUT\n")
        for _, s_row in top_out_sectors.iterrows():
            sec_name = s_row['sector']
            top_exits = stock_merged[stock_merged['sector'] == sec_name].sort_values(by='stock_delta_cr', ascending=True).head(3)
            exit_strs = [f"**{sr['security_name']}** ({sr['stock_delta_cr']:,.1f} Cr)" for _, sr in top_exits.iterrows() if sr['stock_delta_cr'] < 0]
            if exit_strs:
                report_lines.append(f"📌 **{sec_name}** ({s_row['mom_weight_change']:.2f}% AUM):")
                report_lines.append(f"   - Stock Outflows: {', '.join(exit_strs)}")

    return "\n".join(report_lines)


def render_sector_rotation_console(amc_name: str = "QUANT", lookback_months: int = 12) -> None:
    """
    Render stunning Rich Console table & panel for AMC sector rotation in terminal CLI.
    """
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel

    console = Console()
    report_text = get_sector_rotation_report(amc_name=amc_name, lookback_months=lookback_months)

    table = Table(
        title=f"[bold yellow]🔄 {amc_name.upper()} AMC Sector Rotation Matrix ({lookback_months}-Month Lookback)[/bold yellow]",
        show_header=True,
        header_style="bold cyan",
        border_style="dim bright_blue"
    )

    table.add_column("Sector / Industry", style="bold white", width=34)
    table.add_column("Prior Wt", justify="right", style="dim")
    table.add_column("Curr Wt", justify="right", style="bold")
    table.add_column("Shift (% AUM)", justify="right")
    table.add_column("Capital Shift (₹ Cr)", justify="right")
    table.add_column("Rotation Signal", justify="center")

    for line in report_text.split("\n"):
        if line.startswith("| **"):
            parts = [p.strip() for p in line.split("|")[1:-1]]
            if len(parts) >= 5:
                sec = parts[0].replace("**", "")
                prior = parts[1]
                curr = parts[2].replace("**", "")
                shift = parts[3].replace("**", "")
                cap = parts[4]
                sig = parts[5] if len(parts) >= 6 else ""

                shift_style = "bold green" if "+" in shift else ("bold red" if "-" in shift else "white")
                table.add_row(sec, prior, curr, f"[{shift_style}]{shift}[/{shift_style}]", cap, sig)

    console.print(Panel(table, border_style="green", expand=False))


@tool
def detect_amc_sector_rotation(
    amc_name: str = "QUANT",
    month_current: str = "latest",
    month_prior: str = "auto",
    lookback_months: int = 1
) -> str:
    """
    Detect sector rotation, capital shifts, and stock drivers for an AMC across a specified lookback horizon.

    Parameters:
      amc_name: Target AMC ('QUANT', 'DSP', 'HDFC', 'NIPPON', 'BAJAJ', 'ICICI', 'SBI', or 'ALL')
      month_current: Portfolio current month ('latest' or 'YYYY-MM-DD')
      month_prior: Portfolio prior comparison month ('auto' or 'YYYY-MM-DD')
      lookback_months: Months to look back if month_prior='auto' (e.g. 1 for MoM, 12 for 1-Year YoY)
    """
    try:
        return get_sector_rotation_report(
            amc_name=amc_name, 
            month_current=month_current, 
            month_prior=month_prior,
            lookback_months=lookback_months
        )
    except Exception as exc:
        logger.error("Error in detect_amc_sector_rotation: %s", exc)
        return f"Error detecting AMC sector rotation: {exc}"


def get_exhaustive_shift_ledger(
    amc_name: str = "QUANT",
    month_current: str = "latest",
    month_prior: str = "auto",
    lookback_months: int = 12
) -> str:
    """
    Exhaustively audit 100% of stock additions (+₹ Cr) and subtractions (-₹ Cr) without missing any position.
    """
    pool = get_pool()
    clean_amc = amc_name.upper().strip()
    amc_filter_sql = f"AND (fund_name LIKE '{clean_amc}%' OR fund_name LIKE 'RELIANCE%')" if clean_amc == "NIPPON" else f"AND fund_name LIKE '{clean_amc}%'"

    # 1. Resolve months
    months_df = pool.query_df(f"""
        SELECT DISTINCT as_of_month 
        FROM market_data.mf_holdings FINAL 
        WHERE fund_name NOT LIKE '%INDEX%' AND fund_name NOT LIKE '%ETF%' {amc_filter_sql}
        ORDER BY as_of_month DESC
    """)

    if len(months_df) == 0:
        return f"No holdings found for AMC '{amc_name}'."

    available_months = [m.strftime("%Y-%m-%d") for m in months_df['as_of_month']]
    curr_m = available_months[0] if month_current == "latest" else month_current

    if month_prior == "auto":
        idx_curr = available_months.index(curr_m) if curr_m in available_months else 0
        idx_prior = min(idx_curr + lookback_months, len(available_months) - 1)
        prior_m = available_months[idx_prior]
    else:
        prior_m = month_prior

    df_curr = pool.query_df(f"""
        SELECT security_name, sum(market_value_cr) as val_curr
        FROM market_data.mf_holdings FINAL
        WHERE as_of_month = '{curr_m}'
          AND fund_name NOT LIKE '%INDEX%' AND fund_name NOT LIKE '%ETF%'
          AND lower(asset_type) = 'equity' {amc_filter_sql}
        GROUP BY security_name
    """)

    df_prior = pool.query_df(f"""
        SELECT security_name, sum(market_value_cr) as val_prior
        FROM market_data.mf_holdings FINAL
        WHERE as_of_month = '{prior_m}'
          AND fund_name NOT LIKE '%INDEX%' AND fund_name NOT LIKE '%ETF%'
          AND lower(asset_type) = 'equity' {amc_filter_sql}
        GROUP BY security_name
    """)

    m_df = pd.merge(df_curr, df_prior, on='security_name', how='outer').fillna(0)
    m_df['delta'] = m_df['val_curr'] - m_df['val_prior']

    adds = m_df[m_df['delta'] > 0].sort_values(by='delta', ascending=False)
    subs = m_df[m_df['delta'] < 0].sort_values(by='delta', ascending=True)

    report_lines = [
        f"# 📋 EXHAUSTIVE STOCK ADDITIONS & SUBTRACTIONS LEDGER: {clean_amc} AMC",
        f"**Audit Period:** `{prior_m}` ➔ `{curr_m}` ({lookback_months}-Month Horizon)",
        f"**Total Additions:** `{len(adds)} stocks` (+₹{adds['delta'].sum():,.2f} Cr)",
        f"**Total Subtractions:** `{len(subs)} stocks` (-₹{abs(subs['delta'].sum()):,.2f} Cr)\n",
        "### 🟢 100% EXHAUSTIVE STOCK ADDITIONS & NEW ENTRIES\n",
        "| # | Security Name | Value Added (₹ Cr) | Prior Holding (₹ Cr) | Current Holding (₹ Cr) | Shift Status |",
        "| :--- | :--- | :---: | :---: | :---: | :---: |"
    ]

    for idx, (_, r) in enumerate(adds.iterrows(), 1):
        status = "🚪 NEW ENTRY" if r['val_prior'] == 0 else "🟢 ACCUMULATION"
        report_lines.append(
            f"| {idx} | **{r['security_name']}** | **+₹{r['delta']:,.2f} Cr** | ₹{r['val_prior']:,.1f} Cr | **₹{r['val_curr']:,.1f} Cr** | {status} |"
        )

    report_lines.append("\n### 🔴 100% EXHAUSTIVE STOCK SUBTRACTIONS & COMPLETE EXITS\n")
    report_lines.append("| # | Security Name | Capital Removed (₹ Cr) | Prior Holding (₹ Cr) | Current Holding (₹ Cr) | Shift Status |")
    report_lines.append("| :--- | :--- | :---: | :---: | :---: | :---: |")

    for idx, (_, r) in enumerate(subs.iterrows(), 1):
        status = "🚪 COMPLETE EXIT" if r['val_curr'] == 0 else "🔴 TRIM / REDUCTION"
        report_lines.append(
            f"| {idx} | **{r['security_name']}** | **-₹{abs(r['delta']):,.2f} Cr** | ₹{r['val_prior']:,.1f} Cr | **₹{r['val_curr']:,.1f} Cr** | {status} |"
        )

    return "\n".join(report_lines)


@tool
def audit_exhaustive_stock_shifts(
    amc_name: str = "QUANT",
    month_current: str = "latest",
    month_prior: str = "auto",
    lookback_months: int = 12
) -> str:
    """
    Audit 100% of all stock additions (+₹ Cr) and subtractions (-₹ Cr) without missing any position.

    Parameters:
      amc_name: Target AMC ('QUANT', 'DSP', 'HDFC', 'NIPPON', 'BAJAJ', 'ICICI', 'SBI')
      month_current: Portfolio current month ('latest' or 'YYYY-MM-DD')
      month_prior: Portfolio prior comparison month ('auto' or 'YYYY-MM-DD')
      lookback_months: Months to look back if month_prior='auto' (e.g. 12 for 1-Year YoY)
    """
    try:
        return get_exhaustive_shift_ledger(
            amc_name=amc_name,
            month_current=month_current,
            month_prior=month_prior,
            lookback_months=lookback_months
        )
    except Exception as exc:
        logger.error("Error in audit_exhaustive_stock_shifts: %s", exc)
        return f"Error auditing stock shifts: {exc}"


def render_exhaustive_shift_visual_dashboard(amc_name: str = "QUANT", lookback_months: int = 12, max_display_rows: int = 20) -> None:
    """
    Render ultra-visually appealing executive side-by-side shift ledger in terminal CLI.
    """
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.text import Text

    console = Console()
    pool = get_pool()
    clean_amc = amc_name.upper().strip()
    amc_filter_sql = f"AND (fund_name LIKE '{clean_amc}%' OR fund_name LIKE 'RELIANCE%')" if clean_amc == "NIPPON" else f"AND fund_name LIKE '{clean_amc}%'"

    # Query historical months
    months_df = pool.query_df(f"""
        SELECT DISTINCT as_of_month 
        FROM market_data.mf_holdings FINAL 
        WHERE fund_name NOT LIKE '%INDEX%' AND fund_name NOT LIKE '%ETF%' {amc_filter_sql}
        ORDER BY as_of_month DESC
    """)

    if len(months_df) == 0:
        console.print(f"[bold red]No holdings found for AMC '{amc_name}'.[/bold red]")
        return

    available_months = [m.strftime("%Y-%m-%d") for m in months_df['as_of_month']]
    curr_m = available_months[0]
    idx_prior = min(lookback_months, len(available_months) - 1)
    prior_m = available_months[idx_prior]

    df_curr = pool.query_df(f"""
        SELECT security_name, sum(market_value_cr) as val_curr
        FROM market_data.mf_holdings FINAL
        WHERE as_of_month = '{curr_m}'
          AND fund_name NOT LIKE '%INDEX%' AND fund_name NOT LIKE '%ETF%'
          AND lower(asset_type) = 'equity' {amc_filter_sql}
        GROUP BY security_name
    """)

    df_prior = pool.query_df(f"""
        SELECT security_name, sum(market_value_cr) as val_prior
        FROM market_data.mf_holdings FINAL
        WHERE as_of_month = '{prior_m}'
          AND fund_name NOT LIKE '%INDEX%' AND fund_name NOT LIKE '%ETF%'
          AND lower(asset_type) = 'equity' {amc_filter_sql}
        GROUP BY security_name
    """)

    m_df = pd.merge(df_curr, df_prior, on='security_name', how='outer').fillna(0)
    m_df['delta'] = m_df['val_curr'] - m_df['val_prior']

    adds = m_df[m_df['delta'] > 0].sort_values(by='delta', ascending=False)
    subs = m_df[m_df['delta'] < 0].sort_values(by='delta', ascending=True)

    header = Panel(
        Text.from_markup(
            f"[bold gold1]🏛️ EXHAUSTIVE PORTFOLIO SHIFT DASHBOARD ({clean_amc} AMC)[/bold gold1]\n"
            f"[dim]Audit Horizon: {prior_m} ➔ {curr_m} ({lookback_months}-Month Shift Audit)[/dim]\n\n"
            f"[bold green]🟢 Total Additions: {len(adds)} Stocks (+₹{adds['delta'].sum():,.1f} Cr)[/bold green]   │   "
            f"[bold red]🔴 Total Subtractions: {len(subs)} Stocks (-₹{abs(subs['delta'].sum()):,.1f} Cr)[/bold red]"
        ),
        border_style="yellow",
        title="[bold white]MOSAIC QUANTITATIVE AUDIT ENGINE[/bold white]",
        expand=True
    )
    console.print(header)

    table = Table(
        title=f"[bold yellow]⚖️ EXHAUSTIVE SIDE-BY-SIDE ADDITIONS vs SUBTRACTIONS LEDGER ({clean_amc})[/bold yellow]",
        show_header=True,
        header_style="bold white on blue",
        border_style="dim cyan",
        expand=True
    )

    table.add_column("🟢 ALL ADDITIONS & NEW ENTRIES (+₹ Cr)", style="bold white", width=42)
    table.add_column("Magnitude Bar", justify="left", width=18)
    table.add_column("🔴 ALL SUBTRACTIONS & EXITS (-₹ Cr)", style="bold white", width=42)
    table.add_column("Magnitude Bar", justify="left", width=18)

    max_rows = max(len(adds), len(subs))
    adds_list = list(adds.iterrows())
    subs_list = list(subs.iterrows())

    max_add_v = adds['delta'].max() if len(adds) > 0 else 1.0
    max_sub_v = abs(subs['delta'].min()) if len(subs) > 0 else 1.0

    display_limit = min(max_display_rows, max_rows)

    for i in range(display_limit):
        if i < len(adds_list):
            _, r_add = adds_list[i]
            val_a = r_add['delta']
            tag_a = "NEW" if r_add['val_prior'] == 0 else "ADD"
            left_str = f"[green]{i+1:2d}. {r_add['security_name'][:20]}[/green] [bold green]+₹{val_a:,.0f} Cr[/bold green] [dim]({tag_a})[/dim]"
            bar_a_len = int((val_a / max_add_v) * 12)
            left_bar = f"[bold green]{'🟩' * max(1, bar_a_len)}[/bold green]"
        else:
            left_str, left_bar = "", ""

        if i < len(subs_list):
            _, r_sub = subs_list[i]
            val_s = abs(r_sub['delta'])
            tag_s = "EXIT" if r_sub['val_curr'] == 0 else "TRIM"
            right_str = f"[red]{i+1:2d}. {r_sub['security_name'][:20]}[/red] [bold red]-₹{val_s:,.0f} Cr[/bold red] [dim]({tag_s})[/dim]"
            bar_s_len = int((val_s / max_sub_v) * 12)
            right_bar = f"[bold red]{'🟥' * max(1, bar_s_len)}[/bold red]"
        else:
            right_str, right_bar = "", ""

        table.add_row(left_str, left_bar, right_str, right_bar)

    console.print(table)

