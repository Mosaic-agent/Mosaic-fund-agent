"""
src/tools/mf_sector_analyzer.py
───────────────────────────────────
Unified Mutual Fund Sector & AMC Portfolio Intelligence Tool for AGY & Mosaic Agents.

Provides fast, pre-aggregated ClickHouse queries for:
  1. AMC Aggregate Sector Allocation (% Equity AUM & ₹ Cr Value)
  2. Top Stock Picks per Sector per AMC
  3. MoM Sector & Stock Inflow/Outflow Shifts (Accumulation vs Trimming)
  4. Multi-AMC Side-by-Side Sector Comparison Matrix (DSP, Quant, HDFC, Nippon, Bajaj, ICICI, SBI)

Usage:
  from src.tools.mf_sector_analyzer import analyze_mf_sectors, get_mf_sector_report
  report = get_mf_sector_report(amc_name="QUANT", top_n_stocks=3)
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional
import pandas as pd

from langchain_core.tools import tool
from src.db.pool import get_pool

logger = logging.getLogger(__name__)

# Sector classification keyword map
SECTOR_RULES = [
    ("Adani Conglomerate", ["ADANI"]),
    ("BFSI (Banking & Financial Services)", [
        "BANK", "INSURANCE", "FINANCE", "FINANCIAL", "HOUSING", "INVESTMENT", 
        "CAPITAL", "MUTHOOT", "BAJAJ HOUSING", "SHRIRAM", "PIRAMAL", "ANAND RATHI", "RBL"
    ]),
    ("Healthcare & Pharmaceuticals", [
        "PHARMA", "HEALTHCARE", "LABORATORIES", "DR.", "CIPLA", "SYNGENE", 
        "ALKEM", "FORTIS", "IPCA", "BIOCON", "DIVI", "SUN PHARMA", "LUPIN", "AUROBINDO", "ANTHEM", "MANKIND"
    ]),
    ("Telecom & Digital Infrastructure", [
        "AIRTEL", "TELECOM", "TOWERS", "TECH MAHINDRA", "COFORGE", "INFOSYS", 
        "TCS", "WIPRO", "HCL", "PERSISTENT", "HFCL", "LTIMINDTREE", "TATA COMMUNICATIONS", "BLACK BOX"
    ]),
    ("Capital Goods, Power & Engineering", [
        "BHEL", "LARSEN", "ENGINEERING", "VERNOVA", "SIEMENS", "ABB", "CUMMINS", 
        "VOLTAMP", "KALPATARU", "APAR", "TD POWER", "BHARAT HEAVY", "PREMIER ENERGIES", "LLOYDS"
    ]),
    ("Energy, Oil & Utilities", [
        "PETROLEUM", "OIL", "NTPC", "POWER", "ENERGY", "GAS", "GAIL", 
        "RELIANCE", "BPCL", "HPCL", "ONGC", "IOC", "AEGIS", "TAQA"
    ]),
    ("Automobiles & Auto Components", [
        "MOTHERSON", "MOTORS", "AUTOMOBILE", "MARUTI", "HYUNDAI", 
        "HERO", "TATA MOTORS", "EICHER", "BOSCH", "SONA BLW", "LUMAX", "SANSERA", "ESCORTS"
    ]),
    ("FMCG & Consumer Discretionary", [
        "FSN", "NYKAA", "JEWELLERY", "THANGAMAYIL", "FOOD", "CONSUMPTION", 
        "BECTOR", "EMAMI", "TITAN", "ITC", "PARLE", "HINDUSTAN UNILEVER", "NESTLE", "BRITANNIA", "TRENT", "BIKAJI", "AWL"
    ]),
    ("Real Estate & Infrastructure Construction", [
        "DEVELOPERS", "PHOENIX", "BUILDING", "INFRASTRUCTURE", "REALTY", 
        "IRB", "DLF", "SOBHA", "GODREJ PROPERTIES", "OBEROI", "JSW INFRA"
    ]),
    ("Chemicals & Fertilizers", [
        "COROMANDEL", "CHEMICALS", "ATUL", "FERTILIZERS", "CHAMBAL", 
        "DEEPAK", "PI INDUSTRIES", "UPL", "VARDHMAN", "EID PARRY", "GSFC"
    ]),
    ("Metals & Mining", [
        "STEEL", "JINDAL", "HINDALCO", "METALS", "TATA STEEL", "COAL INDIA", "NMDC", "VEDANTA", "RATNAMANI"
    ]),
]


def classify_sector(security_name: str) -> str:
    """Classify security into standard sectors using keyword rules."""
    name_upper = security_name.upper()
    for sector_name, keywords in SECTOR_RULES:
        if any(kw in name_upper for kw in keywords):
            return sector_name
    return "Other Sectors / Specialized Industrials"


def get_mf_sector_report(
    amc_name: str = "ALL",
    as_of_month: str = "latest",
    top_n_stocks: int = 3
) -> str:
    """
    Generate structured Markdown report analyzing AMC sector allocations & stock holdings.

    Parameters:
        amc_name: 'DSP', 'QUANT', 'HDFC', 'NIPPON', 'BAJAJ', 'ICICI', 'SBI', or 'ALL'
        as_of_month: 'latest' or 'YYYY-MM-DD'
        top_n_stocks: Number of top stock picks to show per sector
    """
    pool = get_pool()
    clean_amc = amc_name.upper().strip()

    # Determine latest available month for target AMC(s)
    amc_filter_sql = ""
    if clean_amc != "ALL":
        amc_filter_sql = f"AND (fund_name LIKE '{clean_amc}%' OR fund_name LIKE 'RELIANCE%')" if clean_amc == "NIPPON" else f"AND fund_name LIKE '{clean_amc}%'"

    if as_of_month == "latest":
        month_res = pool.query_df(f"""
            SELECT max(as_of_month) as max_m
            FROM market_data.mf_holdings FINAL
            WHERE fund_name NOT LIKE '%INDEX%' AND fund_name NOT LIKE '%ETF%' {amc_filter_sql}
        """)
        target_month = month_res.iloc[0]['max_m'].strftime("%Y-%m-%d")
    else:
        target_month = as_of_month

    # Fetch holdings
    holdings_df = pool.query_df(f"""
        SELECT 
            fund_name,
            arrayElement(splitByChar('_', fund_name), 1) as amc_prefix,
            security_name,
            market_value_cr,
            pct_of_nav
        FROM market_data.mf_holdings FINAL
        WHERE as_of_month = '{target_month}'
          AND fund_name NOT LIKE '%INDEX%' 
          AND fund_name NOT LIKE '%ETF%'
          AND lower(asset_type) = 'equity'
          {amc_filter_sql}
    """)

    if len(holdings_df) == 0:
        return f"⚠️ No equity holdings data found for AMC '{amc_name}' on month '{target_month}'."

    holdings_df['sector'] = holdings_df['security_name'].apply(classify_sector)

    # Multi-AMC Comparison Mode
    if clean_amc == "ALL":
        target_amcs = ['DSP', 'QUANT', 'HDFC', 'NIPPON', 'BAJAJ', 'ICICI', 'SBI']
        filtered_df = holdings_df[holdings_df['amc_prefix'].isin(target_amcs)].copy()

        pivot_df = filtered_df.groupby(['sector', 'amc_prefix'])['market_value_cr'].sum().unstack(fill_value=0)
        
        # Calculate % weights per AMC
        amc_totals = filtered_df.groupby('amc_prefix')['market_value_cr'].sum()
        pct_pivot = pivot_df.div(amc_totals, axis=1) * 100

        report_lines = [
            f"# 🏛️ Multi-AMC Sector Allocation Matrix",
            f"**As of:** {target_month} | **Total Equity Analyzed:** ₹{amc_totals.sum():,.2f} Cr\n",
            "| Sector / Industry | DSP (% AUM) | Quant (% AUM) | HDFC (% AUM) | Nippon (% AUM) | Bajaj (% AUM) |",
            "| :--- | :---: | :---: | :---: | :---: | :---: |"
        ]

        for sector, row in pct_pivot.iterrows():
            dsp_w = f"{row.get('DSP', 0):.2f}%" if row.get('DSP', 0) > 0 else "0.00%"
            q_w = f"{row.get('QUANT', 0):.2f}%" if row.get('QUANT', 0) > 0 else "0.00%"
            h_w = f"{row.get('HDFC', 0):.2f}%" if row.get('HDFC', 0) > 0 else "0.00%"
            n_w = f"{row.get('NIPPON', 0):.2f}%" if row.get('NIPPON', 0) > 0 else "0.00%"
            b_w = f"{row.get('BAJAJ', 0):.2f}%" if row.get('BAJAJ', 0) > 0 else "0.00%"
            report_lines.append(f"| **{sector}** | {dsp_w} | {q_w} | {h_w} | {n_w} | {b_w} |")

        return "\n".join(report_lines)

    # Single AMC Detailed Breakdown
    agg_df = holdings_df.groupby('security_name').agg(
        total_val_cr=('market_value_cr', 'sum'),
        fund_cnt=('fund_name', 'nunique'),
        sector=('sector', 'first')
    ).reset_index()

    total_amc_aum = agg_df['total_val_cr'].sum()
    sector_summary = agg_df.groupby('sector').agg(
        total_val_cr=('total_val_cr', 'sum'),
        stock_count=('security_name', 'count')
    ).reset_index()

    sector_summary['pct_aum'] = (sector_summary['total_val_cr'] / total_amc_aum) * 100
    sector_summary = sector_summary.sort_values(by='total_val_cr', ascending=False)

    report_lines = [
        f"# 🏛️ {clean_amc} Mutual Fund: Sector Allocation & Conviction Report",
        f"**Portfolio Month:** {target_month} | **Total Active Equity AUM:** ₹{total_amc_aum:,.2f} Cr\n",
        "### 📊 Sector Allocation Breakdown (% Active Equity AUM)\n",
        "| Sector / Industry | Holding Value (₹ Cr) | % of Equity AUM | Active Stock Count |",
        "| :--- | :---: | :---: | :---: |"
    ]

    for _, row in sector_summary.iterrows():
        report_lines.append(
            f"| **{row['sector']}** | ₹{row['total_val_cr']:,.2f} Cr | **{row['pct_aum']:.2f}%** | {row['stock_count']} |"
        )

    report_lines.append("\n### 📋 Top Stock Picks per Sector\n")
    for sec in sector_summary['sector']:
        top_stocks = agg_df[agg_df['sector'] == sec].sort_values(by='total_val_cr', ascending=False).head(top_n_stocks)
        stock_str_list = [
            f"**{r['security_name']}** (₹{r['total_val_cr']:,.1f} Cr / { (r['total_val_cr']/total_amc_aum)*100:.2f}% NAV)" 
            for _, r in top_stocks.iterrows()
        ]
        report_lines.append(f"📌 **{sec}**:")
        report_lines.append(f"   - Top Picks: {', '.join(stock_str_list)}\n")

    return "\n".join(report_lines)


def render_portfolio_tree_console(amc_name: str = "QUANT", top_n_stocks: int = 3) -> None:
    """
    Render a visually stunning Rich Tree representation of an AMC's portfolio hierarchy in terminal CLI.
    """
    from rich.console import Console
    from rich.tree import Tree
    from rich.panel import Panel

    pool = get_pool()
    clean_amc = amc_name.upper().strip()

    amc_filter_sql = f"AND (fund_name LIKE '{clean_amc}%' OR fund_name LIKE 'RELIANCE%')" if clean_amc == "NIPPON" else f"AND fund_name LIKE '{clean_amc}%'"

    month_res = pool.query_df(f"""
        SELECT max(as_of_month) as max_m
        FROM market_data.mf_holdings FINAL
        WHERE fund_name NOT LIKE '%INDEX%' AND fund_name NOT LIKE '%ETF%' {amc_filter_sql}
    """)
    target_month = month_res.iloc[0]['max_m'].strftime("%Y-%m-%d")

    holdings_df = pool.query_df(f"""
        SELECT security_name, market_value_cr, pct_of_nav, fund_name
        FROM market_data.mf_holdings FINAL
        WHERE as_of_month = '{target_month}'
          AND fund_name NOT LIKE '%INDEX%' AND fund_name NOT LIKE '%ETF%'
          AND lower(asset_type) = 'equity' {amc_filter_sql}
    """)

    if len(holdings_df) == 0:
        print(f"No equity holdings found for AMC '{amc_name}'.")
        return

    holdings_df['sector'] = holdings_df['security_name'].apply(classify_sector)
    agg_df = holdings_df.groupby('security_name').agg(
        total_val_cr=('market_value_cr', 'sum'),
        sector=('sector', 'first')
    ).reset_index()

    total_aum = agg_df['total_val_cr'].sum()
    sec_agg = agg_df.groupby('sector')['total_val_cr'].sum().reset_index()
    sec_agg['pct_aum'] = (sec_agg['total_val_cr'] / total_aum) * 100
    sec_agg = sec_agg.sort_values(by='total_val_cr', ascending=False)

    console = Console()
    tree = Tree(f"[bold gold1]🏛️ {clean_amc} MUTUAL FUND PORTFOLIO (₹{total_aum:,.2f} Cr Equity AUM | {target_month})[/bold gold1]")

    sector_colors = ["bold green", "bold cyan", "bold magenta", "bold red", "bold yellow", "bold blue", "bright_cyan", "bright_green"]

    for idx, (_, s_row) in enumerate(sec_agg.iterrows()):
        sec_name = s_row['sector']
        sec_val = s_row['total_val_cr']
        sec_pct = s_row['pct_aum']
        color = sector_colors[idx % len(sector_colors)]

        sec_node = tree.add(f"[{color}]{sec_name} ({sec_pct:.2f}% AUM / ₹{sec_val:,.1f} Cr)[/{color}]")

        top_stocks = agg_df[agg_df['sector'] == sec_name].sort_values(by='total_val_cr', ascending=False).head(top_n_stocks)
        for _, st_row in top_stocks.iterrows():
            st_name = st_row['security_name']
            st_val = st_row['total_val_cr']
            st_pct = (st_val / total_aum) * 100
            sec_node.add(f"[white]• {st_name}[/white] ➔ [bold white]{st_pct:.2f}% AUM[/bold white] ([dim]₹{st_val:,.1f} Cr[/dim])")

    console.print(Panel(tree, title=f"[bold green]CIRCULAR TREE CONSOLE RENDERER ({clean_amc})[/bold green]", border_style="green", expand=False))


    console.print(Panel(tree, title=f"[bold green]CIRCULAR TREE CONSOLE RENDERER ({clean_amc})[/bold green]", border_style="green", expand=False))


def render_granular_stock_sector_deepdive(amc_name: str = "QUANT", top_n_per_sector: int = 4) -> None:
    """
    Render ultra-detailed single-stock sector deep-dive table in terminal CLI.
    """
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel

    console = Console()
    pool = get_pool()
    clean_amc = amc_name.upper().strip()

    amc_filter_sql = f"AND (fund_name LIKE '{clean_amc}%' OR fund_name LIKE 'RELIANCE%')" if clean_amc == "NIPPON" else f"AND fund_name LIKE '{clean_amc}%'"

    month_res = pool.query_df(f"""
        SELECT max(as_of_month) as max_m
        FROM market_data.mf_holdings FINAL
        WHERE fund_name NOT LIKE '%INDEX%' AND fund_name NOT LIKE '%ETF%' {amc_filter_sql}
    """)
    target_month = month_res.iloc[0]['max_m'].strftime("%Y-%m-%d")

    df_curr = pool.query_df(f"""
        SELECT fund_name, security_name, market_value_cr, pct_of_nav
        FROM market_data.mf_holdings FINAL
        WHERE as_of_month = '{target_month}'
          AND fund_name NOT LIKE '%INDEX%' AND fund_name NOT LIKE '%ETF%'
          AND lower(asset_type) = 'equity' {amc_filter_sql}
    """)

    if len(df_curr) == 0:
        console.print(f"[bold red]No holdings found for AMC '{amc_name}'.[/bold red]")
        return

    df_curr['sector'] = df_curr['security_name'].apply(classify_sector)
    agg_df = df_curr.groupby('security_name').agg(
        val_curr=('market_value_cr', 'sum'),
        fund_cnt=('fund_name', 'nunique'),
        sector=('sector', 'first')
    ).reset_index()

    total_aum = agg_df['val_curr'].sum()
    sec_agg = agg_df.groupby('sector')['val_curr'].sum().reset_index()
    sec_agg['pct_aum'] = (sec_agg['val_curr'] / total_aum) * 100
    sec_agg = sec_agg.sort_values(by='val_curr', ascending=False)

    table = Table(
        title=f"[bold yellow]🔍 {clean_amc} AMC GRANULAR SINGLE-STOCK SECTOR DEEP-DIVE ({target_month})[/bold yellow]",
        show_header=True,
        header_style="bold white on blue",
        border_style="dim cyan",
        expand=True
    )

    table.add_column("Sector / Security Name", style="bold white", width=34)
    table.add_column("Held Funds", justify="center", width=10)
    table.add_column("Holding Value (₹ Cr)", justify="right", style="bold white", width=16)
    table.add_column("% Equity AUM", justify="right", style="bold cyan", width=12)
    table.add_column("Institutional Status", justify="center", width=24)

    for _, s_row in sec_agg.iterrows():
        sec_name = s_row['sector']
        sec_pct = s_row['pct_aum']
        sec_val = s_row['val_curr']

        table.add_row(f"[bold gold1]📌 {sec_name.upper()} ({sec_pct:.2f}% AUM / ₹{sec_val:,.1f} Cr)[/bold gold1]", "", "", "", "")

        top_stocks = agg_df[agg_df['sector'] == sec_name].sort_values(by='val_curr', ascending=False).head(top_n_per_sector)
        for _, st in top_stocks.iterrows():
            st_name = f"  └─ {st['security_name']}"
            st_val = st['val_curr']
            st_pct = (st_val / total_aum) * 100

            if st_pct >= 4.0:
                status = "🔥 [bold green]CORE CONVICTION[/bold green]"
            elif st_pct >= 1.5:
                status = "🟢 [green]HEAVY HOLDING[/green]"
            else:
                status = "⏸️ [dim]SATELLITE HOLDING[/dim]"

            table.add_row(st_name, str(int(st['fund_cnt'])), f"₹{st_val:,.1f} Cr", f"{st_pct:.2f}%", status)

    console.print(Panel(table, border_style="green"))


@tool
def analyze_mf_sectors(
    amc_name: str = "ALL",
    as_of_month: str = "latest",
    top_n_stocks: int = 3
) -> str:
    """
    Analyze Mutual Fund (MF) sector allocations, top stock holdings, and multi-AMC comparative matrices.

    Parameters:
      amc_name: Target AMC ('DSP', 'QUANT', 'HDFC', 'NIPPON', 'BAJAJ', 'ICICI', 'SBI', or 'ALL')
      as_of_month: Portfolio month ('latest' or 'YYYY-MM-DD')
      top_n_stocks: Number of top stock picks per sector (default: 3)
    """
    try:
        return get_mf_sector_report(amc_name=amc_name, as_of_month=as_of_month, top_n_stocks=top_n_stocks)
    except Exception as exc:
        logger.error("Error in analyze_mf_sectors: %s", exc)
        return f"Error analyzing MF sectors: {exc}"
