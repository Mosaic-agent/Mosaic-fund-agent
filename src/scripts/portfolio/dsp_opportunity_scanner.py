"""
src/scripts/portfolio/dsp_opportunity_scanner.py
─────────────────────────────────────────────────
Cross-references DSP active-fund holdings from ClickHouse with technical indicators
from Yahoo Finance to identify high-quality investment opportunities.

Logic:
1. Filter holdings by active DSP funds (user rule).
2. Calculate cross-fund conviction (held by >= 2 active funds for >= 24 months)
   and top adds (largest MoM increase in pct_of_nav).
3. Fetch daily prices via yfinance using bulk ISIN download.
4. Calculate technical setups (RSI, drawdown from 52-week high, volume surge).
5. Intersect conviction (DSP holding) and setup (drawdown/oversold RSI) to score and rank.
"""

import os
import sys
import logging
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import date, timedelta

# Ensure project root is on sys.path
_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.insert(0, os.path.join(_ROOT, "src"))
sys.path.insert(0, _ROOT)

from config.settings import settings
from src.db.pool import get_pool
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import box

console = Console()

ACTIVE_FUNDS = [
    'DSP_SMALL_CAP', 'DSP_MID_CAP', 'DSP_LARGE_AND_MID_CAP', 'DSP_FLEXI_CAP',
    'DSP_MULTICAP', 'DSP_FOCUSED', 'DSP_VALUE', 'DSP_TIGER', 'DSP_BUSINESS_CYCLE',
    'DSP_ELSS_TAX_SAVER', 'DSP_HEALTHCARE', 'DSP_BANKING_FINANCIAL_SERVICES', 'DSP_QUANT'
]

def _rsi(closes: pd.Series, period: int = 14) -> float | None:
    if len(closes) < period + 1:
        return None
    delta = closes.diff().dropna()
    gain  = delta.clip(lower=0).rolling(period).mean().iloc[-1]
    loss  = (-delta.clip(upper=0)).rolling(period).mean().iloc[-1]
    if loss == 0:
        return 100.0
    rs = gain / loss
    return round(float(100 - 100 / (1 + rs)), 1)

def run_scanner():
    console.print(Panel("[bold cyan]DSP Active-Fund holdings Opportunity Scanner[/bold cyan]", subtitle="Analyzing Conviction + Technical Setups"))

    pool = get_pool()
    
    # 1. Get the latest two months for active DSP funds
    active_funds_str = ", ".join(f"'{f}'" for f in ACTIVE_FUNDS)
    months_query = f"""
        SELECT DISTINCT as_of_month FROM market_data.mf_holdings FINAL
        WHERE fund_name IN ({active_funds_str})
        ORDER BY as_of_month DESC LIMIT 2
    """
    df_months = pool.query_df(months_query)
    if len(df_months) < 2:
        console.print("[red]Insufficient monthly historical data in ClickHouse to run MoM scan.[/red]")
        return
        
    latest_month = df_months.iloc[0, 0].strftime('%Y-%m-%d')
    prev_month = df_months.iloc[1, 0].strftime('%Y-%m-%d')
    console.print(f"[dim]Latest month: {latest_month} | Previous month: {prev_month}[/dim]")

    # 2. Query historical presence (months held) per ISIN
    console.print("[dim]Querying historical holdings presence...[/dim]")
    months_held_query = f"""
        SELECT isin, count(DISTINCT as_of_month) as months_held
        FROM market_data.mf_holdings FINAL
        WHERE fund_name IN ({active_funds_str})
          AND asset_type = 'equity'
          AND isin != ''
        GROUP BY isin
    """
    df_months_held = pool.query_df(months_held_query)
    months_held_dict = dict(zip(df_months_held['isin'], df_months_held['months_held']))

    # 3. Query holdings details for latest month
    console.print("[dim]Querying latest active holdings...[/dim]")
    latest_query = f"""
        SELECT isin, security_name, fund_name, pct_of_nav, market_value_cr
        FROM market_data.mf_holdings FINAL
        WHERE as_of_month = '{latest_month}'
          AND fund_name IN ({active_funds_str})
          AND asset_type = 'equity'
          AND security_name NOT LIKE '%Mutual Fund%'
          AND security_name NOT LIKE '%ETF%'
          AND isin != ''
    """
    df_latest = pool.query_df(latest_query)
    if df_latest.empty:
        console.print("[red]No holdings found for the latest month.[/red]")
        return

    # 4. Query holdings details for previous month (for MoM delta)
    console.print("[dim]Querying previous active holdings...[/dim]")
    prev_query = f"""
        SELECT isin, fund_name, pct_of_nav, market_value_cr
        FROM market_data.mf_holdings FINAL
        WHERE as_of_month = '{prev_month}'
          AND fund_name IN ({active_funds_str})
          AND asset_type = 'equity'
          AND isin != ''
    """
    df_prev = pool.query_df(prev_query)

    # 5. Process latest data (group by ISIN)
    latest_grouped = df_latest.groupby('isin').agg({
        'security_name': 'first',
        'fund_name': 'nunique',
        'pct_of_nav': 'sum',
        'market_value_cr': 'sum'
    }).rename(columns={'fund_name': 'active_fund_count'}).reset_index()

    # Process prev data (group by ISIN)
    prev_grouped = df_prev.groupby('isin').agg({
        'pct_of_nav': 'sum',
        'market_value_cr': 'sum'
    }).reset_index()
    prev_dict = dict(zip(prev_grouped['isin'], prev_grouped['pct_of_nav']))

    # 6. Merge and calculate MoM change
    latest_grouped['prev_pct_of_nav'] = latest_grouped['isin'].map(prev_dict).fillna(0.0)
    latest_grouped['mom_change_pct'] = latest_grouped['pct_of_nav'] - latest_grouped['prev_pct_of_nav']
    latest_grouped['months_held'] = latest_grouped['isin'].map(months_held_dict).fillna(1)

    # Filter rules
    # Criterion A: High Conviction (>= 2 active funds held for >= 24 months)
    # Criterion B: Top adds (MoM delta > 0.05% of NAV)
    high_conviction_mask = (latest_grouped['active_fund_count'] >= 2) & (latest_grouped['months_held'] >= 24)
    top_adds_mask = latest_grouped['mom_change_pct'] >= 0.05

    df_candidates = latest_grouped[high_conviction_mask | top_adds_mask].copy()
    if df_candidates.empty:
        console.print("[yellow]No candidates met the conviction or top-adds criteria. Lowering thresholds...[/yellow]")
        df_candidates = latest_grouped[latest_grouped['active_fund_count'] >= 1].copy()

    # Limit to top 25 candidates by active fund count + MoM changes to avoid API rate limits
    df_candidates = df_candidates.sort_values(
        by=['active_fund_count', 'mom_change_pct'], 
        ascending=[False, False]
    ).head(25)

    candidate_isins = df_candidates['isin'].tolist()
    console.print(f"[green]Found {len(candidate_isins)} candidates. Downloading technical indicators...[/green]")

    # 7. Download technical data in bulk
    try:
        df_prices = yf.download(candidate_isins, period='1y', group_by='ticker', progress=False)
    except Exception as exc:
        console.print(f"[red]Failed to download price data from yfinance: {exc}[/red]")
        return

    # 8. Compute technical indicators per candidate
    rows_data = []
    for _, row in df_candidates.iterrows():
        isin = row['isin']
        sec_name = row['security_name']
        funds_cnt = int(row['active_fund_count'])
        months = int(row['months_held'])
        nav_pct = float(row['pct_of_nav'])
        mom_chg = float(row['mom_change_pct'])

        ticker_symbol = isin
        rsi_val = None
        drawdown_pct = None
        volume_surge = None

        # Check if isin is in the columns
        has_data = False
        close_series = pd.Series()
        vol_series = pd.Series()

        if len(candidate_isins) == 1:
            # Single ticker download format
            if not df_prices.empty:
                close_series = df_prices['Close']
                vol_series = df_prices['Volume']
                has_data = True
        else:
            # Multi-ticker download format
            if isin in df_prices.columns.levels[0]:
                close_series = df_prices[isin]['Close'].dropna()
                vol_series = df_prices[isin]['Volume'].dropna()
                has_data = True

        if has_data and len(close_series) > 20:
            latest_close = float(close_series.iloc[-1])
            
            # RSI-14
            rsi_val = _rsi(close_series)
            
            # Drawdown from 52-week high
            high_52w = float(close_series.max())
            drawdown_pct = float((latest_close - high_52w) / high_52w * 100) if high_52w > 0 else 0.0
            
            # Volume surge (latest / 20d moving avg)
            avg_vol_20d = float(vol_series.rolling(20).mean().iloc[-1])
            latest_vol = float(vol_series.iloc[-1])
            volume_surge = float(latest_vol / avg_vol_20d) if avg_vol_20d > 0 else 1.0

            # Try to retrieve ticker name from yfinance
            try:
                ticker_symbol = yf.Ticker(isin).info.get('symbol', isin)
            except Exception:
                pass

        # Score calculations
        # Conviction score: active_fund_count * 20 + min(months, 36) * 1.0 (capped at 100)
        conv_score = min(100.0, (funds_cnt * 18.0) + (min(months, 36) * 1.2))
        
        # Technical opportunity score: lower RSI + deeper drawdown = higher score
        tech_score = 50.0
        if rsi_val is not None and drawdown_pct is not None:
            # Drawdown is negative: e.g. -25% drawdown -> 50 points; RSI of 30 -> 50 points
            dd_pts = min(50.0, max(0.0, -drawdown_pct * 1.5))
            rsi_pts = min(50.0, max(0.0, (75.0 - rsi_val) * 1.0))
            tech_score = dd_pts + rsi_pts

        comp_score = (conv_score * 0.5) + (tech_score * 0.5)

        rows_data.append({
            'name': sec_name,
            'isin': isin,
            'ticker': ticker_symbol,
            'funds': funds_cnt,
            'months': months,
            'nav': nav_pct,
            'mom': mom_chg,
            'rsi': rsi_val,
            'drawdown': drawdown_pct,
            'vol_surge': volume_surge,
            'score': comp_score
        })

    df_results = pd.DataFrame(rows_data)
    if df_results.empty:
        console.print("[yellow]No technical setups calculated.[/yellow]")
        return

    # Sort by composite score
    df_results = df_results.sort_values(by='score', ascending=False)

    # 9. Render Table
    table = Table(title="DSP Active-Fund Opportunity Scan Results", box=box.ROUNDED)
    table.add_column("Security (Ticker)", style="bold cyan")
    table.add_column("Funds", justify="right")
    table.add_column("Months", justify="right")
    table.add_column("NAV %", justify="right")
    table.add_column("MoM Δ", justify="right")
    table.add_column("RSI-14", justify="right")
    table.add_column("Drawdown %", justify="right")
    table.add_column("Vol Surge", justify="right")
    table.add_column("Opportunity Score", justify="right", style="bold green")

    for _, r in df_results.iterrows():
        # Formatting cells
        mom_str = f"{r['mom']:+.2f}%" if r['mom'] != 0 else "—"
        rsi_str = f"{r['rsi']:.1f}" if r['rsi'] is not None else "—"
        dd_str = f"{r['drawdown']:+.1f}%" if r['drawdown'] is not None else "—"
        vol_str = f"{r['vol_surge']:.2f}x" if r['vol_surge'] is not None else "—"
        
        # Color codes
        rsi_col = "green" if r['rsi'] is not None and r['rsi'] < 35 else "red" if r['rsi'] is not None and r['rsi'] > 70 else "white"
        dd_col = "green" if r['drawdown'] is not None and r['drawdown'] < -15 else "white"
        
        table.add_row(
            f"{r['name']}\n[dim]({r['ticker']})[/dim]",
            str(r['funds']),
            str(r['months']),
            f"{r['nav']:.2f}%",
            mom_str,
            f"[{rsi_col}]{rsi_str}[/{rsi_col}]",
            f"[{dd_col}]{dd_str}[/{dd_col}]",
            vol_str,
            f"{r['score']:.1f}/100"
        )

    console.print(table)

if __name__ == "__main__":
    run_scanner()
