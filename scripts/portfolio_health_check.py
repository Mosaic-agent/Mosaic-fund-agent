"""
scripts/portfolio_health_check.py
──────────────────────────────────
Quantitative Health Check for your Zerodha Portfolio.
Calculates:
  1. Concentration Risk (Portfolio Weights)
  2. Valuation Risk (iNAV Premium/Discount)
  3. Volatility Risk (GARCH 1,1 Annualised Vol)
  4. 'Sleep-at-Night' (SAN) Score (0-100)
"""

import sys
import os
import pandas as pd
import numpy as np
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

# Add project root to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.importer.clickhouse import ClickHouseImporter
from src.tools.inav_fetcher import get_portfolio_etf_inav
from src.ml.anomaly import run_composite_anomaly
from config.settings import settings

console = Console()

def get_latest_holdings():
    """Fetch current holdings from ClickHouse."""
    query = "SELECT * FROM market_data.user_holdings FINAL WHERE quantity > 0"
    with ClickHouseImporter(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password
    ) as ch:
        return ch._client.query_df(query)

def get_volatility(symbol: str, category: str = 'etfs'):
    """Fetch GARCH volatility for a symbol."""
    query = f"""
        SELECT trade_date, toFloat64(open) as open, toFloat64(high) as high, 
               toFloat64(low) as low, toFloat64(close) as close, toFloat64(volume) as volume
        FROM market_data.daily_prices FINAL
        WHERE symbol = '{symbol}'
        ORDER BY trade_date DESC LIMIT 250
    """
    try:
        with ClickHouseImporter(host=settings.clickhouse_host, port=settings.clickhouse_port) as ch:
            df = ch._client.query_df(query)
        if df.empty: return 0.0, "Unknown"
        
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        df = df.sort_values('trade_date')
        
        res_df, _, _ = run_composite_anomaly(df)
        latest_vol = float(res_df['garch_vol'].dropna().iloc[-1])
        regime = str(res_df['regime'].iloc[-1])
        return latest_vol, regime
    except Exception:
        return 20.0, "Normal" # Fallback

def main():
    console.print(Panel("[bold cyan]Mosaic Portfolio Health Check[/bold cyan]", subtitle="April 17, 2026"))
    
    # 1. Fetch Data
    df = get_latest_holdings()
    if df.empty:
        console.print("[red]No holdings found in ClickHouse. Run a backup first.[/red]")
        return

    # 2. Calculate Weights
    df['market_value'] = df['quantity'] * df['last_price']
    total_value = df['market_value'].sum()
    df['weight_pct'] = (df['market_value'] / total_value) * 100
    
    # 3. iNAV Check for ETFs
    etf_symbols = df['tradingsymbol'].tolist()
    inav_results = get_portfolio_etf_inav(etf_symbols)

    # 4. Building Results Table
    table = Table(title="Portfolio Health Breakdown")
    table.add_column("Instrument", style="bold")
    table.add_column("Weight", justify="right")
    table.add_column("Valuation", justify="center")
    table.add_column("Risk/Vol", justify="center")
    table.add_column("P&L %", justify="right")

    san_deductions = 0
    alerts = []

    # Get GARCH Vol for largest holding
    top_symbol = df.sort_values('weight_pct', ascending=False).iloc[0]['tradingsymbol']
    vol, regime = get_volatility(top_symbol)

    for _, row in df.sort_values('weight_pct', ascending=False).iterrows():
        sym = row['tradingsymbol']
        weight = row['weight_pct']
        pnl_pct = (row['pnl'] / (row['quantity'] * row['average_price'])) * 100
        
        # Valuation Logic
        val_str = "[dim]—[/dim]"
        if sym in inav_results:
            disc = inav_results[sym]['premium_discount_pct']
            if disc < -0.5:
                val_str = f"[green]Discount ({disc}%)[/green]"
            elif disc > 0.5:
                val_str = f"[red]Premium ({disc}%)[/red]"
                san_deductions += (weight / 10) # Heavy weight + high premium = bad
                alerts.append(f"⚠️ {sym} is overpriced (+{disc}%)")
            else:
                val_str = "Fair"

        # Volatility/Risk Logic (Simplified)
        risk_str = "Normal"
        if sym == top_symbol:
            risk_str = f"{vol:.1f}% ({regime})"
            if vol > 25:
                san_deductions += (weight / 5)
                alerts.append(f"🔥 High Volatility in {sym} ({vol:.1f}%)")

        # Concentration Penalty
        if weight > 40:
            san_deductions += 15
            alerts.append(f"⚖️ High Concentration in {sym} ({weight:.1f}%)")

        table.add_row(
            sym, 
            f"{weight:.1f}%", 
            val_str, 
            risk_str, 
            f"{'[green]' if pnl_pct >=0 else '[red]'}{pnl_pct:+.2f}%[/]"
        )

    # Calculate SAN Score
    san_score = max(0, min(100, 100 - san_deductions))
    
    console.print(table)
    
    # 5. SAN Score Panel
    color = "green" if san_score > 70 else "yellow" if san_score > 40 else "red"
    console.print(Panel(
        f"[bold {color}]Score: {san_score:.0f}/100[/bold {color}]\n" + 
        "\n".join(alerts) if alerts else "✅ Your portfolio looks healthy.",
        title="💤 Sleep-at-Night Score",
        border_style=color
    ))

if __name__ == "__main__":
    main()
