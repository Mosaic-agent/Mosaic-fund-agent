import os
import sys
import warnings
import pandas as pd
import numpy as np
import clickhouse_connect
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.markdown import Markdown

# Ensure project paths
sys.path.insert(0, os.getcwd())
sys.path.insert(0, os.path.join(os.getcwd(), "src"))

from config.settings import settings
from ml.anomaly import run_composite_anomaly
from tools.risk_governor import compute_position_weight, explain_decision

warnings.filterwarnings("ignore")
console = Console()

# Mapping Omni FoF holdings to proxy ETF symbols in our database
# (Holding Name, Weight, Proxy Symbol)
PROXY_WEIGHTS = [
    ("ICICI Prudential Nifty FMCG ETF", 7.29,  "FMCGIETF"),
    ("Nippon India Large Cap Fund",     8.20,  "NIFTYBEES"),
    ("DSP Gilt Fund",                   24.22, "GILT5YBEES"),
    ("DSP Short Term Fund",             5.09,  "LIQUIDBEES"),
    ("DSP Large Cap Fund",              7.74,  "NIFTYBEES"),
    ("DSP Healthcare Fund",             3.44,  "PHARMABEES"),
    ("DSP Gold ETF",                    11.18, "GOLDBEES"),
    ("DSP NIFTY IT ETF",                12.37, "ITBEES"),
    ("DSP Nifty Private Bank ETF",      7.24,  "BANKBEES"),
    ("DSP BSE Liquid Rate ETF",         1.93,  "LIQUIDBEES"),
    ("DSP Nifty Top 10 Equal Weight",   5.65,  "NIFTYBEES"),
    ("Parag Parikh Flexicap Fund",      3.51,  "JUNIORBEES"),
    ("Cash / TREPS / Receivables",      2.14,  "LIQUIDBEES"),
]

def run_omni_fof_risk():
    console.print("[bold cyan]🛡️  Risk Governor: DSP Multi Asset Omni FoF (Proxy Model)[/bold cyan]")
    
    client = clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        database=settings.clickhouse_database,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
    )
    
    # 1. Fetch daily returns for all proxies
    symbols = list(set([p[2] for p in PROXY_WEIGHTS]))
    all_data = []
    
    for symbol in symbols:
        df = client.query_df(f"""
            SELECT
                trade_date,
                toFloat64(argMax(close, imported_at)) AS close
            FROM market_data.daily_prices
            WHERE symbol = '{symbol}'
            GROUP BY trade_date
            ORDER BY trade_date ASC
        """)
        if df.empty:
            continue
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        df = df.set_index('trade_date')
        df[f'ret_{symbol}'] = df['close'].pct_change()
        all_data.append(df[[f'ret_{symbol}']])
    
    client.close()
    
    if not all_data:
        console.print("[red]Error: No proxy price history found in ClickHouse.[/red]")
        return
        
    # 2. Join returns on common dates
    ret_matrix = pd.concat(all_data, axis=1).dropna()
    
    # 3. Calculate weighted portfolio returns
    # Normalize weights to sum to 1.0 (some residuals might exist)
    total_w = sum(p[1] for p in PROXY_WEIGHTS)
    portfolio_ret = pd.Series(0.0, index=ret_matrix.index)
    
    for name, weight, symbol in PROXY_WEIGHTS:
        col = f'ret_{symbol}'
        if col in ret_matrix.columns:
            portfolio_ret += ret_matrix[col] * (weight / total_w)
            
    # Convert series back to OHLC-like format for the anomaly pipeline
    # We'll create a synthetic 'close' starting at 100
    portfolio_prices = (1 + portfolio_ret).cumprod() * 100
    df_proxy = pd.DataFrame({
        'trade_date': portfolio_prices.index,
        'close': portfolio_prices.values,
        'open': portfolio_prices.values, # placeholder
        'high': portfolio_prices.values, # placeholder
        'low': portfolio_prices.values,  # placeholder
        'volume': 1000.0                 # placeholder
    })
    
    # 4. Run GARCH(1,1) anomaly pipeline
    console.print("  [dim]→ running GARCH(1,1) risk pipeline on proxy portfolio…[/dim]")
    df_res, _, _ = run_composite_anomaly(df_proxy)
    
    valid = df_res.dropna(subset=["garch_vol"])
    if valid.empty:
        console.print("[red]Error: GARCH produced no valid vol estimates.[/red]")
        return

    latest    = valid.iloc[-1]
    garch_vol = float(latest["garch_vol"])
    regime    = str(latest["regime"])
    
    # 5. Position sizing decision
    # Omni FoF uses "Netra" which is quant, so we assume a neutral score of 50
    decision = compute_position_weight(
        garch_annual_vol_pct=garch_vol,
        regime=regime,
        composite_score=50.0, 
    )
    
    # 6. Display results
    console.print(
        Panel(
            Markdown(explain_decision(decision)),
            title="[bold white]Risk Governor: DSP Omni FoF (Synthetic Analysis)[/bold white]",
            border_style="cyan",
        )
    )
    
    # Show component breakdown
    table = Table(title="Synthetic Proxy Allocation Breakdown")
    table.add_column("Holding", style="dim")
    table.add_column("Weight (%)", justify="right")
    table.add_column("Proxy Used", style="cyan")
    
    for name, weight, symbol in PROXY_WEIGHTS:
        table.add_row(name, f"{weight:.2f}%", symbol)
    
    console.print(table)

if __name__ == "__main__":
    run_omni_fof_risk()
