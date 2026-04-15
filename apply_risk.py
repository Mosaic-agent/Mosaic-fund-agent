import os
import sys
import warnings

import clickhouse_connect
import pandas as pd
from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel

sys.path.insert(0, os.path.join(os.getcwd(), "src"))

from config.settings import settings
from ml.anomaly import run_composite_anomaly
from tools.risk_governor import compute_position_weight, explain_decision

warnings.filterwarnings("ignore")
console = Console()

def run_risk_governor_generic(symbol: str, category: str, comp_score: float):
    console.print(f"[bold cyan]🛡️  Running Risk Governor for {symbol}…[/bold cyan]")

    client = clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        database=settings.clickhouse_database,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
    )
    
    # Check what the actual symbol is in daily_prices for commodities
    if symbol == 'SILVERBEES':
        db_symbol = 'SILVER'
        db_category = 'commodities'
    elif symbol == 'GOLDBEES':
        db_symbol = 'GOLD'
        db_category = 'commodities'
    else:
        db_symbol = symbol
        db_category = category

    df = client.query_df(f"""
        SELECT
            trade_date,
            toFloat64(argMax(open,   imported_at)) AS open,
            toFloat64(argMax(high,   imported_at)) AS high,
            toFloat64(argMax(low,    imported_at)) AS low,
            toFloat64(argMax(close,  imported_at)) AS close,
            toFloat64(argMax(volume, imported_at)) AS volume
        FROM market_data.daily_prices
        WHERE symbol = '{db_symbol}' AND category = '{db_category}'
        GROUP BY trade_date
        ORDER BY trade_date ASC
    """)

    client.close()

    if df.empty:
        console.print(f"[red]Error: No data found for {db_symbol}.[/red]")
        return

    console.print(f"  [dim]→ running GARCH(1,1) anomaly pipeline for {symbol}…[/dim]")
    df["trade_date"] = pd.to_datetime(df["trade_date"])

    df_res, _, _ = run_composite_anomaly(df)

    valid = df_res.dropna(subset=["garch_vol"])
    if valid.empty:
        console.print("[red]Error: GARCH produced no valid vol estimates.[/red]")
        return

    latest    = valid.iloc[-1]
    garch_vol = float(latest["garch_vol"])
    regime    = str(latest["regime"])

    # Position sizing
    decision = compute_position_weight(
        garch_annual_vol_pct=garch_vol,
        regime=regime,
        composite_score=comp_score,
    )

    console.print(
        Panel(
            Markdown(explain_decision(decision)),
            title=f"[bold white]Risk Governor: {symbol}[/bold white]",
            border_style="cyan",
        )
    )

if __name__ == "__main__":
    # Top Picks from previous step: SILVERBEES (60), CPSEETF (60), GOLDBEES (60)
    run_risk_governor_generic("SILVERBEES", "etfs", 60.0)
    print("\n")
    run_risk_governor_generic("CPSEETF", "etfs", 60.0)
