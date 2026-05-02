import os
import sys
import warnings
from datetime import date, timedelta

import clickhouse_connect
import pandas as pd
from rich.console import Console
from rich.table import Table

# insert(0) ensures project root and src are in path
sys.path.insert(0, os.getcwd())
sys.path.insert(0, os.path.join(os.getcwd(), "src"))

from config.settings import settings
from ml.anomaly import run_composite_anomaly
from tools.quant_scorecard import compute_gold_scorecard, compute_silver_scorecard
from tools.risk_governor import compute_position_weight

warnings.filterwarnings("ignore")
console = Console()

def run_all_etf_risk():
    console.print("[bold cyan]🛡️  Running Risk Governor across all ETFs…[/bold cyan]")

    client = clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        database=settings.clickhouse_database,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
    )

    # 1. Fetch all ETF symbols
    etf_symbols = [r[0] for r in client.query("SELECT DISTINCT symbol FROM market_data.daily_prices WHERE category = 'etfs'").result_rows]
    
    # 2. Pre-fetch common cross-asset data
    df_cot_gold = client.query_df("SELECT report_date, mm_net, open_interest FROM market_data.cot_gold")
    df_fx = client.query_df("SELECT symbol, trade_date, toFloat64(close) AS close FROM market_data.fx_rates FINAL WHERE symbol = 'USDINR'")
    
    results = []

    for symbol in etf_symbols:
        try:
            # Fetch price history
            df = client.query_df(f"""
                SELECT
                    trade_date,
                    toFloat64(argMax(open,   imported_at)) AS open,
                    toFloat64(argMax(high,   imported_at)) AS high,
                    toFloat64(argMax(low,    imported_at)) AS low,
                    toFloat64(argMax(close,  imported_at)) AS close,
                    toFloat64(argMax(volume, imported_at)) AS volume
                FROM market_data.daily_prices
                WHERE symbol = '{symbol}' AND category = 'etfs'
                GROUP BY trade_date
                ORDER BY trade_date ASC
            """)

            if len(df) < 60:
                continue

            df["trade_date"] = pd.to_datetime(df["trade_date"])
            
            # Anomaly pipeline
            df_res, _, _ = run_composite_anomaly(
                df,
                df_cot=df_cot_gold if symbol == 'GOLDBEES' else None,
                df_fx=df_fx if not df_fx.empty else None,
            )

            valid = df_res.dropna(subset=["garch_vol"])
            if valid.empty:
                continue

            latest = valid.iloc[-1]
            garch_vol = float(latest["garch_vol"])
            regime = str(latest["regime"])

            # Quant Scorecard (if applicable)
            comp_score = None
            if symbol == 'GOLDBEES':
                scorecard = compute_gold_scorecard(
                    ch_host=settings.clickhouse_host,
                    ch_port=settings.clickhouse_port,
                    ch_user=settings.clickhouse_user,
                    ch_database=settings.clickhouse_database,
                )
                comp_score = scorecard.get("composite_score")
            elif symbol == 'SILVERBEES':
                scorecard = compute_silver_scorecard(
                    ch_host=settings.clickhouse_host,
                    ch_port=settings.clickhouse_port,
                    ch_user=settings.clickhouse_user,
                    ch_database=settings.clickhouse_database,
                )
                comp_score = scorecard.get("composite_score")

            # Position sizing
            decision = compute_position_weight(
                garch_annual_vol_pct=garch_vol,
                regime=regime,
                composite_score=float(comp_score) if comp_score is not None else None,
            )

            results.append({
                "Symbol": symbol,
                "Vol %": f"{garch_vol:.1f}%",
                "Regime": regime.split(' (')[0], # Simplify label
                "Score": f"{comp_score:.0f}" if comp_score is not None else "-",
                "Weight": f"{decision.final_weight:.0%}",
                "Tier": decision.tier
            })

        except Exception as e:
            console.print(f"[red]Error processing {symbol}: {e}[/red]")

    client.close()

    # 3. Display results table
    table = Table(title="ETF Risk Portfolio Summary (Apr 15, 2026)")
    table.add_column("Symbol", style="cyan")
    table.add_column("GARCH Vol", justify="right")
    table.add_column("Regime", style="magenta")
    table.add_column("Quant Score", justify="right")
    table.add_column("Rec. Weight", justify="right", style="bold green")
    table.add_column("Tier", style="yellow")

    # Sort results by weight descending
    results.sort(key=lambda x: float(x["Weight"].strip('%')), reverse=True)

    for r in results:
        table.add_row(
            r["Symbol"],
            r["Vol %"],
            r["Regime"],
            r["Score"],
            r["Weight"],
            r["Tier"]
        )

    console.print(table)

if __name__ == "__main__":
    run_all_etf_risk()
