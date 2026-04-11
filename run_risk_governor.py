import os
import sys
import warnings

import clickhouse_connect
import pandas as pd
from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel

# insert(0) ensures project src shadows any installed package with same name
sys.path.insert(0, os.path.join(os.getcwd(), "src"))

from config.settings import settings
from ml.anomaly import run_composite_anomaly
from tools.quant_scorecard import compute_gold_scorecard
from tools.risk_governor import compute_position_weight, explain_decision

warnings.filterwarnings("ignore")
console = Console()


def run_risk_governor():
    console.print("[bold cyan]🛡️  Running Risk Governor for GOLDBEES…[/bold cyan]")

    # ── 1. Composite quant score ──────────────────────────────────────────────
    console.print("  [dim]→ computing quant scorecard…[/dim]")
    scorecard   = compute_gold_scorecard(
        ch_host=settings.clickhouse_host,
        ch_port=settings.clickhouse_port,
        ch_user=settings.clickhouse_user,
        ch_pass=settings.clickhouse_password,
        ch_database=settings.clickhouse_database,
    )
    comp_score = scorecard.get("composite_score")

    # ── 2. Fetch GOLDBEES OHLCV ───────────────────────────────────────────────
    console.print("  [dim]→ fetching GOLDBEES price history…[/dim]")
    client = clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        database=settings.clickhouse_database,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
    )
    df = client.query_df("""
        SELECT
            trade_date,
            toFloat64(argMax(open,   imported_at)) AS open,
            toFloat64(argMax(high,   imported_at)) AS high,
            toFloat64(argMax(low,    imported_at)) AS low,
            toFloat64(argMax(close,  imported_at)) AS close,
            toFloat64(argMax(volume, imported_at)) AS volume
        FROM market_data.daily_prices
        WHERE symbol = 'GOLDBEES' AND category = 'etfs'
        GROUP BY trade_date
        ORDER BY trade_date ASC
    """)

    # Optional cross-asset enrichment for Isolation Forest
    df_cot = client.query_df(
        "SELECT report_date, mm_net, open_interest FROM market_data.cot_gold"
    )
    df_fx = client.query_df(
        "SELECT symbol, trade_date, toFloat64(close) AS close "
        "FROM market_data.fx_rates FINAL WHERE symbol = 'USDINR'"
    )
    client.close()

    if df.empty:
        console.print("[red]Error: No GOLDBEES data found. Run import first.[/red]")
        return

    # ── 3. GARCH anomaly pipeline ─────────────────────────────────────────────
    console.print("  [dim]→ running GARCH(1,1) anomaly pipeline…[/dim]")
    df["trade_date"] = pd.to_datetime(df["trade_date"])   # ← critical: date → datetime64
    if not df_cot.empty:
        df_cot["report_date"] = pd.to_datetime(df_cot["report_date"])
    if not df_fx.empty:
        df_fx["trade_date"] = pd.to_datetime(df_fx["trade_date"])

    df_res, _, _ = run_composite_anomaly(
        df,
        df_cot=df_cot if not df_cot.empty else None,
        df_fx=df_fx   if not df_fx.empty  else None,
    )

    # Use last row that has a valid garch_vol — warmup can leave tail as NaN
    valid = df_res.dropna(subset=["garch_vol"])
    if valid.empty:
        console.print("[red]Error: GARCH produced no valid vol estimates.[/red]")
        return

    latest    = valid.iloc[-1]
    garch_vol = float(latest["garch_vol"])
    regime    = str(latest["regime"])

    # ── 4. Position sizing ────────────────────────────────────────────────────
    decision = compute_position_weight(
        garch_annual_vol_pct=garch_vol,
        regime=regime,
        composite_score=float(comp_score) if comp_score is not None else None,
    )

    # ── 5. Rich output ────────────────────────────────────────────────────────
    console.print(
        Panel(
            Markdown(explain_decision(decision)),   # ← render ## headers and **bold**
            title="[bold white]Risk Governor[/bold white]",
            border_style="cyan",
        )
    )


if __name__ == "__main__":
    run_risk_governor()
