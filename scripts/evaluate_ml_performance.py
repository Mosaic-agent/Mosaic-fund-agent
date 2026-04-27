"""
scripts/evaluate_ml_performance.py
────────────────────────────────────
Evaluate live ML prediction accuracy by joining ml_predictions to
realised daily_prices at the forward horizon.

Fixes vs original:
  - Use clickhouse_connect directly (ClickHouseImporter has no __enter__)
  - Align on trading-day horizon (skip N trading days, not N calendar days)
  - Use log return throughout (matches the model's target basis)
  - Hit = sign(predicted) == sign(actual); zero-prediction → no hit either way
  - Configurable display rows via --rows CLI arg
"""
import argparse
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

try:
    import clickhouse_connect
    from config.settings import settings
except ImportError as e:
    print(f"Error importing project modules: {e}")
    sys.exit(1)

from rich.console import Console
from rich.table import Table

console = Console()


def _get_client():
    return clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        database=settings.clickhouse_database,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
    )


def evaluate_ml(display_rows: int = 15):
    c = _get_client()
    try:
        # 1. Fetch predictions
        preds = c.query_df("""
            SELECT
                as_of,
                horizon_days,
                expected_return_pct,
                regime_signal,
                goldbees_close AS start_price
            FROM market_data.ml_predictions FINAL
            ORDER BY as_of ASC
        """)
        if preds.empty:
            console.print("[yellow]No predictions found in database.[/yellow]")
            return

        # 2. Fetch full GOLDBEES price series (deduplicated via argMax)
        prices = c.query_df("""
            SELECT
                trade_date,
                argMax(close, imported_at) AS close
            FROM market_data.daily_prices
            WHERE symbol = 'GOLDBEES' AND category = 'etfs'
            GROUP BY trade_date
            ORDER BY trade_date ASC
        """)
    finally:
        c.close()

    preds["as_of"]       = pd.to_datetime(preds["as_of"])
    prices["trade_date"] = pd.to_datetime(prices["trade_date"])

    # Build a sorted DatetimeIndex for O(log n) horizon look-ups
    price_idx = pd.DatetimeIndex(prices["trade_date"])
    price_map = prices.set_index("trade_date")["close"].to_dict()

    results = []
    for _, row in preds.iterrows():
        as_of      = row["as_of"]
        horizon    = int(row["horizon_days"])
        start_price = float(row["start_price"])

        if start_price <= 0:
            continue

        # Advance by `horizon` *trading* days using the sorted price index
        entry_pos = price_idx.searchsorted(as_of, side="left")
        exit_pos  = entry_pos + horizon
        if exit_pos >= len(price_idx):
            continue  # horizon hasn't closed yet

        end_date  = price_idx[exit_pos]
        end_price = float(price_map[end_date])

        # Log return to match model target basis
        actual_logret    = np.log(end_price / start_price) * 100
        predicted_logret = float(row["expected_return_pct"])

        # Hit: predicted and actual log-returns have same sign (both non-zero)
        pred_sign   = np.sign(predicted_logret)
        actual_sign = np.sign(actual_logret)
        if pred_sign == 0:
            hit = None          # no directional call — exclude from hit count
        else:
            hit = int(pred_sign == actual_sign)

        results.append({
            "as_of":     as_of.date(),
            "end_date":  end_date.date(),
            "regime":    row["regime_signal"],
            "predicted": predicted_logret,
            "actual":    actual_logret,
            "error":     actual_logret - predicted_logret,
            "hit":       hit,
        })

    if not results:
        console.print(
            "[yellow]No realised data points yet — "
            "wait for horizon_days to pass after each prediction.[/yellow]"
        )
        return

    eval_df = pd.DataFrame(results)

    # ── Summary stats ────────────────────────────────────────────────────────
    directional = eval_df[eval_df["hit"].notna()]
    hit_ratio   = directional["hit"].mean() if not directional.empty else float("nan")
    mae         = eval_df["error"].abs().mean()
    rmse        = np.sqrt((eval_df["error"] ** 2).mean())
    n_total     = len(eval_df)
    n_dir       = len(directional)

    # ── Display table ─────────────────────────────────────────────────────────
    table = Table(
        title="ML Prediction Performance",
        show_header=True, header_style="bold cyan",
    )
    table.add_column("As Of",      style="dim")
    table.add_column("Actual At",  style="dim")
    table.add_column("Regime")
    table.add_column("Pred %",     justify="right")
    table.add_column("Actual %",   justify="right")
    table.add_column("Error",      justify="right")
    table.add_column("Hit",        justify="center")

    for _, r in eval_df.tail(display_rows).iterrows():
        if r["hit"] is None:
            hit_str = "[dim]—[/dim]"
        else:
            hit_str = "[green]✓[/green]" if r["hit"] else "[red]✗[/red]"
        pred_style = "green" if r["predicted"] > 0 else "red"
        act_style  = "green" if r["actual"]    > 0 else "red"
        table.add_row(
            str(r["as_of"]),
            str(r["end_date"]),
            r["regime"],
            f"[{pred_style}]{r['predicted']:+.2f}%[/{pred_style}]",
            f"[{act_style}]{r['actual']:+.2f}%[/{act_style}]",
            f"{r['error']:+.2f}",
            hit_str,
        )

    console.print(table)
    console.print(f"\n[bold]Metrics (n={n_total}, directional n={n_dir}):[/bold]")
    console.print(f"• Hit Ratio : [bold]{hit_ratio:.1%}[/bold]  (>50% = directional edge)")
    console.print(f"• MAE       : [bold]{mae:.2f}%[/bold]")
    console.print(f"• RMSE      : [bold]{rmse:.2f}%[/bold]")
    console.print(
        "\n[dim]Note: predicted % is log-return (model target basis). "
        "Simple return ≈ log return for small moves.[/dim]"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Evaluate ML prediction accuracy")
    parser.add_argument("--rows", type=int, default=15,
                        help="Number of recent rows to display (default 15)")
    args = parser.parse_args()
    evaluate_ml(display_rows=args.rows)
