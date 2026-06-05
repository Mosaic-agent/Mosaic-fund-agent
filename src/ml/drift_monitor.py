"""
src/ml/drift_monitor.py
────────────────────────
Model Drift Monitor for GOLDBEES LightGBM predictions.
Queries predictions, computes realized returns from daily prices,
logs them back into ClickHouse, and alerts/retrains if performance drops.
"""

import logging
import numpy as np
import pandas as pd
import pathlib
from datetime import datetime
from sklearn.metrics import roc_auc_score
from src.db.pool import get_pool, query_df, execute

log = logging.getLogger(__name__)

# Model Cache Directory (for invalidation during retraining)
_MODEL_CACHE_DIR = pathlib.Path(__file__).parents[2] / "output" / ".cache" / "ml_models"


def update_realized_returns() -> None:
    """
    Query past predictions from ml_predictions, calculate their actual
    5-day realized returns using daily price history, and write the updated
    rows back to ClickHouse (idempotent ReplacingMergeTree upsert).
    """
    # 1. Run migrations first to ensure column exists
    try:
        execute("ALTER TABLE market_data.ml_predictions ADD COLUMN IF NOT EXISTS realized_return_pct Float64 DEFAULT 0.0")
    except Exception as e:
        log.warning("Could not execute migration on ClickHouse (is database running?): %s", e)
        return

    # 2. Fetch all predictions from ClickHouse
    try:
        df_preds = query_df("SELECT * FROM market_data.ml_predictions FINAL ORDER BY as_of ASC")
    except Exception as e:
        log.warning("Could not read from ml_predictions: %s", e)
        return

    if df_preds.empty:
        log.info("No predictions found to evaluate.")
        return

    # 3. Fetch GOLDBEES price history
    try:
        df_prices = query_df(
            "SELECT trade_date, close FROM market_data.daily_prices FINAL "
            "WHERE symbol = 'GOLDBEES' AND category = 'etfs' ORDER BY trade_date ASC"
        )
    except Exception as e:
        log.warning("Could not read from daily_prices: %s", e)
        return

    if df_prices.empty:
        log.warning("No price history found for GOLDBEES.")
        return

    # Convert prices to a quick mapping and a sorted list of dates
    prices_list = list(df_prices.itertuples(index=False))
    prices_map = {row.trade_date: row.close for row in prices_list}
    dates_sorted = [row.trade_date for row in prices_list]
    date_to_idx = {d: idx for idx, d in enumerate(dates_sorted)}

    rows_to_update = []

    # 4. Compute realized return over the forward trading horizon
    for row in df_preds.itertuples():
        as_of = row.as_of
        horizon = row.horizon_days

        if as_of not in date_to_idx:
            continue

        current_idx = date_to_idx[as_of]
        future_idx = current_idx + horizon

        # If future index is in bounds, we have the realized return
        if future_idx < len(dates_sorted):
            close_current = prices_map[as_of]
            close_future = prices_list[future_idx].close
            
            # Target metric matching label_forward_return: ln(close[t+horizon] / close[t]) * 100
            realized_log_ret = np.log(close_future / close_current)
            realized_ret_pct = round(float(realized_log_ret * 100), 4)

            # Avoid redundant writes if already logged correctly
            if hasattr(row, "realized_return_pct") and row.realized_return_pct == realized_ret_pct:
                continue

            updated_row = {
                "as_of": as_of,
                "horizon_days": horizon,
                "expected_return_pct": row.expected_return_pct,
                "confidence_low": row.confidence_low,
                "confidence_high": row.confidence_high,
                "regime_signal": row.regime_signal,
                "cv_r2_mean": row.cv_r2_mean,
                "n_training_rows": row.n_training_rows,
                "goldbees_close": row.goldbees_close,
                "prob_up": getattr(row, "prob_up", 0.5),
                "cv_auc_mean": getattr(row, "cv_auc_mean", 0.5),
                "realized_return_pct": realized_ret_pct,
                "created_at": datetime.now()  # Updates the version for ReplacingMergeTree
            }
            rows_to_update.append(updated_row)

    if not rows_to_update:
        log.info("All logged predictions have up-to-date realized returns.")
        return

    # 5. Insert back into ClickHouse
    try:
        client = get_pool().get_client()
        columns = list(rows_to_update[0].keys())
        data = [[r[col] for col in columns] for r in rows_to_update]
        client.insert("market_data.ml_predictions", data, column_names=columns)
        client.close()
        log.info("Updated %d predictions in ClickHouse with ground truth realized returns.", len(rows_to_update))
    except Exception as e:
        log.warning("Could not write updated prediction rows to ClickHouse: %s", e)


def retrain_model() -> None:
    """
    Purges model cache from disk and runs trend prediction with
    expanded splits (7 splits instead of default 5) to retrain on wider history.
    """
    if _MODEL_CACHE_DIR.exists():
        deleted = 0
        for path in _MODEL_CACHE_DIR.glob("goldbees_lgbm_*.joblib"):
            path.unlink(missing_ok=True)
            deleted += 1
        log.info("Cleared %d cached joblib models in %s to force retrain.", deleted, _MODEL_CACHE_DIR)
        print(f"[green]✓ Evicted cached models from {_MODEL_CACHE_DIR.name}[/green]")
    else:
        print("Model cache directory empty. Proceeding to retrain.")

    print("[yellow]Retraining LightGBM models with wider lookback window (7-fold CV)...[/yellow]")
    try:
        from src.ml.trend_predictor import run_trend_prediction
        # Retrain with 7 validation folds to increase lookback context evaluation
        res = run_trend_prediction(n_splits=7, verbose=True)
        print("[green]✓ Model retraining completed successfully.[/green]")
        print(f"New prediction: expected_return={res['expected_return_pct']:+.3f}% (CV AUC={res['cv_auc_mean']:.3f})")
    except Exception as e:
        print(f"[red]Error executing retrain pipeline: {e}[/red]")


def run_drift_monitor(lookback_days: int = 90, auto_retrain: bool = True) -> None:
    """
    Evaluate prediction quality, compute stats, and retrain model if thresholds are breached.
    """
    print("[yellow]Updating realized returns in ClickHouse...[/yellow]")
    update_realized_returns()

    # Fetch predictions back from ClickHouse
    try:
        df = query_df("SELECT * FROM market_data.ml_predictions FINAL ORDER BY as_of DESC")
    except Exception as e:
        print(f"[red]Failed to connect to ClickHouse database: {e}[/red]")
        return

    if df.empty:
        print("[red]No records found in market_data.ml_predictions.[/red]")
        return

    # Filter for evaluations that occurred in the past (already realized)
    # Since horizon_days is 5 trading days (~7 calendar days), filter out recent records
    eval_cutoff = pd.Timestamp(datetime.now().date()) - pd.Timedelta(days=7)
    df = df.copy()
    df["as_of"] = pd.to_datetime(df["as_of"])
    df_eval = df[df["as_of"] <= eval_cutoff].head(lookback_days).copy()

    if len(df_eval) < 5:
        print(f"[yellow]Insufficient matured predictions (found {len(df_eval)}, need >= 5) to evaluate drift.[/yellow]")
        return

    # Check directions
    df_eval["pred_dir"] = df_eval["expected_return_pct"] > 0
    df_eval["realized_dir"] = df_eval["realized_return_pct"] > 0
    df_eval["hit"] = df_eval["pred_dir"] == df_eval["realized_dir"]

    hit_ratio = float(df_eval["hit"].mean())

    # Calculate ROC-AUC using prob_up as predictions
    true_labels = (df_eval["realized_return_pct"] > 0).astype(int).tolist()
    pred_probs = df_eval["prob_up"].tolist()

    try:
        auc = roc_auc_score(true_labels, pred_probs)
    except ValueError:
        auc = 0.5  # default/fallback if only one class exists in true_labels

    mae = float(np.abs(df_eval["expected_return_pct"] - df_eval["realized_return_pct"]).mean())

    print("\n==================================================")
    print("        GOLDBEES ML MODEL DRIFT MONITOR           ")
    print("==================================================")
    print(f"Evaluated predictions: {len(df_eval)} (Lookback: {lookback_days}d)")
    print(f"Rolling Hit Ratio    : {hit_ratio * 100:.2f}%")
    print(f"Rolling ROC-AUC       : {auc:.3f}")
    print(f"Mean Absolute Error  : {mae:.3f}%")
    print("--------------------------------------------------")

    # Render a preview table of recent items
    df_table = df_eval.head(10)[["as_of", "goldbees_close", "expected_return_pct", "realized_return_pct", "hit"]].copy()
    df_table["expected_return_pct"] = df_table["expected_return_pct"].map(lambda x: f"{x:+.3f}%")
    df_table["realized_return_pct"] = df_table["realized_return_pct"].map(lambda x: f"{x:+.3f}%")
    df_table["hit"] = df_table["hit"].map(lambda x: "✅ Hit" if x else "❌ Miss")
    print(df_table.to_string(index=False))
    print("==================================================")

    # Perform drift detection checks
    drift_detected = False
    reasons = []

    if hit_ratio < 0.50:
        drift_detected = True
        reasons.append(f"Rolling hit ratio fell below 50% ({hit_ratio * 100:.1f}%)")
    if auc < 0.50:
        drift_detected = True
        reasons.append(f"Model AUC fell below 0.50 ({auc:.3f})")

    if drift_detected:
        print("\n[red]🚨 ALERT: MODEL DRIFT DETECTED![/red]")
        for r in reasons:
            print(f"  - {r}")

        if auto_retrain:
            retrain_model()
        else:
            print("[yellow]Auto-retrain disabled by configuration option.[/yellow]")
    else:
        print("\n[green]✅ Model health is optimal. No drift detected.[/green]")
