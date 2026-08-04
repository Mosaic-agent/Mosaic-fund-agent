"""
scripts/backfill_ml_predictions.py
────────────────────────────────────
Walk-forward replay that fills market_data.ml_predictions for past trading dates.

Problem
───────
The live `run_trend_prediction()` only stores one prediction row per run (today's
date). After the classifier upgrade, the table only has ~6 rows covering the
last month. That prevents evaluating Kelly/Blended performance over longer
time horizons.

This script replays the prediction logic historically using only data that was
available on each past date — no look-ahead. It produces a complete prediction
history that the backtest harness (tests/_backtest_adaptive_kelly.py) can use
to evaluate all four sizing methods side-by-side.

Walk-forward design
───────────────────
For each prediction date D (stepping --step trading days, newest → oldest):
  1. Slice the pre-built feature table to rows ≤ D
  2. Label forward returns on the slice — last `horizon` rows get NaN target
     (they look past D; fit_walk_forward skips them automatically)
  3. Refit LightGBM classifier + quantile regressors using only the slice
  4. Predict direction prob + expected return for date D
  5. Insert into ml_predictions with as_of = D (idempotent)

Dates already present in ml_predictions are skipped unless --force is set.

Performance
───────────
With --step 5 and --months 12: ~50 retrains × 10-30 s each ≈ 8-25 min.
With --step 10 and --months 12: ~25 retrains × 10-30 s each ≈ 4-12 min.
The master table is fetched and engineered ONCE before the loop.

Usage
─────
  .venv_new/bin/python3 scripts/backfill_ml_predictions.py
  .venv_new/bin/python3 scripts/backfill_ml_predictions.py --months 12
  .venv_new/bin/python3 scripts/backfill_ml_predictions.py --months 6 --step 5
  .venv_new/bin/python3 scripts/backfill_ml_predictions.py --dry-run
  .venv_new/bin/python3 scripts/backfill_ml_predictions.py --months 3 --force
"""
from __future__ import annotations

import argparse
import os
import sys
import time
import warnings
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd

sys.path.insert(0, os.getcwd())
warnings.filterwarnings("ignore")

from config.settings import settings
from src.ml.trend_predictor import (
    build_master_table,
    engineer_features,
    label_forward_return,
    fit_walk_forward,
    _HORIZON,
    _N_SPLITS,
    _GAP,
    _MIN_ROWS,
)

# ── Thresholds (mirror run_trend_prediction) ──────────────────────────────────
_BUY_THRESH        =  0.25
_WATCH_LONG_THRESH =  0.05
_WATCH_SHORT_THRESH = -0.05
_SELL_THRESH        = -0.25


def _regime_from_pred(pred_pct: float, horizon: int) -> tuple[str, str]:
    if pred_pct >= _BUY_THRESH:
        return "BUY", f"Model expects +{pred_pct:.2f}% over {horizon}d."
    elif pred_pct >= _WATCH_LONG_THRESH:
        return "WATCH_LONG", f"Model expects +{pred_pct:.2f}% over {horizon}d."
    elif pred_pct >= _WATCH_SHORT_THRESH:
        return "HOLD", f"Model expects {pred_pct:+.2f}% over {horizon}d."
    elif pred_pct >= _SELL_THRESH:
        return "WATCH_SHORT", f"Model expects {pred_pct:.2f}% over {horizon}d."
    else:
        return "SELL", f"Model expects {pred_pct:.2f}% over {horizon}d."


def _load_existing_dates(client) -> set:
    """Return the set of as_of dates already in ml_predictions."""
    try:
        df = client.query_df(
            "SELECT DISTINCT as_of FROM market_data.ml_predictions FINAL"
        )
        return set(pd.to_datetime(df["as_of"]).dt.date.tolist())
    except Exception:
        return set()


def _insert_prediction(client, row: dict, dry_run: bool = False) -> None:
    """Insert one prediction row into ml_predictions."""
    if dry_run:
        return
    client.insert(
        "market_data.ml_predictions",
        [[
            row["as_of"],
            row["horizon_days"],
            row["expected_return_pct"],
            row["confidence_low"],
            row["confidence_high"],
            row["regime_signal"],
            row["cv_r2_mean"],
            row["n_training_rows"],
            row["goldbees_close"],
            row["prob_up"],
            row["cv_auc_mean"],
        ]],
        column_names=[
            "as_of", "horizon_days", "expected_return_pct",
            "confidence_low", "confidence_high", "regime_signal",
            "cv_r2_mean", "n_training_rows", "goldbees_close",
            "prob_up", "cv_auc_mean",
        ],
    )


def _predict_at_date(
    as_of: date,
    df_feat_full: pd.DataFrame,
    horizon: int,
    n_splits: int,
) -> dict | None:
    """
    Fit the model on data up to `as_of` and return a prediction dict.
    Returns None if there is insufficient data for this date.
    """
    # Slice: keep only rows available on or before as_of, starting from 2013-01-01
    as_of_ts = pd.Timestamp(as_of)
    df_slice = df_feat_full[(df_feat_full["trade_date"] >= "2013-01-01") & (df_feat_full["trade_date"] <= as_of_ts)].copy()

    if len(df_slice) < _MIN_ROWS + horizon + 10:
        return None   # not enough history

    # Label forward returns on the slice — last `horizon` rows get NaN target
    # (they'd require prices past as_of, which we don't use here)
    df_labeled = label_forward_return(df_slice, horizon=horizon)

    try:
        (
            (m_clf, m_mean, m_low, m_high),
            _fi_df, scores, hit_ratios, df_clean, feature_cols,
            aucs, r2_scores,
        ) = fit_walk_forward(df_labeled, n_splits=n_splits, gap=_GAP)
    except ValueError:
        return None   # skipped — too few rows after feature engineering

    # Predict on the last row of the slice (= date as_of)
    coverage = df_slice[feature_cols].notna().sum(axis=1)
    df_pred_eligible = df_slice[coverage >= (len(feature_cols) // 2)]
    if df_pred_eligible.empty:
        return None

    latest_row = df_pred_eligible[feature_cols].iloc[[-1]]

    # Direction probability (primary)
    prob_up = float(m_clf.predict_proba(latest_row)[0, 1])

    # Calibrated expected return = direction × historical magnitude
    train_targets   = df_clean["target"].dropna().values
    mean_abs_logret = float(np.mean(np.abs(train_targets))) if len(train_targets) else 0.0
    pred_logret     = (2.0 * prob_up - 1.0) * mean_abs_logret
    pred_pct        = (np.exp(pred_logret) - 1) * 100

    # Quantile confidence bands
    low_logret  = float(m_low.predict(latest_row)[0])
    high_logret = float(m_high.predict(latest_row)[0])
    conf_low    = min((np.exp(low_logret)  - 1) * 100, pred_pct - 0.2)
    conf_high   = max((np.exp(high_logret) - 1) * 100, pred_pct + 0.2)

    regime, _ = _regime_from_pred(pred_pct, horizon)

    cv_skill_mean = float(np.mean(scores))   # AUC − 0.5
    cv_auc_mean   = float(np.mean(aucs))

    goldbees_close = float(df_slice["goldbees_close"].iloc[-1])

    return {
        "as_of":               as_of,
        "horizon_days":        horizon,
        "expected_return_pct": round(pred_pct, 3),
        "prob_up":             round(prob_up, 4),
        "confidence_low":      round(conf_low, 3),
        "confidence_high":     round(conf_high, 3),
        "regime_signal":       regime,
        "cv_r2_mean":          round(cv_skill_mean, 4),
        "cv_auc_mean":         round(cv_auc_mean, 4),
        "n_training_rows":     len(df_clean),
        "goldbees_close":      round(goldbees_close, 4),
    }


def run_backfill(
    months: int = 12,
    step: int = _HORIZON,
    horizon: int = _HORIZON,
    n_splits: int = _N_SPLITS,
    dry_run: bool = False,
    force: bool = False,
    verbose: bool = True,
) -> None:
    from src.db.pool import get_client
    client = get_client()

    # ── Ensure schema has prob_up + cv_auc_mean columns (idempotent) ──────────
    for col_ddl in [
        "ALTER TABLE market_data.ml_predictions ADD COLUMN IF NOT EXISTS prob_up Float64 DEFAULT 0.5",
        "ALTER TABLE market_data.ml_predictions ADD COLUMN IF NOT EXISTS cv_auc_mean Float64 DEFAULT 0.5",
    ]:
        try:
            client.command(col_ddl)
        except Exception:
            pass

    # ── Load existing dates to skip ───────────────────────────────────────────
    existing: set[date] = set() if force else _load_existing_dates(client)
    if existing and verbose:
        print(f"  Skipping {len(existing)} dates already in ml_predictions "
              f"(use --force to overwrite)")

    # ── Fetch master table + engineer features (ONCE) ─────────────────────────
    print("Loading master table from ClickHouse…", end=" ", flush=True)
    t0 = time.time()
    df_raw  = build_master_table(client)
    df_feat = engineer_features(df_raw)
    df_feat = df_feat.sort_values("trade_date").reset_index(drop=True)
    print(f"{len(df_feat)} rows in {time.time()-t0:.1f}s")

    # Close connection — will reopen once for the batch insert
    client.close()

    # ── Determine trading dates to predict ────────────────────────────────────
    all_dates   = pd.to_datetime(df_feat["trade_date"]).dt.date.tolist()
    cutoff_date = date.today() - timedelta(days=1)          # don't overwrite today
    start_date  = cutoff_date - timedelta(days=months * 31) # approximate

    candidate_dates = [
        d for d in all_dates
        if start_date <= d <= cutoff_date
    ]

    # Step: every `step` trading days (index-based, newest → oldest)
    stepped = candidate_dates[::-1][::step][::-1]

    # Remove dates already in DB
    to_predict = [d for d in stepped if d not in existing]

    if not to_predict:
        print("✓ Nothing to backfill — all dates already present.")
        return

    print(f"\nBackfilling {len(to_predict)} dates "
          f"({stepped[0]} → {stepped[-1]}, step={step}d)")
    if dry_run:
        print("  [DRY RUN — no writes to ClickHouse]")
    print()

    # ── Reconnect for inserts ─────────────────────────────────────────────────
    from src.db.pool import get_client
    client = get_client()

    saved = 0
    skipped = 0
    errors  = 0

    for i, as_of in enumerate(to_predict, 1):
        t_start = time.time()
        pct = i / len(to_predict) * 100

        # ETA
        elapsed_so_far = time.time() - t0
        eta_s = (elapsed_so_far / i) * (len(to_predict) - i) if i > 1 else 0
        eta_str = f"ETA ~{eta_s/60:.0f}m" if eta_s > 60 else f"ETA ~{eta_s:.0f}s"

        print(f"  [{i:3d}/{len(to_predict)}] {as_of}  ({pct:.0f}%  {eta_str})  ",
              end="", flush=True)

        try:
            pred = _predict_at_date(as_of, df_feat, horizon, n_splits)
        except Exception as e:
            print(f"ERROR: {e}")
            errors += 1
            continue

        if pred is None:
            print("skip (insufficient data)")
            skipped += 1
            continue

        elapsed = time.time() - t_start
        regime  = pred["regime_signal"]
        skill   = pred["cv_r2_mean"]
        prob    = pred["prob_up"]

        print(f"prob_up={prob:.3f}  skill={skill:+.4f}  "
              f"regime={regime:<12}  ({elapsed:.1f}s)")

        try:
            _insert_prediction(client, pred, dry_run=dry_run)
            saved += 1
        except Exception as e:
            print(f"    ✗ Insert failed: {e}")
            errors += 1

    client.close()

    total_time = time.time() - t0
    print(f"\n{'='*60}")
    print(f"  Done in {total_time/60:.1f}m")
    print(f"  Saved:   {saved}  |  Skipped: {skipped}  |  Errors: {errors}")
    if dry_run:
        print("  (dry run — no rows written)")
    else:
        print(f"  ml_predictions now has {saved + len(existing)} rows for GOLDBEES")
    print()
    print("  Next step: run the backtest with ML coverage")
    print("  .venv_new/bin/python3 tests/_backtest_adaptive_kelly.py --months 12")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill ml_predictions walk-forward")
    parser.add_argument(
        "--months", default=12, type=int,
        help="How many months of history to backfill (default: 12)",
    )
    parser.add_argument(
        "--step", default=_HORIZON, type=int,
        help=f"Step size in trading days between predictions (default: {_HORIZON} = horizon)",
    )
    parser.add_argument(
        "--horizon", default=_HORIZON, type=int,
        help=f"Forward return horizon in days (default: {_HORIZON})",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Compute predictions but do NOT write to ClickHouse",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Overwrite dates already present in ml_predictions",
    )
    parser.add_argument(
        "--n-splits", default=_N_SPLITS, type=int,
        help=f"Walk-forward CV folds (default: {_N_SPLITS}; reduce to 3 for speed)",
    )
    args = parser.parse_args()

    run_backfill(
        months   = args.months,
        step     = args.step,
        horizon  = args.horizon,
        n_splits = args.n_splits,
        dry_run  = args.dry_run,
        force    = args.force,
    )
