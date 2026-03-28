"""
src/ml/trend_predictor.py
──────────────────────────
LightGBM 5-day forward return predictor for GOLDBEES.

Feature-to-Forward-Target strategy — the "Who Is Selling?" signals become
alpha factors with *soft* thresholds learned from data, not hard IF/THEN rules.

Alpha factors
─────────────
  f_cot_pct_oi     COT Leverage      mm_net / open_interest × 100
  f_spread_pct     Retail Spread     (price − nav) / nav × 100
  f_aum_mom_30d    AUM Momentum      30-day rolling % Δ of GLD total assets
  f_usdinr_vol14   Currency Stress   14-day USDINR log-return volatility × 100
  f_usdinr_60d     INR Trend         60-day USDINR % change
  f_goldbees_ret5  Price Momentum    5-day GOLDBEES return
  f_goldbees_ret20 Price Momentum    20-day GOLDBEES return
  f_ma_ratio       Mean-Reversion    close / 20-day MA
  f_spread_delta5  Spread Momentum   5-day change in retail spread

Target: (price[t + horizon] / price[t] − 1) × 100

Training: TimeSeriesSplit walk-forward — training window never overlaps test.
Model:    LGBMRegressor — handles NaN rows natively (sparse COT / AUM columns).

Public API
──────────
    run_trend_prediction(horizon, n_splits, verbose) → dict
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Any

import numpy as np
import pandas as pd

__all__ = [
    "build_master_table",
    "engineer_features",
    "label_forward_return",
    "fit_walk_forward",
    "run_trend_prediction",
]

log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
_HORIZON   = 5     # forward return horizon in trading days
_N_SPLITS  = 5     # TimeSeriesSplit folds
_GAP       = 5     # gap between train end and test start (prevents leakage)
_MIN_ROWS  = 120   # minimum clean rows required to train

# Regime thresholds (on predicted return %)
_BUY_THRESH         =  1.5
_WATCH_LONG_THRESH  =  0.5
_WATCH_SHORT_THRESH = -0.5
_SELL_THRESH        = -1.5

# ── ClickHouse SQL ─────────────────────────────────────────────────────────────
# CROSS JOIN + argMax forward-fills weekly COT onto daily dates (same validated
# pattern as the Global Anomaly Index chart — no experimental flags needed).
_MASTER_SQL = """
    SELECT
        p.trade_date  AS trade_date,
        p.close       AS goldbees_close,
        n.nav         AS goldbees_nav,
        f.close       AS usdinr,
        aum.aum_usd   AS gld_aum_usd,
        cot.mm_net    AS cot_mm_net,
        cot.oi        AS cot_oi
    FROM (
        SELECT trade_date, close
        FROM market_data.daily_prices FINAL
        WHERE symbol = 'GOLDBEES' AND category = 'etfs'
    ) p
    JOIN (
        SELECT nav_date AS trade_date, nav
        FROM market_data.mf_nav FINAL
        WHERE symbol = 'GOLDBEES'
    ) n ON p.trade_date = n.trade_date
    LEFT JOIN (
        SELECT trade_date, close
        FROM market_data.fx_rates FINAL
        WHERE symbol = 'USDINR'
    ) f ON p.trade_date = f.trade_date
    LEFT JOIN (
        SELECT trade_date, aum_usd
        FROM market_data.etf_aum FINAL
        WHERE symbol = 'GLD'
    ) aum ON p.trade_date = aum.trade_date
    LEFT JOIN (
        SELECT
            d.trade_date,
            argMax(c.mm_net,        c.report_date) AS mm_net,
            argMax(c.open_interest, c.report_date) AS oi
        FROM (
            SELECT DISTINCT trade_date
            FROM market_data.daily_prices FINAL
            WHERE symbol = 'GOLDBEES' AND category = 'etfs'
        ) d
        CROSS JOIN market_data.cot_gold c
        WHERE c.report_date <= d.trade_date
        GROUP BY d.trade_date
    ) cot ON p.trade_date = cot.trade_date
    ORDER BY p.trade_date ASC
"""


# ── Step 1: Data assembly ──────────────────────────────────────────────────────

def build_master_table(ch_client) -> pd.DataFrame:
    """
    Pull and join all signal sources from ClickHouse into one flat table.

    GLD AUM is sparse (daily snapshots; most rows are NaN) — forward-filled
    from the closest available value so LightGBM receives a usable signal.
    """
    df = ch_client.query_df(_MASTER_SQL)
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df = df.sort_values("trade_date").reset_index(drop=True)
    df["gld_aum_usd"] = df["gld_aum_usd"].replace(0, np.nan).ffill()
    log.info(
        "Master table: %d rows, %s → %s",
        len(df),
        df["trade_date"].min().date(),
        df["trade_date"].max().date(),
    )
    return df


# ── Step 2: Feature engineering ───────────────────────────────────────────────

def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform raw columns into alpha factors.

    All feature columns are prefixed with ``f_`` for automatic selection.
    Input must have: goldbees_close, goldbees_nav, usdinr, gld_aum_usd,
                     cot_mm_net, cot_oi.
    """
    df = df.copy().sort_values("trade_date").reset_index(drop=True)

    # 1. COT leverage — the speculator over-positioning signal
    df["f_cot_pct_oi"] = (
        df["cot_mm_net"] / df["cot_oi"].replace(0, np.nan) * 100
    )

    # 2. Retail spread — GOLDBEES premium/discount to AMFI NAV
    df["f_spread_pct"] = (
        (df["goldbees_close"] - df["goldbees_nav"])
        / df["goldbees_nav"].replace(0, np.nan) * 100
    )

    # 3. AUM momentum — 30-day % change in GLD total assets (institutional flow)
    df["f_aum_mom_30d"] = df["gld_aum_usd"].pct_change(30, fill_method=None) * 100

    # 4. Currency stress — 14-day USDINR log-return volatility × 100
    # Replace 0/NaN before log to avoid divide-by-zero RuntimeWarning
    usdinr_safe  = df["usdinr"].replace(0, np.nan)
    usdinr_logret = np.log(usdinr_safe / usdinr_safe.shift(1))
    df["f_usdinr_vol14"] = usdinr_logret.rolling(14).std() * 100

    # 5. INR trend — 60-day USDINR % change (macro regime)
    df["f_usdinr_60d"] = df["usdinr"].pct_change(60) * 100

    # 6. GOLDBEES near/medium momentum
    df["f_goldbees_ret5"]  = df["goldbees_close"].pct_change(5)  * 100
    df["f_goldbees_ret20"] = df["goldbees_close"].pct_change(20) * 100

    # 7. Mean-reversion: close / 20-day MA (> 1 = extended, < 1 = oversold)
    df["f_ma_ratio"] = (
        df["goldbees_close"] / df["goldbees_close"].rolling(20).mean()
    )

    # 8. Spread momentum: 5-day change in retail discount (accelerating panic?)
    df["f_spread_delta5"] = df["f_spread_pct"].diff(5)

    return df


# ── Step 3: Target labeling ───────────────────────────────────────────────────

def label_forward_return(df: pd.DataFrame, horizon: int = _HORIZON) -> pd.DataFrame:
    """
    Add the forward return target column.

        target = (close[t + horizon] / close[t] − 1) × 100

    The last `horizon` rows will have NaN targets and are excluded from training.
    They are kept so the latest row can still be used for live prediction.
    """
    df = df.copy()
    df["target"] = (
        df["goldbees_close"].shift(-horizon) / df["goldbees_close"] - 1
    ) * 100
    return df


# ── Step 4: Walk-forward training ─────────────────────────────────────────────

def fit_walk_forward(
    df: pd.DataFrame,
    n_splits: int = _N_SPLITS,
    gap: int = _GAP,
) -> tuple[Any, pd.DataFrame, list[float], pd.DataFrame, list[str]]:
    """
    Train LGBMRegressor with TimeSeriesSplit walk-forward validation.

    Parameters
    ----------
    df       : DataFrame with all f_* columns and 'target'
    n_splits : number of CV folds
    gap      : rows between train end and test start (prevents look-ahead)

    Returns
    -------
    model        : LGBMRegressor fit on the last fold's full train set
    fi_df        : feature importances averaged over last 3 folds
    cv_r2_scores : list of out-of-sample R² per fold
    df_clean     : training-eligible rows
    feature_cols : list of feature names used
    """
    try:
        import lightgbm as lgb
    except ImportError as exc:
        raise ImportError(
            "lightgbm is not installed. Run: .venv/bin/pip install lightgbm"
        ) from exc

    from sklearn.model_selection import TimeSeriesSplit

    feature_cols = sorted(c for c in df.columns if c.startswith("f_"))

    # Require non-NaN target; LightGBM handles missing feature values natively.
    # Keep rows where at least half the features are present so sparse columns
    # like f_aum_mom_30d (only a few AUM snapshots in etf_aum) don't eliminate
    # every row during dropna.
    df_clean = df.dropna(subset=["target"]).copy()
    min_features_required = max(1, len(feature_cols) // 2)
    feature_coverage = df_clean[feature_cols].notna().sum(axis=1)
    df_clean = df_clean[feature_coverage >= min_features_required].reset_index(drop=True)

    if len(df_clean) < _MIN_ROWS:
        raise ValueError(
            f"Only {len(df_clean)} clean rows available — need ≥ {_MIN_ROWS}. "
            "Run: mosaic import --category etfs mf cot"
        )

    X = df_clean[feature_cols]   # keep as DataFrame so LightGBM retains feature names
    y = df_clean["target"].values

    tscv = TimeSeriesSplit(n_splits=n_splits, gap=gap)
    models: list[Any] = []
    scores: list[float] = []

    for train_idx, test_idx in tscv.split(X):
        m = lgb.LGBMRegressor(
            n_estimators      = 300,
            learning_rate     = 0.04,
            max_depth         = 4,
            num_leaves        = 15,
            subsample         = 0.8,
            colsample_bytree  = 0.8,
            min_child_samples = 10,
            reg_alpha         = 0.1,
            reg_lambda        = 0.1,
            random_state      = 42,
            verbose           = -1,
        )
        m.fit(X.iloc[train_idx], y[train_idx])
        scores.append(float(m.score(X.iloc[test_idx], y[test_idx])))
        models.append(m)

    # Average importances over the last k folds for stability
    k = min(3, len(models))
    avg_imp = np.mean(
        [m.feature_importances_ for m in models[-k:]], axis=0
    ).tolist()
    fi_df = (
        pd.DataFrame({"feature": feature_cols, "importance": avg_imp})
        .sort_values("importance", ascending=False)
        .reset_index(drop=True)
    )

    log.info(
        "Walk-forward: %d folds, R² = %s, mean = %.4f",
        n_splits,
        [f"{s:.4f}" for s in scores],
        np.mean(scores),
    )
    return models[-1], fi_df, scores, df_clean, feature_cols


# ── Step 5: Public API ────────────────────────────────────────────────────────

def run_trend_prediction(
    horizon: int = _HORIZON,
    n_splits: int = _N_SPLITS,
    verbose: bool = True,
    ch_host: str = "localhost",
    ch_port: int = 8123,
    ch_database: str = "market_data",
    ch_user: str = "default",
    ch_password: str = "",
) -> dict[str, Any]:
    """
    End-to-end LightGBM trend predictor.

    Connects to ClickHouse, assembles the master table, engineers features,
    trains with walk-forward CV, and predicts the expected return for the
    *next* `horizon` trading days from today.

    Returns
    -------
    dict with keys:
      expected_return_pct  float   model's predicted forward return %
      confidence_low/high  float   heuristic ±band based on CV R² spread
      regime_signal        str     BUY | WATCH_LONG | HOLD | WATCH_SHORT | SELL
      regime_rationale     str     plain-English explanation
      feature_importances  DataFrame  (feature, importance) sorted desc
      cv_r2_scores         list[float]
      cv_r2_mean           float
      n_training_rows      int
      as_of                date
      horizon_days         int
    """
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=ch_host, port=ch_port, database=ch_database,
        username=ch_user, password=ch_password,
    )
    try:
        df_raw = build_master_table(client)
    finally:
        client.close()

    df_feat    = engineer_features(df_raw)
    df_labeled = label_forward_return(df_feat, horizon=horizon)

    model, fi_df, scores, df_clean, feature_cols = fit_walk_forward(
        df_labeled, n_splits=n_splits, gap=_GAP
    )

    # Predict on the latest row that has sufficient feature coverage.
    # Use the same threshold as training (>=50% features non-NaN).
    # Pass as DataFrame so LightGBM keeps feature names and handles NaN.
    df_feat_recent = df_feat[feature_cols].copy()
    coverage_pred  = df_feat_recent.notna().sum(axis=1)
    df_pred_eligible = df_feat_recent[coverage_pred >= (len(feature_cols) // 2)]
    if df_pred_eligible.empty:
        raise ValueError(
            "No recent rows with sufficient feature coverage for prediction. "
            "Run: mosaic import --category etfs mf cot"
        )
    latest_row = df_pred_eligible.iloc[[-1]]
    pred       = float(model.predict(latest_row)[0])

    # Confidence band widens when CV scores are inconsistent
    cv_std    = float(np.std(scores))
    band_half = max(abs(pred) * (1.0 + cv_std), 0.3)

    # Regime classification
    if pred >= _BUY_THRESH:
        regime    = "BUY"
        rationale = (
            f"Model expects +{pred:.2f}% over {horizon}d. "
            "All signals pointing constructive — consider adding on dips."
        )
    elif pred >= _WATCH_LONG_THRESH:
        regime    = "WATCH_LONG"
        rationale = (
            f"Model expects +{pred:.2f}% over {horizon}d. "
            "Mild upside expected — hold current position."
        )
    elif pred >= _WATCH_SHORT_THRESH:
        regime    = "HOLD"
        rationale = (
            f"Model expects {pred:+.2f}% over {horizon}d. "
            "No strong edge in either direction."
        )
    elif pred >= _SELL_THRESH:
        regime    = "WATCH_SHORT"
        rationale = (
            f"Model expects {pred:.2f}% over {horizon}d. "
            "Mild downside risk — reduce aggressive long exposure."
        )
    else:
        regime    = "SELL"
        rationale = (
            f"Model expects {pred:.2f}% over {horizon}d. "
            "Strong negative signal — consider reducing GOLDBEES holdings."
        )

    result: dict[str, Any] = {
        "expected_return_pct": round(pred, 3),
        "confidence_low":      round(pred - band_half, 3),
        "confidence_high":     round(pred + band_half, 3),
        "regime_signal":       regime,
        "regime_rationale":    rationale,
        "feature_importances": fi_df,
        "cv_r2_scores":        [round(s, 4) for s in scores],
        "cv_r2_mean":          round(float(np.mean(scores)), 4),
        "n_training_rows":     len(df_clean),
        "horizon_days":        horizon,
        "as_of":               date.today(),
    }
    # ── Persist prediction to ClickHouse + JSONL fallback ────────────────────
    import json, pathlib

    _pred_row = {
        "as_of":               result["as_of"],
        "horizon_days":        result["horizon_days"],
        "expected_return_pct": result["expected_return_pct"],
        "confidence_low":      result["confidence_low"],
        "confidence_high":     result["confidence_high"],
        "regime_signal":       result["regime_signal"],
        "cv_r2_mean":          result["cv_r2_mean"],
        "n_training_rows":     result["n_training_rows"],
        "goldbees_close":      round(float(df_feat["goldbees_close"].iloc[-1]), 4),
    }

    # ClickHouse — create table if missing, then upsert
    try:
        import clickhouse_connect as _cc
        _ch = _cc.get_client(
            host=ch_host, port=ch_port, database=ch_database,
            username=ch_user, password=ch_password,
        )
        _ch.command("""
            CREATE TABLE IF NOT EXISTS market_data.ml_predictions (
                as_of                Date,
                horizon_days         UInt8,
                expected_return_pct  Float64,
                confidence_low       Float64,
                confidence_high      Float64,
                regime_signal        String,
                cv_r2_mean           Float64,
                n_training_rows      UInt32,
                goldbees_close       Float64,
                created_at           DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(created_at)
            ORDER BY (as_of, horizon_days)
        """)
        _ch.insert(
            "market_data.ml_predictions",
            [[
                _pred_row["as_of"], _pred_row["horizon_days"],
                _pred_row["expected_return_pct"], _pred_row["confidence_low"],
                _pred_row["confidence_high"], _pred_row["regime_signal"],
                _pred_row["cv_r2_mean"], _pred_row["n_training_rows"],
                _pred_row["goldbees_close"],
            ]],
            column_names=[
                "as_of", "horizon_days", "expected_return_pct",
                "confidence_low", "confidence_high", "regime_signal",
                "cv_r2_mean", "n_training_rows", "goldbees_close",
            ],
        )
        _ch.close()
        log.info("Prediction logged to market_data.ml_predictions")
    except Exception as _e:
        log.warning("Could not write prediction to ClickHouse: %s", _e)

    # JSONL fallback — git-trackable, one line per (as_of, horizon_days)
    _log_path = pathlib.Path(__file__).parents[2] / "predictions_log.jsonl"
    _existing: set = set()
    if _log_path.exists():
        for _line in _log_path.read_text().splitlines():
            try:
                _e2 = json.loads(_line)
                _existing.add((_e2.get("as_of"), _e2.get("horizon_days")))
            except json.JSONDecodeError:
                pass
    _entry = {k: str(v) if isinstance(v, date) else v for k, v in _pred_row.items()}
    if (_entry["as_of"], _entry["horizon_days"]) not in _existing:
        with _log_path.open("a") as _f:
            _f.write(json.dumps(_entry) + "\n")
    if verbose:
        log.info(
            "Prediction: %+.3f%% in %dd | regime=%s | CV R²=%.4f",
            pred, horizon, regime, result["cv_r2_mean"],
        )
    return result


# ── CLI smoke-test ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    import os
    import logging as _logging

    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
    _logging.basicConfig(level=_logging.INFO, format="%(levelname)s %(message)s")

    out = run_trend_prediction()
    print(f"\n{'='*62}")
    print(f"  LGBM TREND PREDICTOR — {out['as_of']}")
    print(f"{'='*62}")
    print(f"  Expected {out['horizon_days']}-day return : {out['expected_return_pct']:+.3f}%")
    print(f"  Confidence band        : [{out['confidence_low']:+.3f}%, {out['confidence_high']:+.3f}%]")
    print(f"  Regime signal          : {out['regime_signal']}")
    print(f"\n  {out['regime_rationale']}")
    print(f"\n  CV R² per fold : {out['cv_r2_scores']}")
    print(f"  CV R² mean     : {out['cv_r2_mean']}")
    print(f"  Training rows  : {out['n_training_rows']}")
    print(f"\n  Feature importances (top 5):")
    max_imp = out["feature_importances"]["importance"].max()
    for _, row in out["feature_importances"].head(5).iterrows():
        bar = "█" * max(1, int(row["importance"] / max_imp * 20))
        print(f"    {row['feature']:25s}  {bar}  {row['importance']:.1f}")
    print()
