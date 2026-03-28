"""
src/ml/anomaly.py
─────────────────
Composite anomaly detection pipeline for daily OHLCV time series.

3-step workflow
───────────────
1. Robust Z-Score (MAD)
   Standard Z inflates σ during trends, masking shocks.
   MAD Z stays centred on the median and resists outlier inflation.
   Formula: Z_robust = 0.6745 × (x − median) / MAD

2. Random Forest Residual Z-Score
   Train RF on lagged prices + moving averages.  Residual = actual − predicted.
   Z_resid is the MAD Z-score of those residuals, isolating the *unexpected* component.

   Regime matrix:
     High Z_robust + Low  Z_resid  → 📈 Strong Trend (HODL)
     Low  Z_robust + High Z_resid  → ⚡ Flash Crash / Black Swan (EXIT)
     High Z_robust + High Z_resid  → 🔥 Volatile Breakout
     Low  Z_robust + Low  Z_resid  → ✅ Normal

3. Isolation Forest Confidence Multiplier
   IF score_samples normalised to [0 → 1] (1 = most anomalous).
   Final_Z = Z_robust × (1 + IF_confidence)
   Boosts only days suspicious to **both** algorithms, filtering noise.

Public API
──────────
    run_composite_anomaly(df, rf_lags, contamination, z_threshold)
        → (df_result, df_flagged, r2_train)

    Individual step functions are also exported for testing / extension:
        robust_zscore(s)
        build_features(df, rf_lags)
        fit_rf_residuals(df, rf_lags, train_frac)
        fit_isolation_forest(df, contamination)
        classify_regime(df)
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest, RandomForestRegressor
from sklearn.preprocessing import StandardScaler

__all__ = [
    "robust_zscore",
    "build_features",
    "fit_rf_residuals",
    "fit_isolation_forest",
    "classify_regime",
    "run_composite_anomaly",
]


# ── Step helpers ──────────────────────────────────────────────────────────────

def robust_zscore(s: pd.Series, window: int = 30) -> pd.Series:
    """
    MAD-based rolling robust Z-score.
    Formula: 0.6745 × (x − rolling_median) / rolling_MAD
    The constant 0.6745 makes the scale consistent with σ for Gaussian data.
    """
    rolling_med = s.rolling(window=window, min_periods=window // 2).median()
    rolling_mad = (s - rolling_med).abs().rolling(window=window, min_periods=window // 2).median()
    return 0.6745 * (s - rolling_med) / (rolling_mad + 1e-10)


def build_features(df: pd.DataFrame, rf_lags: int = 5) -> pd.DataFrame:
    """
    Add engineered features to a daily OHLCV DataFrame (sorted ascending by trade_date).

    Added columns: daily_return, range_pct, lag_1..lag_N, ma7, ma30, vol_lag1.
    Input must have: trade_date, open, high, low, close, volume.
    Returns a new DataFrame — does NOT mutate the input.
    """
    df = df.copy().sort_values("trade_date").reset_index(drop=True)
    df["daily_return"] = df["close"].pct_change() * 100
    df["range_pct"]    = (df["high"] - df["low"]) / df["close"] * 100
    for lag in range(1, rf_lags + 1):
        df[f"lag_{lag}"] = df["close"].shift(lag)
    df["ma7"]      = df["close"].rolling(7).mean()
    df["ma30"]     = df["close"].rolling(30).mean()
    df["vol_lag1"] = df["volume"].shift(1)
    return df


def fit_rf_residuals(
    df: pd.DataFrame,
    rf_lags: int = 5,
    train_frac: float = 0.8,
) -> tuple[pd.DataFrame, float]:
    """
    Train a Random Forest regressor on lagged OHLCV features,
    compute residual and its robust Z-score.

    Returns:
        df  — copy with new columns: rf_pred, residual, z_resid, z_resid_abs
        r2  — RF R² on the training partition (first train_frac of rows)
    """
    lag_cols = [f"lag_{i}" for i in range(1, rf_lags + 1)] + ["ma7", "ma30", "vol_lag1"]
    df = df.dropna(subset=lag_cols + ["close"]).copy().reset_index(drop=True)

    X      = df[lag_cols].values
    y      = df["close"].values
    n_train = int(len(df) * train_frac)

    rf = RandomForestRegressor(
        n_estimators=200, max_depth=6,
        random_state=42, n_jobs=-1,
    )
    rf.fit(X[:n_train], y[:n_train])

    df["rf_pred"]     = rf.predict(X)
    df["residual"]    = df["close"] - df["rf_pred"]
    df["z_resid"]     = robust_zscore(df["residual"])
    df["z_resid_abs"] = df["z_resid"].abs()
    r2 = float(rf.score(X[:n_train], y[:n_train]))
    return df, r2


def fit_isolation_forest(
    df: pd.DataFrame,
    contamination: float = 0.05,
) -> pd.DataFrame:
    """
    Fit Isolation Forest on [daily_return, range_pct, z_robust].
    Normalises score_samples to [0 → 1] (1 = most anomalous = highest IF confidence).

    Added columns: if_confidence, if_label (-1 = anomaly, 1 = normal).
    Returns a new DataFrame — does NOT mutate the input.
    """
    feat_cols = ["daily_return", "range_pct", "z_robust"]
    df = df.dropna(subset=feat_cols).copy().reset_index(drop=True)
    X  = StandardScaler().fit_transform(df[feat_cols].values)

    iso = IsolationForest(
        n_estimators=300, contamination=contamination,
        random_state=42, n_jobs=-1,
    )
    iso.fit(X)
    raw    = iso.score_samples(X)
    s_min, s_max = raw.min(), raw.max()

    # Higher raw score → more normal → lower confidence; invert so 1 = most anomalous
    df["if_confidence"] = 1.0 - (raw - s_min) / (s_max - s_min + 1e-10)
    df["if_label"]      = iso.predict(X)
    return df


def classify_regime(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute Final Z and add a human-readable regime label.

    Final_Z = Z_robust × (1 + IF_confidence)

    Regime thresholds are relative (median of each signal over the full window),
    making them robust to different asset price scales.

    Added columns: final_z, final_z_abs, regime.
    """
    df = df.copy()
    df["final_z"]     = df["z_robust"] * (1.0 + df["if_confidence"])
    df["final_z_abs"] = df["final_z"].abs()

    z_med   = df["z_robust"].abs().median()
    res_med = df["z_resid_abs"].median()

    def _label(row) -> str:
        hi_z   = abs(row["z_robust"])   > z_med
        hi_res = row["z_resid_abs"] > res_med
        if hi_z and not hi_res:   return "📈 Strong Trend (HODL)"
        if not hi_z and hi_res:   return "⚡ Flash Crash / Black Swan (EXIT)"
        if hi_z and hi_res:       return "🔥 Volatile Breakout"
        return "✅ Normal"

    df["regime"] = df.apply(_label, axis=1)
    return df


# ── Full pipeline ─────────────────────────────────────────────────────────────

def run_composite_anomaly(
    df: pd.DataFrame,
    rf_lags: int = 5,
    contamination: float = 0.05,
    z_threshold: float = 2.5,
    z_window: int = 30,
) -> tuple[pd.DataFrame, pd.DataFrame, float]:
    """
    End-to-end composite anomaly detection.

    Parameters
    ----------
    df            : Daily OHLCV DataFrame with columns:
                    trade_date, open, high, low, close, volume  (≥ 60 rows)
    rf_lags       : Number of lagged close prices used as RF features  (default 5)
    contamination : Fraction of data expected to be anomalous for IF  (default 0.05)
    z_threshold   : |Final Z| cutoff above which a day is flagged     (default 2.5)
    z_window      : Rolling window size for robust Z calculation      (default 30)

    Returns
    -------
    df_result   : Full DataFrame with all signal columns added
    df_flagged  : Subset where |final_z| > z_threshold
    r2_train    : RF R² on the training partition (first 80 % of rows)
    """
    df = build_features(df, rf_lags=rf_lags)

    # Step 1 — Robust Z on return + range
    df["z_return"] = robust_zscore(df["daily_return"].fillna(0), window=z_window)
    df["z_range"]  = robust_zscore(df["range_pct"], window=z_window)
    df["z_robust"] = (df["z_return"].abs() + df["z_range"]) / 2.0

    # Step 2 — RF residual Z
    df, r2 = fit_rf_residuals(df, rf_lags=rf_lags)

    # Step 3 — Isolation Forest confidence multiplier
    df = fit_isolation_forest(df, contamination=contamination)

    # Classify regimes + compute Final Z
    df = classify_regime(df)

    df_flagged = df[df["final_z_abs"] > z_threshold].copy()
    return df, df_flagged, r2
