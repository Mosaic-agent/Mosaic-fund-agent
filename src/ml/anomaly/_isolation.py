"""Isolation Forest confidence multiplier for the anomaly pipeline."""
from __future__ import annotations

import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

# Module-level cache: keyed by (n_rows, contamination, feat_cols_tuple)
# Avoids refitting IsolationForest when the same data + params are reused
# within a session (e.g. repeated signal aggregator runs before new data arrives).
_IF_CACHE: dict = {}


def fit_isolation_forest(
    df: pd.DataFrame,
    contamination: float = 0.05,
) -> pd.DataFrame:
    """
    Fit Isolation Forest on price-based + cross-asset features.

    Core features (always used):
        daily_return, range_pct, z_robust, z_volume

    Cross-asset features (used when available — joined upstream):
        usdinr_logret  : USD/INR daily log-return (dollar stress)
        usdinr_vol14   : 14-day USDINR annualised vol (stress regime)
        cot_pct_oi     : COT MM net / open interest × 100 (speculator crowding)

    Added columns: if_confidence [0→1], if_label (-1=anomaly, 1=normal).
    Returns a new DataFrame — does NOT mutate the input.
    """
    core_cols  = ["daily_return", "range_pct", "z_robust", "z_volume"]
    extra_cols = [c for c in ["usdinr_logret", "usdinr_vol14", "cot_pct_oi"]
                  if c in df.columns and df[c].notna().sum() > 30]
    feat_cols  = core_cols + extra_cols

    df = df.dropna(subset=core_cols).copy().reset_index(drop=True)
    # Fill any missing cross-asset columns with 0 (neutral)
    for c in extra_cols:
        df[c] = df[c].fillna(0)

    X = StandardScaler().fit_transform(df[feat_cols].values)

    cache_key = (len(df), contamination, tuple(feat_cols))
    if cache_key not in _IF_CACHE:
        iso = IsolationForest(
            n_estimators=300, contamination=contamination,
            random_state=42, n_jobs=-1,
        )
        iso.fit(X)
        _IF_CACHE[cache_key] = iso
    else:
        iso = _IF_CACHE[cache_key]

    raw          = iso.score_samples(X)
    s_min, s_max = raw.min(), raw.max()

    # Invert: 1 = most anomalous
    df["if_confidence"] = 1.0 - (raw - s_min) / (s_max - s_min + 1e-10)
    df["if_label"]      = iso.predict(X)
    return df
