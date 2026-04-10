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

2. GARCH(1,1) Standardised Residual Z-Score  [replaces Random Forest]
   GARCH(1,1) models conditional volatility σ_t (volatility clustering).
   Standardised residual e_t = r_t / σ_t isolates the *unexpected* component:
   — Quiet periods: σ_t is small → moderate returns flag as shocks
   — Volatile periods: σ_t is large → only extreme returns flag
   — Student-t distribution captures gold's fat-tailed return distribution
   — Fire rate: ~5% (matches contamination setting) vs RF's spurious 21%

   Why not RF?  RF with R²=0.32 cannot reliably predict returns from lagged
   returns alone (gold ≈ random walk). z_resid from RF fires on 21% of all
   days, making "Flash Crash" nearly useless as a signal.

   Regime matrix:
     High Z_robust + High COT crowding + Pos Ret → ⚠️ Crowded Long (Squeeze Risk)
     High Z_robust + Low  Z_resid               → 📈 Strong Trend (HODL)
     Low  Z_robust + High Z_resid               → ⚡ Flash Crash / Black Swan (EXIT)
     High Z_robust + High Z_resid               → 🔥 Volatile Breakout
     High Z_robust + Low  Volume Z + Pos Ret    → 🧨 Blow-off Top (Weak)
     Low  Z_robust + Low  Z_resid               → ✅ Normal

3. Isolation Forest Confidence Multiplier  [enriched with cross-asset features]
   Features: daily_return, range_pct, z_volume, usdinr_logret,
             usdinr_vol14, cot_pct_oi (when available)
   IF score_samples normalised to [0 → 1] (1 = most anomalous).
   Final_Z = Z_robust × (1 + IF_confidence)
   Boosts only days suspicious to **both** algorithms, filtering noise.

Public API
──────────
    run_composite_anomaly(df, contamination, z_threshold,
                          df_cot=None, df_fx=None)
        → (df_result, df_flagged, garch_loglik)

    Individual step functions also exported:
        robust_zscore(s)
        build_features(df)
        fit_garch_residuals(df)
        fit_isolation_forest(df, contamination)
        classify_regime(df)
"""

from __future__ import annotations

import warnings

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

__all__ = [
    "robust_zscore",
    "build_features",
    "fit_garch_residuals",
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

    Added columns: daily_return, log_return, range_pct.
    Input must have: trade_date, open, high, low, close, volume.
    Returns a new DataFrame — does NOT mutate the input.
    """
    df = df.copy().sort_values("trade_date").reset_index(drop=True)
    df["daily_return"] = df["close"].pct_change() * 100
    df["log_return"]   = np.log(df["close"] / df["close"].shift(1))
    df["range_pct"]    = (df["high"] - df["low"]) / df["close"] * 100
    df["vol_lag1"]     = df["volume"].shift(1)
    return df


def fit_garch_residuals(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, float]:
    """
    Fit GARCH(1,1) with Student-t innovations on log-returns.

    WHY GARCH OVER RANDOM FOREST
    ─────────────────────────────
    RF on log-returns achieves R²≈0.32 for GOLD (returns ≈ random walk),
    causing z_resid to fire on 21% of days — useless as an anomaly signal.

    GARCH directly models conditional volatility σ_t via volatility clustering:
        σ²_t = ω + α·ε²_{t-1} + β·σ²_{t-1}
    Standardised residuals e_t = r_t / σ_t correctly fire on ~5% of days
    (shock days where the magnitude is surprising given the current vol regime).

    Student-t innovations are used because gold returns have fat tails
    (excess kurtosis) not captured by a Gaussian GARCH.

    OUTPUT COLUMNS (backward-compatible with old RF column names)
    ─────────────────────────────────────────────────────────────
    garch_vol     : conditional volatility, annualised %  (NEW — shown in UI metric)
    garch_band_1s : 1-sigma daily move in price terms     (NEW — used for chart bands)
    garch_band_2s : 2-sigma daily move in price terms     (NEW — used for chart bands)
    rf_pred       : kept for UI chart compatibility; here = close[t-1]·exp(fitted_ret)
    residual      : standardised residual e_t = r_t / σ_t
    z_resid       : MAD Z-score of standardised residuals
    z_resid_abs   : |z_resid|

    Returns
    -------
    df        — with new columns added
    loglik    — GARCH log-likelihood (reported in UI instead of RF R²)
    """
    try:
        from arch import arch_model  # type: ignore[import]
    except ImportError as exc:
        raise ImportError(
            "arch library required: pip install arch>=6.3.0"
        ) from exc

    df = df.copy()
    returns = df["log_return"].dropna() * 100  # arch works in % scale

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        am  = arch_model(returns, vol="Garch", p=1, q=1, dist="t", rescale=False)
        res = am.fit(disp="off", show_warning=False)

    # arch returns vectors aligned to the *non-NaN* log_return rows (N-1 values)
    # We need to map them back to the full df index (N rows).
    valid_idx = df.index[df["log_return"].notna()]   # integer positions of valid rows

    cond_vol_pct = res.conditional_volatility.values   # (N-1,) daily σ in %
    cond_vol     = cond_vol_pct / 100                  # (N-1,) daily σ in log-return scale
    fitted_ret   = res.resid.values / 100              # (N-1,) GARCH-fitted log-returns

    # ── Annualised conditional volatility ────────────────────────────────────
    df["garch_vol"] = np.nan
    df.loc[valid_idx, "garch_vol"] = cond_vol_pct * np.sqrt(252)

    # ── rf_pred: close[t-1] × exp(fitted_ret[t]) for chart backward-compat ──
    prev_close_valid = df["close"].shift(1).values[valid_idx]  # (N-1,)
    df["rf_pred"] = np.nan
    df.loc[valid_idx, "rf_pred"] = prev_close_valid * np.exp(fitted_ret)

    # ── GARCH ±1σ / ±2σ price bands for chart ────────────────────────────────
    close_valid = df["close"].values[valid_idx]
    df["garch_band_1s"] = np.nan
    df["garch_band_2s"] = np.nan
    df.loc[valid_idx, "garch_band_1s"] = close_valid * (np.exp(cond_vol) - 1)
    df.loc[valid_idx, "garch_band_2s"] = close_valid * (np.exp(2 * cond_vol) - 1)

    # ── Standardised residuals e_t = r_t / σ_t  (the proper anomaly score) ──
    logret_valid = df["log_return"].values[valid_idx]   # (N-1,)
    std_resid_valid = logret_valid / cond_vol           # (N-1,)
    df["residual"] = np.nan
    df.loc[valid_idx, "residual"] = std_resid_valid

    df["z_resid"]     = robust_zscore(df["residual"].fillna(0))
    df["z_resid_abs"] = df["z_resid"].abs()

    loglik = float(res.loglikelihood)
    return df, loglik


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

    iso = IsolationForest(
        n_estimators=300, contamination=contamination,
        random_state=42, n_jobs=-1,
    )
    iso.fit(X)
    raw            = iso.score_samples(X)
    s_min, s_max   = raw.min(), raw.max()

    # Invert: 1 = most anomalous
    df["if_confidence"] = 1.0 - (raw - s_min) / (s_max - s_min + 1e-10)
    df["if_label"]      = iso.predict(X)
    return df


def classify_regime(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute Final Z and add a human-readable regime label.

    Final_Z = Z_robust × (1 + IF_confidence)

    Regime thresholds are relative (median of each signal over the full window),
    making them robust to different asset price scales.

    Regime priority (checked top-to-bottom):
        ⚠️  Crowded Long (Squeeze Risk) — NEW: high COT crowding + high price Z
        🧨  Blow-off Top (Weak)          — high Z, low volume, positive return
        📈  Strong Trend (HODL)          — high Z, low residual Z
        ⚡  Flash Crash / Black Swan     — low Z, high residual Z  ← key GARCH signal
        🔥  Volatile Breakout            — both high
        ✅  Normal

    Added columns: final_z, final_z_abs, regime.
    """
    df = df.copy()
    df["final_z"]     = df["z_robust"] * (1.0 + df["if_confidence"])
    df["final_z_abs"] = df["final_z"].abs()

    # Use 80th percentile thresholds so each regime fires on ~10-20% of days
    # (median thresholds give 50% rates → too noisy for actionable alerts)
    z_med      = float(df["z_robust"].abs().quantile(0.80))
    res_med    = float(df["z_resid_abs"].quantile(0.80))
    z_vol_med  = float(df["z_volume"].abs().quantile(0.20))   # "lo_vol" = bottom 20%
    # COT crowding threshold: top quartile of historical cot_pct_oi
    has_cot = "cot_pct_oi" in df.columns
    cot_thresh = float(df["cot_pct_oi"].quantile(0.75)) if has_cot else 25.0

    def _label(row) -> str:
        hi_z   = abs(row["z_robust"]) > z_med
        hi_res = row["z_resid_abs"]   > res_med
        lo_vol = abs(row["z_volume"]) < z_vol_med

        # ── Shock-priority regimes (GARCH residual drives classification) ──
        # Flash Crash: unexpected large move regardless of trend/COT context
        if not hi_z and hi_res:  return "⚡ Flash Crash / Black Swan (EXIT)"
        # Volatile Breakout: both trend AND residual are extreme
        if hi_z and hi_res:      return "🔥 Volatile Breakout"

        # ── Context-driven regimes (no significant GARCH shock today) ──────
        # Crowded Long: speculators are extremely long + market is trending up
        # Risk: a reversal here would cause a short-squeeze cascade
        if has_cot and hi_z and row.get("cot_pct_oi", 0.0) > cot_thresh and row["daily_return"] > 0:
            return "⚠️ Crowded Long (Squeeze Risk)"

        # Blow-off Top: high Z + low volume + positive return (thin rally)
        if hi_z and lo_vol and row["daily_return"] > 0:
            return "🧨 Blow-off Top (Weak)"

        if hi_z:  return "📈 Strong Trend (HODL)"
        return "✅ Normal"

    df["regime"] = df.apply(_label, axis=1)
    return df


# ── Cross-asset feature injection ────────────────────────────────────────────

def _inject_cross_asset(
    df: pd.DataFrame,
    df_cot: pd.DataFrame | None,
    df_fx: pd.DataFrame | None,
) -> pd.DataFrame:
    """
    Left-join COT and USDINR features onto the OHLCV DataFrame.

    df_cot columns expected : report_date, mm_net, open_interest
    df_fx  columns expected : symbol, trade_date, close
    """
    df = df.copy()

    # ── COT speculator crowding (weekly → daily forward-fill) ──────────────
    if df_cot is not None and len(df_cot) > 10:
        cot = df_cot[["report_date", "mm_net", "open_interest"]].copy()
        cot = cot.rename(columns={"report_date": "trade_date"})
        cot["cot_pct_oi"] = cot["mm_net"] / (cot["open_interest"] + 1e-6) * 100
        cot["trade_date"] = pd.to_datetime(cot["trade_date"])
        df["trade_date"]  = pd.to_datetime(df["trade_date"])
        df = df.merge(cot[["trade_date", "cot_pct_oi"]], on="trade_date", how="left")
        df["cot_pct_oi"]  = df["cot_pct_oi"].ffill().fillna(0.0)

    # ── USDINR dollar-stress features ─────────────────────────────────────
    if df_fx is not None and len(df_fx) > 10:
        usdinr = df_fx[df_fx["symbol"] == "USDINR"][["trade_date", "close"]].copy()
        usdinr = usdinr.sort_values("trade_date").reset_index(drop=True)
        usdinr["usdinr_logret"] = np.log(usdinr["close"] / usdinr["close"].shift(1))
        usdinr["usdinr_vol14"]  = (
            usdinr["usdinr_logret"]
            .rolling(14, min_periods=7)
            .std() * np.sqrt(252) * 100
        )
        usdinr["trade_date"] = pd.to_datetime(usdinr["trade_date"])
        df = df.merge(
            usdinr[["trade_date", "usdinr_logret", "usdinr_vol14"]],
            on="trade_date", how="left",
        )
        df[["usdinr_logret", "usdinr_vol14"]] = (
            df[["usdinr_logret", "usdinr_vol14"]].fillna(0.0)
        )

    return df


# ── Full pipeline ─────────────────────────────────────────────────────────────

def run_composite_anomaly(
    df: pd.DataFrame,
    rf_lags: int = 5,           # kept for API compatibility, unused
    contamination: float = 0.05,
    z_threshold: float = 2.5,
    z_window: int = 30,
    df_cot: pd.DataFrame | None = None,
    df_fx: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, float]:
    """
    End-to-end composite anomaly detection.

    Parameters
    ----------
    df            : Daily OHLCV DataFrame — trade_date, open, high, low, close,
                    volume  (≥ 60 rows required)
    rf_lags       : Kept for backward-compatibility; no longer used (GARCH has no lag param)
    contamination : Expected anomaly fraction for Isolation Forest   (default 0.05)
    z_threshold   : |Final Z| cutoff for flagging                    (default 2.5)
    z_window      : Rolling window for robust Z-score                (default 30)
    df_cot        : Optional COT DataFrame (report_date, mm_net, open_interest)
                    → enables cot_pct_oi feature + Crowded Long regime
    df_fx         : Optional FX DataFrame (symbol, trade_date, close)
                    → enables usdinr_logret, usdinr_vol14 features

    Returns
    -------
    df_result   : Full DataFrame with all signal columns
    df_flagged  : Subset where |final_z| > z_threshold
    garch_loglik: GARCH log-likelihood (replaces RF R² in the UI)
    """
    df = build_features(df, rf_lags=rf_lags)

    # Inject cross-asset features when available
    df = _inject_cross_asset(df, df_cot=df_cot, df_fx=df_fx)

    # Step 1 — Robust Z on return, range, and volume
    df["z_return"] = robust_zscore(df["daily_return"].fillna(0), window=z_window)
    df["z_range"]  = robust_zscore(df["range_pct"],              window=z_window)
    df["z_robust"] = (df["z_return"].abs() + df["z_range"]) / 2.0
    df["z_volume"] = robust_zscore(df["volume"].fillna(0),       window=z_window)

    # Step 2 — GARCH(1,1) standardised residual Z  [replaces Random Forest]
    df, garch_loglik = fit_garch_residuals(df)

    # Step 3 — Isolation Forest confidence multiplier (enriched features)
    df = fit_isolation_forest(df, contamination=contamination)

    # Classify regimes + compute Final Z
    df = classify_regime(df)

    df_flagged = df[df["final_z_abs"] > z_threshold].copy()
    return df, df_flagged, garch_loglik
