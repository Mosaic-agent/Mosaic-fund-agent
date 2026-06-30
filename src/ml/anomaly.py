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

4. PELT Change-Point Detection (ruptures, rbf cost)  [regime-shift confirmation]
   GARCH + IF detect *point shocks* — single surprising days. PELT detects
   *structural breaks*: the boundary where the return distribution shifts to a
   new variance regime (e.g. calm → turbulent). These are different objects:
   a one-day spike is not a regime change, and a regime change need not have a
   single dramatic day.

   We run PELT (`model="rbf"`) on standardised log-returns. The rbf kernel cost
   is sensitive to changes in the *whole distribution* (mean + variance), so it
   pinpoints volatility-regime boundaries. Each detected breakpoint marks a date
   where the market's behaviour structurally changed.

   Role = CONFIRMATION BOOSTER (does not replace the Final-Z gate):
   — `is_changepoint`  : True on a detected breakpoint date.
   — `cp_confirmed`    : True when a date lies within ±proximity_days of a break.
   — A point anomaly that *coincides* with a structural break is corroborated by
     two independent views → its Final Z is boosted ×cp_boost and its regime is
     relabelled "🔀 Regime Shift (Change Point)". The Final-Z threshold still
     gates which dates are flagged; CPD only sharpens confidence and labelling.

   Graceful degradation: if `ruptures` is not installed or there are too few
   rows, both columns are set False and the pipeline behaves exactly as before.

Public API
──────────
    run_composite_anomaly(df, contamination, z_threshold,
                          df_cot=None, df_fx=None,
                          cp_penalty=None, cp_proximity_days=3, cp_boost=1.15)
        → (df_result, df_flagged, garch_loglik)

    Individual step functions also exported:
        robust_zscore(s)
        build_features(df)
        fit_garch_residuals(df)
        fit_isolation_forest(df, contamination)
        fit_change_points(df, ...)
        classify_regime(df)
"""

from __future__ import annotations

import logging
import warnings
from abc import ABC, abstractmethod
from typing import Any

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

log = logging.getLogger(__name__)

__all__ = [
    "robust_zscore",
    "build_features",
    "fit_garch_residuals",
    "fit_isolation_forest",
    "fit_change_points",
    "classify_regime",
    "run_composite_anomaly",
    "retrieve_similar_anomalies",
    "AnomalyDetectorStrategy",
    "RobustZScoreStrategy",
    "GarchResidualStrategy",
    "IsolationForestStrategy",
    "PeltChangePointStrategy",
    "CompositeAnomalyPipeline",
]


def retrieve_similar_anomalies(
    symbol: str,
    regime: str,
    trade_date: Any,
    k: int = 5,
    category: str = "",
    same_asset_only: bool = False,
) -> list[dict]:
    """Retrieve past anomaly events semantically similar to the given regime+context.

    Delegates to src.db.anomaly_vector.retrieve_similar_anomalies.
    Returns [] gracefully if Qdrant is unavailable or collection is empty.
    """
    try:
        from src.db.anomaly_vector import retrieve_similar_anomalies as _retrieve
        return _retrieve(
            symbol=symbol,
            regime=regime,
            trade_date=trade_date,
            k=k,
            category=category,
            same_asset_only=same_asset_only,
        )
    except Exception as e:
        log.debug("retrieve_similar_anomalies unavailable: %s", e)
        return []

# Module-level cache: keyed by (n_rows, contamination, feat_cols_tuple)
# Avoids refitting IsolationForest when the same data + params are reused
# within a session (e.g. repeated signal aggregator runs before new data arrives).
_IF_CACHE: dict = {}


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


def repair_decimal_glitches(df: pd.DataFrame) -> pd.DataFrame:
    """
    Detects and repairs decimal scaling errors (e.g. 10x or 100x shift for 1-2 days)
    on the fly.
    """
    if df.empty or len(df) < 5:
        return df

    df = df.copy()
    close = df["close"].values
    
    n = len(df)
    for i in range(1, n - 1):
        c_prev = close[i - 1]
        c_curr = close[i]
        c_next = close[i + 1]
        if c_prev <= 0 or c_curr <= 0 or c_next <= 0:
            continue
            
        ratio_down = c_curr / c_prev
        ratio_up = c_next / c_curr
        
        # Check for 1-day glitches
        for factor in [10.0, 100.0]:
            tol = 0.15
            if (abs(ratio_down - 1.0/factor) < tol/factor and abs(ratio_up - factor) < tol) or \
               (abs(ratio_down - factor) < tol and abs(ratio_up - 1.0/factor) < tol/factor):
                mult = factor if ratio_down < 1.0 else 1.0 / factor
                df.loc[i, ["open", "high", "low", "close"]] *= mult
                close = df["close"].values
                log.debug("Repaired single-day decimal scaling glitch on index %d (%s) for factor %f", i, df.iloc[i]["trade_date"], factor)
                break
                
        # Check for 2-day glitches
        if i < n - 2:
            c_next2 = close[i + 2]
            if c_next2 <= 0:
                continue
            ratio_down = c_curr / c_prev
            ratio_flat = c_next / c_curr
            ratio_up = c_next2 / c_next
            
            if abs(ratio_flat - 1.0) < 0.15:
                for factor in [10.0, 100.0]:
                    tol = 0.15
                    if (abs(ratio_down - 1.0/factor) < tol/factor and abs(ratio_up - factor) < tol) or \
                       (abs(ratio_down - factor) < tol and abs(ratio_up - 1.0/factor) < tol/factor):
                        mult = factor if ratio_down < 1.0 else 1.0 / factor
                        df.loc[i, ["open", "high", "low", "close"]] *= mult
                        df.loc[i+1, ["open", "high", "low", "close"]] *= mult
                        close = df["close"].values
                        log.debug("Repaired 2-day decimal scaling glitch on index %d-%d (%s) for factor %f", i, i+1, df.iloc[i]["trade_date"], factor)
                        break
    return df


def build_features(df: pd.DataFrame, rf_lags: int = 5) -> pd.DataFrame:
    """
    Add engineered features to a daily OHLCV DataFrame (sorted ascending by trade_date).

    Added columns: daily_return, log_return, range_pct.
    Input must have: trade_date, open, high, low, close, volume.
    Returns a new DataFrame — does NOT mutate the input.
    """
    # First repair decimal glitches on the fly to clean up noise
    df = repair_decimal_glitches(df)
    
    df = df.copy().sort_values("trade_date").reset_index(drop=True)
    df["daily_return"] = df["close"].pct_change() * 100
    
    # Yield protection: avoid negative or zero values in log return
    close_val = df["close"].values
    prev_close = df["close"].shift(1).values
    
    # Calculate log ratio with absolute value protection and safety epsilon
    with np.errstate(divide="ignore", invalid="ignore"):
        ratio = np.abs(close_val) / (np.abs(prev_close) + 1e-10)
        ratio[ratio <= 0] = 1.0
        df["log_return"] = np.log(ratio)
        
    df["range_pct"]    = (df["high"] - df["low"]) / df["close"] * 100
    df["vol_lag1"]     = df["volume"].shift(1)
    return df


_GARCH_CACHE: dict = {}

def fit_garch_residuals(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, float]:
    """
    Fit GARCH(1,1) with Student-t innovations on log-returns.
    Applies caching to prevent redundant MLE fits on the same dataset.
    """
    if df.empty:
        return df, 0.0

    last_row = df.iloc[-1]
    cache_key = (len(df), str(last_row["trade_date"]), float(last_row["close"]))
    if cache_key in _GARCH_CACHE:
        cached_cols, loglik = _GARCH_CACHE[cache_key]
        # Merge cached columns back and return
        df_out = df.copy()
        for col in cached_cols.columns:
            if col != "trade_date":
                df_out[col] = cached_cols[col].values
        return df_out, loglik

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

    # Cache the computed GARCH columns
    cols_to_cache = [
        "trade_date", "garch_vol", "rf_pred", "garch_band_1s",
        "garch_band_2s", "residual", "z_resid", "z_resid_abs"
    ]
    _GARCH_CACHE[cache_key] = (df[cols_to_cache].copy(), loglik)

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

    raw            = iso.score_samples(X)
    s_min, s_max   = raw.min(), raw.max()

    # Invert: 1 = most anomalous
    df["if_confidence"] = 1.0 - (raw - s_min) / (s_max - s_min + 1e-10)
    df["if_label"]      = iso.predict(X)
    return df


def fit_change_points(
    df: pd.DataFrame,
    penalty: float | None = None,
    min_size: int = 5,
    jump: int = 1,
    proximity_days: int = 3,
) -> pd.DataFrame:
    """
    PELT change-point detection on standardised log-returns (rbf kernel cost).

    Detects STRUCTURAL BREAKS — the dates where the return distribution shifts
    to a new variance/mean regime — as opposed to single-day point shocks.

    Method
    ──────
    1. Standardise log-returns (z = (r − mean) / std) so the penalty is
       scale-invariant across assets of any price level.
    2. Fit `ruptures.Pelt(model="rbf")` and predict breakpoints with penalty
       `penalty`. The rbf cost reacts to changes in the whole distribution,
       making it ideal for volatility-regime boundaries.
    3. Auto penalty (when None): 2·log(n_valid). Higher penalty → fewer breaks.

    Added columns
    ─────────────
        is_changepoint : bool — True exactly on a detected breakpoint date.
        cp_confirmed   : bool — True within ±proximity_days of any breakpoint
                                (used to corroborate point anomalies).

    Graceful degradation: if `ruptures` is unavailable or there are fewer than
    2·min_size valid returns, both columns are set False (no-op). Returns a new
    DataFrame — does NOT mutate the input.
    """
    df = df.copy()
    df["is_changepoint"] = False
    df["cp_confirmed"]   = False

    ret = df["log_return"].to_numpy(dtype="float64")
    valid_mask = ~np.isnan(ret)
    valid_pos  = np.flatnonzero(valid_mask)   # positions in df of non-NaN returns

    if valid_pos.size < 2 * min_size:
        return df  # too short to segment — leave all False

    # Limit the time series length for change-point detection to the last 750 elements (~3 years of data)
    # to bound O(N^3) complexity of RBF kernel.
    limit = 750
    if valid_pos.size > limit:
        valid_pos_run = valid_pos[-limit:]
        valid_mask_run = np.zeros(len(ret), dtype=bool)
        valid_mask_run[valid_pos_run] = True
    else:
        valid_pos_run = valid_pos
        valid_mask_run = valid_mask

    # Dynamically scale jump size to prevent O(N^3) RBF kernel complexity slowdowns
    if jump == 1:
        n_val = valid_pos_run.size
        if n_val > 1000:
            jump = 10
        elif n_val > 500:
            jump = 5
        elif n_val > 250:
            jump = 2

    try:
        import ruptures as rpt  # type: ignore[import]
    except ImportError:
        warnings.warn(
            "ruptures not installed — change-point confirmation disabled "
            "(pip install ruptures>=1.1.9)",
            stacklevel=2,
        )
        return df

    signal = ret[valid_mask_run]
    std = signal.std()
    signal_z = ((signal - signal.mean()) / (std + 1e-12)).reshape(-1, 1)

    pen = float(penalty) if penalty is not None else 2.0 * np.log(signal_z.shape[0])

    try:
        algo = rpt.Pelt(model="rbf", min_size=min_size, jump=jump).fit(signal_z)
        bkps = algo.predict(pen=pen)
    except Exception as exc:  # noqa: BLE001 — never let CPD break the pipeline
        warnings.warn(f"PELT change-point detection failed: {exc}", stacklevel=2)
        return df

    # ruptures returns 1-based indices into the *valid* signal, with the final
    # element always == len(signal) (the series end, not a real break) → drop it.
    bkps = [b for b in bkps if 0 < b < signal_z.shape[0]]
    if not bkps:
        return df

    # Map signal-relative breakpoint indices back to df row positions.
    bkp_df_pos = valid_pos_run[[b - 1 for b in bkps]]
    df.iloc[bkp_df_pos, df.columns.get_loc("is_changepoint")] = True

    # cp_confirmed: any row within ±proximity_days *rows* of a breakpoint.
    confirmed = np.zeros(len(df), dtype=bool)
    for pos in bkp_df_pos:
        lo = max(0, pos - proximity_days)
        hi = min(len(df), pos + proximity_days + 1)
        confirmed[lo:hi] = True
    df["cp_confirmed"] = confirmed
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

# ── OOP Strategies & Pipeline Orchestrator ───────────────────────────────────

class AnomalyDetectorStrategy(ABC):
    """Abstract interface for all individual anomaly detection algorithms."""

    @abstractmethod
    def fit_predict(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        Fit model on daily prices DataFrame and return it with computed scores.
        """
        pass


class RobustZScoreStrategy(AnomalyDetectorStrategy):
    """Calculates MAD-based robust Z-scores on daily return, trading range, and volume."""

    def __init__(self, window: int = 30):
        self.window = window

    def fit_predict(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        df = df.copy()
        df["z_return"] = robust_zscore(df["daily_return"].fillna(0), window=self.window)
        df["z_range"]  = robust_zscore(df["range_pct"],              window=self.window)
        df["z_robust"] = (df["z_return"].abs() + df["z_range"]) / 2.0
        df["z_volume"] = robust_zscore(df["volume"].fillna(0),       window=self.window)
        return df


class GarchResidualStrategy(AnomalyDetectorStrategy):
    """Fits a GARCH(1,1) model and standardizes residuals by conditional volatility."""

    def __init__(self):
        self.loglik: float = 0.0

    def fit_predict(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        df_res, loglik = fit_garch_residuals(df)
        self.loglik = loglik
        return df_res


class IsolationForestStrategy(AnomalyDetectorStrategy):
    """Runs Isolation Forest on price-based and cross-asset features to compute confidence."""

    def __init__(self, contamination: float = 0.03):
        self.contamination = contamination

    def fit_predict(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        return fit_isolation_forest(df, contamination=self.contamination)


class PeltChangePointStrategy(AnomalyDetectorStrategy):
    """Applies PELT Change-Point Detection to identify structural regime shifts."""

    def __init__(self, penalty: float | None = None, proximity_days: int = 3):
        self.penalty = penalty
        self.proximity_days = proximity_days

    def fit_predict(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        return fit_change_points(
            df, penalty=self.penalty, proximity_days=self.proximity_days
        )


class CompositeAnomalyPipeline:
    """Orchestrates sequential anomaly strategies and computes consolidated regimes."""

    def __init__(
        self,
        z_threshold: float = 3.0,
        cp_boost: float = 1.15,
        df_cot: pd.DataFrame | None = None,
        df_fx: pd.DataFrame | None = None,
        df_corp_actions: pd.DataFrame | None = None,
        symbol: str = "",
        category: str = "",
    ):
        self.z_threshold     = z_threshold
        self.cp_boost        = cp_boost
        self.df_cot          = df_cot
        self.df_fx           = df_fx
        self.df_corp_actions = df_corp_actions
        self.symbol          = symbol
        self.category        = category
        self.garch_loglik: float = 0.0

    def run(
        self,
        df: pd.DataFrame,
        rf_lags: int = 5,
        contamination: float = 0.03,
        z_window: int = 30,
        cp_penalty: float | None = None,
        cp_proximity_days: int = 3,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        df = build_features(df, rf_lags=rf_lags)

        # Inject cross-asset features when available
        df = _inject_cross_asset(df, df_cot=self.df_cot, df_fx=self.df_fx)

        # ── Instantiate and execute strategies sequentially ──────────────────
        strategies = [
            RobustZScoreStrategy(window=z_window),
            GarchResidualStrategy(),
            IsolationForestStrategy(contamination=contamination),
            PeltChangePointStrategy(penalty=cp_penalty, proximity_days=cp_proximity_days),
        ]

        for strategy in strategies:
            strat_name = strategy.__class__.__name__
            log.info(f"Running strategy: {strat_name}...")
            import time
            t0 = time.time()
            df = strategy.fit_predict(df)
            log.info(f"Finished strategy {strat_name} in {time.time() - t0:.4f}s")
            if isinstance(strategy, GarchResidualStrategy):
                self.garch_loglik = strategy.loglik

        # Classify regimes + compute Final Z
        df = classify_regime(df)

        # Change-point confirmation booster
        if self.cp_boost != 1.0 and bool(df["cp_confirmed"].any()):
            pre_flagged = df["final_z_abs"] > self.z_threshold
            mask = df["cp_confirmed"] & pre_flagged
            if mask.any():
                df.loc[mask, "final_z"] = df.loc[mask, "final_z"] * self.cp_boost
                df.loc[mask, "final_z_abs"] = df.loc[mask, "final_z"].abs()
                _keep = df["regime"].str.contains("Flash Crash|Volatile Breakout", na=False)
                relabel = mask & ~_keep
                df.loc[relabel, "regime"] = "🔀 Regime Shift (Change Point)"

        # ── Corporate action suppression ─────────────────────────────────────
        df["is_corporate_action"]    = False
        df["suppress_corp_action"]   = False
        if self.df_corp_actions is not None and not self.df_corp_actions.empty:
            from src.importer.fetchers.nse_corporate_actions_fetcher import PRICE_IMPACTING_TYPES
            ca = self.df_corp_actions.copy()
            ca["ex_date"] = pd.to_datetime(ca["ex_date"]).dt.normalize()
            all_ca_dates      = set(ca["ex_date"])
            suppress_dates    = set(ca.loc[ca["action_type"].isin(PRICE_IMPACTING_TYPES), "ex_date"])
            df["trade_date"]  = pd.to_datetime(df["trade_date"]).dt.normalize()
            df["is_corporate_action"]  = df["trade_date"].isin(all_ca_dates)
            df["suppress_corp_action"] = df["trade_date"].isin(suppress_dates)
            df.loc[df["suppress_corp_action"], "regime"] = "🏢 Price Driven by Company Event"

        # Populate is_anomaly flag
        df["is_anomaly"] = (df["final_z_abs"] > self.z_threshold)

        df_flagged = df[df["is_anomaly"]].copy()

        if self.symbol and not df_flagged.empty:
            try:
                from src.db.anomaly_vector import store_anomalies
                store_anomalies(df_flagged, self.symbol, self.category)
            except Exception:
                pass

        return df, df_flagged



# ── Full pipeline wrapper (Backward Compatible) ──────────────────────────────

def run_composite_anomaly(
    df: pd.DataFrame,
    rf_lags: int = 5,           # kept for API compatibility, unused
    contamination: float = 0.03,
    z_threshold: float = 3.0,
    z_window: int = 30,
    df_cot: pd.DataFrame | None = None,
    df_fx: pd.DataFrame | None = None,
    cp_penalty: float | None = None,
    cp_proximity_days: int = 3,
    cp_boost: float = 1.15,
    df_corp_actions: pd.DataFrame | None = None,
    symbol: str = "",
    category: str = "",
) -> tuple[pd.DataFrame, pd.DataFrame, float]:
    """
    End-to-end composite anomaly detection.
    Defers execution to the OOP CompositeAnomalyPipeline.

    symbol / category: when provided, flagged anomalies are stored in Qdrant
    for future semantic retrieval (retrieve_similar_anomalies).
    """
    pipeline = CompositeAnomalyPipeline(
        z_threshold=z_threshold,
        cp_boost=cp_boost,
        df_cot=df_cot,
        df_fx=df_fx,
        df_corp_actions=df_corp_actions,
        symbol=symbol,
        category=category,
    )
    df_res, df_flagged = pipeline.run(
        df,
        rf_lags=rf_lags,
        contamination=contamination,
        z_window=z_window,
        cp_penalty=cp_penalty,
        cp_proximity_days=cp_proximity_days,
    )
    return df_res, df_flagged, pipeline.garch_loglik
