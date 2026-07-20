"""Regime classification for the anomaly pipeline."""
from __future__ import annotations

import pandas as pd


def classify_regime(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute Final Z and add a human-readable regime label.

    Final_Z = Z_robust × (1 + IF_confidence)

    Regime thresholds are relative (median of each signal over the full window),
    making them robust to different asset price scales.

    Regime priority (checked top-to-bottom):
        📊  Volume Anomaly (Institutional Block) — NEW: high volume Z + low price Z
        ⚠️  Crowded Long (Squeeze Risk)          — high COT crowding + high price Z
        🧨  Blow-off Top (Weak)                  — high Z, low volume, positive return
        📈  Strong Trend (HODL)                  — high Z, low residual Z
        ⚡  Flash Crash / Black Swan             — low Z, high residual Z  ← key GARCH signal
        🔥  Volatile Breakout                    — both high
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
    z_vol_hi   = float(df["z_volume"].abs().quantile(0.80))   # "hi_vol" = top 20%
    z_vol_med  = float(df["z_volume"].abs().quantile(0.20))   # "lo_vol" = bottom 20%
    # COT crowding threshold: top quartile of historical cot_pct_oi
    has_cot = "cot_pct_oi" in df.columns
    cot_thresh = float(df["cot_pct_oi"].quantile(0.75)) if has_cot else 25.0

    def _label(row) -> str:
        hi_z   = abs(row["z_robust"]) > z_med
        hi_res = row["z_resid_abs"]   > res_med
        lo_vol = abs(row["z_volume"]) < z_vol_med
        hi_vol = abs(row["z_volume"]) > z_vol_hi

        # ── Volume-priority regime (catches crossed block deals before price moves) ──
        # Volume Anomaly: extraordinary volume with no commensurate price move.
        # Classic signature of an institutional crossed bulk/block deal that will
        # be disclosed the next day via exchange filing, triggering a price reaction.
        if hi_vol and not hi_z and not hi_res:
            return "📊 Volume Anomaly (Institutional Block)"

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
