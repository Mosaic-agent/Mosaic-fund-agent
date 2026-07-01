"""PELT change-point detection for the anomaly pipeline."""
from __future__ import annotations

import warnings

import numpy as np
import pandas as pd


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

    # Dynamically scale jump size — n_val is at most 750 (capped above), so only
    # the > 500 and > 250 branches are reachable; the > 1000 branch was dead.
    if jump == 1:
        n_val = valid_pos_run.size
        if n_val > 500:
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
