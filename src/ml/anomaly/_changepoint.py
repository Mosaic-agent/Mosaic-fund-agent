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


# Adaptive change-point sensitivity. A single fixed penalty is miscalibrated
# across asset classes: a value giving ~2 clean breaks on a volatile ETF gives
# 0 on a steadier large-cap — confirmed empirically: the fixed 2·log(n)
# auto-penalty found ZERO breaks on 21/22 diverse symbols tested (large-cap
# stocks + ETFs spanning 0.75%-3.71% daily volatility). Instead scan penalties
# from conservative → sensitive (fractions of the auto-penalty) and accept the
# FIRST that yields a sensible 1..max_breaks count — so each symbol surfaces
# its own structural breaks at comparable rates, while the cap rejects the
# over-segmentation you get at very low penalties (noise).
ADAPTIVE_PENALTY_FACTORS = (0.6, 0.45, 0.35, 0.27, 0.2)


def fit_change_points_adaptive(
    df: pd.DataFrame,
    factors: tuple[float, ...] = ADAPTIVE_PENALTY_FACTORS,
    max_breaks_divisor: int = 120,
    min_size: int = 5,
    jump: int = 1,
    proximity_days: int = 3,
) -> pd.DataFrame:
    """Like ``fit_change_points()``, but scans ``factors`` (as fractions of the
    ``2·log(n)`` auto-penalty) from conservative to sensitive and keeps the
    first factor whose break count falls in ``[1, max_breaks]`` — instead of
    relying on a single fixed penalty that in practice is too conservative for
    most symbols (see module-level comment on ``ADAPTIVE_PENALTY_FACTORS``).

    ``max_breaks`` scales with series length: ``max(2, n_valid // max_breaks_divisor)``
    (~1 break per ``max_breaks_divisor`` trading days), so longer histories can
    surface proportionally more breaks without over-segmenting shorter ones.

    Falls back to an all-False result (same shape as ``fit_change_points``)
    when no factor in the scan yields a sensible break count — i.e. it never
    forces a break to be found.
    """
    n_valid = int(df["log_return"].notna().sum()) if "log_return" in df.columns else 0
    auto_pen = 2.0 * np.log(max(n_valid, 2))
    max_breaks = max(2, n_valid // max_breaks_divisor)

    chosen = None
    for fct in factors:
        res = fit_change_points(
            df, penalty=auto_pen * fct, min_size=min_size, jump=jump,
            proximity_days=proximity_days,
        )
        nb = int(res["is_changepoint"].sum())
        if nb == 0:
            continue           # too conservative — go more sensitive
        if nb > max_breaks:
            break               # over-segmented — reject; nothing trustworthy
        chosen = res
        break                   # first sensible count wins

    if chosen is not None:
        return chosen
    # No factor gave a sensible count — return the (all-False) conservative
    # base result, computed at the plain auto-penalty for a consistent shape.
    return fit_change_points(
        df, penalty=auto_pen, min_size=min_size, jump=jump,
        proximity_days=proximity_days,
    )

