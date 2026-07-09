"""
src/ml/premium_regime.py
─────────────────────────
PELT-first regime detection engine for ETF premium-to-iNAV series.

Pipeline (per call)
───────────────────
  premiums[0..t]
    → PELT (ruptures, rbf model, pen=pen_multiplier*var, min_size=30)
    → segment = premiums[last_break..t]
    → if len(segment) < min_segment: status=INSUFFICIENT_DATA
    → ADF(segment, lags=None, trend="c")    arch.unitroot (Schwert lag selection)
    → KPSS(segment, lags=None, trend="ct") arch.unitroot (data-dependent lags)
    → stationary = (adf_p < adf_threshold)   [ADF alone; KPSS adjusts confidence]
    → if not stationary: status=NON_STATIONARY
    NOTE: KPSS is NOT a hard gate — ETF premiums have ARCH heteroskedasticity
    which causes KPSS to over-reject even for genuinely mean-reverting series.
    KPSS evidence enters only the confidence score (s2 term, weight 0.25).
    → fit_ou(segment) → OUState
    → solve_double_stopping(ou, c_buy, c_sell, r_daily) → DStopState
    → compute confidence score 0–100

Confidence scoring
──────────────────
  s1 = ADF evidence     : max(0, 1 − adf_p/adf_threshold)       weight 0.30
  s2 = KPSS evidence    : min(1, kpss_p/kpss_threshold)          weight 0.30
  s3 = No recent break   : min(1, segment_age / shift_window)    weight 0.20
  s4 = OU R²             : max(0, ou.fit_r2)                      weight 0.20
  confidence = round((0.30*s1+0.30*s2+0.20*s3+0.20*s4)*100, 1)

Public API
──────────
  detect_regime(premiums, dates, ...) -> RegimeState
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import date
from typing import Optional

import numpy as np

from src.ml.ou_estimator import OUState, fit_ou
from src.ml.ou_double_stopping import DStopState, solve_double_stopping

log = logging.getLogger(__name__)

# ── Status constants ──────────────────────────────────────────────────────────
STATUS_STATIONARY        = "STATIONARY"
STATUS_NON_STATIONARY    = "NON_STATIONARY"
STATUS_INSUFFICIENT_DATA = "INSUFFICIENT_DATA"
STATUS_STRUCTURAL_SHIFT  = "STRUCTURAL_SHIFT"


@dataclass
class RegimeState:
    """Full regime assessment for a single point in time."""
    status: str                         # STATUS_* constant
    regime_start: Optional[date]        # first date of current PELT segment
    segment_n_obs: int                  # observations in current segment
    segment_age: int                    # days since last PELT break
    adf_pvalue: Optional[float]         # None if segment too short
    kpss_pvalue: Optional[float]
    ou: Optional[OUState]               # None if not stationary
    dstop: Optional[DStopState]         # None if not stationary
    confidence: float                   # 0–100 composite score
    n_breaks: int                       # total PELT break count found
    theta_history: list[float] = field(default_factory=list, repr=False)


def detect_regime(
    premiums: np.ndarray,
    dates: Optional[list[date]] = None,
    pen_multiplier: float = 3.0,
    pen_window: int = 250,
    penalty_method: str = "heuristic",
    min_segment: int = 30,
    adf_threshold: float = 0.05,
    kpss_threshold: float = 0.05,
    r_daily: float = 0.05 / 252,
    c_buy_bps: float = 10.0,
    c_sell_bps: float = 10.0,
    theta_history: Optional[list[float]] = None,
    structural_shift_window: int = 10,
) -> RegimeState:
    """
    Run the full PELT → stationarity → OU → threshold pipeline.

    Parameters
    ----------
    premiums       : time-ordered array of premium_pct values (%)
    dates          : corresponding dates (same length); used only for regime_start
    pen_multiplier : PELT penalty = pen_multiplier × var(trailing pen_window obs)
    pen_window     : trailing window (in obs) used to estimate variance for the PELT
                     penalty (default 250 ≈ 1yr). Using a rolling window instead of
                     the full history keeps the penalty tied to *current* volatility —
                     a full-history variance would keep inflating after a vol regime
                     shift, making PELT progressively less willing to carve out the
                     new regime as its own segment (self-reinforcing false negative).
                     Ignored when penalty_method="crops".
    penalty_method : "heuristic" (default) = pen_multiplier × var(pen_window).
                     "crops" = data-driven penalty selection: sweep a penalty grid,
                     fit PELT once, and pick the elbow of the (n_breakpoints, cost)
                     curve — no pen_multiplier tuning required. Falls back to the
                     heuristic automatically if the sweep degenerates.
    min_segment    : minimum observations required for stationarity test + OU fit
    adf_threshold  : reject unit root if adf_p < this (default 0.05)
    kpss_threshold : KPSS advisory threshold — does NOT gate trading, only adjusts
                     the confidence score s2 term (default 0.05)
    r_daily        : daily discount rate for ZJL DP
    c_buy_bps      : entry cost in bps
    c_sell_bps     : exit cost in bps
    theta_history  : list of theta values from previous refits (for stability scoring)
    structural_shift_window : pause trading for this many days after a PELT break.
                     A new break invalidates the OU fit until the segment has aged
                     enough to re-establish stationarity (default 10 days).

    Returns
    -------
    RegimeState with full diagnosis.
    """
    premiums = np.asarray(premiums, dtype=np.float64)
    n_total = len(premiums)
    theta_history = list(theta_history or [])

    # ── Step 1: PELT change-point detection ───────────────────────────────────
    try:
        import ruptures as rpt
    except ImportError:
        log.error("ruptures not installed — cannot run PELT")
        return _insufficient(0, dates, 0, theta_history)

    # Guard: need at least 2×min_segment to have any useful segmentation
    if n_total < min_segment:
        return _insufficient(0, dates, 0, theta_history)

    finite_mask = np.isfinite(premiums)
    if finite_mask.sum() < min_segment:
        return _insufficient(0, dates, 0, theta_history)

    # Limit the time series length for change-point detection to bound O(N^3) complexity of RBF kernel.
    limit = 750
    offset = max(0, n_total - limit)
    signal = premiums[offset:]

    # Dynamically scale jump size based on signal length to bound PELT's O(N²) cost.
    # Uses floor(log2(N/50)) so the step size grows gradually:
    #   N ≤  199 → jump=1  (dense: every point is a candidate)
    #   N ~  200 → jump=2
    #   N ~  400 → jump=3
    #   N ~  800 → jump=4  (at the 750-point cap this is the practical ceiling)
    # Clamped to [1, 5] so the jump never becomes coarse enough to miss a real break.
    _n = len(signal)
    jump = max(1, min(5, math.floor(math.log2(max(_n, 50) / 50))))

    # Penalty selection
    if penalty_method == "crops":
        try:
            pen, _crops_n_bkps = _select_penalty_crops(signal, min_segment, jump)
            log.debug("CROPS penalty selected: pen=%.4f (n_bkps=%d)", pen, _crops_n_bkps)
        except Exception as exc:
            log.warning("CROPS penalty selection failed: %s — falling back to heuristic", exc)
            penalty_method = "heuristic"

    if penalty_method != "crops":
        # Heuristic BIC-motivated penalty = pen_multiplier × variance.
        # Estimated on a trailing window (not the full history) so the penalty tracks
        # the CURRENT volatility regime rather than an ever-growing blend of old+new
        # regimes — see pen_window docstring above.
        clean_premiums = premiums[finite_mask]
        var_window = clean_premiums[-pen_window:] if pen_window > 0 else clean_premiums
        pen = pen_multiplier * float(np.var(var_window))
        if pen <= 0:
            pen = 1.0  # degenerate: constant series

    try:
        algo = rpt.Pelt(model="rbf", min_size=min_segment, jump=jump)
        algo.fit(signal.reshape(-1, 1))
        # predict returns break indices (exclusive end), last element = len(signal)
        breaks = algo.predict(pen=pen)
    except Exception as exc:
        log.warning("PELT failed: %s — treating full series as one segment", exc)
        breaks = [len(signal)]

    # Adjust breaks by offset to get indices in the original premiums array
    real_breaks = [b + offset for b in breaks[:-1] if 0 < b < len(signal)]
    n_breaks = len(real_breaks)

    # Current segment: from last break to end
    seg_start_idx = real_breaks[-1] if real_breaks else 0
    segment = premiums[seg_start_idx:]
    regime_start_date = dates[seg_start_idx] if (dates and seg_start_idx < len(dates)) else None
    segment_age = n_total - seg_start_idx   # days since last PELT break

    # ── Step 2: structural shift detection ────────────────────────────────────
    # If we have a real break (not the initial start) and the segment is younger
    # than structural_shift_window days, the OU fit from the old regime is invalid.
    # Pause trading until the new segment is established.
    if real_breaks and segment_age < structural_shift_window:
        log.debug(
            "STRUCTURAL_SHIFT: segment_age=%d < window=%d (break at idx %d)",
            segment_age, structural_shift_window, seg_start_idx,
        )
        return RegimeState(
            status=STATUS_STRUCTURAL_SHIFT,
            regime_start=regime_start_date,
            segment_n_obs=len(segment[np.isfinite(segment)]),
            segment_age=segment_age,
            adf_pvalue=None,
            kpss_pvalue=None,
            ou=None,
            dstop=None,
            confidence=0.0,
            n_breaks=n_breaks,
            theta_history=theta_history,
        )

    # ── Step 3: check segment length ─────────────────────────────────────────
    seg_clean = segment[np.isfinite(segment)]
    if len(seg_clean) < min_segment:
        return _insufficient(seg_start_idx, dates, n_breaks, theta_history,
                             regime_start=regime_start_date)

    # ── Step 3: ADF stationarity gate (KPSS is advisory, not a hard gate) ──────
    # KPSS over-rejects on ETF premium data due to ARCH heteroskedasticity.
    # ADF alone is sufficient; KPSS adjusts the confidence score (s2, weight 0.25).
    adf_p, kpss_p = _run_stationarity_tests(seg_clean)

    stationary = (adf_p is not None) and (adf_p < adf_threshold)

    if not stationary:
        log.debug(
            "Stationarity gate FAIL: adf_p=%.4f, kpss_p=%.4f (n=%d)",
            adf_p or -1, kpss_p or -1, len(seg_clean),
        )
        return RegimeState(
            status=STATUS_NON_STATIONARY,
            regime_start=regime_start_date,
            segment_n_obs=len(seg_clean),
            segment_age=segment_age,
            adf_pvalue=adf_p,
            kpss_pvalue=kpss_p,
            ou=None,
            dstop=None,
            confidence=0.0,
            n_breaks=n_breaks,
            theta_history=theta_history,
        )

    # ── Step 4: OU fit ────────────────────────────────────────────────────────
    ou = fit_ou(seg_clean)
    if ou is None:
        log.debug("OU fit failed on stationary segment (n=%d)", len(seg_clean))
        return RegimeState(
            status=STATUS_NON_STATIONARY,
            regime_start=regime_start_date,
            segment_n_obs=len(seg_clean),
            segment_age=segment_age,
            adf_pvalue=adf_p,
            kpss_pvalue=kpss_p,
            ou=None,
            dstop=None,
            confidence=0.0,
            n_breaks=n_breaks,
            theta_history=theta_history,
        )

    # ── Step 5: ZJL optimal thresholds ───────────────────────────────────────
    dstop = solve_double_stopping(
        ou, c_buy_bps=c_buy_bps, c_sell_bps=c_sell_bps, r_daily=r_daily,
    )

    # ── Step 6: confidence score ──────────────────────────────────────────────
    x = seg_clean
    updated_theta_history = theta_history + [ou.theta]

    conf = _confidence(
        adf_p=adf_p,
        kpss_p=kpss_p,
        r2=ou.fit_r2,
        segment_age=segment_age,
        structural_shift_window=structural_shift_window,
        adf_threshold=adf_threshold,
        kpss_threshold=kpss_threshold,
    )

    log.debug(
        "Regime STATIONARY: seg=%d obs, adf=%.4f, kpss=%.4f, θ=%.4f, μ=%.3f, "
        "σ∞=%.3f, b*=%.3f, s*=%.3f, conf=%.1f",
        len(seg_clean), adf_p, kpss_p,
        ou.theta, ou.mu, ou.sigma_inf,
        dstop.b_star, dstop.s_star, conf,
    )

    return RegimeState(
        status=STATUS_STATIONARY,
        regime_start=regime_start_date,
        segment_n_obs=len(seg_clean),
        segment_age=segment_age,
        adf_pvalue=adf_p,
        kpss_pvalue=kpss_p,
        ou=ou,
        dstop=dstop,
        confidence=conf,
        n_breaks=n_breaks,
        theta_history=updated_theta_history,
    )


# ── Helpers ───────────────────────────────────────────────────────────────────

def _select_penalty_crops(
    signal: np.ndarray,
    min_size: int,
    jump: int,
    n_grid: int = 25,
) -> tuple[float, int]:
    """
    Data-driven PELT penalty selection — approximates CROPS (Haynes, Eckley &
    Fearnhead 2017) via a log-spaced penalty grid + kneedle-style elbow
    detection, rather than the exact interval-splitting algorithm (sufficient
    for daily walk-forward use; avoids hand-tuning a fixed pen_multiplier).

    Sweeps penalties spanning 0.001×–10× var(signal), fits PELT once, and for
    each penalty records (n_breakpoints, segmentation_cost). Larger penalties
    give fewer breakpoints but worse (higher) cost; smaller penalties give
    more breakpoints but risk fitting noise. The elbow — point of maximum
    perpendicular distance from the line joining the sparsest and densest
    segmentations — is the standard diminishing-returns tradeoff point.
    (Empirically on premium series, most of the interesting curvature sits
    well below 1× var — a 0.3×-floor grid collapses to only 0-1 breakpoints.)

    Returns
    -------
    (penalty, n_breakpoints) at the elbow. Raises on degenerate input (caller
    is expected to catch and fall back to the heuristic pen_multiplier×var).
    """
    import ruptures as rpt

    var = float(np.var(signal))
    if var <= 0:
        raise ValueError("degenerate signal: zero variance")

    algo = rpt.Pelt(model="rbf", min_size=min_size, jump=jump).fit(signal.reshape(-1, 1))

    grid = np.geomspace(0.001, 10.0, n_grid) * var
    best_cost_for_nbkps: dict[int, float] = {}
    pen_for_nbkps: dict[int, float] = {}
    for pen in grid:
        bkps = algo.predict(pen=float(pen))
        n_bkps = len(bkps) - 1   # last element is always len(signal), not a real break
        cost = algo.cost.sum_of_costs(bkps)
        if n_bkps not in best_cost_for_nbkps or cost < best_cost_for_nbkps[n_bkps]:
            best_cost_for_nbkps[n_bkps] = cost
            pen_for_nbkps[n_bkps] = float(pen)

    if len(best_cost_for_nbkps) < 3:
        # Not enough spread across the grid to find a meaningful elbow —
        # let the caller fall back to the heuristic.
        raise ValueError("penalty sweep degenerate: <3 distinct segmentations")

    pairs = sorted(best_cost_for_nbkps.items())   # ascending by n_bkps
    xs = np.array([p[0] for p in pairs], dtype=float)
    ys = np.array([p[1] for p in pairs], dtype=float)

    x_norm = (xs - xs.min()) / (xs.max() - xs.min() + 1e-12)
    y_norm = (ys - ys.min()) / (ys.max() - ys.min() + 1e-12)

    p1 = np.array([x_norm[0], y_norm[0]])
    p2 = np.array([x_norm[-1], y_norm[-1]])
    line_vec = p2 - p1
    line_len = float(np.linalg.norm(line_vec))
    if line_len < 1e-12:
        elbow_idx = 0
    else:
        line_unit = line_vec / line_len
        pts = np.stack([x_norm, y_norm], axis=1) - p1
        proj = np.outer(pts @ line_unit, line_unit)
        dist = np.linalg.norm(pts - proj, axis=1)
        elbow_idx = int(np.argmax(dist))

    chosen_n_bkps = int(xs[elbow_idx])
    return pen_for_nbkps[chosen_n_bkps], chosen_n_bkps


def _insufficient(
    seg_start_idx: int,
    dates: Optional[list],
    n_breaks: int,
    theta_history: list,
    regime_start: Optional[date] = None,
) -> RegimeState:
    start = regime_start
    if start is None and dates and seg_start_idx < len(dates):
        start = dates[seg_start_idx]
    return RegimeState(
        status=STATUS_INSUFFICIENT_DATA,
        regime_start=start,
        segment_n_obs=0,
        segment_age=0,
        adf_pvalue=None,
        kpss_pvalue=None,
        ou=None,
        dstop=None,
        confidence=0.0,
        n_breaks=n_breaks,
        theta_history=theta_history,
    )


def _run_stationarity_tests(segment: np.ndarray) -> tuple[Optional[float], Optional[float]]:
    """
    Run ADF and KPSS on segment. Returns (adf_p, kpss_p) or (None, None) on error.

    ADF : H0 = unit root. Reject H0 (p < 0.05) = evidence of stationarity.
    KPSS: H0 = stationary. Fail to reject (p > 0.05) = no evidence against stationarity.
    Both must pass for the segment to be considered stationary.
    """
    try:
        from arch.unitroot import ADF, KPSS
    except ImportError:
        log.error("arch.unitroot not available — cannot run stationarity tests")
        return None, None

    try:
        adf_result = ADF(segment, lags=None, trend="c")
        adf_p = float(adf_result.pvalue)
    except Exception as exc:
        log.debug("ADF test failed: %s", exc)
        return None, None

    try:
        kpss_result = KPSS(segment, lags=None, trend="ct")
        kpss_p = float(kpss_result.pvalue)
    except Exception as exc:
        log.debug("KPSS test failed: %s", exc)
        return adf_p, None

    return adf_p, kpss_p


def _ljungbox_p(residuals: np.ndarray, nlags: int = 10) -> float:
    """
    Ljung-Box Q-test for residual autocorrelation at `nlags` lags.
    Returns the minimum p-value across lags (conservative).
    Falls back to 0.5 (neutral) on any error.
    """
    try:
        n = len(residuals)
        if n < nlags + 5:
            return 0.5   # too short to be meaningful — neutral score

        # Compute sample autocorrelations at lags 1..nlags
        r = residuals - residuals.mean()
        c0 = float(np.dot(r, r))
        if c0 < 1e-15:
            return 1.0   # zero variance residuals → perfectly white

        q = 0.0
        p_min = 1.0
        from scipy.stats import chi2
        for k in range(1, nlags + 1):
            rk = float(np.dot(r[k:], r[:-k])) / c0
            q += n * (n + 2) * rk ** 2 / (n - k)
            # Approximate p-value at this lag
            p_k = 1.0 - float(chi2.cdf(q, df=k))
            p_min = min(p_min, p_k)

        return float(p_min)
    except Exception:
        return 0.5   # neutral on any failure


def _confidence(
    adf_p: float,
    kpss_p: float,
    r2: float,
    segment_age: int,
    structural_shift_window: int,
    adf_threshold: float,
    kpss_threshold: float,
) -> float:
    """
    Composite confidence score 0–100.

    Weights (user-specified):
      30%  ADF confirms stationarity
      30%  KPSS confirms stationarity
      20%  No recent PELT break (segment maturity)
      20%  OU fit R² > threshold
    """
    # s1: ADF evidence (lower p → more confidence)
    s1 = max(0.0, 1.0 - adf_p / adf_threshold) if adf_threshold > 0 else 0.0

    # s2: KPSS evidence (higher p → more confidence stationarity holds)
    s2 = min(1.0, kpss_p / (kpss_threshold * 2)) if kpss_threshold > 0 else 0.0

    # s3: Segment maturity — ramps from 0 to 1 as segment ages past shift window
    # At exactly structural_shift_window days: 1.0. Below: proportional.
    # Above: capped at 1.0.
    s3 = min(1.0, segment_age / max(structural_shift_window, 1))

    # s4: R² of AR(1) fit
    s4 = max(0.0, r2)

    raw = 0.30 * s1 + 0.30 * s2 + 0.20 * s3 + 0.20 * s4
    return round(raw * 100.0, 1)
