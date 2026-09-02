"""
VLRT v3 — composite score to portfolio weights.

Continuous and monotone throughout. v2 mapped its score through a five-bucket lookup
table, which froze the output in 44% of months and made its top regime unreachable.
Three specific pathologies are avoided here:

* **A constant risk budget.** ``VOL_TARGETS["equity"]`` is 20% but NIFTYBEES realised
  vol has a median near 12%, so ``min(w_max, target/sigma)`` with ``w_max=1.0`` pins at
  1.0 in essentially every month — injecting a *constant*, which is the frozen-step
  pathology in a new costume. Fixed by targeting each sleeve's own **rolling median
  volatility** and allowing the relative budget to exceed 1.0 before normalisation.
* **Discrete overrides.** ``compute_position_weight`` also applies a discrete regime
  lookup and a hard ``composite_score < 35`` cliff. Both are deliberately neutralised
  (``regime="Normal"``, ``composite_score=None``) so only the continuous inverse-vol
  term is used; direction is expressed by the tilt instead, and never twice.
* **Clamp-then-renormalise.** That is discontinuous at every bound: the instant the
  metals floor binds, equity jumps. Replaced by Euclidean projection onto the
  box-constrained simplex, which is continuous everywhere.

Sizing is delegated to :func:`src.tools.risk_governor.compute_position_weight` so this
shares the repo's existing inverse-vol logic rather than reimplementing it.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from src.tools.risk_governor import compute_position_weight
from src.vlrt.data import SLEEVES

SLEEVE_ORDER: tuple[str, ...] = ("equity", "pm", "cash")

#: Strategic anchor. The risk budget and the tilt scale *these*, so a neutral signal at
#: neutral volatility reproduces the anchor exactly. Without it the blend collapses toward
#: equal risk sleeves and precious metals pins against its cap in most months.
ANCHOR: dict[str, float] = {"equity": 0.55, "pm": 0.20, "cash": 0.25}

#: Tilt curvature. exp(kappa * (c - 0.5)) for c in [0,1] gives a multiplier in
#: [exp(-k/2), exp(k/2)] — strictly monotone, no flat regions.
KAPPA_EQUITY = 1.4
KAPPA_PM = 1.0

#: Box bounds per sleeve. ``pm`` floor reflects the SEBI multi-asset commodity-class
#: minimum, which applies to the combined precious-metals bucket, not to gold alone.
BOUNDS: dict[str, tuple[float, float]] = {
    "equity": (0.20, 0.80),
    "pm": (0.10, 0.35),
    "cash": (0.05, 0.60),
}

#: Relative risk budget cap. Not leverage — the simplex projection downstream enforces
#: sum-to-one, so this only bounds how far one sleeve may outrank another.
W_MAX = 3.0

VOL_HALFLIFE_DAYS = 21
VOL_MIN_PERIODS = 60
#: Rolling window for the vol target. Rolling, never full-sample: a full-sample median
#: would leak the future volatility regime into every historical weight.
VOL_TARGET_WINDOW_M = 36
#: India VIX earns equal weight in the equity vol estimate: measured VIX_t -> realised
#: vol_{t+1} Spearman +0.618 vs realised-vol persistence +0.549.
VIX_BLEND = 0.5


def causal_realised_vol(
    close: pd.Series,
    halflife: int = VOL_HALFLIFE_DAYS,
    min_periods: int = VOL_MIN_PERIODS,
) -> pd.Series:
    """EWMA annualised volatility in percent. Uses only past observations."""
    r = np.log(close.astype(float)).diff()
    var = r.pow(2).ewm(halflife=halflife, min_periods=min_periods).mean()
    return np.sqrt(var * 252.0) * 100.0


def sleeve_vols(
    daily: pd.DataFrame, sleeve_px: pd.DataFrame, month_ends: pd.DatetimeIndex
) -> pd.DataFrame:
    """Point-in-time vol estimate and its rolling-median target, sampled at month-end."""
    eq_rv = causal_realised_vol(sleeve_px["equity"])
    pm_rv = causal_realised_vol(sleeve_px["pm"])
    vix = daily["INDIAVIX"].astype(float)

    out = pd.DataFrame(index=sleeve_px.index)
    out["equity_vol"] = (VIX_BLEND * vix + (1.0 - VIX_BLEND) * eq_rv).fillna(eq_rv)
    out["pm_vol"] = pm_rv
    m = out.loc[out.index.intersection(month_ends)]
    for c in ("equity", "pm"):
        m[f"{c}_target"] = (
            m[f"{c}_vol"].rolling(VOL_TARGET_WINDOW_M, min_periods=12).median().shift(1)
        )
    return m


def project_box_simplex(
    w: np.ndarray, lo: np.ndarray, hi: np.ndarray, iters: int = 80
) -> np.ndarray:
    """
    Euclidean projection onto ``{w : lo <= w <= hi, sum(w) == 1}``.

    Bisection on a scalar shift ``lam`` such that ``sum(clip(w + lam, lo, hi)) == 1``.
    The sum is monotone non-decreasing in ``lam``, so bisection converges exactly.
    Continuous everywhere, unlike clamp-then-renormalise.
    """
    if lo.sum() > 1.0 + 1e-12 or hi.sum() < 1.0 - 1e-12:
        raise ValueError("infeasible box: lo.sum() must be <= 1 <= hi.sum()")
    a, b = -2.0, 2.0
    for _ in range(iters):
        lam = 0.5 * (a + b)
        if np.clip(w + lam, lo, hi).sum() < 1.0:
            a = lam
        else:
            b = lam
    return np.clip(w + 0.5 * (a + b), lo, hi)


def _risk_budget(vol_pct: float, target_pct: float) -> float:
    """Continuous inverse-vol term only — regime and score overrides neutralised."""
    if not np.isfinite(vol_pct) or not np.isfinite(target_pct) or vol_pct <= 0:
        return 1.0
    return compute_position_weight(
        garch_annual_vol_pct=float(vol_pct),
        regime="✅ Normal",
        composite_score=None,
        vol_target_pct=float(target_pct),
        w_max=W_MAX,
    ).final_weight


def _tilt(c: float, kappa: float) -> float:
    return float(np.exp(kappa * (c - 0.5))) if np.isfinite(c) else 1.0


def target_weights(
    pillars: pd.DataFrame,
    vols: pd.DataFrame,
    kappa_equity: float = KAPPA_EQUITY,
    kappa_pm: float = KAPPA_PM,
    anchor: dict[str, float] | None = None,
    bounds: dict[str, tuple[float, float]] | None = None,
) -> pd.DataFrame:
    """
    Month-end target weights over (equity, pm, cash).

    Precious metals is driven by its own composite rather than being the residual of
    the equity call. That is not a stylistic choice: over the disclosed months the
    fund's equity and metals decisions are statistically independent, and the two
    sleeve returns are essentially uncorrelated, so a residual construction would
    impose a negative weight correlation with no basis in the data.
    """
    bnd = dict(BOUNDS if bounds is None else bounds)
    a = dict(ANCHOR if anchor is None else anchor)
    lo = np.array([bnd[s][0] for s in SLEEVE_ORDER])
    hi = np.array([bnd[s][1] for s in SLEEVE_ORDER])

    rows = []
    for t in pillars.index:
        c_eq = pillars.at[t, "composite"]
        c_pm = pillars.at[t, "pm_signal"]
        if not np.isfinite(c_eq) or t not in vols.index:
            rows.append([np.nan] * 5)
            continue

        b_eq = _risk_budget(vols.at[t, "equity_vol"], vols.at[t, "equity_target"])
        b_pm = _risk_budget(vols.at[t, "pm_vol"], vols.at[t, "pm_target"])

        u = np.array([
            a["equity"] * b_eq * _tilt(c_eq, kappa_equity),
            a["pm"] * b_pm * _tilt(c_pm, kappa_pm),
            a["cash"],
        ])
        w = project_box_simplex(u / u.sum(), lo, hi)
        rows.append([w[0], w[1], w[2], b_eq, b_pm])

    return pd.DataFrame(
        rows, index=pillars.index,
        columns=["equity", "pm", "cash", "risk_budget_eq", "risk_budget_pm"],
    )
