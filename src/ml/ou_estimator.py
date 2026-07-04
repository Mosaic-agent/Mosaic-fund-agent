"""
src/ml/ou_estimator.py
──────────────────────
Ornstein-Uhlenbeck (OU) mean-reversion estimator for ETF premium/discount.

The OU process models premium as:
    dX_t = θ(μ − X_t)dt + σ dW_t

where:
    θ  = speed of mean reversion (higher → faster reversion)
    μ  = long-term equilibrium premium
    σ  = volatility of the premium process
    half_life = ln(2) / θ  (days until premium halves the gap to μ)

Estimation uses exact discrete-time OLS on the AR(1) representation:
    X_{t+1} = a + b·X_t + ε_t
    θ = −ln(b) / Δt
    μ = a / (1 − b)
    σ = std(ε) × √(−2·ln(b) / (Δt·(1 − b²)))

Public API
──────────
    fit_ou(premiums, dt=1.0) -> OUState | None
    expected_premium(current, theta, mu, horizon) -> float
    prob_revert(current, theta, mu, sigma, threshold, horizon) -> float
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass

import numpy as np

log = logging.getLogger(__name__)

_MIN_OBS = 30  # minimum observations for a meaningful OU fit


@dataclass(frozen=True)
class OUState:
    """Fitted OU parameters for a single symbol."""
    theta: float          # mean-reversion speed (per day)
    mu: float             # long-term equilibrium premium (%)
    sigma: float          # OU volatility (annualised if dt=1 day)
    half_life_days: float # ln(2) / theta
    n_obs: int            # number of observations used
    fit_r2: float         # R² of the AR(1) regression


def fit_ou(premiums: list[float] | np.ndarray, dt: float = 1.0) -> OUState | None:
    """
    Fit OU parameters via exact-discrete OLS on AR(1) representation.

    Parameters
    ----------
    premiums : time-ordered premium values (%, one per period)
    dt       : time step in days (1.0 for daily, 1/6.5 for hourly market hours)

    Returns
    -------
    OUState with fitted parameters, or None if fitting fails
    (insufficient data, non-stationary, or numerical issues).
    """
    x = np.asarray(premiums, dtype=np.float64)
    if len(x) < _MIN_OBS:
        log.warning("OU fit: only %d obs (need ≥ %d)", len(x), _MIN_OBS)
        return None

    # Remove NaN/Inf
    mask = np.isfinite(x)
    x = x[mask]
    if len(x) < _MIN_OBS:
        return None

    # AR(1) regression: X_{t+1} = a + b * X_t + ε
    x_lag = x[:-1]
    x_lead = x[1:]

    n = len(x_lag)
    sx = x_lag.sum()
    sy = x_lead.sum()
    sxx = (x_lag * x_lag).sum()
    sxy = (x_lag * x_lead).sum()

    denom = n * sxx - sx * sx
    if abs(denom) < 1e-15:
        log.warning("OU fit: degenerate (constant premium series)")
        return None

    b = (n * sxy - sx * sy) / denom
    a = (sy - b * sx) / n

    # b must be in (0, 1) for mean-reverting OU
    if b <= 0 or b >= 1:
        log.warning("OU fit: b=%.4f outside (0,1) — series is not mean-reverting", b)
        return None

    # Map AR(1) → continuous OU parameters
    theta = -math.log(b) / dt
    mu = a / (1 - b)

    # Residual volatility → OU sigma
    residuals = x_lead - (a + b * x_lag)
    var_eps = np.var(residuals, ddof=2)  # unbiased
    if var_eps <= 0:
        return None

    # σ² = var(ε) × (−2 ln(b)) / (Δt × (1 − b²))
    sigma_sq = var_eps * (-2 * math.log(b)) / (dt * (1 - b * b))
    if sigma_sq <= 0:
        return None
    sigma = math.sqrt(sigma_sq)

    half_life = math.log(2) / theta

    # R² of AR(1) fit
    ss_res = (residuals ** 2).sum()
    ss_tot = ((x_lead - x_lead.mean()) ** 2).sum()
    r2 = 1 - ss_res / ss_tot if ss_tot > 1e-15 else 0.0

    return OUState(
        theta=round(theta, 6),
        mu=round(mu, 4),
        sigma=round(sigma, 6),
        half_life_days=round(half_life, 2),
        n_obs=n + 1,
        fit_r2=round(r2, 4),
    )


def expected_premium(current: float, theta: float, mu: float, horizon_days: float) -> float:
    """
    Expected premium after `horizon_days` given current premium.

    E[X_h] = μ + (current − μ) × exp(−θ × h)
    """
    return mu + (current - mu) * math.exp(-theta * horizon_days)


def expected_reversion(current: float, theta: float, mu: float, horizon_days: float) -> float:
    """
    Expected change in premium over `horizon_days` (positive = premium moving toward μ).

    This replaces the naive `mean − current` formula with OU-adjusted expectation.
    Returns (μ − current) × (1 − exp(−θ × horizon)).
    """
    return (mu - current) * (1 - math.exp(-theta * horizon_days))


def prob_revert(
    current: float,
    theta: float,
    mu: float,
    sigma: float,
    threshold: float,
    horizon_days: float,
) -> float:
    """
    Probability that premium crosses `threshold` within `horizon_days`.

    Uses the OU conditional distribution:
        X_h | X_0 ~ N(E[X_h], Var[X_h])
        E[X_h] = μ + (X_0 − μ) exp(−θh)
        Var[X_h] = σ² / (2θ) × (1 − exp(−2θh))

    If current > threshold (premium above target):
        P(X_h ≤ threshold) = Φ((threshold − E[X_h]) / √Var[X_h])
    If current < threshold (premium below target):
        P(X_h ≥ threshold) = 1 − Φ((threshold − E[X_h]) / √Var[X_h])
    """
    from scipy.stats import norm

    e_xh = expected_premium(current, theta, mu, horizon_days)
    var_xh = (sigma ** 2) / (2 * theta) * (1 - math.exp(-2 * theta * horizon_days))
    if var_xh <= 0:
        return 0.0
    std_xh = math.sqrt(var_xh)

    z = (threshold - e_xh) / std_xh

    if current > threshold:
        # Premium is high, asking P(drops to or below threshold)
        return float(norm.cdf(z))
    else:
        # Premium is low, asking P(rises to or above threshold)
        return float(1 - norm.cdf(z))
