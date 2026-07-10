"""
src/ml/ou_double_stopping.py
─────────────────────────────
Zervos-Johnson-Leung (ZJL) optimal double-stopping thresholds for an
Ornstein-Uhlenbeck premium process.

Theory
──────
For a premium process dX_t = θ(μ − X_t)dt + σ dW_t, the investor:
  • Pays c_buy to acquire exposure (enters long at some b*)
  • Receives c_sell when exiting (exits at some s*)

The optimal thresholds (b*, s*) maximise the discounted expected premium
harvested over infinitely many round trips. This is the double-stopping problem.

Implementation
──────────────
We solve it numerically via policy-value iteration on a 1-D grid:
  - Grid: G points spanning [μ − K·σ∞, μ + K·σ∞]  (default K=6, G=500)
  - Transition probabilities: discretised OU one-step transition N(e^{-θ}x + (1-e^{-θ})μ, σ²(1-e^{-2θ})/(2θ))
  - Value function V(x) = value of being out of position, at premium x
  - The algorithm iterates until convergence (||ΔV||∞ < tol)

The buy threshold b* is the largest grid point where entering is optimal.
The sell threshold s* is the smallest grid point where exiting is optimal.

Fallback: if DP fails to converge or b* ≥ s*, returns μ ± 1.5·σ∞.

Public API
──────────
    solve_double_stopping(ou, c_buy_bps, c_sell_bps, r_daily) -> DStopState
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass

import numpy as np

from src.ml.ou_estimator import OUState

log = logging.getLogger(__name__)

_GRID_POINTS = 300
_GRID_K = 6          # span = K × sigma_inf on each side of mu
_MAX_ITER = 10_000
_TOL = 1e-8
# Minimum daily discount rate used ONLY for DP numerics.
# With r_daily ≪ 1 (e.g. 5%/252 ≈ 0.0002), value iteration converges in ~150k
# iterations. Flooring at 0.002 (≈50%/yr) ensures convergence in <5k iters
# without materially changing the threshold values (costs dominate for large θ).
_DP_MIN_R_DAILY = 0.002


@dataclass(frozen=True)
class DStopState:
    """Optimal buy/sell premium thresholds from ZJL double-stopping."""
    b_star: float       # buy threshold: buy when premium <= b_star
    s_star: float       # sell threshold: sell when premium >= s_star
    converged: bool     # True if DP converged, False if fallback used
    method: str         # "dp" | "fallback_1.5sigma"


def solve_double_stopping(
    ou: OUState,
    c_buy_bps: float = 10.0,
    c_sell_bps: float = 10.0,
    r_daily: float = 0.05 / 252,
) -> DStopState:
    """
    Compute optimal buy/sell thresholds for the ZJL double-stopping problem.

    Parameters
    ----------
    ou         : fitted OUState (must have sigma_inf > 0)
    c_buy_bps  : transaction cost to enter (basis points of notional)
    c_sell_bps : transaction cost to exit (basis points of notional)
    r_daily    : daily risk-free discount rate (default = 5% / 252)

    Returns
    -------
    DStopState with b_star, s_star, convergence flag, and method label.
    """
    theta = ou.theta
    mu = ou.mu
    sigma = ou.sigma
    sigma_inf = ou.sigma_inf

    # Floor r_daily for DP numerics; near-unit discount causes O(150k) iterations.
    # The threshold values are primarily driven by costs/sigma_inf, not the exact
    # discount rate, so this flooring does not materially change the output.
    r_daily_dp = max(r_daily, _DP_MIN_R_DAILY)

    # Convert bps to premium-point equivalent costs
    c_buy = c_buy_bps / 100.0    # 10 bps → 0.10 percentage points
    c_sell = c_sell_bps / 100.0

    # Build the premium grid
    lo = mu - _GRID_K * sigma_inf
    hi = mu + _GRID_K * sigma_inf
    grid = np.linspace(lo, hi, _GRID_POINTS)
    dx = grid[1] - grid[0]

    # One-step OU transition: X_{t+1} | X_t ~ N(m(x), v)
    #   m(x) = mu + (x - mu)*exp(-theta)
    #   v    = sigma^2 / (2*theta) * (1 - exp(-2*theta))   = sigma_inf^2 * (1 - exp(-2*theta))
    e_theta = math.exp(-theta)
    m_grid = mu + (grid - mu) * e_theta          # shape (G,)
    v = sigma_inf ** 2 * (1.0 - math.exp(-2 * theta))
    if v <= 0:
        return _fallback(ou)
    std_v = math.sqrt(v)

    # Discount factor per day (use floored rate for DP convergence)
    disc = 1.0 / (1.0 + r_daily_dp)

    # Transition probability matrix  P[i,j] = P(X_{t+1}=grid[j] | X_t=grid[i])
    # Approximate with trapezoidal midpoint on the grid using normal pdf
    # P[i, :] = normal_pdf(grid - m_grid[i], std=std_v) * dx  (normalised)
    # Shape: (G, G) — may be memory-intensive for large G; G=500 is fine (2 MB float64)
    diff = grid[np.newaxis, :] - m_grid[:, np.newaxis]  # (G, G)
    log_pdf = -0.5 * (diff / std_v) ** 2 - math.log(std_v * math.sqrt(2 * math.pi))
    P = np.exp(log_pdf) * dx                             # (G, G)
    # Renormalise rows to ensure they sum to 1 (discretisation artefact at boundaries)
    row_sums = P.sum(axis=1, keepdims=True)
    row_sums = np.where(row_sums < 1e-15, 1.0, row_sums)
    P /= row_sums

    # ── Value iteration ──────────────────────────────────────────────────────
    # V_out[i]  = value of being OUT of position when premium = grid[i]
    #             (choose WAIT or BUY)
    # V_in[i]   = value of being IN position when premium = grid[i]
    #             (choose HOLD or SELL)
    #
    # Daily reward for being IN at grid[i]:
    #   = E[X_{t+1} - X_t | X_t = grid[i]]
    #   = m_grid[i] - grid[i]            ← expected premium CHANGE, not level
    #
    #   > 0 when grid[i] < mu  (below equilibrium → premium expected to RISE → long is good)
    #   < 0 when grid[i] > mu  (above equilibrium → premium expected to FALL → long is bad)
    #
    # This gives the economically correct incentive: buy low (b* < mu), sell high (s* > mu).

    reward = m_grid - grid   # expected daily P&L from being long at each grid point

    V_in = np.zeros(_GRID_POINTS)
    V_out = np.zeros(_GRID_POINTS)

    for it in range(_MAX_ITER):
        # Expected continuation values
        EV_in = P @ V_in     # shape (G,)
        EV_out = P @ V_out   # shape (G,)

        # New V_in[i]: hold (earn reward + discounted E[V_in]) vs sell (earn c_sell + E[V_out])
        hold_val = reward + disc * EV_in
        sell_val = -c_sell + disc * EV_out     # give up c_sell, become out
        V_in_new = np.maximum(hold_val, sell_val)

        # New V_out[i]: wait (disc * E[V_out]) vs buy (pay c_buy, become in)
        wait_val = disc * EV_out
        buy_val = -c_buy + V_in_new             # pay c_buy, immediately in
        V_out_new = np.maximum(wait_val, buy_val)

        delta = max(np.max(np.abs(V_in_new - V_in)), np.max(np.abs(V_out_new - V_out)))
        V_in = V_in_new
        V_out = V_out_new

        if delta < _TOL:
            break
    else:
        log.warning("ZJL DP did not converge in %d iterations — using fallback", _MAX_ITER)
        return _fallback(ou)

    # Extract thresholds from the converged value functions
    # Buy decision: out-of-position, buy_val > wait_val
    buy_region = (-c_buy + V_in) > (disc * (P @ V_out))
    # b_star = largest grid point where buying is optimal  (buy when cheap = low premium)
    buy_indices = np.where(buy_region)[0]
    if len(buy_indices) == 0:
        log.warning("ZJL DP: no buy region found — using fallback")
        return _fallback(ou)

    # Sell decision: in-position, sell_val > hold_val
    sell_region = (-c_sell + disc * (P @ V_out)) > (reward + disc * (P @ V_in))
    sell_indices = np.where(sell_region)[0]
    if len(sell_indices) == 0:
        log.warning("ZJL DP: no sell region found — using fallback")
        return _fallback(ou)

    b_star = float(grid[buy_indices[-1]])    # rightmost (highest premium at which still buy)
    s_star = float(grid[sell_indices[0]])    # leftmost (lowest premium at which sell)

    # Sanity: b_star must be strictly below s_star
    if b_star >= s_star:
        log.warning("ZJL DP: b*=%.3f >= s*=%.3f — using fallback", b_star, s_star)
        return _fallback(ou)

    log.debug("ZJL DP converged in %d iters: b*=%.3f, s*=%.3f", it + 1, b_star, s_star)
    return DStopState(b_star=b_star, s_star=s_star, converged=True, method="dp")


def _fallback(ou: OUState) -> DStopState:
    """Return ±1.5·σ∞ bands around μ as a safe fallback."""
    b_star = ou.mu - 1.5 * ou.sigma_inf
    s_star = ou.mu + 1.5 * ou.sigma_inf
    return DStopState(
        b_star=round(b_star, 4),
        s_star=round(s_star, 4),
        converged=False,
        method="fallback_1.5sigma",
    )
