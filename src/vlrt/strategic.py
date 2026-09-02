"""
VLRT v3 — annual strategic-tilt variant.

Motivated directly by measured evidence: ``v_px_vs_3y``'s information coefficient
roughly triples from a 3-month forward horizon (+0.30) to a 24-month one (+0.59) — the
textbook signature of CAPE-style valuation mean-reversion, real over years and unusable
for monthly timing because the volatility during the multi-year "waiting period" swamps
the edge. Confirmed empirically: sweeping the monthly allocator's tilt strength from
0.5x to 4x found no sweet spot, only monotonic decay — the problem is the horizon, not
the tilt magnitude.

This variant rebalances **annually** with a **smaller** tilt (+-5pp equity, +-4pp metals
— half the monthly version's) and is judged on multi-year terminal wealth and rolling
CAGR rather than monthly Sharpe, since Sharpe penalises exactly the volatility a slow
reversion trade must tolerate to earn its edge.

Statistical honesty note: annual rebalancing over the ~2016-2026 window during which the
composite is valid yields roughly 10 decisions. That is too thin for a confident
hypothesis test on its own. This module reports descriptive comparisons — terminal
wealth, rolling-window CAGR — as the primary output. Any significance test built on top
of ~10 points should be read as suggestive, not confirmatory; the harness does not
compute one for that reason.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from src.vlrt.allocate import target_weights
from src.vlrt.backtest import TRADING_DAYS, BacktestResult

#: Half the monthly allocator's default kappa (1.4 / 1.0) -- a smaller tilt for a
#: signal meant to express conviction slowly, not to time monthly moves.
STRATEGIC_KAPPA_EQUITY = 0.7
STRATEGIC_KAPPA_PM = 0.5
#: December year-end signal, held through the following November.
ANNIVERSARY_MONTH = 12


def annual_decision_dates(monthly_index: pd.DatetimeIndex, month: int = ANNIVERSARY_MONTH) -> pd.DatetimeIndex:
    """One decision date per calendar year: the last observation in the given month."""
    s = pd.Series(monthly_index, index=monthly_index)
    at_month = s[s.index.month == month]
    if at_month.empty:
        return pd.DatetimeIndex([])
    return pd.DatetimeIndex(at_month.groupby(at_month.index.year).last().values)


def annual_target_weights(
    pillars: pd.DataFrame,
    vols: pd.DataFrame,
    kappa_equity: float = STRATEGIC_KAPPA_EQUITY,
    kappa_pm: float = STRATEGIC_KAPPA_PM,
    month: int = ANNIVERSARY_MONTH,
) -> pd.DataFrame:
    """
    One weight decision per year, at a smaller tilt curvature than the monthly allocator.

    Reuses the exact same composite and inverse-vol machinery as the monthly
    allocator (:func:`src.vlrt.allocate.target_weights`) — only the decision
    frequency and the tilt curvature differ, so any difference in outcome is
    attributable to horizon and sizing, not to a different signal or risk model.
    """
    dates = annual_decision_dates(pillars.index, month=month)
    dates = dates.intersection(vols.index).intersection(pillars.index)
    return target_weights(
        pillars.loc[dates], vols.loc[dates],
        kappa_equity=kappa_equity, kappa_pm=kappa_pm,
    )


def rolling_cagr(equity_curve: pd.Series, years: float) -> pd.Series:
    """Trailing N-year annualised return, sampled at each trading day."""
    window = int(round(years * TRADING_DAYS))
    if window >= len(equity_curve):
        return pd.Series(dtype=float)
    return (equity_curve / equity_curve.shift(window)) ** (1.0 / years) - 1.0


def summarise(results: dict[str, BacktestResult], rolling_years: tuple[float, ...] = (3.0, 5.0)) -> pd.DataFrame:
    """
    Terminal wealth and rolling-CAGR summary. Descriptive by design — see module
    docstring on why no significance test is attached to these numbers.
    """
    rows = []
    for name, r in results.items():
        row = {
            "name": name,
            "years": round(r.metrics["n_days"] / TRADING_DAYS, 2),
            "terminal_wealth": float(r.equity_curve.iloc[-1]),
            "cagr_pct": r.metrics["ann_return_pct"],
            "max_dd_pct": r.metrics["max_drawdown_pct"],
            "sharpe": r.metrics["sharpe"],
        }
        for y in rolling_years:
            rc = rolling_cagr(r.equity_curve, y) * 100
            row[f"roll{int(y)}y_median_pct"] = float(rc.median()) if len(rc) else np.nan
            row[f"roll{int(y)}y_min_pct"] = float(rc.min()) if len(rc) else np.nan
        rows.append(row)
    return pd.DataFrame(rows)
