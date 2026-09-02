"""
VLRT v3 — walk-forward backtest harness.

The evaluation discipline here exists because v2 was never compared against a naive
baseline and therefore never discovered that it lost to one.

Design points that matter:

* **Execution lag.** The signal is computed from month-end closes; the trade executes
  at the *next trading day's* close. Weights are indexed by real trade dates, never by
  synthetic calendar month-ends, and the portfolio return on day ``t`` is formed from
  weights known at ``t-1``.
* **Costs are charged, not assumed away** — on realised turnover, at the moment of trade.
* **A block-shuffled-signal null** answers "how good would a signal with this
  autocorrelation look purely by luck?", which is the question that actually matters
  when the sample is short.
* **Paired circular block bootstrap** on daily returns for the Sharpe difference.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from src.vlrt.allocate import target_weights
from src.vlrt.allocate import SLEEVE_ORDER

TRADING_DAYS = 252
DEFAULT_COST_BPS = 15.0
DEFAULT_NO_TRADE_BAND = 0.02
BOOTSTRAP_BLOCK_DAYS = 21
BOOTSTRAP_DRAWS = 5000
NULL_DRAWS = 1000
NULL_BLOCK_MONTHS = 6
MIN_HOLDOUT_MONTHS = 12


@dataclass
class BacktestResult:
    name: str
    daily_returns: pd.Series
    equity_curve: pd.Series
    executed_weights: pd.DataFrame
    turnover: pd.Series
    metrics: dict[str, float] = field(default_factory=dict)


def sleeve_returns(sleeve_px: pd.DataFrame) -> pd.DataFrame:
    """Daily simple returns per sleeve, from the integrity-checked total-return frame."""
    return sleeve_px.astype(float).pct_change().dropna(how="all")


def metrics(returns: pd.Series, name: str = "") -> dict[str, float]:
    """Same field set as the repo's existing backtest scripts."""
    r = returns.dropna()
    if r.empty:
        return {"name": name}
    curve = (1.0 + r).cumprod()
    years = len(r) / TRADING_DAYS
    ann_ret = curve.iloc[-1] ** (1 / years) - 1 if years > 0 else np.nan
    ann_vol = r.std() * np.sqrt(TRADING_DAYS)
    dd = (curve / curve.cummax() - 1.0).min()
    return {
        "name": name,
        "total_return_pct": (curve.iloc[-1] - 1.0) * 100,
        "ann_return_pct": ann_ret * 100,
        "ann_vol_pct": ann_vol * 100,
        "sharpe": (ann_ret / ann_vol) if ann_vol > 0 else np.nan,
        "max_drawdown_pct": dd * 100,
        "n_days": len(r),
    }


def _apply_no_trade_band(targets: pd.DataFrame, band: float) -> pd.DataFrame:
    """Hold the previous vector until any sleeve drifts more than ``band`` from target."""
    held: np.ndarray | None = None
    rows = []
    for _, row in targets.iterrows():
        tgt = row.to_numpy(dtype=float)
        if np.isnan(tgt).any():
            rows.append([np.nan] * len(tgt))
            continue
        if held is None or np.abs(tgt - held).max() > band:
            held = tgt
        rows.append(list(held))
    return pd.DataFrame(rows, index=targets.index, columns=targets.columns)


def _execution_dates(month_ends: pd.DatetimeIndex, daily_index: pd.DatetimeIndex) -> pd.DatetimeIndex:
    """First trading day strictly after each month-end signal date."""
    pos = daily_index.searchsorted(month_ends, side="right")
    keep = pos < len(daily_index)
    return pd.DatetimeIndex(daily_index[pos[keep]]), keep


def run_backtest(
    weights_m: pd.DataFrame,
    sleeve_px: pd.DataFrame,
    name: str = "VLRT",
    cost_bps: float = DEFAULT_COST_BPS,
    no_trade_band: float = DEFAULT_NO_TRADE_BAND,
    start: str | None = None,
    end: str | None = None,
) -> BacktestResult:
    sleeves = list(SLEEVE_ORDER)
    rets = sleeve_returns(sleeve_px)

    targets = weights_m[sleeves].dropna()
    held = _apply_no_trade_band(targets, no_trade_band)

    exec_dates, keep = _execution_dates(held.index, rets.index)
    held = held.loc[keep]
    held.index = exec_dates

    turnover = held.diff().abs().sum(axis=1)
    turnover.iloc[0] = held.iloc[0].abs().sum()  # initial build

    w_daily = held.reindex(rets.index).ffill()
    # Return on day t uses the weight set at the close of t-1 — the look-ahead guard.
    port = (w_daily.shift(1) * rets[sleeves]).sum(axis=1, min_count=1)

    cost_daily = (turnover * cost_bps / 1e4).reindex(rets.index).fillna(0.0)
    port = port - cost_daily
    port = port.loc[w_daily.dropna().index.min():].dropna()

    if start:
        port = port.loc[start:]
    if end:
        port = port.loc[:end]

    m = metrics(port, name)
    years = m.get("n_days", 0) / TRADING_DAYS
    to_in_window = turnover.reindex(port.index).fillna(0.0)
    m["turnover_ann"] = float(to_in_window.sum() / years) if years > 0 else np.nan
    m["cost_drag_ann_pct"] = float(cost_bps / 1e4 * to_in_window.sum() / years * 100) if years > 0 else np.nan
    m["n_rebalances"] = int((to_in_window > 0).sum())

    return BacktestResult(
        name=name,
        daily_returns=port,
        equity_curve=(1.0 + port).cumprod(),
        executed_weights=held,
        turnover=turnover,
        metrics=m,
    )


def static_weights(month_ends: pd.DatetimeIndex, alloc: dict[str, float]) -> pd.DataFrame:
    return pd.DataFrame(
        {k: alloc[k] for k in ("equity", "pm", "cash")}, index=month_ends
    )


# ── Statistics ────────────────────────────────────────────────────────────────

def _sharpe(r: np.ndarray) -> float:
    sd = r.std()
    return float(r.mean() / sd * np.sqrt(TRADING_DAYS)) if sd > 0 else np.nan


def _circular_block_indices(n: int, block: int, rng: np.random.Generator) -> np.ndarray:
    n_blocks = int(np.ceil(n / block))
    starts = rng.integers(0, n, size=n_blocks)
    idx = (starts[:, None] + np.arange(block)[None, :]).ravel() % n
    return idx[:n]


def bootstrap_sharpe_diff(
    strat: pd.Series,
    bench: pd.Series,
    block: int = BOOTSTRAP_BLOCK_DAYS,
    draws: int = BOOTSTRAP_DRAWS,
    seed: int = 7,
    alpha: float = 0.05,
    n_comparisons: int = 1,
) -> dict[str, float]:
    """
    Paired circular block bootstrap of the Sharpe difference.

    Both series are resampled with the *same* indices so their contemporaneous
    relationship survives the resampling. ``n_comparisons`` applies a Bonferroni
    correction: the reported CI is the family-wise ``alpha / n_comparisons`` interval,
    not the nominal 95% one, so testing against several benchmarks does not manufacture
    significance by attrition.
    """
    j = strat.dropna().index.intersection(bench.dropna().index)
    a, b = strat.loc[j].to_numpy(), bench.loc[j].to_numpy()
    n = len(a)
    adj_alpha = alpha / max(n_comparisons, 1)
    if n < block * 3:
        return {
            "diff": np.nan, "lo": np.nan, "hi": np.nan, "p_two_sided": np.nan, "n": n,
            "alpha": alpha, "n_comparisons": n_comparisons, "adj_alpha": adj_alpha,
        }

    rng = np.random.default_rng(seed)
    obs = _sharpe(a) - _sharpe(b)
    diffs = np.empty(draws)
    for i in range(draws):
        k = _circular_block_indices(n, block, rng)
        diffs[i] = _sharpe(a[k]) - _sharpe(b[k])
    lo, hi = np.percentile(diffs, [adj_alpha / 2 * 100, (1 - adj_alpha / 2) * 100])
    # p-value for H0: no difference, from the bootstrap distribution recentred on zero.
    centred = diffs - diffs.mean()
    p = float((np.abs(centred) >= abs(obs)).mean())
    return {
        "diff": float(obs), "lo": float(lo), "hi": float(hi), "p_two_sided": p, "n": n,
        "alpha": alpha, "n_comparisons": n_comparisons, "adj_alpha": adj_alpha,
    }


def _block_shuffle(s: pd.Series, block: int, rng: np.random.Generator) -> pd.Series:
    v = s.to_numpy(dtype=float)
    idx = _circular_block_indices(len(v), block, rng)
    return pd.Series(v[idx], index=s.index, name=s.name)


def shuffled_signal_null(
    pillars: pd.DataFrame,
    vols: pd.DataFrame,
    sleeve_px: pd.DataFrame,
    draws: int = NULL_DRAWS,
    block_months: int = NULL_BLOCK_MONTHS,
    seed: int = 11,
    **bt_kwargs,
) -> np.ndarray:
    """
    Sharpe distribution from signals that keep their autocorrelation but lose their
    alignment to returns. This is the bar a short-sample strategy must actually clear.
    """
    rng = np.random.default_rng(seed)
    out = np.empty(draws)
    base = pillars.copy()
    for i in range(draws):
        p = base.copy()
        p["composite"] = _block_shuffle(base["composite"], block_months, rng)
        p["pm_signal"] = _block_shuffle(base["pm_signal"], block_months, rng)
        w = target_weights(p, vols)
        res = run_backtest(w, sleeve_px, name=f"null{i}", **bt_kwargs)
        out[i] = res.metrics.get("sharpe", np.nan)
    return out
