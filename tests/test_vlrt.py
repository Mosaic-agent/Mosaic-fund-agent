"""
Unit tests for the VLRT v3 package. No ClickHouse, no network — synthetic data only.

The look-ahead guard in ``test_expanding_rank_is_causal`` is the important one: it is
the property that v2 violated implicitly and that no amount of backtesting will reveal.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.vlrt.allocate import BOUNDS, SLEEVE_ORDER, project_box_simplex, target_weights
from src.vlrt.backtest import _apply_no_trade_band, _execution_dates, metrics, run_backtest
from src.vlrt.data import DataIntegrityError, check_sleeve_integrity, month_end_trade_dates, repair_price_glitches
from src.vlrt.pillars import build_pillars, expanding_rank
from src.vlrt.strategic import annual_decision_dates, annual_target_weights, rolling_cagr

rng = np.random.default_rng(0)


def _series(n=120, seed=1):
    r = np.random.default_rng(seed)
    idx = pd.date_range("2015-01-31", periods=n, freq="ME")
    return pd.Series(r.normal(size=n).cumsum(), index=idx)


# ── expanding_rank ────────────────────────────────────────────────────────────

def test_expanding_rank_is_causal():
    """Appending future observations must never change an earlier rank."""
    s = _series(120)
    full = expanding_rank(s)
    prefix = expanding_rank(s.iloc[:80])
    pd.testing.assert_series_equal(full.iloc[:80], prefix, check_names=False)


def test_expanding_rank_bounds_and_warmup():
    s = _series(100)
    r = expanding_rank(s, min_periods=24)
    assert r.iloc[:24].isna().all(), "no rank may be emitted before min_periods"
    v = r.dropna()
    assert ((v >= 0.0) & (v <= 1.0)).all()
    assert v.std() > 0.05, "rank must retain dynamic range"


def test_rank_saturates_far_less_than_a_fixed_band():
    """
    v2 scored via fixed np.clip bands, which pinned R to exactly 0.0 for eleven
    consecutive months. On the same drifting input a rank keeps its resolution.
    """
    r = np.random.default_rng(11)
    idx = pd.date_range("2015-01-31", periods=140, freq="ME")
    s = pd.Series(np.linspace(0, -8, 140) + r.normal(scale=0.5, size=140), index=idx)

    lo, hi = s.iloc[:24].min(), s.iloc[:24].max()          # a v2-style fixed band
    clipped = ((s - lo) / (hi - lo)).clip(0, 1).iloc[24:]
    ranked = expanding_rank(s, min_periods=24).dropna()

    assert (clipped == 0.0).mean() > 0.30, "fixed band should demonstrably saturate here"
    assert (ranked == 0.0).mean() < (clipped == 0.0).mean()
    assert ranked.nunique() > 10, "rank must retain resolution the fixed band loses"


# ── projection ────────────────────────────────────────────────────────────────

def test_projection_respects_box_and_simplex():
    lo = np.array([BOUNDS[s][0] for s in SLEEVE_ORDER])
    hi = np.array([BOUNDS[s][1] for s in SLEEVE_ORDER])
    for _ in range(500):
        w = rng.dirichlet(np.ones(3))
        p = project_box_simplex(w, lo, hi)
        assert abs(p.sum() - 1.0) < 1e-9
        assert (p >= lo - 1e-9).all() and (p <= hi + 1e-9).all()


def test_projection_is_continuous():
    """Clamp-then-renormalise jumps at a binding bound; projection must not."""
    lo = np.array([BOUNDS[s][0] for s in SLEEVE_ORDER])
    hi = np.array([BOUNDS[s][1] for s in SLEEVE_ORDER])
    base = np.array([0.55, 0.10, 0.35])
    prev = project_box_simplex(base, lo, hi)
    for eps in np.linspace(1e-4, 5e-2, 60):
        cur = project_box_simplex(base + np.array([eps, -eps, 0.0]), lo, hi)
        assert np.abs(cur - prev).max() < 5e-2, "discontinuity at a constraint boundary"
        prev = cur


def test_projection_rejects_infeasible_box():
    with pytest.raises(ValueError):
        project_box_simplex(np.array([0.4, 0.3, 0.3]), np.array([0.5, 0.5, 0.5]), np.ones(3))


# ── glitch repair and integrity ───────────────────────────────────────────────

def test_repair_price_glitches_unwinds_10x():
    idx = pd.date_range("2020-01-01", periods=10, freq="D")
    s = pd.Series([100, 101, 102, 1030, 1040, 1050, 106, 107, 108, 109.0], index=idx)
    fixed = repair_price_glitches(s)
    assert fixed.pct_change().abs().max() < 0.15
    assert fixed.iloc[0] == pytest.approx(100.0)


def test_integrity_gate_raises_on_broken_series():
    idx = pd.date_range("2020-01-01", periods=5, freq="D")
    df = pd.DataFrame({"equity": [100, 101, 60, 101, 102.0]}, index=idx)
    with pytest.raises(DataIntegrityError):
        check_sleeve_integrity(df)


def test_month_end_uses_real_trade_dates():
    cal = pd.DatetimeIndex(["2024-03-27", "2024-03-28", "2024-04-01", "2024-04-30"])
    me = month_end_trade_dates(cal)
    assert pd.Timestamp("2024-03-28") in me, "must pick the last trading day, not the 31st"
    assert pd.Timestamp("2024-03-31") not in me


# ── weights ───────────────────────────────────────────────────────────────────

def _fake_inputs(n=60):
    idx = pd.date_range("2020-01-31", periods=n, freq="ME")
    r = np.random.default_rng(3)
    pillars = pd.DataFrame({"composite": r.uniform(0, 1, n), "pm_signal": r.uniform(0, 1, n)}, index=idx)
    vols = pd.DataFrame({"equity_vol": r.uniform(8, 30, n), "pm_vol": r.uniform(8, 30, n)}, index=idx)
    vols["equity_target"] = 15.0
    vols["pm_target"] = 14.0
    return pillars, vols


def test_target_weights_are_valid_and_unfrozen():
    pillars, vols = _fake_inputs()
    w = target_weights(pillars, vols).dropna()
    assert abs(w[list(SLEEVE_ORDER)].sum(axis=1) - 1.0).max() < 1e-9
    assert (w["pm"] >= BOUNDS["pm"][0] - 1e-9).all(), "precious-metals floor must hold"
    assert (w[list(SLEEVE_ORDER)] >= -1e-12).all().all()
    assert (w["equity"].diff() == 0).mean() < 0.10, "must not re-create v2's frozen output"


def test_equity_weight_is_monotone_in_the_composite():
    _, vols = _fake_inputs(1)
    idx = vols.index
    prev = None
    for c in np.linspace(0.01, 0.99, 40):
        p = pd.DataFrame({"composite": [c], "pm_signal": [0.5]}, index=idx)
        eq = float(target_weights(p, vols)["equity"].iloc[0])
        if prev is not None:
            assert eq >= prev - 1e-9, "equity weight must be non-decreasing in the composite"
        prev = eq


# ── execution mechanics ───────────────────────────────────────────────────────

def test_execution_date_is_strictly_after_signal_date():
    cal = pd.DatetimeIndex(pd.date_range("2024-01-01", periods=40, freq="B"))
    signals = pd.DatetimeIndex([cal[5], cal[20]])
    ex, keep = _execution_dates(signals, cal)
    assert (ex > signals[keep]).all()


def test_no_trade_band_holds_previous_vector():
    idx = pd.date_range("2024-01-31", periods=4, freq="ME")
    tgt = pd.DataFrame(
        {"equity": [0.50, 0.505, 0.60, 0.601], "pm": [0.25, 0.25, 0.20, 0.20], "cash": [0.25, 0.245, 0.20, 0.199]},
        index=idx,
    )
    held = _apply_no_trade_band(tgt, band=0.02)
    assert held.iloc[1].equals(held.iloc[0]), "sub-band move must not trade"
    assert not held.iloc[2].equals(held.iloc[1]), "above-band move must trade"


def test_backtest_has_no_lookahead():
    """A weight set at month-end t must not earn t's own return."""
    cal = pd.DatetimeIndex(pd.date_range("2024-01-01", periods=200, freq="B"))
    px = pd.DataFrame(
        {"equity": np.linspace(100, 130, 200), "pm": np.linspace(100, 110, 200), "cash": np.linspace(100, 104, 200)},
        index=cal,
    )
    me = month_end_trade_dates(cal)
    w = pd.DataFrame({"equity": 0.5, "pm": 0.3, "cash": 0.2}, index=me)
    res = run_backtest(w, px, "t")
    assert res.daily_returns.index.min() > me[0], "first P&L must post-date the first signal"
    assert np.isfinite(res.metrics["sharpe"])


def test_metrics_on_empty_returns_is_safe():
    assert metrics(pd.Series(dtype=float), "empty") == {"name": "empty"}


# ── annual strategic-tilt variant ────────────────────────────────────────────

def test_annual_decision_dates_one_per_year():
    idx = pd.date_range("2016-01-31", periods=96, freq="ME")  # 8 years
    dates = annual_decision_dates(idx, month=12)
    assert len(dates) == 8
    assert (dates.month == 12).all()
    assert dates.is_unique and dates.is_monotonic_increasing


def test_annual_decision_dates_empty_when_month_absent():
    idx = pd.date_range("2016-01-31", periods=6, freq="ME")  # Jan-Jun only
    assert len(annual_decision_dates(idx, month=12)) == 0


def test_annual_target_weights_valid_and_fewer_than_monthly():
    pillars, vols = _fake_inputs(96)
    annual_w = annual_target_weights(pillars, vols).dropna()
    assert len(annual_w) <= 8
    assert abs(annual_w[list(SLEEVE_ORDER)].sum(axis=1) - 1.0).max() < 1e-9
    assert (annual_w["pm"] >= BOUNDS["pm"][0] - 1e-9).all()


def test_rolling_cagr_has_warmup_nans():
    idx = pd.date_range("2016-01-01", periods=1000, freq="B")
    curve = pd.Series(np.linspace(1.0, 2.0, 1000), index=idx)
    rc = rolling_cagr(curve, years=3.0)
    assert len(rc.dropna()) < len(curve), "the trailing window must leave a warmup NaN region"
    assert rc.dropna().between(-1, 5).all()


def test_rolling_cagr_empty_when_window_exceeds_history():
    idx = pd.date_range("2016-01-01", periods=100, freq="B")
    curve = pd.Series(np.linspace(1.0, 1.1, 100), index=idx)
    assert rolling_cagr(curve, years=3.0).empty
