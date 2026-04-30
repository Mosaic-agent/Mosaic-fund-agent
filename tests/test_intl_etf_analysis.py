"""
tests/test_intl_etf_analysis.py
────────────────────────────────
Unit tests for src/ui/intl_etf_analysis.py.

All compute functions accept plain DataFrames so no ClickHouse connection
is needed.  load_data() is tested via a mock pool.

Run: .venv/bin/python3 tests/test_intl_etf_analysis.py

Tests
─────
  1.  _premium_series  — correct formula, MASPTOP50 excluded
  2.  compute_performance — shape, required columns, numeric ranges
  3.  compute_performance — short series (< 60 rows) gracefully skipped
  4.  compute_premium_stats — stats df shape, anomaly_dates type
  5.  compute_regimes — 3 distinct labels, summary df shape
  6.  compute_correlation — symmetric matrix, diagonal = 1.0
  7.  compute_seasonality — 12 month rows, best/worst df shape
  8.  compute_lgbm — accuracy in [0,1], importance data non-empty
  9.  compute_drawdowns — episode detection for a known drawdown
  10. compute_drawdowns — flat series produces no episodes
  11. Chart functions — return plotly Figure without raising
  12. load_data — calls pool correctly (mock)
  13. run_full_analysis — all 15 keys present (mock pool)
"""
from __future__ import annotations

import os
import sys
import warnings
warnings.filterwarnings("ignore")

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import plotly.graph_objects as go

from src.ui.intl_etf_analysis import (
    INTL_ETFS,
    PREMIUM_EXCLUDE,
    _premium_series,
    compute_correlation,
    compute_drawdowns,
    compute_lgbm,
    compute_performance,
    compute_premium_stats,
    compute_regimes,
    compute_seasonality,
    correlation_heatmap,
    drawdown_gantt,
    lgbm_importance_chart,
    perf_bar_chart,
    premium_chart,
    regime_chart,
    seasonality_heatmap,
    run_full_analysis,
)

# ── Synthetic data helpers ────────────────────────────────────────────────────

N_DAYS = 756  # ~3 years of business days
RNG = np.random.default_rng(42)

def _price_series(name: str, drift: float = 0.0003, vol: float = 0.012,
                  start: float = 100.0) -> pd.Series:
    log_ret = drift + vol * RNG.standard_normal(N_DAYS)
    prices = start * np.exp(np.cumsum(log_ret))
    idx = pd.bdate_range(end=date.today(), periods=N_DAYS)
    idx.name = "date"
    return pd.Series(prices, index=idx, name=name)


def _make_price_wide(etfs=None, include_usdinr=True) -> pd.DataFrame:
    if etfs is None:
        etfs = INTL_ETFS
    cols = {sym: _price_series(sym) for sym in etfs}
    if include_usdinr:
        cols["USDINR"] = _price_series("USDINR", drift=0.0001, vol=0.004, start=83.0)
    df = pd.DataFrame(cols)
    df.index.name = "date"
    return df


def _make_nav_wide(price_wide: pd.DataFrame, premium_pct: float = 0.10) -> pd.DataFrame:
    etfs = [c for c in price_wide.columns if c in INTL_ETFS]
    # Add ±3% noise so the premium series isn't perfectly flat
    noise = 1 + 0.03 * RNG.standard_normal((len(price_wide), len(etfs)))
    nav = price_wide[etfs] / ((1 + premium_pct) * noise)
    # MASPTOP50 intentionally on a different unit base
    if "MASPTOP50" in nav.columns:
        nav["MASPTOP50"] = nav["MASPTOP50"] / 3.0
    nav.index.name = "date"
    return nav


# ── Test 1: _premium_series ───────────────────────────────────────────────────

def test_premium_series_formula():
    print("\n" + "=" * 60)
    print("TEST 1: _premium_series — formula and MASPTOP50 exclusion")
    print("=" * 60)

    pw = _make_price_wide()
    nw = _make_nav_wide(pw, premium_pct=0.15)
    prem = _premium_series(pw, nw)

    # MASPTOP50 must not appear
    assert "MASPTOP50" not in prem.columns, \
        "MASPTOP50 must be excluded from premium series"
    print("  ✓ MASPTOP50 excluded")

    # All other ETFs (that have NAV) should appear
    expected = {s for s in INTL_ETFS if s not in PREMIUM_EXCLUDE}
    assert expected == set(prem.columns), \
        f"Expected {expected}, got {set(prem.columns)}"
    print(f"  ✓ columns match: {sorted(prem.columns)}")

    # Premium ≈ 15% (synthetic data has exact 15% premium)
    for sym in prem.columns:
        mean_prem = prem[sym].dropna().mean()
        assert 12 < mean_prem < 18, \
            f"{sym}: expected ~15% premium, got {mean_prem:.2f}%"
    print("  ✓ premium values ≈ 15% for all symbols")


# ── Test 2: compute_performance — normal data ─────────────────────────────────

def test_performance_shape_and_ranges():
    print("\n" + "=" * 60)
    print("TEST 2: compute_performance — shape and numeric ranges")
    print("=" * 60)

    pw = _make_price_wide()
    df = compute_performance(pw)

    assert len(df) == len(INTL_ETFS), \
        f"Expected {len(INTL_ETFS)} rows, got {len(df)}"
    print(f"  ✓ {len(df)} rows (one per ETF)")

    required = {"ETF", "3Y Ret %", "1Y Ret %", "6M Ret %", "Ann Vol %", "Sharpe", "Max DD %", "Calmar"}
    assert required.issubset(df.columns), \
        f"Missing columns: {required - set(df.columns)}"
    print(f"  ✓ all required columns present")

    assert (df["Ann Vol %"] > 0).all(), "Annualised vol must be positive"
    assert (df["Max DD %"] <= 0).all(), "Max DD must be ≤ 0"
    assert df["Sharpe"].notna().all(), "Sharpe must not be NaN"
    print("  ✓ vol > 0, max DD ≤ 0, Sharpe not NaN")


# ── Test 3: compute_performance — short series skipped ───────────────────────

def test_performance_short_series_skipped():
    print("\n" + "=" * 60)
    print("TEST 3: compute_performance — short series (< 60 rows) skipped")
    print("=" * 60)

    short_idx = pd.bdate_range(end=date.today(), periods=30)
    pw = pd.DataFrame({
        "MAFANG":    pd.Series(np.linspace(100, 110, 30), index=short_idx),
        "MON100":    pd.Series(np.linspace(200, 220, 30), index=short_idx),
    })
    df = compute_performance(pw)
    assert df.empty, f"Expected empty df for short series, got {len(df)} rows"
    print("  ✓ returns empty DataFrame for series with < 60 rows")


# ── Test 4: compute_premium_stats ────────────────────────────────────────────

def test_premium_stats_shape():
    print("\n" + "=" * 60)
    print("TEST 4: compute_premium_stats — shape and anomaly_dates type")
    print("=" * 60)

    pw = _make_price_wide()
    nw = _make_nav_wide(pw)
    stats_df, prem_wide, anomaly_dates = compute_premium_stats(pw, nw)

    n_valid = len(INTL_ETFS) - len(PREMIUM_EXCLUDE)
    assert len(stats_df) == n_valid, \
        f"Expected {n_valid} rows, got {len(stats_df)}"
    print(f"  ✓ {len(stats_df)} rows (MASPTOP50 excluded)")

    assert set(anomaly_dates.keys()) == set(prem_wide.columns), \
        "anomaly_dates keys must match prem_wide columns"
    print("  ✓ anomaly_dates keys match premium columns")

    assert (stats_df["Anomaly Days"] >= 0).all(), "Anomaly count must be ≥ 0"
    total_anomalies = stats_df["Anomaly Days"].sum()
    assert total_anomalies > 0, "IsolationForest should flag at least some anomalies"
    print(f"  ✓ total anomaly days flagged: {total_anomalies}")


# ── Test 5: compute_regimes ───────────────────────────────────────────────────

def test_regimes_three_labels():
    print("\n" + "=" * 60)
    print("TEST 5: compute_regimes — 3 distinct labels, correct shape")
    print("=" * 60)

    pw = _make_price_wide()
    nw = _make_nav_wide(pw)
    prem = _premium_series(pw, nw)
    reg_df, regime_series = compute_regimes(pw, prem)

    assert len(reg_df) == len(INTL_ETFS), \
        f"Expected {len(INTL_ETFS)} rows, got {len(reg_df)}"
    print(f"  ✓ {len(reg_df)} rows in summary DataFrame")

    for sym, rs in regime_series.items():
        unique_labels = set(rs.unique())
        assert unique_labels == {0, 1, 2}, \
            f"{sym}: expected labels {{0,1,2}}, got {unique_labels}"
    print(f"  ✓ all {len(regime_series)} ETFs have exactly 3 regime labels (0=Bear,1=Sideways,2=Bull)")

    assert set(reg_df["Current"]).issubset({"Bear", "Sideways", "Bull"}), \
        "Current regime must be one of Bear/Sideways/Bull"
    print("  ✓ Current column values are valid regime names")


# ── Test 6: compute_correlation ───────────────────────────────────────────────

def test_correlation_matrix():
    print("\n" + "=" * 60)
    print("TEST 6: compute_correlation — symmetric, diagonal = 1.0")
    print("=" * 60)

    pw = _make_price_wide()
    corr_df, usdinr_df = compute_correlation(pw)

    # Square matrix
    assert corr_df.shape[0] == corr_df.shape[1], "Correlation matrix must be square"
    print(f"  ✓ square matrix {corr_df.shape}")

    # Diagonal = 1.0
    diag = np.diag(corr_df.values)
    assert np.allclose(diag, 1.0, atol=1e-9), f"Diagonal != 1.0: {diag}"
    print("  ✓ diagonal = 1.0")

    # Symmetric
    assert np.allclose(corr_df.values, corr_df.values.T, atol=1e-9), \
        "Matrix must be symmetric"
    print("  ✓ matrix is symmetric")

    # USDINR correlation table
    assert len(usdinr_df) == len(INTL_ETFS), \
        f"Expected {len(INTL_ETFS)} USDINR rows"
    assert {"ETF", "Full-Period", "Last 6M"}.issubset(usdinr_df.columns)
    print(f"  ✓ USDINR correlation: {len(usdinr_df)} rows")


# ── Test 7: compute_seasonality ───────────────────────────────────────────────

def test_seasonality_twelve_months():
    print("\n" + "=" * 60)
    print("TEST 7: compute_seasonality — 12 month rows, bw table shape")
    print("=" * 60)

    pw = _make_price_wide()
    med, bw = compute_seasonality(pw)

    assert len(med) == 12, f"Expected 12 month rows, got {len(med)}"
    print("  ✓ 12 monthly rows")

    MONTH_NAMES = {"Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"}
    assert set(med.index) == MONTH_NAMES, \
        f"Unexpected month labels: {set(med.index) - MONTH_NAMES}"
    print("  ✓ index contains all 12 month names")

    assert len(bw) == len(INTL_ETFS), \
        f"Expected {len(INTL_ETFS)} rows in best/worst table"
    assert {"Best Month", "Worst Month", "Best Ret %", "Worst Ret %"}.issubset(bw.columns)
    print(f"  ✓ best/worst table: {len(bw)} rows, required columns present")

    # Best ≥ Worst for each ETF
    assert (bw["Best Ret %"] >= bw["Worst Ret %"]).all(), \
        "Best Ret % must be >= Worst Ret %"
    print("  ✓ Best Ret % ≥ Worst Ret % for all ETFs")


# ── Test 8: compute_lgbm ─────────────────────────────────────────────────────

def test_lgbm_accuracy_range():
    print("\n" + "=" * 60)
    print("TEST 8: compute_lgbm — accuracy in [0,1], importance data non-empty")
    print("=" * 60)

    pw = _make_price_wide()
    nw = _make_nav_wide(pw)
    prem = _premium_series(pw, nw)
    lgbm_df, imp_data = compute_lgbm(pw, prem)

    assert len(lgbm_df) == len(INTL_ETFS), \
        f"Expected {len(INTL_ETFS)} rows, got {len(lgbm_df)}"
    print(f"  ✓ {len(lgbm_df)} rows in summary table")

    assert (lgbm_df["CV Accuracy"] >= 0).all() and (lgbm_df["CV Accuracy"] <= 100).all(), \
        f"Accuracy out of [0,100]: {lgbm_df['CV Accuracy'].values}"
    print(f"  ✓ all accuracies in [0, 100]%: {lgbm_df['CV Accuracy'].values}")

    assert len(imp_data) == len(INTL_ETFS), \
        f"Expected importance data for {len(INTL_ETFS)} ETFs"
    for sym, fi in imp_data.items():
        assert len(fi) > 0, f"Empty importance Series for {sym}"
        assert (fi >= 0).all(), f"Negative importance for {sym}"
    print(f"  ✓ importance data present and non-negative for all {len(imp_data)} ETFs")


# ── Test 9: compute_drawdowns — known drawdown ───────────────────────────────

def test_drawdowns_known_episode():
    print("\n" + "=" * 60)
    print("TEST 9: compute_drawdowns — detects a known -25% drawdown")
    print("=" * 60)

    idx = pd.bdate_range(end=date.today(), periods=300)
    # Construct: rises to 100, drops to 75 (-25%), recovers to 102
    prices = np.concatenate([
        np.linspace(80, 100, 100),   # rise
        np.linspace(100, 75, 80),    # -25% drawdown
        np.linspace(75, 102, 120),   # recovery
    ])
    pw = pd.DataFrame({"MAFANG": pd.Series(prices, index=idx)})
    dd = compute_drawdowns(pw)

    assert len(dd) >= 1, "Expected at least one drawdown episode"
    # Find the recovered episode (the series recovers to 102 which is above the 100 peak)
    recovered = dd[dd["Recovered"] == True]  # noqa: E712  — numpy bool needs ==
    assert len(recovered) >= 1, f"Expected at least one recovered episode, got:\n{dd}"
    ep = recovered.iloc[0]
    assert ep["Max DD %"] < -10, f"Max DD should be < -10%, got {ep['Max DD %']}"
    assert ep["Recovery Days"] > 0, "Recovery days must be positive"
    print(f"  ✓ detected drawdown: {ep['Max DD %']:.1f}%, recovered in {ep['Recovery Days']} days")


# ── Test 10: compute_drawdowns — flat series ──────────────────────────────────

def test_drawdowns_flat_series():
    print("\n" + "=" * 60)
    print("TEST 10: compute_drawdowns — flat series has no episodes")
    print("=" * 60)

    idx = pd.bdate_range(end=date.today(), periods=200)
    # Perfectly flat — no drawdown
    pw = pd.DataFrame({"MON100": pd.Series(np.ones(200) * 100.0, index=idx)})
    dd = compute_drawdowns(pw)
    assert dd.empty, f"Expected no drawdown episodes for flat series, got {len(dd)}"
    print("  ✓ flat series produces empty drawdown DataFrame")


# ── Test 11: Chart functions return go.Figure ─────────────────────────────────

def test_charts_return_figure():
    print("\n" + "=" * 60)
    print("TEST 11: chart functions return plotly Figure without raising")
    print("=" * 60)

    pw = _make_price_wide()
    nw = _make_nav_wide(pw)
    prem = _premium_series(pw, nw)

    perf_df = compute_performance(pw)
    _, prem_wide, anomaly_dates = compute_premium_stats(pw, nw)
    _, regime_series = compute_regimes(pw, prem)
    corr_df, _ = compute_correlation(pw)
    season_med, _ = compute_seasonality(pw)
    _, imp_data = compute_lgbm(pw, prem)
    dd_df = compute_drawdowns(pw)

    charts = {
        "perf_bar_chart":      perf_bar_chart(perf_df),
        "premium_chart":       premium_chart(prem_wide, anomaly_dates),
        "regime_chart":        regime_chart(pw, regime_series),
        "correlation_heatmap": correlation_heatmap(corr_df),
        "seasonality_heatmap": seasonality_heatmap(season_med),
        "lgbm_importance_chart": lgbm_importance_chart(imp_data),
        "drawdown_gantt":      drawdown_gantt(dd_df),
    }

    for name, fig in charts.items():
        assert isinstance(fig, go.Figure), \
            f"{name} must return go.Figure, got {type(fig)}"
        print(f"  ✓ {name} → go.Figure ({len(fig.data)} traces)")


# ── Test 12: load_data uses pool correctly (mock) ─────────────────────────────

def test_load_data_mock():
    print("\n" + "=" * 60)
    print("TEST 12: load_data — uses pool.acquire() as context manager")
    print("=" * 60)

    from src.ui.intl_etf_analysis import load_data

    # Build minimal fake query results
    today = date.today()
    days = pd.bdate_range(end=today, periods=10)
    price_rows = [(sym, d.date(), 100.0) for sym in INTL_ETFS + ["USDINR"] for d in days]
    nav_rows   = [(sym, d.date(), 90.0)  for sym in INTL_ETFS for d in days]

    mock_price_result = MagicMock()
    mock_price_result.result_rows = price_rows
    mock_nav_result = MagicMock()
    mock_nav_result.result_rows = nav_rows

    mock_ch = MagicMock()
    mock_ch.query.side_effect = [mock_price_result, mock_nav_result]
    mock_pool = MagicMock()
    mock_pool.acquire.return_value.__enter__ = MagicMock(return_value=mock_ch)
    mock_pool.acquire.return_value.__exit__ = MagicMock(return_value=False)

    price_wide, nav_wide = load_data(mock_pool)

    assert mock_ch.query.call_count == 2, \
        f"Expected 2 queries (prices + NAV), got {mock_ch.query.call_count}"
    print("  ✓ pool.acquire() called, 2 queries issued")

    assert "USDINR" in price_wide.columns, "USDINR must be in price_wide"
    assert set(INTL_ETFS).issubset(price_wide.columns), "All ETFs must appear in price_wide"
    assert set(INTL_ETFS).issubset(nav_wide.columns), "All ETFs must appear in nav_wide"
    print(f"  ✓ price_wide columns: {sorted(price_wide.columns)}")
    print(f"  ✓ nav_wide columns:   {sorted(nav_wide.columns)}")


# ── Test 13: run_full_analysis returns all 15 keys (mock) ─────────────────────

def test_run_full_analysis_all_keys():
    print("\n" + "=" * 60)
    print("TEST 13: run_full_analysis — all 15 output keys present (mock pool)")
    print("=" * 60)

    pw = _make_price_wide()
    nw = _make_nav_wide(pw)

    # Fake price/NAV rows that load_data would produce
    price_rows = [
        (sym, d.date(), float(pw[sym].iloc[i]))
        for sym in pw.columns
        for i, d in enumerate(pw.index)
    ]
    nav_rows = [
        (sym, d.date(), float(nw[sym].iloc[i]))
        for sym in nw.columns
        for i, d in enumerate(nw.index)
    ]

    mock_price_result = MagicMock()
    mock_price_result.result_rows = price_rows
    mock_nav_result = MagicMock()
    mock_nav_result.result_rows = nav_rows

    mock_ch = MagicMock()
    mock_ch.query.side_effect = [mock_price_result, mock_nav_result]
    mock_pool = MagicMock()
    mock_pool.acquire.return_value.__enter__ = MagicMock(return_value=mock_ch)
    mock_pool.acquire.return_value.__exit__ = MagicMock(return_value=False)

    R = run_full_analysis(mock_pool)

    expected_keys = {
        "perf_df", "perf_chart",
        "prem_stats", "prem_chart",
        "regime_df", "regime_chart",
        "corr_df", "usdinr_corr", "corr_chart",
        "season_med", "season_bw", "season_chart",
        "lgbm_df", "lgbm_chart",
        "dd_df", "dd_chart",
    }
    missing = expected_keys - set(R.keys())
    assert not missing, f"Missing keys: {missing}"
    print(f"  ✓ all {len(expected_keys)} keys present in result dict")

    figures = {k for k, v in R.items() if isinstance(v, go.Figure)}
    dfs = {k for k, v in R.items() if isinstance(v, pd.DataFrame)}
    print(f"  ✓ {len(figures)} Plotly Figure(s): {sorted(figures)}")
    print(f"  ✓ {len(dfs)} DataFrame(s): {sorted(dfs)}")


# ── Runner ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    test_premium_series_formula()
    test_performance_shape_and_ranges()
    test_performance_short_series_skipped()
    test_premium_stats_shape()
    test_regimes_three_labels()
    test_correlation_matrix()
    test_seasonality_twelve_months()
    test_lgbm_accuracy_range()
    test_drawdowns_known_episode()
    test_drawdowns_flat_series()
    test_charts_return_figure()
    test_load_data_mock()
    test_run_full_analysis_all_keys()

    print("\n" + "=" * 60)
    print("All 13 tests passed ✓")
    print("=" * 60)
