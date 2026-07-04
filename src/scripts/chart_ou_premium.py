#!/usr/bin/env python3
"""
src/scripts/chart_ou_premium.py
────────────────────────────────
Chart the OU mean-reversion strategy for an ETF premium series.

Panels:
  1. Premium time series + OU equilibrium μ, ±1σ/2σ bands, buy/sell zones,
     and buy/sell signal markers            (with a distribution side-panel
     showing where today's premium sits in the full history)
  2. Price vs iNAV, both rebased to 100 — shows what is actually driving
     the premium (price rallying vs iNAV outperforming)
  3. Rolling half-life (90d window) — regime-speed context
  4. Forward expected premium path from today's level

`build_ou_chart()` is the single shared chart-builder — both this CLI script
and src/tools/chart_tools.py:plot_ou_premium_chart() call it, so the two
entry points never drift out of sync.

Usage:
    python src/scripts/chart_ou_premium.py                    # MAFANG default
    python src/scripts/chart_ou_premium.py --symbol HNGSNGBEES
    python src/scripts/chart_ou_premium.py --lookback 180
"""
from __future__ import annotations

import argparse
import math
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from src.ml.ou_estimator import fit_ou, expected_premium, expected_reversion, prob_revert, OUState

# ── Shared color roles (fixed per entity, never re-cycled) ─────────────────
_C_PREMIUM = "#2196F3"    # the premium series itself
_C_MU = "#4CAF50"         # OU equilibrium / "good" reference
_C_ROLLING_MU = "#FF9800" # adaptive fair value
_C_SIGMA = "#FFC107"      # ±1σ reference lines
_C_BUY = "#00C853"        # buy zone / signal (status: good)
_C_SELL = "#FF1744"       # sell zone / signal (status: critical)
_C_TODAY = "#E91E63"      # "today" marker — always this color, everywhere
_C_HALFLIFE = "#9C27B0"
_C_PRICE = "#1565C0"
_C_INAV = "#00897B"


def _load_premium_data(symbol: str) -> pd.DataFrame:
    """Load daily EOD premium/price/iNAV from ClickHouse."""
    from src.db.pool import get_pool
    pool = get_pool()
    df = pool.query_df(f"""
        SELECT
            toDate(snapshot_at) AS trade_date,
            argMax(premium_discount_pct, snapshot_at) AS premium,
            argMax(market_price, snapshot_at) AS price,
            argMax(inav, snapshot_at) AS inav
        FROM market_data.inav_snapshots
        WHERE symbol = '{symbol}'
        GROUP BY trade_date
        ORDER BY trade_date ASC
    """)
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df = df.set_index("trade_date").sort_index()
    return df


def _rolling_ou_fit(premiums: np.ndarray, window: int = 90) -> list[OUState | None]:
    """Fit OU on a rolling window, returning one OUState per row (None for initial rows)."""
    results = [None] * len(premiums)
    for i in range(window, len(premiums)):
        chunk = premiums[i - window : i]
        results[i] = fit_ou(chunk, dt=1.0)
    return results


def build_ou_chart(df: pd.DataFrame, symbol: str):
    """
    Build the 4-panel OU premium strategy figure.

    Parameters
    ----------
    df : DataFrame indexed by trade_date with columns "premium", "price", "inav"
    symbol : ETF symbol, used in titles only

    Returns
    -------
    (fig, summary) where summary is a dict of every computed stat, so callers
    can print/report without recomputing anything.
    """
    premiums = df["premium"].values
    dates = df.index

    state = fit_ou(premiums, dt=1.0)
    if state is None:
        return None, {"error": f"OU fit failed for {symbol} — insufficient data or non-mean-reverting series."}

    mu, theta, sigma, half_life = state.mu, state.theta, state.sigma, state.half_life_days
    ou_std = sigma / math.sqrt(2 * theta)

    roll_window = 90
    rolling_fits = _rolling_ou_fit(premiums, window=roll_window)
    rolling_half_life = [s.half_life_days if s else np.nan for s in rolling_fits]
    rolling_mu = [s.mu if s else np.nan for s in rolling_fits]

    buy_threshold = mu - 1.5 * ou_std
    sell_threshold = mu + 1.5 * ou_std
    buy_mask = premiums < buy_threshold
    sell_mask = premiums > sell_threshold

    current = premiums[-1]
    percentile = float((premiums < current).mean() * 100)

    horizons = np.arange(0, 61)
    fwd_expected = [expected_premium(current, theta, mu, h) for h in horizons]
    fwd_upper = [expected_premium(current, theta, mu, h) + ou_std for h in horizons]
    fwd_lower = [expected_premium(current, theta, mu, h) - ou_std for h in horizons]

    prob_5d = prob_revert(current, theta, mu, sigma, mu, 5)
    prob_10d = prob_revert(current, theta, mu, sigma, mu, 10)
    prob_20d = prob_revert(current, theta, mu, sigma, mu, 20)
    rev_10d = expected_reversion(current, theta, mu, 10)

    has_price_data = "price" in df.columns and "inav" in df.columns and df["price"].notna().any() and df["inav"].notna().any()

    # ═══════════════════════════════════════════════════════════════════════
    # FIGURE — 4 panels: [premium+distribution] / [price vs iNAV] / [half-life] / [forward path]
    # ═══════════════════════════════════════════════════════════════════════
    fig = plt.figure(figsize=(16, 19), constrained_layout=True)
    gs = fig.add_gridspec(
        4, 4,
        height_ratios=[4, 2, 1.4, 2],
        width_ratios=[3, 3, 3, 1],
        hspace=0.12, wspace=0.08,
    )
    fig.suptitle(
        f"{symbol} — OU Mean-Reversion Strategy\n"
        f"θ={theta:.4f}  μ={mu:.2f}%  σ={sigma:.4f}  half-life={half_life:.1f}d  "
        f"R²={state.fit_r2:.3f}  (N={state.n_obs})",
        fontsize=13, fontweight="bold",
    )
    fig.get_layout_engine().set(h_pad=0.05)

    # ── Panel 1: Premium + OU bands + buy/sell zones + signals ──────────────
    ax1 = fig.add_subplot(gs[0, :3])
    ax1.plot(dates, premiums, color=_C_PREMIUM, linewidth=1.0, alpha=0.9, label="Premium (%)")
    ax1.plot(dates, rolling_mu, color=_C_ROLLING_MU, linewidth=1.2, alpha=0.7,
              linestyle="--", label=f"Rolling μ ({roll_window}d)")

    ax1.axhline(mu, color=_C_MU, linewidth=1.5, alpha=0.85)
    ax1.axhline(mu + ou_std, color=_C_SIGMA, linewidth=0.8, linestyle=":", alpha=0.6)
    ax1.axhline(mu - ou_std, color=_C_SIGMA, linewidth=0.8, linestyle=":", alpha=0.6)

    y_top = max(np.nanmax(premiums), sell_threshold) * 1.05
    y_bot = min(np.nanmin(premiums), buy_threshold) * 0.95 if np.nanmin(premiums) >= 0 else np.nanmin(premiums) * 1.05
    ax1.fill_between(dates, sell_threshold, y_top, color=_C_SELL, alpha=0.07, zorder=0)
    ax1.fill_between(dates, y_bot, buy_threshold, color=_C_BUY, alpha=0.07, zorder=0)
    ax1.set_ylim(y_bot, y_top)

    # Direct labels instead of a crowded legend — anchored at the right edge
    label_x = dates[-1] + (dates[-1] - dates[0]) * 0.012
    for y, text, color in [
        (mu, f"μ {mu:.1f}%", _C_MU),
        (mu + ou_std, f"+1σ {mu + ou_std:.1f}%", _C_SIGMA),
        (mu - ou_std, f"−1σ {mu - ou_std:.1f}%", _C_SIGMA),
    ]:
        ax1.annotate(text, xy=(dates[-1], y), xytext=(4, 0), textcoords="offset points",
                     fontsize=7.5, color=color, va="center", fontweight="bold", clip_on=False)
    ax1.text(0.985, 0.97, "SELL ZONE", transform=ax1.transAxes, fontsize=7.5,
              color=_C_SELL, alpha=0.8, ha="right", va="top", fontweight="bold")
    ax1.text(0.985, 0.03, "BUY ZONE", transform=ax1.transAxes, fontsize=7.5,
              color=_C_BUY, alpha=0.8, ha="right", va="bottom", fontweight="bold")

    if buy_mask.any():
        ax1.scatter(dates[buy_mask], premiums[buy_mask], color=_C_BUY, marker="^",
                    s=35, zorder=5, alpha=0.85, label="BUY signal")
    if sell_mask.any():
        ax1.scatter(dates[sell_mask], premiums[sell_mask], color=_C_SELL, marker="v",
                    s=35, zorder=5, alpha=0.85, label="SELL signal")

    ax1.scatter([dates[-1]], [current], color=_C_TODAY, marker="D", s=90, zorder=6,
                edgecolors="black", linewidth=0.8, label=f"Today {current:.2f}%")

    ax1.set_ylabel("Premium / Discount (%)", fontsize=11)
    ax1.legend(loc="upper left", fontsize=8, ncol=2, framealpha=0.9)
    ax1.grid(True, alpha=0.25)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%b '%y"))
    ax1.set_title("Premium Series + OU Equilibrium", fontsize=11, pad=8)

    # ── Panel 1b: distribution of premium history, aligned to panel 1's y-axis ─
    ax_hist = fig.add_subplot(gs[0, 3], sharey=ax1)
    ax_hist.hist(premiums, bins=30, orientation="horizontal", color=_C_PREMIUM, alpha=0.5)
    ax_hist.axhline(mu, color=_C_MU, linewidth=1.5, alpha=0.85)
    ax_hist.axhline(mu + ou_std, color=_C_SIGMA, linewidth=0.8, linestyle=":", alpha=0.6)
    ax_hist.axhline(mu - ou_std, color=_C_SIGMA, linewidth=0.8, linestyle=":", alpha=0.6)
    ax_hist.axhline(current, color=_C_TODAY, linewidth=1.8, alpha=0.9)
    ax_hist.set_title(f"Today: {percentile:.0f}th\npercentile", fontsize=8.5, pad=6)
    ax_hist.set_xlabel("Days", fontsize=8)
    plt.setp(ax_hist.get_yticklabels(), visible=False)
    ax_hist.tick_params(axis="y", length=0)
    ax_hist.tick_params(axis="x", labelsize=7)
    ax_hist.grid(True, alpha=0.2, axis="x")

    # ── Panel 2: Price vs iNAV, rebased to 100 (what's driving the premium) ──
    ax_decomp = fig.add_subplot(gs[1, :])
    if has_price_data:
        price_idx = df["price"] / df["price"].iloc[0] * 100
        inav_idx = df["inav"] / df["inav"].iloc[0] * 100
        ax_decomp.plot(dates, price_idx, color=_C_PRICE, linewidth=1.2, label="Market price (indexed=100)")
        ax_decomp.plot(dates, inav_idx, color=_C_INAV, linewidth=1.2, label="iNAV / fair value (indexed=100)")
        ax_decomp.fill_between(dates, price_idx, inav_idx,
                                where=(price_idx >= inav_idx), color=_C_SELL, alpha=0.12, interpolate=True)
        ax_decomp.fill_between(dates, price_idx, inav_idx,
                                where=(price_idx < inav_idx), color=_C_BUY, alpha=0.12, interpolate=True)
        ax_decomp.set_ylabel("Indexed level", fontsize=10)
        ax_decomp.legend(loc="upper left", fontsize=8, ncol=2, framealpha=0.9)
        ax_decomp.set_title(
            "Price vs iNAV (rebased to 100) — gap between the lines is the premium",
            fontsize=10, pad=5,
        )
    else:
        ax_decomp.text(0.5, 0.5, "Price / iNAV data not available", ha="center", va="center",
                        transform=ax_decomp.transAxes, fontsize=10, color="gray")
        ax_decomp.set_title("Price vs iNAV (rebased to 100)", fontsize=10, pad=5)
    ax_decomp.grid(True, alpha=0.25)
    ax_decomp.xaxis.set_major_formatter(mdates.DateFormatter("%b '%y"))

    # ── Panel 3: Rolling half-life ───────────────────────────────────────────
    ax2 = fig.add_subplot(gs[2, :])
    ax2.plot(dates, rolling_half_life, color=_C_HALFLIFE, linewidth=1.2, alpha=0.8)
    ax2.axhline(half_life, color=_C_HALFLIFE, linewidth=1.0, linestyle="--", alpha=0.5,
                label=f"Full-period HL = {half_life:.1f}d")
    ax2.fill_between(dates, 0, rolling_half_life, color=_C_HALFLIFE, alpha=0.15)
    ax2.set_ylabel("Half-life (days)", fontsize=10)
    ax2.set_title(f"Rolling OU Half-life ({roll_window}d window) — how fast the regime reverts", fontsize=10, pad=5)
    ax2.legend(loc="upper right", fontsize=8)
    ax2.grid(True, alpha=0.25)
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%b '%y"))
    ax2.set_ylim(bottom=0)

    # ── Panel 4: Forward expected path ───────────────────────────────────────
    ax3 = fig.add_subplot(gs[3, :])
    ax3.plot(horizons, fwd_expected, color=_C_PREMIUM, linewidth=2.0, label="E[Premium]")
    ax3.fill_between(horizons, fwd_lower, fwd_upper, color=_C_PREMIUM, alpha=0.15, label="±1σ∞ band")
    ax3.axhline(mu, color=_C_MU, linewidth=1.5, alpha=0.6, label=f"μ = {mu:.2f}%")
    ax3.scatter([0], [current], color=_C_TODAY, marker="D", s=90, zorder=5, edgecolors="black")

    ax3.annotate(
        f"P(→μ in 5d) = {prob_5d:.0%}\n"
        f"P(→μ in 10d) = {prob_10d:.0%}\n"
        f"P(→μ in 20d) = {prob_20d:.0%}\n"
        f"E[Δprem 10d] = {rev_10d:+.2f}%",
        xy=(0.98, 0.06), xycoords="axes fraction", ha="right", va="bottom",
        fontsize=9,
        bbox=dict(boxstyle="round,pad=0.4", facecolor="#FFF9C4", alpha=0.9, edgecolor=_C_SIGMA),
    )

    ax3.set_xlabel("Days Forward", fontsize=11)
    ax3.set_ylabel("Expected Premium (%)", fontsize=10)
    ax3.set_title(f"Forward Path from Today ({current:.2f}% → μ = {mu:.2f}%)", fontsize=10, pad=5)
    ax3.legend(loc="upper left", fontsize=8)
    ax3.grid(True, alpha=0.25)

    summary = {
        "symbol": symbol,
        "current": current,
        "mu": mu,
        "theta": theta,
        "sigma": sigma,
        "half_life": half_life,
        "ou_std": ou_std,
        "r2": state.fit_r2,
        "n_obs": state.n_obs,
        "buy_threshold": buy_threshold,
        "sell_threshold": sell_threshold,
        "percentile": percentile,
        "prob_5d": prob_5d,
        "prob_10d": prob_10d,
        "prob_20d": prob_20d,
        "rev_10d": rev_10d,
        "e_5d": expected_premium(current, theta, mu, 5),
        "e_10d": expected_premium(current, theta, mu, 10),
        "e_20d": expected_premium(current, theta, mu, 20),
    }
    return fig, summary


def build_chart(symbol: str = "MAFANG", lookback: int = 365, save_path: str | None = None):
    """Load data, build the chart, save it, and print a text summary (CLI entry point)."""
    df = _load_premium_data(symbol)

    if lookback and lookback < len(df):
        df = df.iloc[-lookback:]

    fig, summary = build_ou_chart(df, symbol)
    if fig is None:
        print(f"ERROR: {summary.get('error')}")
        return

    if save_path is None:
        save_path = f"output/reports/{symbol}_ou_premium_strategy.png"
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    fig.savefig(save_path, dpi=150, bbox_inches="tight", facecolor="white")
    print(f"✅ Chart saved: {save_path}")
    plt.close(fig)

    current, mu, ou_std = summary["current"], summary["mu"], summary["ou_std"]
    print(f"\n{'=' * 60}")
    print(f"  {symbol} OU Premium Strategy Summary")
    print(f"{'=' * 60}")
    print(f"  Current premium     : {current:.2f}%  ({summary['percentile']:.0f}th percentile of history)")
    print(f"  OU equilibrium (μ)  : {mu:.2f}%")
    print(f"  Gap (current − μ)   : {current - mu:+.2f}%")
    print(f"  OU speed (θ)        : {summary['theta']:.4f}")
    print(f"  Half-life           : {summary['half_life']:.1f} days")
    print(f"  Stationary σ∞       : {ou_std:.2f}%")
    print(f"  R² of AR(1) fit     : {summary['r2']:.3f}")
    print(f"  Buy threshold       : < {summary['buy_threshold']:.2f}%  (μ − 1.5σ∞)")
    print(f"  Sell threshold      : > {summary['sell_threshold']:.2f}%  (μ + 1.5σ∞)")
    print(f"")
    print(f"  E[premium in 5d]    : {summary['e_5d']:.2f}%")
    print(f"  E[premium in 10d]   : {summary['e_10d']:.2f}%")
    print(f"  E[premium in 20d]   : {summary['e_20d']:.2f}%")
    print(f"  E[Δprem 10d]        : {summary['rev_10d']:+.2f}%")
    print(f"  P(→μ in 5d)         : {summary['prob_5d']:.1%}")
    print(f"  P(→μ in 10d)        : {summary['prob_10d']:.1%}")
    print(f"  P(→μ in 20d)        : {summary['prob_20d']:.1%}")
    print(f"{'=' * 60}")

    signal = "🟢 BUY" if current < summary["buy_threshold"] else ("🔴 SELL/AVOID" if current > summary["sell_threshold"] else "⚪ HOLD")
    print(f"\n  Signal: {signal}")
    print(f"  Regime: Premium is {(current - mu) / ou_std:+.1f}σ from equilibrium\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OU Premium Strategy Chart")
    parser.add_argument("--symbol", default="MAFANG", help="ETF symbol (default: MAFANG)")
    parser.add_argument("--lookback", type=int, default=365, help="Days of history to display (default: 365)")
    parser.add_argument("--output", type=str, default=None, help="Output file path (default: output/reports/<SYMBOL>_ou_premium_strategy.png)")
    args = parser.parse_args()

    build_chart(symbol=args.symbol, lookback=args.lookback, save_path=args.output)
