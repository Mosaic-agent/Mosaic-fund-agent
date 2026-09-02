"""
VLRT v3 CLI — volatility-targeted allocator with a bounded valuation tilt.

    python src/scripts/portfolio/vlrt_v3.py
    python src/scripts/portfolio/vlrt_v3.py --start 2018-11-01 --holdout 2025-08-01
    python src/scripts/portfolio/vlrt_v3.py --fast          # skip the shuffled-signal null

Hold-out default matches the approved plan: train on data through 2025-07 (inclusive),
test on the untouched 2025-08 -> 2026-08 window (~13 months, never used to fit anything).
"""

from __future__ import annotations

import argparse
import os
import sys

sys.path.append(os.getcwd())

import numpy as np
import pandas as pd
from rich.panel import Panel

from src.vlrt import report as rp
from src.vlrt.allocate import SLEEVE_ORDER, sleeve_vols, target_weights
from src.vlrt.backtest import (
    MIN_HOLDOUT_MONTHS, bootstrap_sharpe_diff, metrics, run_backtest, sleeve_returns,
    shuffled_signal_null, static_weights,
)
from src.vlrt.data import SCHEME_CODES, FUND_MULTI_ASSET, load_all, load_fund_nav_returns
from src.vlrt.pillars import build_pillars
from src.vlrt.replicate import evaluate

ANCHOR_BENCH = {"equity": 0.55, "pm": 0.20, "cash": 0.25}
TRADING_DAYS = 252


def _window(label: str, w, flat_w, data, nav_ret, start, end, draws, min_days=60):
    """
    Every benchmark is sliced to the MODEL's actual earned-return window
    (``model.daily_returns`` bounds), not the raw --start/--end args. The model's own
    weights are NaN until the composite's warmup completes (V needs a 36-month rolling
    mean plus a 24-month rank warmup, so it starts materially later than the requested
    --start), so slicing benchmarks to the requested window instead of the model's real
    window would silently compare different periods — the bootstrap stats intersect
    indices correctly regardless, but the displayed CAGR/Sharpe/Days would not match.
    """
    rets = sleeve_returns(data.sleeve_px)
    model = run_backtest(w, data.sleeve_px, "VLRT v3", start=start, end=end)
    if model.metrics.get("n_days", 0) < min_days:
        rp.console.print(f"[yellow]{label}: too few days to evaluate — skipped[/yellow]")
        return None, {}

    m_start, m_end = model.daily_returns.index.min(), model.daily_returns.index.max()
    benches = [
        run_backtest(static_weights(w.dropna().index, ANCHOR_BENCH), data.sleeve_px,
                     "Static 55/20/25", start=m_start, end=m_end),
        run_backtest(static_weights(w.dropna().index, {"equity": 1 / 3, "pm": 1 / 3, "cash": 1 / 3}),
                     data.sleeve_px, "Equal weight 1/3", start=m_start, end=m_end),
        run_backtest(flat_w, data.sleeve_px, "B4 vol-only", start=m_start, end=m_end),
    ]
    rows = [b.metrics for b in benches]
    eq = rets["equity"].loc[m_start:m_end]
    rows.append(metrics(eq, "100% NIFTYBEES"))

    nav_bench_returns = None
    if nav_ret is not None and not nav_ret.empty:
        nav_slice = nav_ret.loc[m_start:m_end]
        if len(nav_slice) >= min_days:
            nav_bench_returns = nav_slice
            rows.append(metrics(nav_slice, f"Fund NAV ({SCHEME_CODES[FUND_MULTI_ASSET]})"))

    rows.append({**model.metrics, "is_model": True})
    rp.render_performance(rows, f"{label}  ({model.metrics['n_days']} trading days)")

    n_comparisons = len(benches) + (1 if nav_bench_returns is not None else 0)
    comps = {
        b.metrics["name"]: bootstrap_sharpe_diff(model.daily_returns, b.daily_returns,
                                                 draws=draws, n_comparisons=n_comparisons)
        for b in benches
    }
    if nav_bench_returns is not None:
        name = f"Fund NAV ({SCHEME_CODES[FUND_MULTI_ASSET]})"
        comps[name] = bootstrap_sharpe_diff(model.daily_returns, nav_bench_returns,
                                            draws=draws, n_comparisons=n_comparisons)
        rp.console.print(
            f"[dim]note: the fund-NAV comparison is not apples-to-apples — the fund is net of TER, "
            f"may hold silver the model does not, and discloses a short single-stock-futures overlay "
            f"in some months.[/dim]"
        )

    yrs = model.metrics["n_days"] / TRADING_DAYS
    sr = model.metrics["sharpe"]
    mds = 1.96 * np.sqrt((1 + 0.5 * sr**2) / yrs) * np.sqrt(2 * (1 - 0.95)) if yrs > 0 else np.nan
    rp.render_comparisons(comps, mds)
    return model, comps


def _null_p_value(pillars, vols, data, start, end, draws, target_sharpe):
    null = shuffled_signal_null(pillars, vols, data.sleeve_px, draws=draws, start=start, end=end)
    null = null[np.isfinite(null)]
    if len(null) == 0:
        return None, null
    p = (1 + int((null >= target_sharpe).sum())) / (1 + len(null))
    return p, null


def main() -> None:
    ap = argparse.ArgumentParser(description="VLRT v3")
    ap.add_argument("--start", default="2018-11-01", help="first evaluation date")
    ap.add_argument("--holdout", default="2025-08-01",
                     help="train/hold-out boundary (default matches the approved plan: "
                          "train through 2025-07, hold out 2025-08 -> end)")
    ap.add_argument("--draws", type=int, default=2000)
    ap.add_argument("--null-draws", type=int, default=300)
    ap.add_argument("--fast", action="store_true", help="skip the shuffled-signal null")
    ap.add_argument("--no-replicate", action="store_true")
    args = ap.parse_args()

    rp.console.print(Panel(
        "[bold cyan]VLRT v3[/bold cyan] — volatility-targeted allocator with a bounded valuation tilt.\n"
        "Sleeves: equity (NIFTYBEES) / pm (GOLDBEES) / cash (liquid-fund NAV).\n"
        "[dim]Returns at monthly frequency are near-unpredictable with this data; volatility is not.\n"
        "The tilt is therefore capped at what a measured IC near 0.29 justifies.[/dim]",
        border_style="cyan"))

    data = load_all()
    pillars = build_pillars(data.monthly)
    vols = sleeve_vols(data.daily, data.sleeve_px, data.monthly.index)
    w = target_weights(pillars, vols)
    flat_w = target_weights(pillars.assign(composite=0.5, pm_signal=0.5), vols)
    av = w.dropna()

    try:
        nav_ret = load_fund_nav_returns(SCHEME_CODES[FUND_MULTI_ASSET])
    except Exception as exc:  # live network fetch; degrade to no NAV benchmark, not a crash
        rp.console.print(f"[yellow]fund NAV benchmark unavailable: {exc}[/yellow]")
        nav_ret = None

    for n in data.notes:
        rp.console.print(f"[dim]data:[/dim] {n}")

    holdout_days = (av.index.max() - pd.Timestamp(args.holdout)).days if len(av) else 0
    if holdout_days < MIN_HOLDOUT_MONTHS * 30:
        rp.console.print(
            f"[yellow]warning: hold-out window is only ~{holdout_days // 30} months "
            f"(< {MIN_HOLDOUT_MONTHS} required) — gate verdicts below will be unreliable.[/yellow]"
        )

    model_full, comps_full = _window("FULL SAMPLE", w, flat_w, data, nav_ret, args.start, None, args.draws)
    if model_full is None:
        return
    _window(f"TRAIN  (< {args.holdout})", w, flat_w, data, nav_ret, args.start, args.holdout, args.draws)
    model_ho, comps_ho = _window(f"HOLD-OUT  (>= {args.holdout})", w, flat_w, data, nav_ret,
                                  args.holdout, None, args.draws, min_days=30)

    rp.render_pillars(pillars.loc[av.index.min():], av)

    frozen = float((model_full.executed_weights["equity"].diff() == 0).mean() * 100)
    n_bets = rp.independent_bets(av["equity"])
    refusals = rp.build_refusals(n_bets, comps_ho or comps_full, frozen, data.notes)

    p_null_full = p_null_holdout = None
    if not args.fast:
        rp.console.print(f"[dim]running {args.null_draws} block-shuffled-signal draws (full sample)...[/dim]")
        p_null_full, null_full = _null_p_value(pillars, vols, data, args.start, None,
                                                args.null_draws, model_full.metrics["sharpe"])
        if p_null_full is not None:
            rp.console.print(Panel(
                f"Shuffled-signal null over {len(null_full)} draws: median Sharpe {np.median(null_full):.3f}, "
                f"95th pct {np.percentile(null_full, 95):.3f}\n"
                f"VLRT v3 Sharpe {model_full.metrics['sharpe']:.3f}   (p = {p_null_full:.3f})",
                title="[bold]Block-shuffled-signal null — full sample[/bold]", border_style="magenta"))
            if p_null_full > 0.05:
                refusals.append(rp.Refusal(
                    f"Sharpe not distinguishable from the shuffled-signal null (p={p_null_full:.3f})",
                    "any claim of timing skill",
                    "the allocation's result does not require the signal to be informative"))

        if model_ho is not None:
            rp.console.print(f"[dim]running {args.null_draws} block-shuffled-signal draws (hold-out)...[/dim]")
            p_null_holdout, null_ho = _null_p_value(pillars, vols, data, args.holdout, None,
                                                     args.null_draws, model_ho.metrics["sharpe"])
            if p_null_holdout is not None:
                rp.console.print(Panel(
                    f"Shuffled-signal null over {len(null_ho)} draws: median Sharpe {np.median(null_ho):.3f}\n"
                    f"VLRT v3 hold-out Sharpe {model_ho.metrics['sharpe']:.3f}   (p = {p_null_holdout:.3f})",
                    title="[bold]Block-shuffled-signal null — hold-out[/bold]", border_style="magenta"))

    rp.console.print(f"[dim]independent bets ~ {n_bets:.1f} (from {len(av)} months, "
                     f"AR(1)={av['equity'].autocorr(1):+.3f})[/dim]")
    rp.render_refusals(refusals)

    panel = None
    if not args.no_replicate:
        panel = evaluate(av)
        rp.render_replication(panel)

    if model_ho is not None and panel is not None:
        gates = rp.build_gate_verdicts(comps_ho, p_null_holdout, panel.pooled_mae, panel.pooled_baselines)
        rp.render_gates(gates)
    else:
        rp.console.print("[yellow]Acceptance gates not evaluated: need both a hold-out backtest and replication.[/yellow]")


if __name__ == "__main__":
    main()
