"""
VLRT v3 — annual strategic-tilt variant CLI.

Rebalances yearly with a small (+-5pp) tilt and reports terminal wealth / rolling CAGR,
not monthly Sharpe. See src/vlrt/strategic.py for why: the one signal that survived
multiple-testing correction (v_px_vs_3y) has IC that triples from 3-month to 24-month
horizons, and monthly Sharpe punishes exactly the volatility this kind of slow-reversion
trade must tolerate.

    python src/scripts/portfolio/vlrt_v3_strategic.py
"""

from __future__ import annotations

import os
import sys

sys.path.append(os.getcwd())

from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from src.vlrt.allocate import ANCHOR, sleeve_vols
from src.vlrt.backtest import run_backtest, static_weights
from src.vlrt.data import load_all
from src.vlrt.pillars import build_pillars
from src.vlrt.strategic import annual_target_weights, summarise

console = Console()


def render(df, title):
    t = Table(box=box.SIMPLE_HEAVY, header_style="bold white")
    t.add_column("Strategy", width=22)
    for c in ("Years", "Terminal $1->", "CAGR %", "MaxDD %", "Sharpe",
              "Roll3y med%", "Roll3y min%", "Roll5y med%", "Roll5y min%"):
        t.add_column(c, justify="right", width=12)
    for _, r in df.iterrows():
        t.add_row(
            r["name"], f"{r['years']:.1f}", f"{r['terminal_wealth']:.2f}",
            f"{r['cagr_pct']:.2f}", f"{r['max_dd_pct']:.2f}", f"{r['sharpe']:.3f}",
            f"{r['roll3y_median_pct']:.2f}", f"{r['roll3y_min_pct']:.2f}",
            f"{r['roll5y_median_pct']:.2f}", f"{r['roll5y_min_pct']:.2f}",
        )
    console.print(Panel(t, title=f"[bold]{title}[/bold]", border_style="cyan"))


def main() -> None:
    console.print(Panel(
        "[bold cyan]VLRT v3 — annual strategic-tilt variant[/bold cyan]\n"
        "[dim]Small (+-5pp) yearly tilt, judged on terminal wealth / rolling CAGR, not monthly Sharpe.\n"
        "Roughly 10 annual decisions are available -- too few for a confident significance test;\n"
        "these numbers are descriptive, not a validated claim.[/dim]",
        border_style="cyan"))

    data = load_all()
    pillars = build_pillars(data.monthly)
    vols = sleeve_vols(data.daily, data.sleeve_px, data.monthly.index)

    w_full = annual_target_weights(pillars, vols)
    w_vonly = annual_target_weights(pillars.assign(composite=pillars["V"]), vols)

    if w_full.dropna().empty:
        console.print("[red]No valid annual decisions -- composite warmup not complete.[/red]")
        return

    start = w_full.dropna().index.min()
    console.print(f"[dim]{len(w_full.dropna())} annual decisions, {start.date()} -> "
                  f"{w_full.dropna().index.max().date()}[/dim]")

    results = {
        "Static 55/20/25": run_backtest(
            static_weights(w_full.dropna().index, ANCHOR), data.sleeve_px,
            "Static 55/20/25", start=start, no_trade_band=0.0),
        "Annual tilt (full composite)": run_backtest(
            w_full, data.sleeve_px, "Annual full composite", start=start, no_trade_band=0.0),
        "Annual tilt (V only)": run_backtest(
            w_vonly, data.sleeve_px, "Annual V only", start=start, no_trade_band=0.0),
    }

    df = summarise(results)
    render(df, "Annual strategic tilt vs static anchor — terminal wealth & rolling CAGR")

    console.print(Panel(
        "[yellow]Read this descriptively.[/yellow] With ~10 annual decisions, no bootstrap or "
        "null test here would be more than noise dressed up as a p-value -- that is why none is "
        "computed. Compare terminal wealth and rolling-CAGR consistency; do not read a single-year "
        "difference as a validated edge.",
        border_style="yellow"))


if __name__ == "__main__":
    main()
