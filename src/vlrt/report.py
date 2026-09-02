"""
VLRT v3 — reporting, with refusals rendered before results.

The previous version failed because it was never compared against a naive baseline.
This module makes that failure mode unreachable: there is no code path that prints
model metrics without the benchmark rows above them, and the refusal block is
evaluated and printed *before* any performance number.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()

MIN_INDEPENDENT_BETS = 10
MAX_FROZEN_PCT = 25.0


@dataclass
class Refusal:
    trigger: str
    refused: str
    permitted: str


def independent_bets(monthly_weight: pd.Series) -> float:
    """T*(1-rho)/(1+rho) — persistence-adjusted count of genuinely distinct decisions."""
    s = monthly_weight.dropna()
    if len(s) < 5:
        return float(len(s))
    rho = float(s.autocorr(1))
    rho = 0.0 if not np.isfinite(rho) else min(max(rho, -0.99), 0.99)
    return float(len(s) * (1 - rho) / (1 + rho))


def build_refusals(
    n_bets: float,
    comparisons: dict[str, dict[str, float]],
    frozen_pct: float,
    data_notes: list[str],
) -> list[Refusal]:
    out: list[Refusal] = []
    if n_bets < MIN_INDEPENDENT_BETS:
        out.append(Refusal(
            f"only {n_bets:.1f} independent bets (< {MIN_INDEPENDENT_BETS})",
            "any claim that the model outperforms",
            "the model was not refuted at this sample size",
        ))
    for name, c in comparisons.items():
        if not np.isfinite(c.get("lo", np.nan)):
            continue
        if c["lo"] <= 0.0 <= c["hi"]:
            out.append(Refusal(
                f"dSharpe vs {name} 95% CI [{c['lo']:+.3f}, {c['hi']:+.3f}] contains zero",
                f"'beats {name}'",
                f"indistinguishable from {name} at this sample size",
            ))
    b4 = comparisons.get("B4 vol-only")
    if b4 and np.isfinite(b4.get("lo", np.nan)) and b4["lo"] <= 0.0 <= b4["hi"]:
        out.append(Refusal(
            "model minus vol-targeting-only lies inside its own confidence interval",
            "any claim that the V/L/R/T composite adds value",
            "the result is attributable to volatility targeting, not to the VLRT composite",
        ))
    if frozen_pct > MAX_FROZEN_PCT:
        out.append(Refusal(
            f"{frozen_pct:.0f}% of months frozen (> {MAX_FROZEN_PCT:.0f}%)",
            "that the mapping is continuous",
            "the no-trade band has re-created a step function",
        ))
    for n in data_notes:
        if "stale" in n.lower():
            out.append(Refusal(n, "any COT-derived number for stale months", "reported separately, excluded from headline"))
    return out


def render_refusals(refusals: list[Refusal]) -> None:
    if not refusals:
        console.print(Panel("[green]No refusal conditions triggered.[/green]", border_style="green"))
        return
    t = Table(box=box.SIMPLE_HEAVY, header_style="bold white", show_lines=False)
    t.add_column("Trigger", style="yellow", width=54)
    t.add_column("Claim refused", style="red", width=40)
    t.add_column("Permitted wording", style="dim", width=52)
    for r in refusals:
        t.add_row(r.trigger, r.refused, r.permitted)
    console.print(Panel(t, title="[bold red]REFUSALS — read before any number below[/bold red]", border_style="red"))


def render_performance(rows: list[dict], title: str) -> None:
    """
    Benchmarks are always rendered above the model. Not configurable.

    Turnover and cost drag print alongside returns whenever a row went through the
    weight/turnover pipeline (``run_backtest``); rows built from a raw return series
    (e.g. the fund's own NAV) show "—" for both rather than a fabricated zero.
    """
    t = Table(box=box.SIMPLE_HEAVY, header_style="bold white")
    t.add_column("Strategy", width=22)
    for c in ("CAGR %", "Vol %", "Sharpe", "MaxDD %", "Turnover/yr", "Cost drag %/yr", "Days"):
        t.add_column(c, justify="right", width=11)
    for m in rows:
        is_model = m.get("is_model", False)
        style = "bold cyan" if is_model else "white"
        to = f"{m['turnover_ann']:.2f}" if "turnover_ann" in m else "—"
        cd = f"{m['cost_drag_ann_pct']:.3f}" if "cost_drag_ann_pct" in m else "—"
        t.add_row(
            f"[{style}]{m['name']}[/{style}]",
            f"{m['ann_return_pct']:.2f}", f"{m['ann_vol_pct']:.2f}",
            f"[{style}]{m['sharpe']:.3f}[/{style}]",
            f"{m['max_drawdown_pct']:.2f}", to, cd, f"{m['n_days']:d}",
        )
    console.print(Panel(t, title=f"[bold]{title}[/bold]", border_style="cyan"))


def render_comparisons(comparisons: dict[str, dict[str, float]], mds: float | None = None) -> None:
    t = Table(box=box.SIMPLE_HEAVY, header_style="bold white")
    t.add_column("vs benchmark", width=24)
    for c in ("dSharpe", "CI low", "CI high", "p"):
        t.add_column(c, justify="right", width=10)
    t.add_column("Verdict", width=34)
    adj_alphas = {c.get("adj_alpha") for c in comparisons.values() if np.isfinite(c.get("adj_alpha", np.nan))}
    n_comp = next((c.get("n_comparisons") for c in comparisons.values() if c.get("n_comparisons")), 1)
    for name, c in comparisons.items():
        crosses = np.isfinite(c.get("lo", np.nan)) and c["lo"] <= 0.0 <= c["hi"]
        verdict = "[yellow]indistinguishable[/yellow]" if crosses else (
            "[green]favours model[/green]" if c["diff"] > 0 else "[red]favours benchmark[/red]")
        t.add_row(name, f"{c['diff']:+.3f}", f"{c['lo']:+.3f}", f"{c['hi']:+.3f}",
                  f"{c['p_two_sided']:.3f}", verdict)
    alpha_note = (
        f"Bonferroni family-wise CI: n_comparisons={n_comp}, per-test alpha={next(iter(adj_alphas), 0.05):.4f}"
        if adj_alphas else "95% CI"
    )
    sub = f"{alpha_note}" + (f"   |   minimum detectable dSharpe ~ {mds:.2f}" if mds else "")
    console.print(Panel(t, title="[bold]Paired block-bootstrap Sharpe differences[/bold]", border_style="magenta",
                        subtitle=f"[dim]{sub}[/dim]"))


def render_pillars(pillars: pd.DataFrame, weights: pd.DataFrame) -> None:
    t = Table(box=box.SIMPLE_HEAVY, header_style="bold white")
    t.add_column("Series", width=16)
    for c in ("n", "mean", "std", "min", "max", "at floor %", "at cap %", "frozen %"):
        t.add_column(c, justify="right", width=10)
    for col in ("V", "L", "R", "T", "composite", "pm_signal"):
        s = pillars[col].dropna()
        t.add_row(col, f"{len(s)}", f"{s.mean():.3f}", f"{s.std():.3f}", f"{s.min():.3f}",
                  f"{s.max():.3f}", f"{(s <= 0.02).mean()*100:.0f}", f"{(s >= 0.98).mean()*100:.0f}",
                  f"{(s.diff() == 0).mean()*100:.0f}")
    for col in ("equity", "pm", "cash"):
        s = weights[col].dropna()
        t.add_row(f"w_{col}", f"{len(s)}", f"{s.mean():.3f}", f"{s.std():.3f}", f"{s.min():.3f}",
                  f"{s.max():.3f}", "—", "—", f"{(s.diff() == 0).mean()*100:.0f}")
    console.print(Panel(t, title="[bold]Pillar and weight diagnostics[/bold] [dim](v2: V dead 18 months, R floored 11 months, 44% frozen)[/dim]",
                        border_style="blue"))


def _mae_table(mae: dict, baselines: dict, title: str) -> Table:
    t = Table(box=box.SIMPLE_HEAVY, header_style="bold white", title=title)
    t.add_column("Sleeve", width=10)
    t.add_column("VLRT v3", justify="right", width=10)
    for c in ("random walk", "constant mean", "static anchor"):
        t.add_column(c, justify="right", width=14)
    t.add_column("Verdict", width=26)
    for s in ("equity", "pm", "cash"):
        rw = baselines["random_walk"][s]
        good = np.isfinite(rw) and mae[s] < rw
        t.add_row(s, f"{mae[s]:.2f}", f"{rw:.2f}",
                  f"{baselines['constant_mean'][s]:.2f}",
                  f"{baselines['static_55_20_25'][s]:.2f}",
                  "[green]beats random walk[/green]" if good else "[red]loses to random walk[/red]")
    return t


def render_replication(panel) -> None:
    """Renders each fund's own diagnostic, then the pooled panel result."""
    for fname, res in panel.per_fund.items():
        console.print(_mae_table(res.mae, res.baselines, f"{fname}"))
    console.print(Panel(
        _mae_table(panel.pooled_mae, panel.pooled_baselines, "Pooled across panel"),
        title="[bold]Replication MAE, pct-pts — pooled across every fund in the panel[/bold]",
        border_style="yellow",
    ))

    d = panel.pooled_direction
    txt = (
        f"Pooled direction accuracy [bold]{d['hit_rate_pct']:.1f}%[/bold] on {d['n_scored']} scored "
        f"fund-months ({d['n_eligible']} eligible of {d['n_total']} total; abstention {d['abstention_pct']:.0f}%)\n"
        f"p vs block-shuffled null: [bold]{d['p_vs_shuffled_null']:.3f}[/bold]   "
        f"Spearman(delta model, delta fund): {d['spearman_delta']:+.3f}\n"
    )
    for fname, res in panel.per_fund.items():
        if res.ceiling:
            txt += (f"\nCeiling [{fname}] — Quant's other equity funds explain "
                    f"[bold]R2 = {res.ceiling['r2']:.3f}[/bold] of this fund's equity weight "
                    f"(p={res.ceiling['p']:.3f}, n={res.ceiling['n']}).")
    for w in panel.warnings:
        txt += f"\n[yellow]warning:[/yellow] {w}"
    console.print(Panel(txt, title="[bold]Direction and ceiling[/bold]", border_style="yellow"))


def build_gate_verdicts(
    holdout_comparisons: dict[str, dict[str, float]],
    holdout_null_p: float | None,
    pooled_mae: dict[str, float],
    pooled_baselines: dict[str, dict[str, float]],
) -> list[tuple[str, bool, str]]:
    """
    The three acceptance gates from the plan, evaluated on the hold-out period only.
    Returns (gate name, passed, evidence) so the caller can render and count them.
    """
    gates: list[tuple[str, bool, str]] = []

    st = holdout_comparisons.get("Static 55/20/25")
    if st and np.isfinite(st.get("lo", np.nan)):
        passed = st["diff"] > 0 and not (st["lo"] <= 0.0 <= st["hi"])
        gates.append((
            "Beats static 55/20/25 in hold-out (CI excludes zero)",
            passed,
            f"dSharpe {st['diff']:+.3f}, CI [{st['lo']:+.3f}, {st['hi']:+.3f}]",
        ))
    else:
        gates.append(("Beats static 55/20/25 in hold-out (CI excludes zero)", False, "insufficient hold-out data"))

    if holdout_null_p is not None and np.isfinite(holdout_null_p):
        passed = holdout_null_p < 0.05
        gates.append(("Beats the block-shuffled-signal null (p<0.05)", passed, f"p={holdout_null_p:.3f}"))
    else:
        gates.append(("Beats the block-shuffled-signal null (p<0.05)", False, "not computed"))

    rw = pooled_baselines.get("random_walk", {})
    eq_ok = np.isfinite(rw.get("equity", np.nan)) and pooled_mae.get("equity", np.inf) < rw["equity"]
    pm_ok = np.isfinite(rw.get("pm", np.nan)) and pooled_mae.get("pm", np.inf) < rw["pm"]
    gates.append((
        "Replication beats random-walk MAE (equity & pm)",
        bool(eq_ok and pm_ok),
        f"equity {pooled_mae.get('equity', float('nan')):.2f} vs {rw.get('equity', float('nan')):.2f}, "
        f"pm {pooled_mae.get('pm', float('nan')):.2f} vs {rw.get('pm', float('nan')):.2f}",
    ))
    return gates


def render_gates(gates: list[tuple[str, bool, str]]) -> None:
    n_passed = sum(1 for _, p, _ in gates if p)
    t = Table(box=box.SIMPLE_HEAVY, header_style="bold white")
    t.add_column("Gate", width=48)
    t.add_column("Result", width=10)
    t.add_column("Evidence", width=48)
    for name, passed, evidence in gates:
        t.add_row(name, "[green]PASS[/green]" if passed else "[red]FAIL[/red]", evidence)
    style = "green" if n_passed == len(gates) else "red"
    console.print(Panel(
        t,
        title=f"[bold {style}]ACCEPTANCE GATES: {n_passed}/{len(gates)} passed[/bold {style}]",
        border_style=style,
        subtitle="[dim]All three must pass for a validated model; otherwise this is a negative result plus the mechanical fixes.[/dim]",
    ))
