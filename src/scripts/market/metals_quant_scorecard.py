"""
scripts/metals_quant_scorecard.py
──────────────────────────────────
Run the composite quant scorecard for Gold (GOLDBEES), Silver (SILVERBEES),
and Copper (HG=F).

Usage:
    python scripts/metals_quant_scorecard.py              # all three
    python scripts/metals_quant_scorecard.py --gold-only
    python scripts/metals_quant_scorecard.py --silver-only
    python scripts/metals_quant_scorecard.py --copper-only
"""

import argparse
import logging
import os
import sys

from rich.console import Console
from rich.table import Table
from rich.rule import Rule

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_ROOT, "src"))
sys.path.insert(0, _ROOT)

try:
    from config.settings import settings
    from tools.quant_scorecard import compute_gold_scorecard, compute_silver_scorecard, compute_copper_scorecard
except ImportError as e:
    print(f"Error importing project modules: {e}")
    sys.exit(1)

logging.basicConfig(level=logging.ERROR)
console = Console()


def _impact_label(score: float | None) -> str:
    if score is None:
        return "N/A"
    if score >= 75:
        return "[bold green]Strong Bullish[/bold green]"
    if score >= 60:
        return "[green]Bullish[/green]"
    if score >= 40:
        return "[yellow]Neutral[/yellow]"
    if score >= 25:
        return "[red]Bearish[/red]"
    return "[bold red]Strong Bearish[/bold red]"


def _fmt(val: float | None, decimals: int = 1, suffix: str = "") -> str:
    return f"{val:.{decimals}f}{suffix}" if val is not None else "N/A"


def _print_scorecard(
    title: str,
    scorecard: dict,
    signal_rows: list[tuple[str, str, str]],
    color: str = "gold1",
) -> None:
    console.print()
    console.print(Rule(f"[bold {color}]{title}[/bold {color}]"))

    # ── Pillar summary table ─────────────────────────────────────────────────
    summary = Table(
        title=title, show_header=True, header_style="bold magenta",
        title_style=f"bold {color}",
    )
    summary.add_column("Pillar",          style="dim")
    summary.add_column("Score (0–100)",   justify="center")
    summary.add_column("Weight",          justify="center")
    summary.add_column("Signal",          justify="center")

    weights = scorecard.get("pillar_weights", {"macro": "30%", "flows": "30%", "valuation": "20%", "momentum": "20%"})
    summary.add_row("Macro",      _fmt(scorecard["macro_score"]),      weights["macro"],     _impact_label(scorecard["macro_score"]))
    summary.add_row("Flows (COT)", _fmt(scorecard["flows_score"]),     weights["flows"],     _impact_label(scorecard["flows_score"]))
    val_label = "Valuation (iNAV)" if scorecard.get("valuation_score") is not None else "Valuation (iNAV)"
    val_weight = weights.get("valuation", "N/A")
    val_display = _fmt(scorecard["valuation_score"]) if scorecard.get("valuation_score") is not None else "[dim]N/A[/dim]"
    summary.add_row(val_label, val_display, val_weight, _impact_label(scorecard.get("valuation_score")))
    summary.add_row("Momentum",   _fmt(scorecard["momentum_score"]),   weights["momentum"],  _impact_label(scorecard["momentum_score"]))

    summary.add_section()
    comp = scorecard["composite_score"]
    summary.add_row(
        "[bold]COMPOSITE[/bold]",
        f"[bold]{_fmt(comp)}[/bold]",
        "100%",
        _impact_label(comp),
    )
    console.print(summary)

    # ── Raw signals table ────────────────────────────────────────────────────
    sig_table = Table(
        title="Underlying Quant Signals", show_header=True, header_style="bold cyan"
    )
    sig_table.add_column("Signal",  style="dim")
    sig_table.add_column("Value",   justify="right")
    sig_table.add_column("Context", style="dim")

    for name, value, context in signal_rows:
        sig_table.add_row(name, value, context)

    console.print(sig_table)

    if as_of := scorecard.get("as_of"):
        console.print(f"  [dim]As of: {as_of}[/dim]")
    if err := scorecard.get("error"):
        console.print(f"  [yellow]Warnings: {err}[/yellow]")


def run_gold(ch_kwargs: dict) -> None:
    console.print("[bold gold1]Fetching Gold data...[/bold gold1]")
    sc = compute_gold_scorecard(**ch_kwargs)
    s  = sc["signals"]

    signal_rows = [
        ("DXY Level",           _fmt(s.get("dxy_level"), 2),              "Lower → Bullish"),
        ("US Real Yield (est)", _fmt(s.get("real_yield_level"), 2, "%"),  "Lower → Bullish"),
        ("Real Yield 5D Δ",     _fmt(s.get("real_yield_delta5"), 2, "%"), "Drop → Bullish"),
        ("COT Spec % of OI",    _fmt(s.get("cot_pct_oi"), 1, "%"),        "20–35% range"),
        ("iNAV Prem/Disc",      _fmt(s.get("inav_disc_pct"), 2, "%"),     "Disc → Bullish"),
        ("LightGBM 5D Return",  _fmt(s.get("lgbm_return_pct"), 2, "%"),   "Next 5D forecast"),
    ]
    _print_scorecard("Gold Quant Scorecard — GOLDBEES", sc, signal_rows, color="gold1")


def run_silver(ch_kwargs: dict) -> None:
    console.print("[bold grey70]Fetching Silver data...[/bold grey70]")
    sc = compute_silver_scorecard(**ch_kwargs)
    s  = sc["signals"]

    cot_date = s.get("cot_report_date") or ""
    cot_label = f"CFTC {cot_date}" if cot_date else "CFTC live"
    signal_rows = [
        ("DXY Level",              _fmt(s.get("dxy_level"), 2),                    "Lower → Bullish"),
        ("US Real Yield (est)",    _fmt(s.get("real_yield_level"), 2, "%"),         "Lower → Bullish"),
        ("Real Yield 5D Δ",        _fmt(s.get("real_yield_delta5"), 2, "%"),        "Drop → Bullish"),
        ("Gold-Silver Ratio (GSR)", _fmt(s.get("gsr"), 1),                         "≥90 = Silver cheap"),
        ("COT Spec % of OI",       _fmt(s.get("cot_pct_oi"), 1, "%"),              cot_label),
        ("iNAV Prem/Disc",         _fmt(s.get("inav_disc_pct"), 2, "%"),            "Disc → Bullish"),
        ("SI=F 5D Return",         _fmt(s.get("momentum_return_pct"), 2, "%"),      "Realised momentum"),
    ]
    _print_scorecard("Silver Quant Scorecard — SILVERBEES", sc, signal_rows, color="grey70")


def run_copper() -> None:
    console.print("[bold orange3]Fetching Copper data...[/bold orange3]")
    sc = compute_copper_scorecard()
    s  = sc["signals"]

    cot_date  = s.get("cot_report_date") or ""
    cot_label = f"CFTC {cot_date}" if cot_date else "CFTC live"
    signal_rows = [
        ("DXY Level",       _fmt(s.get("dxy_level"), 2),        "Lower → Bullish (USD inverse)"),
        ("USD/CNY",         _fmt(s.get("usdcny"), 4),            "≤7.00 = Strong CNY → Bullish"),
        ("COT Spec % of OI", _fmt(s.get("cot_pct_oi"), 1, "%"), cot_label),
        ("HG=F 5D Return",  _fmt(s.get("ret_5d"), 2, "%"),       "Realised momentum"),
        ("HG=F 20D Return", _fmt(s.get("ret_20d"), 2, "%"),      "Trend momentum"),
        ("Last Price (HG=F)", _fmt(s.get("last_price"), 4, " $/lb"), "COMEX copper"),
    ]
    # Copper has no iNAV — pass custom weights for display
    sc["pillar_weights"] = {"macro": "35%", "flows": "30%", "valuation": "—", "momentum": "35%"}
    _print_scorecard("Copper Quant Scorecard — HG=F (COMEX)", sc, signal_rows, color="orange3")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run metals quant scorecards")
    parser.add_argument("--gold-only",   action="store_true")
    parser.add_argument("--silver-only", action="store_true")
    parser.add_argument("--copper-only", action="store_true")
    args = parser.parse_args()

    ch_kwargs = dict(
        ch_host=settings.clickhouse_host,
        ch_port=settings.clickhouse_port,
        ch_user=settings.clickhouse_user,
        ch_pass=settings.clickhouse_password,
        ch_database=settings.clickhouse_database,
    )

    only_one = args.gold_only or args.silver_only or args.copper_only
    run_gold_flag   = args.gold_only   or not only_one
    run_silver_flag = args.silver_only or not only_one
    run_copper_flag = args.copper_only or not only_one

    if run_gold_flag:
        run_gold(ch_kwargs)
    if run_silver_flag:
        run_silver(ch_kwargs)
    if run_copper_flag:
        run_copper()

    console.print()


if __name__ == "__main__":
    main()
