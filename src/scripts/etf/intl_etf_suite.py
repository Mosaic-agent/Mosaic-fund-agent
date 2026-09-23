#!/usr/bin/env python3
"""
src/scripts/etf/intl_etf_suite.py
─────────────────────────────────
Consolidated Command-Line Interface for International ETF Quantitative Arbitrage.

Usage:
  python -m src.scripts.etf.intl_etf_suite scan
  python -m src.scripts.etf.intl_etf_suite backtest [--years 2.0]
  python -m src.scripts.etf.intl_etf_suite signals [--symbol MONQ50] [--limit 30]
  python -m src.scripts.etf.intl_etf_suite plot [--symbol MONQ50]
  python -m src.scripts.etf.intl_etf_suite all [--symbol MONQ50]
"""

from __future__ import annotations

import argparse
import sys
from rich.console import Console
from rich.panel import Panel

from src.tools.intl_etf import IntlETFEngine, IntlETFPresenter

console = Console()


def cmd_scan(args: argparse.Namespace) -> None:
    console.print(
        Panel.fit(
            "[bold cyan]🔍 LIVE INTERNATIONAL ETF SCARCITY & ARBITRAGE SCANNER[/bold cyan]\n"
            "[dim]Tracking live indicative NAV, secondary market prices, and dynamic Z-score regimes[/dim]",
            border_style="cyan",
        )
    )
    engine = IntlETFEngine()
    results = engine.scan_live(lookback_days=args.lookback)
    IntlETFPresenter.show_live_scan(results)


def cmd_backtest(args: argparse.Namespace) -> None:
    console.print(
        Panel.fit(
            f"[bold cyan]🔬 {args.years:g}-YEAR INTERNATIONAL ETF ARBITRAGE BACKTEST[/bold cyan]\n"
            f"[dim]Evaluating forward returns (5d, 10d, 20d) and crash avoidance drawdowns[/dim]",
            border_style="cyan",
        )
    )
    engine = IntlETFEngine()
    b_results = engine.backtest(years=args.years)
    IntlETFPresenter.show_backtest_results(b_results)
    IntlETFPresenter.show_plotext_charts(b_results)


def cmd_signals(args: argparse.Namespace) -> None:
    sym_label = f" for {args.symbol.upper()}" if args.symbol else " across Universe"
    console.print(
        Panel.fit(
            f"[bold yellow]📜 CHRONOLOGICAL ARBITRAGE TRIGGER LOG{sym_label}[/bold yellow]\n"
            f"[dim]State transitions into Entry or Exit regimes with exact prices and outcomes[/dim]",
            border_style="yellow",
        )
    )
    engine = IntlETFEngine()
    signals_df = engine.get_signal_history(years=args.years, symbol=args.symbol, limit=args.limit)
    IntlETFPresenter.show_signals(signals_df)


def cmd_plot(args: argparse.Namespace) -> None:
    console.print(
        Panel.fit(
            f"[bold magenta]📈 GENERATING MATPLOTLIB ARBITRAGE VISUALIZATIONS[/bold magenta]\n"
            f"[dim]Output directory: {args.output_dir} | Target Symbol: {args.symbol.upper()}[/dim]",
            border_style="magenta",
        )
    )
    engine = IntlETFEngine()
    dataset = engine.fetch_historical_dataset(years=args.years)
    overview_path, detail_path = IntlETFPresenter.generate_matplotlib_figures(
        dataset, output_dir=args.output_dir, symbol=args.symbol
    )
    console.print(f"[bold green]✓ Multi-ETF Overview saved:[/bold green] {overview_path}")
    console.print(f"[bold green]✓ {args.symbol.upper()} Deep Dive saved:[/bold green] {detail_path}")
    console.print(f"[dim]Web Access: http://localhost:8502[/dim]")


def cmd_all(args: argparse.Namespace) -> None:
    console.print(
        Panel.fit(
            "[bold white on blue] 🚀 EXECUTING COMPLETE INTERNATIONAL ETF RESEARCH SUITE [/bold white on blue]\n"
            "[dim]Live Scan ──► 2-Year Statistical Backtest ──► Signal Log ──► Matplotlib Generation[/dim]",
            border_style="blue",
        )
    )
    engine = IntlETFEngine()

    # 1. Live scan
    results = engine.scan_live(lookback_days=args.lookback)
    IntlETFPresenter.show_live_scan(results)

    # 2. Backtest
    b_results = engine.backtest(years=args.years)
    IntlETFPresenter.show_backtest_results(b_results)
    IntlETFPresenter.show_plotext_charts(b_results)

    # 3. Signals
    signals_df = engine.get_signal_history(years=args.years, symbol=args.symbol, limit=args.limit)
    IntlETFPresenter.show_signals(signals_df)

    # 4. Plots
    overview_path, detail_path = IntlETFPresenter.generate_matplotlib_figures(
        b_results["dataset"], output_dir=args.output_dir, symbol=args.symbol
    )
    console.print(f"\n[bold green]✓ Charts generated successfully in {args.output_dir}[/bold green]")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Unified International ETF Quantitative Arbitrage & Scarcity Suite",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", help="Suite Subcommand")

    # scan
    p_scan = subparsers.add_parser("scan", help="Run live iNAV & secondary price scarcity scan")
    p_scan.add_argument("--lookback", type=int, default=30, help="Rolling baseline lookback days")

    # backtest
    p_bt = subparsers.add_parser("backtest", help="Run historical quantitative backtest")
    p_bt.add_argument("--years", type=float, default=2.0, help="Backtest lookback window in years")

    # signals
    p_sig = subparsers.add_parser("signals", help="List chronological trigger dates and prices")
    p_sig.add_argument("--years", type=float, default=2.0, help="Lookback window in years")
    p_sig.add_argument("--symbol", type=str, default=None, help="Filter by symbol (e.g. MONQ50)")
    p_sig.add_argument("--limit", type=int, default=30, help="Max signals to display")

    # plot
    p_plot = subparsers.add_parser("plot", help="Generate publication-grade matplotlib figures")
    p_plot.add_argument("--years", type=float, default=2.0, help="Lookback window in years")
    p_plot.add_argument("--symbol", type=str, default="MONQ50", help="Target symbol for 3-panel deep dive")
    p_plot.add_argument("--output-dir", type=str, default="/app/output/reports", help="Output directory")

    # all
    p_all = subparsers.add_parser("all", help="Run complete scan, backtest, and plotting pipeline")
    p_all.add_argument("--years", type=float, default=2.0, help="Backtest lookback window in years")
    p_all.add_argument("--lookback", type=int, default=30, help="Rolling baseline lookback days")
    p_all.add_argument("--symbol", type=str, default="MONQ50", help="Target symbol for deep dive")
    p_all.add_argument("--limit", type=int, default=20, help="Max signals to display")
    p_all.add_argument("--output-dir", type=str, default="/app/output/reports", help="Output directory")

    args = parser.parse_args()

    if args.command == "scan":
        cmd_scan(args)
    elif args.command == "backtest":
        cmd_backtest(args)
    elif args.command == "signals":
        cmd_signals(args)
    elif args.command == "plot":
        cmd_plot(args)
    elif args.command == "all":
        cmd_all(args)
    else:
        # Default behavior: run all
        default_args = argparse.Namespace(
            command="all",
            years=2.0,
            lookback=30,
            symbol="MONQ50",
            limit=20,
            output_dir="/app/output/reports",
        )
        cmd_all(default_args)


if __name__ == "__main__":
    main()
