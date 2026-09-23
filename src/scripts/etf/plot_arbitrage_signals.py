#!/usr/bin/env python3
"""
src/scripts/etf/plot_arbitrage_signals.py
─────────────────────────────────────────
Generate publication-grade matplotlib charts for International ETF Arbitrage.

Delegates to consolidated IntlETFEngine & IntlETFPresenter facade.
"""

from __future__ import annotations

import argparse
from src.tools.intl_etf import IntlETFEngine, IntlETFPresenter


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate Matplotlib Arbitrage Visualizations")
    parser.add_argument("--years", type=float, default=2.0, help="Lookback window in years")
    parser.add_argument("--symbol", type=str, default="MONQ50", help="Target symbol for deep dive")
    parser.add_argument("--output-dir", type=str, default="/app/output/reports", help="Output directory")
    args = parser.parse_args()

    engine = IntlETFEngine()
    dataset = engine.fetch_historical_dataset(years=args.years)
    overview_path, detail_path = IntlETFPresenter.generate_matplotlib_figures(
        dataset, output_dir=args.output_dir, symbol=args.symbol
    )
    print(f"✅ Saved Multi-ETF Overview to {overview_path}")
    print(f"✅ Saved {args.symbol.upper()} Deep Dive to {detail_path}")


if __name__ == "__main__":
    main()
