#!/usr/bin/env python3
"""
src/scripts/etf/backtest_arbitrage.py
────────────────────────────────────
2-Year Bidirectional ETF Arbitrage & Scarcity Premium Engine Backtest.

Delegates to consolidated IntlETFEngine & IntlETFPresenter facade.
"""

from __future__ import annotations

import argparse
from src.tools.intl_etf import IntlETFEngine, IntlETFPresenter


def run_arbitrage_backtest(
    years: float = 2.0,
    show_signals: bool = False,
    symbol_filter: str | None = None,
    limit: int = 50,
) -> None:
    engine = IntlETFEngine()
    b_results = engine.backtest(years=years)
    IntlETFPresenter.show_backtest_results(b_results)
    IntlETFPresenter.show_plotext_charts(b_results)

    if show_signals:
        signals_df = engine.get_signal_history(years=years, symbol=symbol_filter, limit=limit)
        IntlETFPresenter.show_signals(signals_df)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="2-Year ETF Arbitrage Engine Backtest")
    parser.add_argument("--years", type=float, default=2.0, help="Lookback window in years")
    parser.add_argument("--signals", action="store_true", help="Print chronological signal dates & prices")
    parser.add_argument("--symbol", type=str, default=None, help="Filter signals by specific ETF symbol")
    parser.add_argument("--limit", type=int, default=50, help="Number of signals to display")
    args = parser.parse_args()

    show_sig = args.signals or (args.symbol is not None)
    run_arbitrage_backtest(years=args.years, show_signals=show_sig, symbol_filter=args.symbol, limit=args.limit)
