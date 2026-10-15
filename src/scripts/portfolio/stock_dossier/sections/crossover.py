from __future__ import annotations

from ..base import DossierSection
from ..context import DossierContext


class CrossoverBacktestSection(DossierSection):
    key = "crossover"
    title = "Moving Average Crossover Backtest (20d vs 50d SMA)"

    def fetch(self, ctx: DossierContext) -> str:
        from src.scripts.market.ma_crossover_backtest import run_crossover_backtest

        m = run_crossover_backtest(ctx.symbol, fast=20, slow=50, ma_type="sma", plot=True)
        return (
            f"* **Strategy Return:** {m.get('strategy_return', 0.0):.2f}% (CAGR: {m.get('strategy_cagr', 0.0):.2f}%)\n"
            f"* **Benchmark Return:** {m.get('benchmark_return', 0.0):.2f}% (CAGR: {m.get('benchmark_cagr', 0.0):.2f}%)\n"
            f"* **Win Rate:** {m.get('win_rate', 0.0) * 100:.1f}% ({m.get('total_trades', 0)} trades)\n"
            f"* **Sharpe Ratio:** {m.get('sharpe_ratio', 0.0):.2f}\n"
            f"* **Strategy Max Drawdown:** {m.get('max_drawdown', 0.0):.2f}%\n"
            f"* **Benchmark Max Drawdown:** {m.get('benchmark_max_dd', 0.0):.2f}%"
        )
