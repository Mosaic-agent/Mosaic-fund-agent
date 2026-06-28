"""
src/scripts/market/ma_crossover_backtest.py
─────────────────────────────────────────────
Backtests a Moving Average Crossover Strategy (SMA/EMA) on ClickHouse daily prices.
Saves a premium dark-themed performance plot to output/reports/.
"""

import sys
import os
import argparse
import logging
from datetime import datetime
import pandas as pd
import numpy as np

# Setup pathing
_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.insert(0, os.path.join(_ROOT, "src"))
sys.path.insert(0, _ROOT)

# Developer bypass for local runs
os.environ["ALLOW_LOCAL_RUN"] = "1"

from src.db.pool import query_df

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def run_crossover_backtest(
    symbol: str, fast: int, slow: int, ma_type: str, plot: bool = True
) -> dict:
    # 1. Fetch Price Data
    logger.info("Fetching price history for %s from ClickHouse...", symbol)
    try:
        df = query_df(
            f"""
            SELECT trade_date, close, volume 
            FROM market_data.daily_prices FINAL 
            WHERE symbol = '{symbol.upper()}' 
            ORDER BY trade_date ASC
            """
        )
    except Exception as e:
        logger.error("ClickHouse query failed: %s", e)
        return {"error": f"Failed to retrieve data: {e}"}

    if df.empty:
        logger.error("No daily price data found for %s", symbol)
        return {"error": f"No data found for symbol {symbol}"}

    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df = df.sort_values("trade_date").reset_index(drop=True)

    if len(df) < slow + 10:
        return {"error": f"Insufficient data: {len(df)} rows. Need at least {slow + 10} rows."}

    # 2. Calculate Moving Averages
    if ma_type.lower() == "ema":
        df["fast_ma"] = df["close"].ewm(span=fast, adjust=False).mean()
        df["slow_ma"] = df["close"].ewm(span=slow, adjust=False).mean()
    else:
        df["fast_ma"] = df["close"].rolling(window=fast).mean()
        df["slow_ma"] = df["close"].rolling(window=slow).mean()

    # Drop NaNs for backtesting (need slow MA to be computed)
    backtest_df = df.dropna(subset=["slow_ma"]).copy().reset_index(drop=True)

    # 3. Generate Signals
    # Position: 1 = Long, 0 = Cash
    backtest_df["signal"] = np.where(backtest_df["fast_ma"] > backtest_df["slow_ma"], 1, 0)
    # Signal changes: +1 = Golden Cross (Buy), -1 = Death Cross (Sell)
    backtest_df["action"] = backtest_df["signal"].diff().fillna(0)

    # If the first row is already long, buy it immediately
    if backtest_df["signal"].iloc[0] == 1:
        backtest_df.loc[0, "action"] = 1

    # 4. Run Backtest Logic
    capital = 100000.0
    initial_capital = capital
    shares = 0.0
    in_position = False
    
    trades = []
    equity_curve = []
    benchmark_equity = []
    
    entry_date = None
    entry_price = 0.0
    
    for idx, row in backtest_df.iterrows():
        close_price = row["close"]
        curr_date = row["trade_date"]
        
        # Check Buy Signal
        if row["action"] == 1 and not in_position:
            shares = capital / close_price
            entry_price = close_price
            entry_date = curr_date
            in_position = True
            capital = 0.0
            
        # Check Sell Signal
        elif row["action"] == -1 and in_position:
            capital = shares * close_price
            pct_ret = ((close_price - entry_price) / entry_price) * 100
            trades.append({
                "entry_date": entry_date.strftime("%Y-%m-%d"),
                "entry_price": entry_price,
                "exit_date": curr_date.strftime("%Y-%m-%d"),
                "exit_price": close_price,
                "return_pct": pct_ret
            })
            shares = 0.0
            in_position = False
            
        # Record daily equity values
        curr_equity = (shares * close_price) if in_position else capital
        equity_curve.append(curr_equity)
        
        # Benchmark (Buy and Hold)
        bench_val = (initial_capital / backtest_df["close"].iloc[0]) * close_price
        benchmark_equity.append(bench_val)

    # If still in position at the end, liquidate at latest close to complete the loop
    if in_position:
        latest_close = backtest_df["close"].iloc[-1]
        capital = shares * latest_close
        pct_ret = ((latest_close - entry_price) / entry_price) * 100
        trades.append({
            "entry_date": entry_date.strftime("%Y-%m-%d"),
            "entry_price": entry_price,
            "exit_date": backtest_df["trade_date"].iloc[-1].strftime("%Y-%m-%d"),
            "exit_price": latest_close,
            "return_pct": pct_ret
        })

    backtest_df["strategy_equity"] = equity_curve
    backtest_df["benchmark_equity"] = benchmark_equity

    # 5. Compute Metrics
    total_days = (backtest_df["trade_date"].iloc[-1] - backtest_df["trade_date"].iloc[0]).days
    years = max(0.1, total_days / 365.25)
    
    strat_final = equity_curve[-1]
    bench_final = benchmark_equity[-1]
    
    strat_ret = ((strat_final - initial_capital) / initial_capital) * 100
    bench_ret = ((bench_final - initial_capital) / initial_capital) * 100
    
    strat_cagr = (((strat_final / initial_capital) ** (1 / years)) - 1) * 100
    bench_cagr = (((bench_final / initial_capital) ** (1 / years)) - 1) * 100

    # Drawdown
    def get_max_drawdown(equity_series):
        peak = equity_series[0]
        max_dd = 0.0
        for val in equity_series:
            if val > peak:
                peak = val
            dd = (peak - val) / peak
            if dd > max_dd:
                max_dd = dd
        return max_dd * 100

    strat_mdd = get_max_drawdown(equity_curve)
    bench_mdd = get_max_drawdown(benchmark_equity)

    # Sharpe Ratio (daily excess return standard deviation scaling)
    strat_returns = pd.Series(equity_curve).pct_change().dropna()
    excess_daily = strat_returns - (0.05 / 252) # 5% risk-free rate
    std_returns = strat_returns.std()
    sharpe = (np.sqrt(252) * excess_daily.mean() / std_returns) if std_returns > 0 else 0.0

    # Win Rate
    win_trades = sum(1 for t in trades if t["return_pct"] > 0)
    total_trades = len(trades)
    win_rate = (win_trades / total_trades * 100) if total_trades > 0 else 0.0

    metrics = {
        "symbol": symbol.upper(),
        "fast": fast,
        "slow": slow,
        "ma_type": ma_type.upper(),
        "total_days": total_days,
        "years": round(years, 2),
        "strategy_return_pct": round(strat_ret, 2),
        "benchmark_return_pct": round(bench_ret, 2),
        "strategy_cagr": round(strat_cagr, 2),
        "benchmark_cagr": round(bench_cagr, 2),
        "strategy_mdd": round(strat_mdd, 2),
        "benchmark_mdd": round(bench_mdd, 2),
        "sharpe_ratio": round(sharpe, 2),
        "total_trades": total_trades,
        "win_rate": round(win_rate, 2),
        "trades": trades
    }

    # 6. Plotting
    if plot:
        os.makedirs(os.path.join(_ROOT, "output", "reports"), exist_ok=True)
        try:
            import matplotlib
            matplotlib.use("Agg")
            import matplotlib.pyplot as plt
            
            # Premium dark theme
            plt.style.use("dark_background")
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10), sharex=True, gridspec_kw={'height_ratios': [2, 1]})
            
            # Subplot 1: Price and Moving Averages
            ax1.plot(backtest_df["trade_date"], backtest_df["close"], label="Close Price", color="#ffffff", alpha=0.4, linewidth=1)
            ax1.plot(backtest_df["trade_date"], backtest_df["fast_ma"], label=f"{fast}d {ma_type.upper()}", color="#00ffcc", linewidth=1.5)
            ax1.plot(backtest_df["trade_date"], backtest_df["slow_ma"], label=f"{slow}d {ma_type.upper()}", color="#ff3366", linewidth=1.5)
            
            # Plot Golden/Death Cross markers
            golden_crosses = backtest_df[backtest_df["action"] == 1]
            death_crosses = backtest_df[backtest_df["action"] == -1]
            ax1.scatter(golden_crosses["trade_date"], golden_crosses["close"], color="#00ff00", marker="^", s=100, label="Buy (Golden Cross)")
            ax1.scatter(death_crosses["trade_date"], death_crosses["close"], color="#ff0000", marker="v", s=100, label="Sell (Death Cross)")
            
            ax1.set_title(f"{symbol.upper()} Moving Average Crossover Backtest ({fast}d vs {slow}d {ma_type.upper()})", fontsize=14, fontweight="bold", pad=15)
            ax1.grid(color="#222222", linestyle="--")
            ax1.legend(loc="upper left")
            ax1.set_ylabel("Price")

            # Subplot 2: Equity Curves
            ax2.plot(backtest_df["trade_date"], backtest_df["strategy_equity"], label=f"Crossover Strategy ({strat_ret:+.1f}%)", color="#00ffcc", linewidth=2)
            ax2.plot(backtest_df["trade_date"], backtest_df["benchmark_equity"], label=f"Buy & Hold Benchmark ({bench_ret:+.1f}%)", color="#888888", linestyle="--", linewidth=1.5)
            ax2.set_ylabel("Portfolio Value (₹)")
            ax2.grid(color="#222222", linestyle="--")
            ax2.legend(loc="upper left")
            
            # Layout cleanup
            plt.tight_layout()
            plot_path = os.path.join(_ROOT, "output", "reports", f"{symbol.upper()}_crossover.png")
            fig.savefig(plot_path, dpi=150)
            plt.close(fig)
            metrics["plot_path"] = plot_path
            logger.info("Saved backtest visualization to %s", plot_path)
        except Exception as plot_err:
            logger.warning("Could not generate backtest chart: %s", plot_err)

    return metrics

def print_cli_report(metrics: dict) -> None:
    if "error" in metrics:
        print(f"\nError: {metrics['error']}\n")
        return
        
    print("\n" + "="*60)
    print(f"📊 {metrics['symbol']} CROSSOVER BACKTEST: {metrics['fast']}d vs {metrics['slow']}d {metrics['ma_type']}")
    print("="*60)
    print(f"Period Length    : {metrics['years']} years ({metrics['total_days']} days)")
    print(f"Total Trades     : {metrics['total_trades']}")
    print(f"Win Rate         : {metrics['win_rate']}%")
    print(f"Sharpe Ratio     : {metrics['sharpe_ratio']}")
    print("-"*60)
    print(f"Strategy Return  : {metrics['strategy_return_pct']:+.2f}% (CAGR: {metrics['strategy_cagr']:+.2f}%)")
    print(f"Benchmark Return : {metrics['benchmark_return_pct']:+.2f}% (CAGR: {metrics['benchmark_cagr']:+.2f}%)")
    print("-"*60)
    print(f"Strategy Max DD  : {metrics['strategy_mdd']:.2f}%")
    print(f"Benchmark Max DD : {metrics['benchmark_mdd']:.2f}%")
    print("="*60)
    
    if metrics["trades"]:
        print("\nRecent Completed Trades:")
        print(f"{'Entry Date':<12} | {'Entry Px':<9} | {'Exit Date':<12} | {'Exit Px':<9} | {'Return':<8}")
        print("-"*60)
        for t in metrics["trades"][-5:]:
            print(f"{t['entry_date']:<12} | {t['entry_price']:<9.2f} | {t['exit_date']:<12} | {t['exit_price']:<9.2f} | {t['return_pct']:+.2f}%")
        print("="*60 + "\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MA Crossover Backtester")
    parser.add_argument("--symbol", "-s", default="GOLDBEES", help="Symbol to backtest")
    parser.add_argument("--fast", "-f", type=int, default=50, help="Fast MA period")
    parser.add_argument("--slow", "-l", type=int, default=200, help="Slow MA period")
    parser.add_argument("--type", "-t", choices=["sma", "ema"], default="sma", help="MA Type")
    parser.add_argument("--no-plot", action="store_true", help="Disable plotting")
    args = parser.parse_args()
    
    res = run_crossover_backtest(args.symbol, args.fast, args.slow, args.type, not args.no_plot)
    print_cli_report(res)
