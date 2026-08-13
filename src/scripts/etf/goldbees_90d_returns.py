"""
Plot GOLDBEES rolling 90-trading-day returns (%) as an ASCII line chart.

Fetches daily close prices for GOLDBEES from ClickHouse, computes the
rolling 90-day percentage return series (return of buying 90 trading
days ago vs today), and renders it with plotext.

Usage:
    python src/scripts/etf/goldbees_90d_returns.py [--days N]

    --days N   Number of most-recent trading days of the *rolling return*
               series to display (default 250). The underlying price
               history pulled is N + 90 to allow the 90-day lookback.
"""
import argparse
import sys

import pandas as pd

from src.db.pool import get_pool

try:
    import plotext as plt
except ImportError:
    print("plotext not installed. Run: pip install plotext")
    sys.exit(1)


def fetch_prices(symbol: str, lookback_days: int) -> pd.DataFrame:
    pool = get_pool()
    query = f"""
        SELECT trade_date AS date, close
        FROM market_data.daily_prices FINAL
        WHERE symbol = '{symbol}'
        ORDER BY trade_date DESC
        LIMIT {lookback_days}
    """
    df = pool.query_df(query)
    df = df.sort_values("date").reset_index(drop=True)
    return df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=250,
                         help="Number of days of rolling-return series to plot")
    args = parser.parse_args()

    symbol = "GOLDBEES"
    window = 90
    total_lookback = args.days + window + 10  # buffer

    df = fetch_prices(symbol, total_lookback)
    if df.empty:
        print(f"No price data found for {symbol}")
        return

    df["ret_90d"] = (df["close"] / df["close"].shift(window) - 1) * 100
    df = df.dropna(subset=["ret_90d"]).tail(args.days).reset_index(drop=True)

    if df.empty:
        print("Not enough history to compute 90-day rolling returns.")
        return

    dates = df["date"].astype(str).tolist()
    returns = df["ret_90d"].round(2).tolist()

    plt.clear_data()
    plt.clear_figure()
    plt.date_form("Y-m-d")
    plt.plot(dates, returns, marker="dot")
    plt.title(f"{symbol} — Rolling 90-Trading-Day Return (%)")
    plt.xlabel("Date")
    plt.ylabel("90D Return %")
    plt.plotsize(100, 30)
    plt.show()

    latest = df.iloc[-1]
    print(f"\nLatest 90-day return as of {latest['date']}: {round(latest['ret_90d'], 2)}%")
    print(f"Max 90-day return in window: {round(df['ret_90d'].max(), 2)}%")
    print(f"Min 90-day return in window: {round(df['ret_90d'].min(), 2)}%")
    print(f"Mean 90-day return in window: {round(df['ret_90d'].mean(), 2)}%")


if __name__ == "__main__":
    main()
