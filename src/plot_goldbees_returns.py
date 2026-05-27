import os
import sys
from pathlib import Path
import clickhouse_connect
import pandas as pd
import numpy as np

# Ensure config is importable (since cwd is /app inside container)
sys.path.insert(0, "/app")

from config.settings import settings

def main():
    try:
        # Initialize ClickHouse connection
        # Inside the docker container, host is 'clickhouse'
        client = clickhouse_connect.get_client(
            host="clickhouse",
            port=8123,
            username=settings.clickhouse_user,
            password=settings.clickhouse_password,
            database=settings.clickhouse_database
        )
        
        # Query GOLDBEES prices for the last 90 trading days
        query = """
            SELECT trade_date,
                   toFloat64(argMax(close, imported_at)) AS close
            FROM market_data.daily_prices
            WHERE symbol = 'GOLDBEES' AND category = 'etfs'
            GROUP BY trade_date
            ORDER BY trade_date DESC
            LIMIT 90
        """
        df = client.query_df(query)
        client.close()
        
        if df.empty:
            print("Error: No price data found for GOLDBEES in ClickHouse.")
            sys.exit(1)
            
        # Order chronologically (ascending)
        df = df.sort_values("trade_date").reset_index(drop=True)
        
        # Calculate daily returns (%)
        df['daily_return_pct'] = df['close'].pct_change() * 100
        
        # Calculate cumulative returns (%)
        start_price = df['close'].iloc[0]
        df['cum_return_pct'] = ((df['close'] / start_price) - 1) * 100
        
        # Plot using plotext
        import plotext as plt
        
        # Setup data
        dates = df['trade_date'].astype(str).tolist()
        cum_returns = df['cum_return_pct'].tolist()
        
        # Configure plot
        plt.clear_figure()
        plt.plot(list(range(len(cum_returns))), cum_returns, label="GOLDBEES Cumulative Return", color="gold")
        
        # Decorate plot
        latest_return = cum_returns[-1]
        plt.title(f"GOLDBEES 90-Day Cumulative Return Trend ({dates[0]} to {dates[-1]}) | Latest: {latest_return:+.2f}%")
        plt.xlabel("Trading Days")
        plt.ylabel("Return (%)")
        plt.grid(True)
        plt.plot_size(90, 20)
        
        # Print chart
        plt.show()
        
        # Print summary table
        print("\n" + "="*50)
        print(f"GOLDBEES 90-Day Return Metrics:")
        print(f"  Start Date: {dates[0]} (Price: ₹{df['close'].iloc[0]:.2f})")
        print(f"  End Date:   {dates[-1]} (Price: ₹{df['close'].iloc[-1]:.2f})")
        print(f"  Cumulative Return: {latest_return:+.2f}%")
        print(f"  Daily Return Volatility (Std Dev): {df['daily_return_pct'].std():.2f}%")
        print(f"  Max 90d Price: ₹{df['close'].max():.2f}")
        print(f"  Min 90d Price: ₹{df['close'].min():.2f}")
        print("="*50)
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
