"""
tests/test_ma_crossover.py
──────────────────────────
Unit tests for the Moving Average Crossover Backtester.
"""

import os
from unittest.mock import patch
import pandas as pd
from src.scripts.market.ma_crossover_backtest import run_crossover_backtest

def test_run_crossover_backtest_insufficient():
    # Setup less than slow MA window (e.g. 50 periods for 200d MA)
    dates = pd.date_range(end="2026-06-25", periods=50, freq="D")
    prices = [100.0] * 50
    mock_df = pd.DataFrame({
        "trade_date": dates,
        "close": prices,
        "volume": [1000.0] * 50
    })
    
    with patch("src.scripts.market.ma_crossover_backtest.query_df", return_value=mock_df):
        res = run_crossover_backtest("GOLDBEES", fast=10, slow=60, ma_type="sma", plot=False)
        assert "error" in res
        assert "Insufficient data" in res["error"]

def test_run_crossover_backtest_execution():
    # Setup 100 periods with crossover (fast=10, slow=40)
    # Price starts at 100, then rises to 120, then falls back to 90
    dates = pd.date_range(end="2026-06-25", periods=100, freq="D")
    
    prices = []
    for i in range(100):
        if i < 40:
            prices.append(100.0)
        elif i < 70:
            # Rise to trigger Golden Cross
            prices.append(100.0 + (i - 40) * 1.5)
        else:
            # Drop to trigger Death Cross
            prices.append(145.0 - (i - 70) * 2.5)
            
    mock_df = pd.DataFrame({
        "trade_date": dates,
        "close": prices,
        "volume": [10000.0] * 100
    })
    
    with patch("src.scripts.market.ma_crossover_backtest.query_df", return_value=mock_df):
        # Disable plotting to keep it fast and dependency-free
        res = run_crossover_backtest("GOLDBEES", fast=5, slow=30, ma_type="sma", plot=False)
        
        assert "error" not in res
        assert res["symbol"] == "GOLDBEES"
        assert res["total_trades"] >= 1
        assert "strategy_return_pct" in res
        assert "benchmark_return_pct" in res
        assert "strategy_cagr" in res
        assert "strategy_mdd" in res
        assert "sharpe_ratio" in res
        assert "win_rate" in res
        
        # Verify trades list structure
        assert len(res["trades"]) >= 1
        trade = res["trades"][0]
        assert "entry_date" in trade
        assert "entry_price" in trade
        assert "exit_date" in trade
        assert "exit_price" in trade
        assert "return_pct" in trade
