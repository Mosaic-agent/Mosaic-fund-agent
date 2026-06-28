"""
tests/test_etf_setup_scanner.py
───────────────────────────────
Unit tests for the ETF volume-volatility setups scanner.
"""

from unittest.mock import patch
import pandas as pd
import numpy as np
from src.tools.etf_setup_scanner import run_etf_setup_scan, scan_etf_setups, run_etf_trend_scan, scan_etf_trends

def test_run_etf_setup_scan_empty():
    with patch("src.db.pool.query_df", return_value=pd.DataFrame()):
        res = run_etf_setup_scan()
        assert res == []

def test_run_etf_setup_scan_breakout():
    # Construct mock data for 25 periods to trigger calculations
    dates = pd.date_range(end="2026-06-25", periods=25, freq="D")
    
    # Setup breakout: volume is 3x MA, return is large relative to volatility
    prices = [100.0] * 24 + [105.0]  # +5% return at last index
    volumes = [1000.0] * 24 + [3000.0]  # 3x volume MA
    
    mock_df = pd.DataFrame({
        "symbol": ["GOLDBEES"] * 25,
        "trade_date": dates,
        "close": prices,
        "volume": volumes
    })
    
    with patch("src.db.pool.query_df", return_value=mock_df):
        res = run_etf_setup_scan()
        assert len(res) == 1
        gold = res[0]
        assert gold["symbol"] == "GOLDBEES"
        assert gold["pattern"] == "🚀 Volatile Breakout"
        assert gold["volume_vs_ma"] > 1.5
        assert gold["daily_return"] == 5.0

def test_run_etf_setup_scan_exhaustion():
    dates = pd.date_range(end="2026-06-25", periods=25, freq="D")
    
    # Setup exhaustion: volume is 0.2x MA, return is large relative to volatility
    prices = [100.0] * 24 + [95.0]  # -5% return
    volumes = [1000.0] * 24 + [200.0]  # 0.2x volume MA
    
    mock_df = pd.DataFrame({
        "symbol": ["SILVERBEES"] * 25,
        "trade_date": dates,
        "close": prices,
        "volume": volumes
    })
    
    with patch("src.db.pool.query_df", return_value=mock_df):
        res = run_etf_setup_scan()
        assert len(res) == 1
        silver = res[0]
        assert silver["symbol"] == "SILVERBEES"
        assert silver["pattern"] == "⚠️ Volume Exhaustion"
        assert silver["volume_vs_ma"] < 0.7
        assert silver["daily_return"] == -5.0

def test_scan_etf_setups_tool():
    dates = pd.date_range(end="2026-06-25", periods=25, freq="D")
    prices = [100.0] * 25
    volumes = [1000.0] * 25
    mock_df = pd.DataFrame({
        "symbol": ["NIFTYBEES"] * 25,
        "trade_date": dates,
        "close": prices,
        "volume": volumes
    })
    
    with patch("src.db.pool.query_df", return_value=mock_df):
        report = scan_etf_setups.invoke({})
        assert "ETF Volume-Volatility Setup Scan" in report
        assert "NIFTYBEES" in report
        assert "Normal" in report


def test_run_etf_trend_scan():
    # Construct 65 periods of mock data
    dates = pd.date_range(end="2026-06-25", periods=65, freq="D")
    
    # Setup strongly bearish: close decreases consistently
    # prices: 100, 95, 90 ...
    prices = [100.0 - i * 0.5 for i in range(65)]
    
    mock_df = pd.DataFrame({
        "symbol": ["GOLDBEES"] * 65,
        "trade_date": dates,
        "close": prices
    })
    
    with patch("src.db.pool.query_df", return_value=mock_df):
        res = run_etf_trend_scan()
        assert len(res) == 1
        gold = res[0]
        assert gold["symbol"] == "GOLDBEES"
        assert gold["status"] == "🔴 Strongly Bearish"
        assert gold["return_5d"] < 0
        assert gold["return_20d"] < 0
        assert gold["return_60d"] < 0


def test_scan_etf_trends_tool():
    dates = pd.date_range(end="2026-06-25", periods=65, freq="D")
    prices = [100.0] * 65
    mock_df = pd.DataFrame({
        "symbol": ["NIFTYBEES"] * 65,
        "trade_date": dates,
        "close": prices
    })
    
    with patch("src.db.pool.query_df", return_value=mock_df):
        report = scan_etf_trends.invoke({})
        assert "ETF Trend Status Scan" in report
        assert "NIFTYBEES" in report
        assert "🟢 Bullish" in report

