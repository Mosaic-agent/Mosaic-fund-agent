"""
tests/test_market_cap_classifier.py
────────────────────────────────────
Unit tests for get_market_cap_category tool.
"""
from __future__ import annotations

from unittest.mock import patch, MagicMock
import pytest
from src.tools.yahoo_finance import get_market_cap_category, _classify_indian, _classify_us


def test_classify_indian():
    assert _classify_indian(25000) == "Large Cap"
    assert _classify_indian(20000) == "Large Cap"
    assert _classify_indian(10000) == "Mid Cap"
    assert _classify_indian(5000) == "Mid Cap"
    assert _classify_indian(2000) == "Small Cap"
    assert _classify_indian(500) == "Small Cap"
    assert _classify_indian(150) == "Micro Cap"
    assert _classify_indian(0) == "Unknown"
    assert _classify_indian(-10) == "Unknown"


def test_classify_us():
    # raw USD input
    assert _classify_us(250e9) == "Mega Cap"
    assert _classify_us(200e9) == "Mega Cap"
    assert _classify_us(50e9) == "Large Cap"
    assert _classify_us(10e9) == "Large Cap"
    assert _classify_us(5e9) == "Mid Cap"
    assert _classify_us(2e9) == "Mid Cap"
    assert _classify_us(1e9) == "Small Cap"
    assert _classify_us(300e6) == "Small Cap"
    assert _classify_us(100e6) == "Micro Cap"


@patch("yfinance.Ticker")
def test_get_market_cap_category_indian(mock_ticker):
    # Mock ticker.info dict
    mock_instance = MagicMock()
    mock_instance.info = {
        "marketCap": 25000 * 1e7,  # ₹25,000 Crore in raw INR
        "sector": "Technology",
        "industry": "Software—Infrastructure",
    }
    mock_ticker.return_value = mock_instance

    res = get_market_cap_category.invoke({"input_str": "INFY:NSE"})
    assert res["symbol"] == "INFY.NS"
    assert res["cap_category"] == "Large Cap"
    assert res["market_cap_crore"] == 25000
    assert "₹25,000" in res["market_cap_formatted"]
    assert res["exchange"] == "NSE"


@patch("yfinance.Ticker")
def test_get_market_cap_category_us(mock_ticker):
    # Mock ticker.info dict for US stock (e.g. AAPL)
    mock_instance = MagicMock()
    mock_instance.info = {
        "marketCap": 3000 * 1e9,  # $3,000 Billion
        "sector": "Consumer Electronics",
        "industry": "Consumer Electronics",
    }
    mock_ticker.return_value = mock_instance

    res = get_market_cap_category.invoke({"input_str": "AAPL:US"})
    assert res["symbol"] == "AAPL"
    assert res["cap_category"] == "Mega Cap"
    assert res["market_cap_usd_bn"] == 3000
    assert "$3,000" in res["market_cap_formatted"]
    assert res["exchange"] == "US"


@patch("yfinance.Ticker")
def test_get_market_cap_category_not_found(mock_ticker):
    mock_instance = MagicMock()
    mock_instance.info = {}
    mock_ticker.return_value = mock_instance

    res = get_market_cap_category.invoke({"input_str": "INVALID:NSE"})
    assert res["cap_category"] == "Unknown"
    assert res["market_cap_raw"] == 0
    assert "not available" in res["error"]
