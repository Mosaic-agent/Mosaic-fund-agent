import pytest
from unittest.mock import patch
from src.importer.fetchers.nse_corporate_actions_fetcher import fetch_corporate_actions

def test_fetch_corporate_actions_nse_success():
    # Test normal fetch (should return list of dicts from either NSE or yfinance fallback)
    res = fetch_corporate_actions("MSUMI")
    assert isinstance(res, list)
    if res:
        for r in res:
            assert "symbol" in r
            assert r["symbol"] == "MSUMI"
            assert "ex_date" in r
            assert "action_type" in r

@patch("httpx.Client.get")
def test_fetch_corporate_actions_yfinance_fallback(mock_get):
    # Mock httpx.Client.get to raise an exception, forcing the NSE fetch to fail
    mock_get.side_effect = Exception("NSE Blocked or Offline")
    
    res = fetch_corporate_actions("MSUMI")
    assert isinstance(res, list)
    assert len(res) > 0
    # The source should be 'yfinance'
    sources = {r["source"] for r in res}
    assert "yfinance" in sources
    # Check that we got split/bonus or dividend events
    action_types = {r["action_type"] for r in res}
    assert "split" in action_types or "dividend" in action_types
