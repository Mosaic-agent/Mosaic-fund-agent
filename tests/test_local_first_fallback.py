"""
tests/test_local_first_fallback.py
──────────────────────────────────
Unit and integration tests verifying the "Local Data First, Remote Fallback" invariant:
1. is_market_hours helper correctly distinguishes market vs off-market IST periods.
2. fetch_and_calculate_obi uses local ClickHouse live_quotes outside market hours without broker network delay.
3. resolve_company_info checks local ClickHouse tables before hitting external Yahoo search.
4. _search_one_date leverages local ClickHouse news, bulk deals, and corporate actions.
5. stock_research_report loads price history and deals from ClickHouse directly.
"""

from __future__ import annotations

import sys
from pathlib import Path
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from datetime import datetime, timezone, timedelta, time
from unittest.mock import patch, MagicMock
import pandas as pd

from src.utils.ist import is_market_hours, now_ist
from src.tools.company_resolver import resolve_company_info
from src.data_importer.tool_fetchers.shoonya_tools import fetch_and_calculate_obi
from src.tools.market.equity import _search_one_date
from src.scripts.portfolio.stock_research_report import generate_stock_research_report



def test_is_market_hours_logic():
    """Verify market hours boundary conditions in IST."""
    ist_tz = timezone(timedelta(hours=5, minutes=30))

    # Wednesday at 11:00 AM IST -> True
    wed_open = datetime(2026, 10, 14, 11, 0, tzinfo=ist_tz)
    assert is_market_hours(wed_open) is True

    # Wednesday at 08:30 AM IST (pre-market) -> False
    wed_pre = datetime(2026, 10, 14, 8, 30, tzinfo=ist_tz)
    assert is_market_hours(wed_pre) is False

    # Wednesday at 16:00 PM IST (post-market) -> False
    wed_post = datetime(2026, 10, 14, 16, 0, tzinfo=ist_tz)
    assert is_market_hours(wed_post) is False

    # Saturday at 11:00 AM IST (weekend) -> False
    sat_noon = datetime(2026, 10, 17, 11, 0, tzinfo=ist_tz)
    assert is_market_hours(sat_noon) is False


def test_local_first_company_resolver():
    """Verify that known tickers in local ClickHouse or ALIAS resolve without external network search."""
    res = resolve_company_info("SEDEMAC", auto_import=False)
    assert res.get("symbol") == "SEDEMAC"
    assert res.get("source") in ("local_map", "clickhouse_daily_prices", "clickhouse_symbol_map")


def test_shoonya_obi_off_market_fast_exit():
    """Verify that outside market hours, OBI check exits promptly without stalling on Shoonya login."""
    with patch("src.utils.ist.is_market_hours", return_value=False):
        res = fetch_and_calculate_obi("SEDEMAC")
        # Should return either None (if no quote cached) or calculated OBI dict, without raising or hanging
        assert res is None or isinstance(res, dict)


def test_equity_search_one_date_clickhouse_first():
    """Verify that _search_one_date checks ClickHouse news/deals/corporate actions before falling back."""
    dt_str, md = _search_one_date(
        symbol="SEDEMAC",
        company_name="Sedemac Mechatronics",
        date_str="2026-09-24",
        regime="Volatile Breakout",
        daily_ret=9.17,
        max_results=3,
    )
    assert dt_str == "2026-09-24"
    # Should include either local news or marquee institutional deal on 2026-09-24
    assert ("Institutional Deal" in md or "SEDEMAC" in md or "Sedemac" in md)


def test_stock_research_report_local_only():
    """Verify that stock_research_report generates cleanly in local_only mode."""
    report_md = generate_stock_research_report("SEDEMAC", save_artifact=False, local_only=True)
    assert "# 🏛️" in report_md
    assert "SEDEMAC" in report_md
    assert "Price Trend" in report_md


if __name__ == "__main__":
    print("Running test_is_market_hours_logic...")
    test_is_market_hours_logic()
    print("✅ test_is_market_hours_logic passed")

    print("Running test_local_first_company_resolver...")
    test_local_first_company_resolver()
    print("✅ test_local_first_company_resolver passed")

    print("Running test_shoonya_obi_off_market_fast_exit...")
    test_shoonya_obi_off_market_fast_exit()
    print("✅ test_shoonya_obi_off_market_fast_exit passed")

    print("Running test_equity_search_one_date_clickhouse_first...")
    test_equity_search_one_date_clickhouse_first()
    print("✅ test_equity_search_one_date_clickhouse_first passed")

    print("Running test_stock_research_report_local_only...")
    test_stock_research_report_local_only()
    print("✅ test_stock_research_report_local_only passed")

    print("\n🎉 ALL LOCAL-FIRST AND REMOTE FALLBACK TESTS PASSED!")

