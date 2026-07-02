"""Regression tests for the ClickHouse SQL auto-fixer in src/tools/db_tools.py."""

from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.tools.db_tools import _auto_fix_sql


class TestGenericTimestampAliasFix:
    """LLM-guessed generic timestamp columns (ts, timestamp, dt, ...) should be
    auto-corrected to the real per-table date column, same as the existing
    bare `date` fix."""

    def test_ts_alias_fixed_for_daily_prices(self):
        sql = (
            "SELECT date_trunc('day', ts) AS day, symbol, close_price "
            "FROM market_data.daily_prices WHERE symbol = 'GOLDBEES' "
            "ORDER BY ts DESC LIMIT 1"
        )
        fixed, changes = _auto_fix_sql(sql)
        assert "trade_date" in fixed
        assert "ORDER BY trade_date DESC" in fixed
        assert "close" in fixed and "close_price" not in fixed
        assert any("ts" in c and "trade_date" in c for c in changes)

    def test_reported_union_query_fully_fixed(self):
        """The exact query reported failing against live ClickHouse."""
        sql = (
            "SELECT date_trunc('day', ts) AS day, symbol, close_price "
            "FROM market_data.daily_prices WHERE symbol = 'GOLDBEES' "
            "ORDER BY ts DESC LIMIT 1 UNION ALL "
            "SELECT date_trunc('day', ts) AS day, symbol, close_price "
            "FROM market_data.daily_prices WHERE symbol = 'GC=F' "
            "ORDER BY ts DESC LIMIT 1"
        )
        fixed, changes = _auto_fix_sql(sql)
        assert fixed.count("trade_date") == 4  # 2x date_trunc + 2x ORDER BY
        assert "close_price" not in fixed
        assert "ts" not in fixed or "trade_date" in fixed  # no bare 'ts' survives

    def test_timestamp_alias_fixed(self):
        sql = "SELECT symbol, timestamp FROM market_data.daily_prices WHERE timestamp >= today() - 7"
        fixed, _ = _auto_fix_sql(sql)
        assert "trade_date" in fixed
        assert "timestamp" not in fixed.lower()

    def test_dt_alias_fixed_for_different_table(self):
        sql = "SELECT scheme_code, nav FROM market_data.mf_nav WHERE dt >= today() - 30 ORDER BY dt"
        fixed, changes = _auto_fix_sql(sql)
        assert "nav_date" in fixed
        assert any("nav_date" in c for c in changes)

    def test_no_change_when_correct_column_already_used(self):
        sql = "SELECT symbol, trade_date, close FROM market_data.daily_prices WHERE trade_date >= today() - 30"
        fixed, changes = _auto_fix_sql(sql)
        assert fixed == sql
        assert changes == []

    def test_alias_inside_function_call_not_touched(self):
        """toDate(...)/dateDiff(...) style calls must survive untouched."""
        sql = "SELECT toDate(trade_date) FROM market_data.daily_prices WHERE trade_date >= today()"
        fixed, changes = _auto_fix_sql(sql)
        assert "toDate(trade_date)" in fixed

    def test_unrelated_table_without_date_alias_untouched(self):
        sql = "SELECT * FROM market_data.some_unmapped_table WHERE ts > 0"
        fixed, changes = _auto_fix_sql(sql)
        assert fixed == sql
        assert changes == []

    def test_bare_date_fix_still_works(self):
        """Regression guard: the original bare-`date` fix must keep working
        after generalizing to the alias list."""
        sql = "SELECT symbol, date, close FROM market_data.daily_prices WHERE date >= today() - 30"
        fixed, changes = _auto_fix_sql(sql)
        assert "trade_date" in fixed
        assert " date," not in fixed


class TestCommoditySymbolAliasFix:
    """market_data.daily_prices stores commodities as plain names (GOLD,
    SILVER, ...) under category='commodities'. An LLM/user naturally guesses
    ticker or FX-pair notation (XAUUSD, GC=F, XAU) instead — these must be
    silently corrected to the stored symbol."""

    def test_xauusd_fixed_to_gold(self):
        sql = "SELECT symbol, close FROM market_data.daily_prices WHERE symbol = 'XAUUSD' ORDER BY trade_date DESC LIMIT 1"
        fixed, changes = _auto_fix_sql(sql)
        assert "'GOLD'" in fixed
        assert any("XAUUSD" in c and "GOLD" in c for c in changes)

    def test_gc_equals_f_fixed_to_gold(self):
        sql = "SELECT close FROM market_data.daily_prices WHERE symbol = 'GC=F'"
        fixed, changes = _auto_fix_sql(sql)
        assert "'GOLD'" in fixed

    def test_xau_fixed_to_gold(self):
        sql = "SELECT close FROM market_data.daily_prices WHERE symbol = 'XAU'"
        fixed, _ = _auto_fix_sql(sql)
        assert "'GOLD'" in fixed

    def test_silver_aliases_fixed(self):
        for alias in ("XAGUSD", "XAG", "SI=F"):
            sql = f"SELECT close FROM market_data.daily_prices WHERE symbol = '{alias}'"
            fixed, _ = _auto_fix_sql(sql)
            assert "'SILVER'" in fixed, f"failed for alias {alias}: {fixed}"

    def test_full_reported_query_end_to_end(self):
        """Combines the column-alias fix (ts) and symbol-alias fix (XAUUSD)
        together, matching the originally reported failing query shape."""
        sql = (
            "SELECT symbol, ts, close_price FROM market_data.daily_prices "
            "WHERE symbol = 'XAUUSD' ORDER BY ts DESC LIMIT 1"
        )
        fixed, changes = _auto_fix_sql(sql)
        assert "trade_date" in fixed
        assert "'GOLD'" in fixed
        assert "close_price" not in fixed
        assert len(changes) == 3

    def test_correct_symbol_already_used_is_untouched(self):
        sql = "SELECT close FROM market_data.daily_prices WHERE symbol = 'GOLD'"
        fixed, changes = _auto_fix_sql(sql)
        assert fixed == sql
        assert changes == []

    def test_alias_on_unrelated_table_untouched(self):
        """The commodity symbol map is scoped to daily_prices only."""
        sql = "SELECT * FROM market_data.mf_nav WHERE symbol = 'XAU'"
        fixed, changes = _auto_fix_sql(sql)
        assert fixed == sql
        assert changes == []
