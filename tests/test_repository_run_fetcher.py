import sys
import os
import unittest
from datetime import date, timedelta
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.importer.base_fetcher import Fetcher
from src.importer.cli import _resolve_from_date
from src.db.repository import MarketDataRepository


class FlakyFetcher(Fetcher):
    """supports_parallel fetcher where one symbol always raises inside fetch()."""

    supports_parallel = True
    overlap_days = 3

    def __init__(self, category, symbols):
        self.category = category
        self.symbols = symbols
        self.source_name = "flaky"
        self.symbol_key = "FLAKY_GROUP"

    def fetch(self, from_date, to_date, *, source=None):
        sym = self.symbols[0][0]
        if sym == "BAD":
            raise RuntimeError("simulated upstream failure")
        return [{"symbol": sym, "trade_date": to_date, "close": 1.0}]

    def insert(self, rows, ch):
        return len(rows)


class InstantFetcher(Fetcher):
    """supports_parallel fetcher that returns immediately, for progress_cb tests."""

    supports_parallel = True
    overlap_days = 3

    def __init__(self, category, symbols):
        self.category = category
        self.symbols = symbols
        self.source_name = "instant"
        self.symbol_key = "INSTANT_GROUP"

    def fetch(self, from_date, to_date, *, source=None):
        sym = self.symbols[0][0]
        return [{"symbol": sym, "trade_date": to_date, "close": 1.0}]

    def insert(self, rows, ch):
        return len(rows)


class FakeCH:
    """In-memory (source, symbol[, dataset]) -> date watermark store, no ClickHouse needed."""

    def __init__(self, seed: dict):
        self._store = dict(seed)

    def get_watermark(self, source, symbol, dataset="prices"):
        if (source, symbol, dataset) in self._store:
            return self._store[(source, symbol, dataset)]
        return self._store.get((source, symbol))

    def set_watermark(self, source, symbol, last_date, dataset="prices"):
        self._store[(source, symbol, dataset)] = last_date


class TestFetchParallel(unittest.TestCase):
    def test_fetch_parallel_isolates_failures(self):
        """One bad symbol must not fail the batch — matches spec §7.3."""
        fetcher = FlakyFetcher("x", [("GOOD1", "GOOD1.NS"), ("BAD", "BAD.NS"), ("GOOD2", "GOOD2.NS")])
        repo = MarketDataRepository(pool=None)

        rows = repo._fetch_parallel(fetcher, date(2026, 1, 1), date(2026, 1, 2), workers=3)

        self.assertEqual({r["symbol"] for r in rows}, {"GOOD1", "GOOD2"})

    def test_progress_callback_fires_once_per_symbol(self):
        """progress_cb fires exactly once per symbol, in any completion order — spec §7.5."""
        symbols = [("A", "A.NS"), ("B", "B.NS"), ("C", "C.NS"), ("D", "D.NS")]
        fetcher = InstantFetcher("x", symbols)
        repo = MarketDataRepository(pool=None)

        seen = []
        repo._fetch_parallel(
            fetcher, date(2026, 1, 1), date(2026, 1, 2), workers=4,
            progress_cb=seen.append,
        )

        self.assertEqual(sorted(seen), sorted(s for s, _ in symbols))


class TestSourceOverride(unittest.TestCase):
    @patch("src.importer.fetchers.adapters.NSElibFetcher.fetch")
    def test_source_override_routes_to_nselib(self, mock_fetch):
        """source="nse" must route to NSElibFetcher, not silently fall back to shoonya — spec §7.4.

        Constructs ShoonyaFetcher directly rather than via get_registry() —
        get_registry() caches a process-wide singleton, so any earlier test
        (in any file, any order) that mocks get_symbols_for_categories before
        the registry is first built would poison every later test that reads
        get_registry() for the rest of the process.
        """
        from src.importer.fetchers.adapters import ShoonyaFetcher

        mock_fetch.return_value = []
        fetcher = ShoonyaFetcher("etfs", [("GOLDBEES", "GOLDBEES.NS")])

        fetcher.fetch(date(2026, 1, 1), date(2026, 1, 2), source="nse")

        mock_fetch.assert_called_once()


class TestStocksPerSymbolWatermark(unittest.TestCase):
    """StocksFetcher must resolve each symbol's watermark independently —
    a shared group worst-case (like etfs) would mean adding one never-before-
    imported symbol forces every already-caught-up symbol back to a full
    lookback re-fetch. Regression coverage for that specific failure mode."""

    @patch("src.importer.fetchers.adapters.StocksFetcher.fetch", autospec=True)
    def test_new_symbol_does_not_drag_down_caught_up_symbol(self, mock_fetch):
        from src.importer.fetchers.adapters import StocksFetcher

        mock_fetch.return_value = []
        today = date(2026, 7, 31)
        ch = FakeCH({("shoonya", "CAUGHTUP", "prices"): date(2026, 7, 29)})
        fetcher = StocksFetcher("stocks", [("CAUGHTUP", "CAUGHTUP.NS"), ("NEWSYM", "NEWSYM.NS")])
        repo = MarketDataRepository(pool=None)

        repo._fetch_parallel(
            fetcher, today - timedelta(days=3650), today, workers=2,
            ch=ch, per_symbol_watermark=True, lookback_days=3650, full=False,
            effective_source="shoonya",
        )

        from_date_by_symbol = {}
        for c in mock_fetch.call_args_list:
            self_instance, sym_from, _sym_to = c.args
            from_date_by_symbol[self_instance.symbols[0][0]] = sym_from

        self.assertEqual(from_date_by_symbol["NEWSYM"], today - timedelta(days=3650))
        self.assertEqual(from_date_by_symbol["CAUGHTUP"], date(2026, 7, 29) - timedelta(days=3))


class TestStocksSourceOverrideWatermark(unittest.TestCase):
    """A --data-source override must be reflected in the watermark key that
    gets written, or a "nse"-sourced run's watermark is invisible to
    tomorrow's default-source ("shoonya") run and vice versa."""

    def test_write_group_watermarks_uses_effective_source(self):
        from src.importer.fetchers.adapters import StocksFetcher

        fetcher = StocksFetcher("stocks", [("RELIANCE", "RELIANCE.NS")])
        ch = FakeCH({})
        rows = [{"symbol": "RELIANCE", "trade_date": date(2026, 7, 30), "close": 100.0, "_dataset": "prices"}]

        fetcher.write_group_watermarks(ch, rows, dry_run=False, source="nse")

        self.assertEqual(ch.get_watermark("nse", "RELIANCE", dataset="prices"), date(2026, 7, 30))
        self.assertIsNone(ch.get_watermark("shoonya", "RELIANCE", dataset="prices"))

    def test_dry_run_count_matches_real_insert_count(self):
        """dry-run must report the prices-only count insert() would really
        write, not len(rows) across all four datasets combined — caught via
        a live dry-run smoke test where stocks showed ~7x too many rows."""
        from src.importer.fetchers.adapters import StocksFetcher

        fetcher = StocksFetcher("stocks", [("RELIANCE", "RELIANCE.NS")])
        rows = [
            {"symbol": "RELIANCE", "trade_date": date(2026, 7, 30), "close": 100.0, "_dataset": "prices"},
            {"symbol": "RELIANCE", "trade_date": date(2026, 7, 29), "close": 99.0, "_dataset": "prices"},
            {"symbol": "RELIANCE", "earnings_date": date(2026, 7, 20), "_dataset": "earnings"},
            {"symbol": "RELIANCE", "transaction_date": date(2026, 7, 15), "_dataset": "insider"},
        ]

        self.assertEqual(fetcher.count_insertable(rows), 2)

    def test_us_stocks_ignores_source_override(self):
        """us_stocks has no Shoonya/NSE presence — a --data-source override
        must not affect it, matching parallel_importer's hardcoded
        data_source="yfinance" for us_stocks today."""
        from src.importer.fetchers.adapters import StocksFetcher

        self.assertFalse(StocksFetcher("us_stocks", [("AAPL", "AAPL")]).supports_source_override)
        self.assertTrue(StocksFetcher("stocks", [("RELIANCE", "RELIANCE.NS")]).supports_source_override)


class TestWatermarkEquivalence(unittest.TestCase):
    def test_resolve_group_from_date_matches_cli_resolve_from_date(self):
        """New repository._resolve_group_from_date must agree with the existing
        cli._resolve_from_date on the same fixture watermarks — spec §7.2."""
        today = date(2026, 7, 31)
        seed = {
            ("shoonya", "GOLDBEES"):   date(2026, 7, 20),
            ("shoonya", "NIFTYBEES"):  date(2026, 7, 18),  # earliest — drives worst-case
            ("shoonya", "BANKBEES"):   date(2026, 7, 25),
        }
        symbols = ["GOLDBEES", "NIFTYBEES", "BANKBEES"]

        old_from = _resolve_from_date(
            FakeCH(seed), "shoonya", symbols,
            lookback_days=365, overlap_days=3, full_reimport=False,
            dry_run=False, today=today,
        )

        repo = MarketDataRepository(pool=None)
        new_from = repo._resolve_group_from_date(
            FakeCH(seed), "shoonya", symbols,
            lookback_days=365, overlap_days=3, full=False, today=today,
        )

        self.assertEqual(old_from, new_from)
        self.assertEqual(old_from, date(2026, 7, 18) - timedelta(days=3))

    def test_resolve_group_from_date_full_lookback_when_no_watermark(self):
        today = date(2026, 7, 31)
        new_from = MarketDataRepository(pool=None)._resolve_group_from_date(
            FakeCH({}), "shoonya", ["NEWSYMBOL"],
            lookback_days=365, overlap_days=3, full=False, today=today,
        )
        self.assertEqual(new_from, today - timedelta(days=365))


if __name__ == "__main__":
    unittest.main()
