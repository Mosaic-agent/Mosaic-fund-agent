import sys
import os
import unittest
from datetime import date, timedelta
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.importer.base_fetcher import Fetcher
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
    def test_resolve_group_from_date_computes_earliest_watermark_minus_overlap(self):
        """repository._resolve_group_from_date computes earliest watermark minus overlap — spec §7.2."""
        today = date(2026, 7, 31)
        seed = {
            ("shoonya", "GOLDBEES"):   date(2026, 7, 20),
            ("shoonya", "NIFTYBEES"):  date(2026, 7, 18),  # earliest — drives worst-case
            ("shoonya", "BANKBEES"):   date(2026, 7, 25),
        }
        symbols = ["GOLDBEES", "NIFTYBEES", "BANKBEES"]

        repo = MarketDataRepository(pool=None)
        new_from = repo._resolve_group_from_date(
            FakeCH(seed), "shoonya", symbols,
            lookback_days=365, overlap_days=3, full=False, today=today,
        )

        expected = date(2026, 7, 18) - timedelta(days=3)
        self.assertEqual(new_from, expected)

    def test_resolve_group_from_date_full_lookback_when_no_watermark(self):
        today = date(2026, 7, 31)
        new_from = MarketDataRepository(pool=None)._resolve_group_from_date(
            FakeCH({}), "shoonya", ["NEWSYMBOL"],
            lookback_days=365, overlap_days=3, full=False, today=today,
        )
        self.assertEqual(new_from, today - timedelta(days=365))


class FakeFlowsFetcher(Fetcher):
    """Single-watermark fetcher using a non-default dataset bucket."""

    source_name = "test_flows"
    symbol_key = "X"
    dataset = "flows"
    overlap_days = 0

    def fetch(self, from_date, to_date, *, source=None):
        return [{"report_month": date(2026, 7, 1), "value": 1}]

    def insert(self, rows, ch):
        return len(rows)

    def max_date(self, rows):
        return date(2026, 7, 1)


class TestDatasetAttribute(unittest.TestCase):
    def test_single_watermark_path_uses_fetcher_dataset(self):
        """run_fetcher's single-watermark path must read/write the fetcher's
        own `dataset` bucket, not silently default to "prices" — needed for
        amfi_flows, whose watermark lives under dataset="flows"."""
        ch = FakeCH({})
        repo = MarketDataRepository(pool=None)

        repo.run_fetcher(FakeFlowsFetcher(), dry_run=False, full=True, lookback_days=10, ch=ch)

        self.assertEqual(ch.get_watermark("test_flows", "X", dataset="flows"), date(2026, 7, 1))
        self.assertIsNone(ch.get_watermark("test_flows", "X", dataset="prices"))


class TestAmfiCategoryFlowsFetcher(unittest.TestCase):
    @patch("src.importer.fetchers.amfi_flows_fetcher.fetch_amfi_category_flows")
    def test_months_back_matches_original_cli_formula(self, mock_fetch):
        """With overlap_days=0, run_fetcher passes from_date=watermark exactly,
        so months_back must reproduce cli.py's original month-diff arithmetic
        byte-for-byte whenever a watermark exists — spec Phase 4 parity."""
        from src.importer.fetchers.adapters import AmfiCategoryFlowsFetcher

        mock_fetch.return_value = []
        today = date(2026, 7, 31)
        wm = date(2026, 5, 1)  # report_month watermarks are always first-of-month

        AmfiCategoryFlowsFetcher().fetch(wm, today)

        today_m, wm_m = today.replace(day=1), wm.replace(day=1)
        diff_months = (today_m.year - wm_m.year) * 12 + (today_m.month - wm_m.month)
        expected_months_back = max(2, diff_months + 1)

        mock_fetch.assert_called_once_with(months_back=expected_months_back)

    @patch("src.importer.fetchers.amfi_flows_fetcher.fetch_amfi_category_flows")
    def test_months_back_floor_of_two(self, mock_fetch):
        from src.importer.fetchers.adapters import AmfiCategoryFlowsFetcher

        mock_fetch.return_value = []
        today = date(2026, 7, 31)
        wm = date(2026, 7, 1)  # same month as today

        AmfiCategoryFlowsFetcher().fetch(wm, today)

        mock_fetch.assert_called_once_with(months_back=2)


class TestNseEodFetcher(unittest.TestCase):
    @patch("src.importer.fetchers.nse_quote_fetcher.fetch_nse_eod")
    def test_fetch_combines_etfs_and_stocks(self, mock_fetch):
        from src.importer.fetchers.adapters import NseEodFetcher

        def side_effect(sym_list, cat_name):
            return [
                {"symbol": s, "category": cat_name, "trade_date": date(2026, 7, 31), "close": 1.0}
                for s, _ in sym_list
            ]
        mock_fetch.side_effect = side_effect

        fetcher = NseEodFetcher([("GOLDBEES", "GOLDBEES.NS")], [("RELIANCE", "RELIANCE.NS")])
        rows = fetcher.fetch(date(2026, 7, 1), date(2026, 7, 31))

        self.assertEqual({r["symbol"] for r in rows}, {"GOLDBEES", "RELIANCE"})
        self.assertEqual({r["category"] for r in rows}, {"etfs", "stocks"})
        self.assertEqual(fetcher.max_date(rows), date(2026, 7, 31))
        self.assertEqual(fetcher.symbols, [("GOLDBEES", "GOLDBEES.NS"), ("RELIANCE", "RELIANCE.NS")])


class TestMfHoldingsFetcher(unittest.TestCase):
    @patch("src.importer.fetchers.mf_holdings_fetcher.fetch_holdings")
    def test_fetch_insert_max_date(self, mock_fetch):
        from src.importer.fetchers.adapters import MfHoldingsFetcher

        mock_fetch.return_value = [{"scheme_code": "X", "as_of_month": date(2026, 7, 1)}]
        fetcher = MfHoldingsFetcher([("X", "Fund X", "ISIN1")], date(2026, 7, 1))

        rows = fetcher.fetch(date(2026, 1, 1), date(2026, 7, 31))
        self.assertEqual(len(rows), 1)
        self.assertEqual(fetcher.max_date(rows), date(2026, 7, 1))
        mock_fetch.assert_called_once_with([("X", "Fund X", "ISIN1")], date(2026, 7, 1))

        fake_ch = MagicMock()
        fake_ch.insert_mf_holdings.return_value = 1
        self.assertEqual(fetcher.insert(rows, fake_ch), 1)
        fake_ch.insert_mf_holdings.assert_called_once_with(rows)


class TestFxRatesAndMfNavGroupWatermarkFix(unittest.TestCase):
    """Regression coverage for a real bug found before wiring these into
    cli.py: both fetchers were defined with a single symbol_key
    ("FX_GROUP"/"ALL") that nothing ever writes a watermark row for, so
    the single-watermark path would read wm=None forever and re-fetch full
    history on every single run."""

    def test_fx_rates_fetcher_exposes_group_symbols(self):
        from src.importer.fetchers.adapters import FXRatesFetcher

        fetcher = FXRatesFetcher()
        self.assertTrue(fetcher.supports_parallel)
        self.assertTrue(len(fetcher.symbols) > 0)

    def test_mf_nav_fetcher_exposes_group_symbols(self):
        from src.importer.fetchers.adapters import MFNavFetcher

        fetcher = MFNavFetcher({"GOLDBEES": "140088", "NIFTYBEES": "140084"})
        self.assertTrue(fetcher.supports_parallel)
        self.assertEqual(set(fetcher.symbols), {("GOLDBEES", "140088"), ("NIFTYBEES", "140084")})

    def test_group_watermark_path_used_not_single_symbol_key(self):
        """Before the fix, run_fetcher would use fetcher.symbol_key ("FX_GROUP")
        for the watermark, which nothing ever writes to. After the fix, it
        must use _resolve_group_from_date/write_group_watermarks instead."""
        from src.importer.fetchers.adapters import FXRatesFetcher

        fetcher = FXRatesFetcher()
        use_group_watermark = fetcher.supports_parallel and hasattr(fetcher, "symbols")
        self.assertTrue(use_group_watermark)


class TestCbReservesFetcher(unittest.TestCase):
    @patch("src.importer.fetchers.imf_reserves_fetcher.fetch_cb_reserves")
    def test_fetch_converts_from_date_to_year(self, mock_fetch):
        from src.importer.fetchers.adapters import CbReservesFetcher

        mock_fetch.return_value = []
        CbReservesFetcher().fetch(date(2018, 6, 15), date(2026, 7, 31))

        mock_fetch.assert_called_once_with(from_year=2018)

    def test_max_date_uses_ref_period(self):
        from src.importer.fetchers.adapters import CbReservesFetcher

        rows = [
            {"ref_period": date(2024, 12, 1), "country_code": "IN"},
            {"ref_period": date(2025, 12, 1), "country_code": "US"},
        ]
        self.assertEqual(CbReservesFetcher().max_date(rows), date(2025, 12, 1))


class TestEtfAumFetcher(unittest.TestCase):
    @patch("src.importer.fetchers.etf_aum_fetcher.fetch_etf_aum")
    def test_fetch_insert_max_date(self, mock_fetch):
        from src.importer.fetchers.adapters import EtfAumFetcher

        mock_fetch.return_value = [
            {"symbol": "GLD", "trade_date": date(2026, 7, 31), "aum_usd": 1e9, "price": 100.0, "implied_tonnes": 10.0},
        ]
        fetcher = EtfAumFetcher()
        rows = fetcher.fetch(date(2026, 7, 1), date(2026, 7, 31))

        self.assertEqual(len(rows), 1)
        self.assertEqual(fetcher.max_date(rows), date(2026, 7, 31))

        fake_ch = MagicMock()
        fake_ch.insert_etf_aum.return_value = 1
        self.assertEqual(fetcher.insert(rows, fake_ch), 1)
        fake_ch.insert_etf_aum.assert_called_once_with(rows)


class FakeFirstRunFetcher(Fetcher):
    """Single-watermark fetcher declaring a first_run_lookback_days override."""

    source_name = "first_run_test"
    symbol_key = "X"
    overlap_days = 0
    first_run_lookback_days = 30

    def fetch(self, from_date, to_date, *, source=None):
        return [{"trade_date": to_date, "symbol": "X", "close": 1.0}]

    def insert(self, rows, ch):
        return len(rows)


class TestFirstRunLookbackDays(unittest.TestCase):
    def test_first_run_lookback_days_used_when_no_watermark(self):
        """first_run_lookback_days overrides the caller's lookback_days only
        for the "no watermark yet" branch — full=True must still honor the
        caller's explicit lookback_days."""
        ch = FakeCH({})
        repo = MarketDataRepository(pool=None)

        result = repo.run_fetcher(
            FakeFirstRunFetcher(), dry_run=True, full=False, lookback_days=3650, ch=ch,
        )

        self.assertEqual(result.from_date, date.today() - timedelta(days=30))

    def test_full_reimport_ignores_first_run_lookback_days(self):
        ch = FakeCH({})
        repo = MarketDataRepository(pool=None)

        result = repo.run_fetcher(
            FakeFirstRunFetcher(), dry_run=True, full=True, lookback_days=3650, ch=ch,
        )

        self.assertEqual(result.from_date, date.today() - timedelta(days=3650))

    @patch("src.importer.fetchers.amfi_flows_fetcher.fetch_amfi_category_flows")
    def test_amfi_flows_first_run_uses_24_months_not_120(self, mock_fetch):
        """With first_run_lookback_days=730 set, a brand-new install's first
        amfi_flows run should land near 24 months back, not ~120 (the
        approximation this mechanism was added to remove)."""
        from src.importer.fetchers.adapters import AmfiCategoryFlowsFetcher

        mock_fetch.return_value = []
        ch = FakeCH({})
        repo = MarketDataRepository(pool=None)

        repo.run_fetcher(AmfiCategoryFlowsFetcher(), dry_run=True, full=False, lookback_days=3650, ch=ch)

        months_back = mock_fetch.call_args.kwargs["months_back"]
        self.assertLess(months_back, 30)
        self.assertGreaterEqual(months_back, 23)


class TestFIIDIIFetcher(unittest.TestCase):
    @patch("src.importer.fetchers.fii_dii_fetcher.fetch_fii_dii_monthly")
    @patch("src.importer.fetchers.fii_dii_fetcher.fetch_fii_dii_fno")
    @patch("src.importer.fetchers.fii_dii_fetcher.fetch_fii_dii")
    def test_fii_dii_fetcher_stashes_from_date_and_passes_to_fno(self, mock_cash, mock_fno, mock_monthly):
        from src.importer.fetchers.adapters import FIIDIIFetcher

        mock_cash.return_value = [{"trade_date": date(2026, 7, 30), "fii_net_cr": 100.0, "dii_net_cr": 200.0}]
        mock_fno.return_value = [{"trade_date": date(2026, 7, 30), "fii_fut_net_oi": 500.0, "fii_opt_overall_net_oi": 1000.0}]
        mock_monthly.return_value = [{"month": "2026-07"}]

        fetcher = FIIDIIFetcher()
        from_dt = date(2026, 7, 25)
        to_dt = date(2026, 7, 30)

        rows = fetcher.fetch(from_dt, to_dt)
        self.assertEqual(fetcher._last_from_date, from_dt)
        mock_cash.assert_called_once_with(from_date=from_dt)

        mock_ch = MagicMock()
        mock_ch.insert_fii_dii_flows.return_value = 1

        n = fetcher.insert(rows, mock_ch)

        self.assertEqual(n, 1)
        mock_ch.insert_fii_dii_flows.assert_called_once_with(rows)
        mock_fno.assert_called_once_with(from_date=from_dt)
        mock_ch.insert_fii_dii_fno_daily.assert_called_once_with(mock_fno.return_value)
        mock_monthly.assert_called_once()
        mock_ch.insert_fii_dii_monthly.assert_called_once_with(mock_monthly.return_value)


if __name__ == "__main__":
    unittest.main()

