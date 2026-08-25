"""
tests/test_fund_imports_integration.py
─────────────────────────────────────
Unit tests for the Option 2 integration: fund_imports factory categories
(icici, nippon, icici-index) wired into src/importer/cli.run_import().

All tests are offline — ClickHouse and HTTP calls are mocked.

Run:
    pytest tests/test_fund_imports_integration.py -v
"""

import sys
import os
from io import StringIO
from unittest.mock import MagicMock, patch, call

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


# ── helpers ───────────────────────────────────────────────────────────────────

def _mock_ch():
    """Return a pre-configured ClickHouseImporter mock (no real DB calls)."""
    ch = MagicMock()
    ch.get_watermark.return_value = None
    ch.insert_prices.return_value = 0
    ch.close.return_value = None
    return ch


def _quiet_console():
    from rich.console import Console
    return Console(file=StringIO())


# ═══════════════════════════════════════════════════════════════════════════════
# 1. Registry — ALL_CATEGORIES contains the three new entries
# ═══════════════════════════════════════════════════════════════════════════════

class TestAllCategories:
    def test_contains_icici(self):
        from src.importer.registry import ALL_CATEGORIES
        assert "icici" in ALL_CATEGORIES

    def test_contains_nippon(self):
        from src.importer.registry import ALL_CATEGORIES
        assert "nippon" in ALL_CATEGORIES

    def test_contains_icici_index(self):
        from src.importer.registry import ALL_CATEGORIES
        assert "icici-index" in ALL_CATEGORIES

    def test_amc_categories_come_after_yfinance_categories(self):
        from src.importer.registry import ALL_CATEGORIES
        assert ALL_CATEGORIES.index("stocks") < ALL_CATEGORIES.index("icici")
        assert ALL_CATEGORIES.index("etfs")   < ALL_CATEGORIES.index("nippon")
        assert ALL_CATEGORIES.index("mf")     < ALL_CATEGORIES.index("icici-index")

    def test_no_duplicates(self):
        from src.importer.registry import ALL_CATEGORIES
        assert len(ALL_CATEGORIES) == len(set(ALL_CATEGORIES))


# ═══════════════════════════════════════════════════════════════════════════════
# 2. Factory — create_importer() returns the right class and stores kwargs
# ═══════════════════════════════════════════════════════════════════════════════

class TestFactory:
    def test_creates_icici_importer(self):
        from src.scripts.fund_imports.factory import create_importer
        from src.scripts.fund_imports.importers.icici_mf import IciciMFImporter
        assert isinstance(create_importer("icici"), IciciMFImporter)

    def test_creates_nippon_importer(self):
        from src.scripts.fund_imports.factory import create_importer
        from src.scripts.fund_imports.importers.nippon import NipponImporter
        assert isinstance(create_importer("nippon"), NipponImporter)

    def test_creates_icici_index_importer(self):
        from src.scripts.fund_imports.factory import create_importer
        from src.scripts.fund_imports.importers.icici_index import IciciIndexImporter
        assert isinstance(create_importer("icici-index"), IciciIndexImporter)

    def test_unknown_name_raises_value_error(self):
        from src.scripts.fund_imports.factory import create_importer
        with pytest.raises(ValueError, match="Unknown importer"):
            create_importer("does-not-exist")

    def test_registry_keys(self):
        from src.scripts.fund_imports.factory import REGISTRY
        expected = {
            "icici", "nippon", "icici-index", "dsp", "quant", "bajaj", "amfi", "kotak", "hdfc",
            "abakkus", "abacus", "helios", "invesco", "canara", "canara_robeco", "canara-robeco",
            "mirae", "mirae_asset", "mirae-asset", "axis", "axis_mf", "axis-mf",
            "motilal", "motilal_oswal", "motilal-oswal", "qsif", "edelweiss"
        }
        assert set(REGISTRY.keys()) == expected

    def test_nippon_from_year_stored(self):
        from src.scripts.fund_imports.factory import create_importer
        imp = create_importer("nippon", from_year=2024)
        assert imp._from_year == 2024

    def test_nippon_full_reimport_stored(self):
        from src.scripts.fund_imports.factory import create_importer
        imp = create_importer("nippon", full_reimport=True)
        assert imp._full_reimport is True

    def test_nippon_constructor_defaults(self):
        from src.scripts.fund_imports.factory import create_importer
        imp = create_importer("nippon")
        assert imp._from_year == 2017
        assert imp._full_reimport is False


# ═══════════════════════════════════════════════════════════════════════════════
# 3. run_import() dispatch — factory is called with the right args
#
# Patches applied to every test in this class:
#   - get_symbols_for_categories → {} so the yfinance loop is a no-op
#   - ClickHouseImporter         → mock; avoids real DB connection
#   - scripts.fund_imports.factory.create_importer → captures calls
# ═══════════════════════════════════════════════════════════════════════════════

class TestRunImportDispatch:

    # ── icici ─────────────────────────────────────────────────────────────────

    @patch("src.data_importer.registry.get_symbols_for_categories", return_value={})
    @patch("src.data_importer.clickhouse.ClickHouseImporter")
    @patch("src.data_importer.amc_holdings.factory.create_importer")
    def test_icici_calls_factory(self, mock_create, mock_ch_cls, _syms):
        mock_ch_cls.return_value = _mock_ch()
        mock_imp = MagicMock()
        mock_create.return_value = mock_imp

        from src.importer.cli import run_import
        run_import(categories=["icici"], dry_run=True, console=_quiet_console())

        mock_create.assert_called_once_with("icici")
        mock_imp.run.assert_called_once_with(dry_run=True)

    # ── icici-index ───────────────────────────────────────────────────────────

    @patch("src.data_importer.registry.get_symbols_for_categories", return_value={})
    @patch("src.data_importer.clickhouse.ClickHouseImporter")
    @patch("src.data_importer.amc_holdings.factory.create_importer")
    def test_icici_index_calls_factory(self, mock_create, mock_ch_cls, _syms):
        mock_ch_cls.return_value = _mock_ch()
        mock_imp = MagicMock()
        mock_create.return_value = mock_imp

        from src.importer.cli import run_import
        run_import(categories=["icici-index"], dry_run=True, console=_quiet_console())

        mock_create.assert_called_once_with("icici-index")
        mock_imp.run.assert_called_once_with(dry_run=True)

    # ── nippon — full_reimport propagation ────────────────────────────────────

    @patch("src.data_importer.registry.get_symbols_for_categories", return_value={})
    @patch("src.data_importer.clickhouse.ClickHouseImporter")
    @patch("src.data_importer.amc_holdings.factory.create_importer")
    def test_nippon_default_full_reimport_is_false(self, mock_create, mock_ch_cls, _syms):
        mock_ch_cls.return_value = _mock_ch()
        mock_imp = MagicMock()
        mock_create.return_value = mock_imp

        from src.importer.cli import run_import
        run_import(categories=["nippon"], dry_run=True, console=_quiet_console())

        mock_create.assert_called_once_with("nippon", full_reimport=False)
        mock_imp.run.assert_called_once_with(dry_run=True)

    @patch("src.data_importer.registry.get_symbols_for_categories", return_value={})
    @patch("src.data_importer.clickhouse.ClickHouseImporter")
    @patch("src.data_importer.amc_holdings.factory.create_importer")
    def test_nippon_full_reimport_true_forwarded(self, mock_create, mock_ch_cls, _syms):
        mock_ch_cls.return_value = _mock_ch()
        mock_imp = MagicMock()
        mock_create.return_value = mock_imp

        from src.importer.cli import run_import
        run_import(
            categories=["nippon"],
            dry_run=True,
            full_reimport=True,
            console=_quiet_console(),
        )

        mock_create.assert_called_once_with("nippon", full_reimport=True)

    # ── all three AMC categories together ─────────────────────────────────────

    @patch("src.data_importer.registry.get_symbols_for_categories", return_value={})
    @patch("src.data_importer.clickhouse.ClickHouseImporter")
    @patch("src.data_importer.amc_holdings.factory.create_importer")
    def test_all_three_amc_categories_each_get_one_call(self, mock_create, mock_ch_cls, _syms):
        mock_ch_cls.return_value = _mock_ch()
        mock_imp = MagicMock()
        mock_create.return_value = mock_imp

        from src.importer.cli import run_import
        run_import(
            categories=["icici", "nippon", "icici-index"],
            dry_run=True,
            console=_quiet_console(),
        )

        assert mock_create.call_count == 3
        mock_create.assert_any_call("icici")
        mock_create.assert_any_call("nippon", full_reimport=False)
        mock_create.assert_any_call("icici-index")
        assert mock_imp.run.call_count == 3

    @patch("src.data_importer.registry.get_symbols_for_categories", return_value={})
    @patch("src.data_importer.clickhouse.ClickHouseImporter")
    @patch("src.data_importer.amc_holdings.factory.create_importer")
    def test_amc_order_is_icici_nippon_iciciindex_regardless_of_input_order(
        self, mock_create, mock_ch_cls, _syms
    ):
        """icici → nippon → icici-index regardless of the order the caller passes them."""
        mock_ch_cls.return_value = _mock_ch()
        mock_imp = MagicMock()
        mock_create.return_value = mock_imp

        from src.importer.cli import run_import
        run_import(
            categories=["icici-index", "nippon", "icici"],  # reversed input
            dry_run=True,
            console=_quiet_console(),
        )

        actual_names = [c.args[0] for c in mock_create.call_args_list]
        assert actual_names == ["icici", "nippon", "icici-index"]

    # ── non-AMC categories must not trigger the factory ───────────────────────

    # NOTE: "stocks"/"etfs"/etc. now route through get_registry() (the
    # Fetcher-registry unification), not get_symbols_for_categories()
    # directly — get_registry() is additionally mocked to {} here so the
    # price-category loop is a no-op instead of running (mocked-Shoonya-
    # bypassing) real network fetches against the full stock watchlist.
    @patch("src.data_importer.fetchers.adapters.get_registry", return_value={})
    @patch("src.data_importer.registry.get_symbols_for_categories", return_value={})
    @patch("src.data_importer.clickhouse.ClickHouseImporter")
    @patch("src.data_importer.amc_holdings.factory.create_importer")
    def test_yfinance_category_does_not_call_factory(self, mock_create, mock_ch_cls, _syms, _registry):
        mock_ch_cls.return_value = _mock_ch()

        from src.importer.cli import run_import
        run_import(categories=["stocks"], dry_run=True, console=_quiet_console())

        mock_create.assert_not_called()

    @patch("src.data_importer.fetchers.adapters.get_registry", return_value={})
    @patch("src.data_importer.registry.get_symbols_for_categories", return_value={})
    @patch("src.data_importer.clickhouse.ClickHouseImporter")
    @patch("src.data_importer.amc_holdings.factory.create_importer")
    def test_mixed_yfinance_and_amc_categories(self, mock_create, mock_ch_cls, mock_syms, mock_registry):
        """Mixing stocks + icici: registry-driven price loop is a no-op (mocked empty) and factory is called once."""
        mock_ch_cls.return_value = _mock_ch()
        mock_imp = MagicMock()
        mock_create.return_value = mock_imp

        from src.importer.cli import run_import
        run_import(
            categories=["stocks", "icici"],
            dry_run=True,
            console=_quiet_console(),
        )

        mock_registry.assert_called_once()
        mock_create.assert_called_once_with("icici")
        mock_imp.run.assert_called_once_with(dry_run=True)
