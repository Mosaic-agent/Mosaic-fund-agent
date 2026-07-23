"""
tests/test_amc_design_patterns.py
──────────────────────────────────
Unit and integration tests for software design patterns across AMC Importers
and Institutional Intelligence engines:
  - Template Method Pattern (BaseFundImporter hooks)
  - Factory Pattern (create_importer & @register_importer decorator)
  - Strategy Pattern (ConvictionScoreStrategy, MidCapClusterStrategy, HousePivotDriftStrategy)
  - Facade Pattern (AmcIntelligenceFacade)

Run:
    pytest tests/test_amc_design_patterns.py -v
"""

from datetime import date
import threading
from unittest.mock import MagicMock, patch
import pandas as pd
import pytest

from src.scripts.fund_imports.base import BaseFundImporter
from src.scripts.fund_imports.factory import (
    REGISTRY,
    create_importer,
    register_importer,
)
from src.scripts.market.amc_house_intelligence import (
    AMCAnalysisStrategy,
    AmcIntelligenceFacade,
    ConvictionScoreStrategy,
    HousePivotDriftStrategy,
    MidCapClusterStrategy,
)


# ── 1. Template Method & Factory Pattern Tests ────────────────────────────────

class DummyImporter(BaseFundImporter):
    """Dummy importer subclass for testing Template Method hooks."""

    def __init__(self) -> None:
        super().__init__()
        self.pre_hook_called = False
        self.post_hook_called = False

    def fund_name(self) -> str:
        return "Dummy AMC"

    def fetch_sources(self) -> list:
        return [("2026-07-01", "dummy_source")]

    def parse_source(self, source, http) -> list[dict]:
        return [
            {
                "scheme_code": "999999",
                "fund_name": "DUMMY_FUND",
                "as_of_month": date(2026, 7, 1),
                "isin": "INE999A01019",
                "security_name": "Dummy Stock Ltd",
                "asset_type": "equity",
                "market_value_cr": 100.0,
                "pct_of_nav": 5.0,
                "imported_at": date(2026, 7, 1),
            }
        ]

    def table_name(self) -> str:
        return "market_data.mf_holdings"

    def column_names(self) -> list[str]:
        return [
            "scheme_code",
            "fund_name",
            "as_of_month",
            "isin",
            "security_name",
            "asset_type",
            "market_value_cr",
            "pct_of_nav",
            "imported_at",
        ]

    def watermark_source(self) -> str:
        return "mf_holdings"

    def pre_insert_hook(self, all_rows: list[dict]) -> list[dict]:
        self.pre_hook_called = True
        return all_rows

    def post_insert_hook(self, all_rows: list[dict], client) -> None:
        self.post_hook_called = True


class TestTemplateMethodAndFactoryPatterns:
    def test_factory_registration_decorator(self):
        """Verify @register_importer decorator adds new importer subclass to REGISTRY."""

        @register_importer("dummy")
        class RegisteredDummyImporter(DummyImporter):
            pass

        assert "dummy" in REGISTRY
        assert REGISTRY["dummy"] == RegisteredDummyImporter

        inst = create_importer("dummy")
        assert isinstance(inst, RegisteredDummyImporter)

        # Cleanup test entry
        del REGISTRY["dummy"]

    @patch("src.scripts.fund_imports.base._ch_client")
    def test_template_method_hooks(self, mock_ch_client):
        """Verify pre_insert_hook and post_insert_hook execution within BaseFundImporter.run()."""
        mock_client = MagicMock()
        mock_ch_client.return_value = mock_client

        importer = DummyImporter()
        importer.run(dry_run=False)

        assert importer.pre_hook_called is True
        assert importer.post_hook_called is True
        assert mock_client.insert.called

    @patch("src.scripts.fund_imports.base._ch_client")
    def test_per_source_exception_isolation(self, mock_ch_client):
        """Verify that BaseFundImporter.run() skips failing sources and continues batch ingestion."""
        mock_client = MagicMock()
        mock_ch_client.return_value = mock_client

        importer = DummyImporter()
        # Override fetch_sources to return 2 sources
        importer.fetch_sources = MagicMock(return_value=["source_1_bad", "source_2_good"])

        # Override parse_source to raise exception on source 1, succeed on source 2
        def side_effect_parse(source, http):
            if source == "source_1_bad":
                raise ValueError("Corrupt file format in source 1")
            return [
                {
                    "scheme_code": "888888",
                    "fund_name": "GOOD_FUND",
                    "as_of_month": date(2026, 7, 1),
                    "isin": "INE888A01018",
                    "security_name": "Good Stock Ltd",
                    "asset_type": "equity",
                    "market_value_cr": 50.0,
                    "pct_of_nav": 3.0,
                    "imported_at": date(2026, 7, 1),
                }
            ]

        importer.parse_source = side_effect_parse
        importer.run(dry_run=False)

        # Batch completes successfully inserting source 2 rows
        assert mock_client.insert.called
        inserted_rows = mock_client.insert.call_args_list[0][0][1]
        assert len(inserted_rows) == 1
        assert inserted_rows[0][1] == "GOOD_FUND"

    def test_sources_parse_concurrently_with_bounded_workers(self):
        """Independent monthly files may parse concurrently; inserts remain batched."""
        class ParallelDummyImporter(DummyImporter):
            MAX_PARALLEL_SOURCES = 2

            def fetch_sources(self):
                return ["a", "b"]

            def parse_source(self, source, http):
                started.append(source)
                if len(started) == 2:
                    both_started.set()
                assert both_started.wait(timeout=2)
                return []

        started: list[str] = []
        both_started = threading.Event()
        importer = ParallelDummyImporter()
        importer.run(dry_run=True)
        assert set(started) == {"a", "b"}

    def test_source_workers_honours_bounded_environment_override(self, monkeypatch):
        importer = DummyImporter()
        monkeypatch.setenv("AMC_IMPORT_MAX_WORKERS", "99")
        assert importer.source_workers() == 4
        monkeypatch.setenv("AMC_IMPORT_MAX_WORKERS", "invalid")
        assert importer.source_workers() == importer.MAX_PARALLEL_SOURCES

    def test_set_source_workers_rejects_out_of_range_values(self):
        importer = DummyImporter()
        with pytest.raises(ValueError):
            importer.set_source_workers(0)
        with pytest.raises(ValueError):
            importer.set_source_workers(5)

    @patch("src.scripts.fund_imports.base._ch_client")
    @patch("src.scripts.fund_imports.base.time.sleep")
    def test_parallel_submissions_are_staggered_by_request_delay(self, mock_sleep, mock_ch_client):
        """Bounded concurrency shouldn't blast every source at once — submissions
        stay paced by REQUEST_DELAY / workers even though fetches overlap."""
        mock_ch_client.return_value = MagicMock()

        class ParallelDummyImporter(DummyImporter):
            MAX_PARALLEL_SOURCES = 2
            REQUEST_DELAY = 2.0

            def fetch_sources(self):
                return ["a", "b", "c"]

            def parse_source(self, source, http):
                return []

        importer = ParallelDummyImporter()
        importer.run(dry_run=True)

        # 2 stagger sleeps between 3 submissions, each REQUEST_DELAY / workers
        assert mock_sleep.call_count == 2
        assert all(call.args[0] == pytest.approx(1.0) for call in mock_sleep.call_args_list)


# ── 2. Strategy Pattern Tests ────────────────────────────────────────────────

class TestStrategyPattern:
    def test_strategy_names(self):
        """Verify strategy names."""
        assert ConvictionScoreStrategy().name() == "Top Conviction Holdings"
        assert MidCapClusterStrategy().name() == "Mid-Cap Alpha Cluster"
        assert HousePivotDriftStrategy().name() == "Synchronized House Pivots"

    def test_conviction_strategy_execution(self):
        """Mock ClickHouse query and test ConvictionScoreStrategy output."""
        mock_client = MagicMock()
        mock_df = pd.DataFrame(
            [
                {
                    "security_name": "HDFC Bank Ltd",
                    "fund_count": 8,
                    "agg_weight": 35.5,
                    "conviction_score": 284,
                }
            ]
        )
        mock_client.query_df.return_value = mock_df

        strat = ConvictionScoreStrategy()
        res_df = strat.execute(
            mock_client,
            "2026-07-01",
            "2026-06-30",
            "fund_name LIKE 'HDFC_%'",
            set(),
        )

        assert not res_df.empty
        assert res_df.iloc[0]["security_name"] == "HDFC Bank Ltd"
        assert res_df.iloc[0]["conviction_score"] == 284
        assert mock_client.query_df.called


# ── 3. Facade Pattern Tests ──────────────────────────────────────────────────

class TestFacadePattern:
    @patch("src.scripts.market.amc_house_intelligence.get_prev_month")
    @patch("src.scripts.market.amc_house_intelligence.get_latest_available_month")
    @patch("src.db.pool.get_client")
    def test_facade_report_generation(self, mock_get_client, mock_latest_month, mock_prev_month):
        """Verify AmcIntelligenceFacade aggregates all strategy reports into a single dictionary."""
        mock_latest_month.return_value = date(2026, 7, 1)
        mock_prev_month.return_value = date(2026, 6, 30)

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.query.return_value.result_rows = []
        mock_client.query_df.return_value = pd.DataFrame(
            [{"security_name": "Infosys Ltd", "agg_weight": 20.0, "fund_count": 5}]
        )

        facade = AmcIntelligenceFacade("hdfc")
        report = facade.get_full_report()

        assert "amc_label" in report
        assert report["amc_label"] == "HDFC"
        assert "cur_month" in report
        assert report["cur_month"] == "2026-07-01"
        assert "Top Conviction Holdings" in report
        assert "Mid-Cap Alpha Cluster" in report
        assert "Synchronized House Pivots" in report

    @patch("src.scripts.market.amc_house_intelligence.get_prev_month")
    @patch("src.scripts.market.amc_house_intelligence.get_latest_available_month")
    @patch("src.db.pool.get_client")
    def test_facade_strategy_exception_isolation(self, mock_get_client, mock_latest_month, mock_prev_month):
        """Verify AmcIntelligenceFacade isolates single strategy failures without crashing full report."""
        mock_latest_month.return_value = date(2026, 7, 1)
        mock_prev_month.return_value = date(2026, 6, 30)

        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.query.return_value.result_rows = []
        # Query 1: nifty50 proxy, Query 2: Strategy 1 (success), Query 3: Strategy 2 (fail), Query 4: Strategy 3 (success)
        mock_client.query_df.side_effect = [
            pd.DataFrame([{"security_name": "RELIANCE"}]),
            pd.DataFrame([{"security_name": "HDFC Bank Ltd"}]),
            RuntimeError("ClickHouse connection lost"),
            pd.DataFrame([{"security_name": "Infosys Ltd"}]),
        ]

        facade = AmcIntelligenceFacade("hdfc")
        report = facade.get_full_report()

        assert not report["Top Conviction Holdings"].empty
        assert report["Mid-Cap Alpha Cluster"].empty
        assert "Mid-Cap Alpha Cluster_error" in report
        assert "ClickHouse connection lost" in report["Mid-Cap Alpha Cluster_error"]
        assert not report["Synchronized House Pivots"].empty
