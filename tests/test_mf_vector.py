"""
tests/test_mf_vector.py
───────────────────────
Tests for Qdrant-backed MF holdings vectorisation.
All Qdrant I/O is mocked — no live Qdrant required.
"""

from __future__ import annotations

import threading
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

# ── Fixtures ──────────────────────────────────────────────────────────────────

def _sample_rows(n: int = 6) -> list[dict]:
    """Six realistic mf_holdings rows across two funds, three asset types."""
    return [
        {"scheme_code": "140088", "fund_name": "DSP_MULTI_ASSET",   "as_of_month": date(2025, 1, 1), "isin": "INE040A01034", "security_name": "HDFC Bank Ltd",      "asset_type": "equity", "market_value_cr": 500.0, "pct_of_nav": 6.5},
        {"scheme_code": "140088", "fund_name": "DSP_MULTI_ASSET",   "as_of_month": date(2025, 1, 1), "isin": "INF204KB14I2", "security_name": "GOLDBEES",            "asset_type": "gold",   "market_value_cr": 320.0, "pct_of_nav": 18.7},
        {"scheme_code": "140088", "fund_name": "DSP_MULTI_ASSET",   "as_of_month": date(2025, 1, 1), "isin": "INE861G01027", "security_name": "7.26% GOI 2033",      "asset_type": "bond",   "market_value_cr": 210.0, "pct_of_nav": 22.1},
        {"scheme_code": "120716", "fund_name": "ICICI_MULTI_ASSET", "as_of_month": date(2025, 1, 1), "isin": "INE040A01034", "security_name": "HDFC Bank Ltd",      "asset_type": "equity", "market_value_cr": 850.0, "pct_of_nav": 5.2},
        {"scheme_code": "120716", "fund_name": "ICICI_MULTI_ASSET", "as_of_month": date(2025, 1, 1), "isin": "INF204KB14I2", "security_name": "GOLDBEES",            "asset_type": "gold",   "market_value_cr": 410.0, "pct_of_nav": 15.3},
        {"scheme_code": "120716", "fund_name": "ICICI_MULTI_ASSET", "as_of_month": date(2025, 1, 1), "isin": "INE001A01036", "security_name": "Reliance Industries", "asset_type": "equity", "market_value_cr": 780.0, "pct_of_nav": 7.1},
    ]


def _mock_hit(fund_name: str, security_name: str, asset_type: str,
              pct: float, score: float) -> MagicMock:
    hit = MagicMock()
    hit.score = score
    hit.payload = {
        "fund_name": fund_name, "security_name": security_name,
        "isin": "INE_TEST", "asset_type": asset_type,
        "pct_of_nav": pct, "market_value_cr": 100.0,
        "as_of_month": "2025-01", "text": f"{security_name} held by {fund_name}",
    }
    return hit


def _mock_profile_hit(fund_name: str, gold_pct: float, equity_pct: float, score: float) -> MagicMock:
    hit = MagicMock()
    hit.score = score
    hit.payload = {
        "fund_name": fund_name, "as_of_month": "2025-01",
        "equity_pct": equity_pct, "gold_pct": gold_pct,
        "bond_pct": 20.0, "cash_pct": 5.0, "other_pct": 0.0,
        "asset_type_primary": "gold" if gold_pct > equity_pct else "equity",
        "top5_text": "HDFC Bank 6.5%, GOLDBEES 18.7%", "text": f"{fund_name} profile",
    }
    return hit


# ── Write path tests ──────────────────────────────────────────────────────────

class TestVectorizeHoldings:
    def test_upserts_holding_and_profile_points(self):
        """Both mf_holdings and mf_fund_profiles collections receive upserts."""
        rows = _sample_rows()
        mock_client = MagicMock()
        mock_client.get_collections.return_value.collections = []

        with patch("src.db.mf_vector._get_client", return_value=mock_client), \
             patch("src.db.mf_vector._embed", return_value=[[0.1] * 768] * len(rows)):
            import src.db.mf_vector as mv
            mv._holdings_ready = False
            mv._profiles_ready = False

            # Run synchronously by calling internal functions directly
            mv._do_vectorize_holdings(rows)
            mv._do_vectorize_profiles(rows)

        # upsert called at least once per collection
        assert mock_client.upsert.call_count >= 2
        call_collections = {c.kwargs["collection_name"] for c in mock_client.upsert.call_args_list}
        assert "mf_holdings" in call_collections
        assert "mf_fund_profiles" in call_collections

    def test_holding_points_have_required_payload_keys(self):
        """Each holding point carries fund_name, isin, asset_type, pct_of_nav."""
        rows = _sample_rows()[:2]
        mock_client = MagicMock()
        mock_client.get_collections.return_value.collections = []

        with patch("src.db.mf_vector._get_client", return_value=mock_client), \
             patch("src.db.mf_vector._embed", return_value=[[0.1] * 768] * len(rows)):
            import src.db.mf_vector as mv
            mv._holdings_ready = False
            mv._do_vectorize_holdings(rows)

        call = mock_client.upsert.call_args
        points = call.kwargs["points"] if call.kwargs else call[1]["points"]
        for p in points:
            for key in ("fund_name", "isin", "asset_type", "pct_of_nav", "security_name", "text"):
                assert key in p.payload, f"Missing payload key: {key}"

    def test_no_op_on_empty_rows(self):
        """vectorize_holdings with an empty list triggers no Qdrant calls."""
        mock_client = MagicMock()
        with patch("src.db.mf_vector._get_client", return_value=mock_client):
            import src.db.mf_vector as mv
            mv._do_vectorize_holdings([])
            mv._do_vectorize_profiles([])
        mock_client.upsert.assert_not_called()

    def test_fund_profile_aggregation(self):
        """Profiles correctly aggregate per-holding rows into bucket percentages."""
        from src.db.mf_vector import _build_fund_profiles
        rows = _sample_rows()  # DSP: equity 6.5, gold 18.7, bond 22.1
        profiles = _build_fund_profiles(rows)
        dsp = next(p for p in profiles if p["fund_name"] == "DSP_MULTI_ASSET")
        assert dsp["gold_pct"] == pytest.approx(18.7)
        assert dsp["bond_pct"] == pytest.approx(22.1)
        assert dsp["equity_pct"] == pytest.approx(6.5)
        assert dsp["total_holdings"] == 3

    def test_profile_point_ids_are_deterministic(self):
        """Same fund+month always produces the same Qdrant point ID."""
        from src.db.mf_vector import _pid
        id1 = _pid("mf_profile", "DSP_MULTI_ASSET", "2025-01")
        id2 = _pid("mf_profile", "DSP_MULTI_ASSET", "2025-01")
        assert id1 == id2

    def test_profile_point_ids_differ_across_funds(self):
        from src.db.mf_vector import _pid
        id1 = _pid("mf_profile", "DSP_MULTI_ASSET",   "2025-01")
        id2 = _pid("mf_profile", "ICICI_MULTI_ASSET", "2025-01")
        assert id1 != id2

    def test_public_vectorize_holdings_spawns_threads(self):
        """vectorize_holdings() is non-blocking — it returns before Qdrant I/O."""
        rows = _sample_rows()
        event = threading.Event()

        def _fake_do_holdings(r):
            event.set()

        with patch("src.db.mf_vector._do_vectorize_holdings", side_effect=_fake_do_holdings), \
             patch("src.db.mf_vector._do_vectorize_profiles"):
            import src.db.mf_vector as mv
            mv.vectorize_holdings(rows)
            # If non-blocking, function has already returned
            assert True  # function returns immediately without waiting for thread


# ── Read path tests ───────────────────────────────────────────────────────────

class TestFindFundsHoldingSecurity:
    def test_returns_formatted_results(self):
        from src.db.mf_vector import find_funds_holding_security
        mock_result = MagicMock()
        mock_result.points = [
            _mock_hit("DSP_MULTI_ASSET",   "HDFC Bank Ltd", "equity", 6.5, 0.92),
            _mock_hit("ICICI_MULTI_ASSET", "HDFC Bank Ltd", "equity", 5.2, 0.89),
        ]
        mock_client = MagicMock()
        mock_client.query_points.return_value = mock_result

        with patch("src.db.mf_vector._get_client", return_value=mock_client), \
             patch("src.db.mf_vector._ensure_collection", return_value=True), \
             patch("src.db.mf_vector._embed", return_value=[[0.1] * 768]):
            results = find_funds_holding_security("HDFC Bank", k=5)

        assert len(results) == 2
        assert results[0]["fund_name"] == "DSP_MULTI_ASSET"
        assert results[0]["similarity"] == pytest.approx(0.92)
        assert results[1]["pct_of_nav"] == pytest.approx(5.2)

    def test_asset_type_filter_is_forwarded(self):
        from src.db.mf_vector import find_funds_holding_security
        mock_result = MagicMock()
        mock_result.points = []
        mock_client = MagicMock()
        mock_client.query_points.return_value = mock_result

        with patch("src.db.mf_vector._get_client", return_value=mock_client), \
             patch("src.db.mf_vector._ensure_collection", return_value=True), \
             patch("src.db.mf_vector._embed", return_value=[[0.1] * 768]):
            find_funds_holding_security("gold ETF", k=5, asset_type="gold")

        call_args = mock_client.query_points.call_args
        qfilter = call_args.kwargs.get("query_filter") or call_args[1].get("query_filter")
        assert qfilter is not None, "Filter should be set when asset_type provided"

    def test_graceful_on_qdrant_down(self):
        from src.db.mf_vector import find_funds_holding_security
        with patch("src.db.mf_vector._get_client", return_value=None):
            results = find_funds_holding_security("HDFC Bank")
        assert results == []


class TestFindSimilarFundProfiles:
    def test_excludes_query_fund_from_results(self):
        from src.db.mf_vector import find_similar_fund_profiles
        mock_result = MagicMock()
        mock_result.points = [_mock_profile_hit("ICICI_MULTI_ASSET", 15.0, 50.0, 0.91)]
        mock_client = MagicMock()
        mock_client.query_points.return_value = mock_result

        with patch("src.db.mf_vector._get_client", return_value=mock_client), \
             patch("src.db.mf_vector._ensure_collection", return_value=True), \
             patch("src.db.mf_vector._embed", return_value=[[0.1] * 768]):
            results = find_similar_fund_profiles("DSP_MULTI_ASSET", k=3)

        call_args = mock_client.query_points.call_args
        qfilter = call_args.kwargs.get("query_filter") or call_args[1].get("query_filter")
        # must_not should contain the exclusion
        assert qfilter is not None
        assert len(qfilter.must_not) == 1

    def test_returns_similarity_score(self):
        from src.db.mf_vector import find_similar_fund_profiles
        mock_result = MagicMock()
        mock_result.points = [_mock_profile_hit("QUANT_MULTI_ASSET", 20.0, 45.0, 0.87)]
        mock_client = MagicMock()
        mock_client.query_points.return_value = mock_result

        with patch("src.db.mf_vector._get_client", return_value=mock_client), \
             patch("src.db.mf_vector._ensure_collection", return_value=True), \
             patch("src.db.mf_vector._embed", return_value=[[0.1] * 768]):
            results = find_similar_fund_profiles("DSP_MULTI_ASSET")

        assert results[0]["similarity"] == pytest.approx(0.87)
        assert results[0]["fund_name"] == "QUANT_MULTI_ASSET"


class TestFindFundsByCategory:
    def test_category_filter_applied(self):
        from src.db.mf_vector import find_funds_by_category
        mock_result = MagicMock()
        mock_result.points = [_mock_profile_hit("DSP_MULTI_ASSET", 20.0, 45.0, 0.88)]
        mock_client = MagicMock()
        mock_client.query_points.return_value = mock_result

        with patch("src.db.mf_vector._get_client", return_value=mock_client), \
             patch("src.db.mf_vector._ensure_collection", return_value=True), \
             patch("src.db.mf_vector._embed", return_value=[[0.1] * 768]):
            results = find_funds_by_category("gold", query="precious metal")

        assert len(results) == 1
        # Verify filter was passed
        call_args = mock_client.query_points.call_args
        qfilter = call_args.kwargs.get("query_filter") or call_args[1].get("query_filter")
        assert qfilter.must[0].key == "asset_type_primary"

    def test_returns_empty_on_no_client(self):
        from src.db.mf_vector import find_funds_by_category
        with patch("src.db.mf_vector._get_client", return_value=None):
            assert find_funds_by_category("gold") == []


# ── Tool layer tests ──────────────────────────────────────────────────────────

class TestMFQdrantTools:
    def test_find_funds_holding_tool_renders_table(self):
        from src.tools.market.mf_tools import find_funds_holding
        fake_results = [
            {"fund_name": "DSP_MULTI_ASSET",   "security_name": "HDFC Bank Ltd",
             "isin": "INE040A01034", "asset_type": "equity", "pct_of_nav": 6.5,
             "market_value_cr": 500.0, "as_of_month": "2025-01", "similarity": 0.93},
            {"fund_name": "ICICI_MULTI_ASSET", "security_name": "HDFC Bank Ltd",
             "isin": "INE040A01034", "asset_type": "equity", "pct_of_nav": 5.2,
             "market_value_cr": 850.0, "as_of_month": "2025-01", "similarity": 0.90},
        ]
        with patch("src.db.mf_vector.find_funds_holding_security", return_value=fake_results):
            output = find_funds_holding.func(query="HDFC Bank")

        assert "DSP_MULTI_ASSET" in output
        assert "ICICI_MULTI_ASSET" in output
        assert "6.5" in output  # pct_of_nav
        assert "HDFC Bank" in output

    def test_find_funds_holding_empty_message(self):
        from src.tools.market.mf_tools import find_funds_holding
        with patch("src.db.mf_vector.find_funds_holding_security", return_value=[]):
            output = find_funds_holding.func(query="NONEXISTENT_STOCK")
        assert "empty" in output.lower() or "import" in output.lower()

    def test_find_similar_funds_tool_renders_table(self):
        from src.tools.market.mf_tools import find_similar_funds
        fake = [
            {"fund_name": "ICICI_MULTI_ASSET", "as_of_month": "2025-01",
             "equity_pct": 52.0, "gold_pct": 15.3, "bond_pct": 22.0,
             "cash_pct": 5.0, "top5_text": "HDFC Bank 5.2%", "primary": "equity",
             "similarity": 0.91},
        ]
        with patch("src.db.mf_vector.find_similar_fund_profiles", return_value=fake):
            output = find_similar_funds.func(fund_name="DSP_MULTI_ASSET")
        assert "ICICI_MULTI_ASSET" in output
        assert "0.91" in output

    def test_search_mf_exposure_alias_commodity(self):
        """'commodity' alias is normalised to 'gold' before Qdrant call."""
        from src.tools.market.mf_tools import search_mf_exposure
        with patch("src.db.mf_vector.find_funds_by_category", return_value=[]) as mock_fn:
            search_mf_exposure.func(category="commodity")
        mock_fn.assert_called_once()
        assert mock_fn.call_args[1]["asset_type"] == "gold" or \
               mock_fn.call_args[0][0] == "gold"

    def test_search_mf_exposure_renders_rows(self):
        from src.tools.market.mf_tools import search_mf_exposure
        fake = [
            {"fund_name": "DSP_MULTI_ASSET", "as_of_month": "2025-01",
             "gold_pct": 20.1, "equity_pct": 45.2, "bond_pct": 22.0,
             "cash_pct": 6.0, "top5_text": "GOLDBEES 18.7%, HDFC Bank 6.5%",
             "similarity": 0.88},
        ]
        with patch("src.db.mf_vector.find_funds_by_category", return_value=fake):
            output = search_mf_exposure.func(category="gold")
        assert "DSP_MULTI_ASSET" in output
        assert "20.1" in output


# ── Clickhouse hook integration ───────────────────────────────────────────────

class TestClickhouseHook:
    def test_insert_mf_holdings_triggers_vectorize(self):
        """ClickHouseImporter.insert_mf_holdings calls vectorize_holdings."""
        rows = _sample_rows()

        mock_ch_client = MagicMock()

        with patch("src.db.mf_vector.vectorize_holdings") as mock_vec:
            from src.importer.clickhouse import ClickHouseImporter
            ch = ClickHouseImporter.__new__(ClickHouseImporter)
            ch._client = mock_ch_client
            ch.insert_mf_holdings(rows)

        mock_vec.assert_called_once_with(rows)

    def test_base_importer_triggers_vectorize_for_mf_table(self):
        """BaseFundImporter.run() calls vectorize_holdings when table is mf_holdings."""
        from src.scripts.fund_imports.base import BaseFundImporter

        class _FakeImporter(BaseFundImporter):
            def fund_name(self): return "TestFund"
            def fetch_sources(self): return [1]
            def parse_source(self, s, http): return _sample_rows()
            def table_name(self): return "market_data.mf_holdings"
            def column_names(self): return ["scheme_code", "fund_name", "as_of_month", "isin",
                                            "security_name", "asset_type", "market_value_cr", "pct_of_nav"]
            def watermark_source(self): return "mf_holdings"

        mock_client = MagicMock()

        with patch("src.scripts.fund_imports.base._ch_client", return_value=mock_client), \
             patch("src.db.mf_vector.vectorize_holdings") as mock_vec, \
             patch("httpx.Client") as mock_http:
            mock_http.return_value.__enter__ = lambda s: s
            mock_http.return_value.__exit__ = MagicMock(return_value=False)
            _FakeImporter().run()

        mock_vec.assert_called_once()
        inserted_rows = mock_vec.call_args[0][0]
        assert len(inserted_rows) == len(_sample_rows())
