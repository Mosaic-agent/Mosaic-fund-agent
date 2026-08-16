"""
tests/test_dsp_importer.py
──────────────────────────
Unit & integration tests for DSP Mutual Fund holdings importer (DspImporter).

All tests are offline — HTTP calls and database calls are mocked.

Run:
    pytest tests/test_dsp_importer.py -v
"""

from datetime import date
from unittest.mock import MagicMock, patch
import pytest

from src.data_importer.amc_holdings.importers.dsp import DspImporter
from src.data_importer.amc_holdings.factory import create_importer, REGISTRY
from src.data_importer.amc_downloaders.dsp_holdings.import_all_dsp_equity import ZIP_FILES, DSP_SCHEME_MAP


class TestDspImporterCatalogue:
    def test_registered_in_factory(self):
        """Verify 'dsp' is registered in fund_imports factory."""
        assert "dsp" in REGISTRY
        assert REGISTRY["dsp"] == DspImporter

    def test_factory_instantiation(self):
        """Verify create_importer('dsp') returns a DspImporter instance."""
        imp = create_importer("dsp", full_reimport=True)
        assert isinstance(imp, DspImporter)
        assert imp.full_reimport is True

    def test_scheme_map_coverage(self):
        """Verify key DSP schemes are present in DSP_SCHEME_MAP."""
        fund_names = [v[1] for v in DSP_SCHEME_MAP.values()]
        assert "DSP_FLEXI_CAP" in fund_names
        assert "DSP_SMALL_CAP" in fund_names
        assert "DSP_MID_CAP" in fund_names
        assert "DSP_LARGE_AND_MID_CAP" in fund_names
        assert "DSP_MULTI_ASSET" in fund_names


class TestDspImporterParams:
    def test_default_params(self):
        """Verify DspImporter default arguments."""
        importer = DspImporter()
        assert importer.full_reimport is False
        assert importer.fund_name() == "DSP Mutual Fund"
        assert importer.table_name() == "market_data.mf_holdings"
        assert importer.watermark_source() == "mf_holdings"

    @patch("src.data_importer.amc_holdings.importers.dsp.discover_latest_zip")
    def test_fetch_sources_full_reimport(self, mock_discover):
        """Verify fetch_sources returns ZIP_FILES when full_reimport is True."""
        mock_discover.return_value = None
        importer = DspImporter(full_reimport=True)
        sources = importer.fetch_sources()
        assert len(sources) >= len(ZIP_FILES)
        assert sources[0][0] == ZIP_FILES[0][0]


class TestDspImporterParsing:
    @patch("src.data_importer.amc_holdings.importers.dsp.process_month")
    def test_parse_source(self, mock_process_month):
        """Mock process_month and verify parse_source outputs correctly formatted rows."""
        mock_process_month.return_value = [
            {
                "scheme_code": "119076",
                "fund_name": "DSP_FLEXI_CAP",
                "as_of_month": "2026-03-31",
                "isin": "INE040A01034",
                "security_name": "HDFC Bank Ltd",
                "asset_type": "equity",
                "market_value_cr": 250.5,
                "pct_of_nav": 7.5,
                "imported_at": "2026-03-31T00:00:00",
            }
        ]

        importer = DspImporter()
        mock_http = MagicMock()
        source = ("2026-03-31", "https://www.dspim.com/dummy.zip")
        rows = importer.parse_source(source, mock_http)

        assert len(rows) == 1
        r = rows[0]
        assert r["scheme_code"] == "119076"
        assert r["fund_name"] == "DSP_FLEXI_CAP"
        assert r["security_name"] == "HDFC Bank Ltd"
        assert r["pct_of_nav"] == 7.5
        assert isinstance(r["as_of_month"], date)
        assert r["as_of_month"] == date(2026, 3, 31)
