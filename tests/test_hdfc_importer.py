"""
tests/test_hdfc_importer.py
───────────────────────────
Unit & integration tests for HDFC Asset Management Company holdings importer (HdfcImporter).

All tests are offline — HTTP calls and database calls are mocked.

Run:
    pytest tests/test_hdfc_importer.py -v
"""

from datetime import date
from unittest.mock import MagicMock, patch
import pytest

from src.data_importer.amc_holdings.importers.hdfc import (
    HDFC_FUNDS,
    HdfcImporter,
    _COLUMNS,
)
from src.data_importer.amc_holdings.factory import create_importer, REGISTRY


class TestHdfcImporterCatalogue:
    def test_catalogue_length(self):
        """Verify HDFC catalogue contains active schemes."""
        assert len(HDFC_FUNDS) == 12

    def test_registered_in_factory(self):
        """Verify 'hdfc' is registered in fund_imports factory."""
        assert "hdfc" in REGISTRY
        assert REGISTRY["hdfc"] == HdfcImporter

    def test_key_schemes_in_catalogue(self):
        """Verify core HDFC schemes are in catalogue."""
        fund_names = [f[1] for f in HDFC_FUNDS]
        assert "HDFC_SMALL_CAP" in fund_names
        assert "HDFC_MIDCAP_OPPORTUNITIES" in fund_names
        assert "HDFC_FLEXI_CAP" in fund_names
        assert "HDFC_TOP_100" in fund_names
        assert "HDFC_LARGE_AND_MID_CAP" in fund_names
        assert "HDFC_BALANCED_ADVANTAGE" in fund_names
        assert "HDFC_TECHNOLOGY" in fund_names


class TestHdfcImporterParams:
    def test_default_params(self):
        """Verify HdfcImporter default arguments."""
        importer = HdfcImporter()
        assert importer.from_year == 2020
        assert importer.full_reimport is False
        assert importer.fund_name() == "HDFC Asset Management Company"
        assert importer.table_name() == "market_data.mf_holdings"
        assert importer.column_names() == _COLUMNS

    def test_custom_params(self):
        """Verify custom from_year and full_reimport arguments."""
        importer = HdfcImporter(full_reimport=True, from_year=2022)
        assert importer.from_year == 2022
        assert importer.full_reimport is True

    def test_fetch_sources_returns_catalogue(self):
        """Verify fetch_sources returns all HDFC schemes."""
        importer = HdfcImporter()
        sources = importer.fetch_sources()
        assert len(sources) == 12
        assert sources == HDFC_FUNDS


class TestHdfcImporterParsing:
    @patch("httpx.Client")
    def test_parse_source(self, mock_httpx_cls):
        """Mock Morningstar API response and verify parse_source outputs correctly formatted rows."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "equityHoldingPage": {
                "holdingList": [
                    {
                        "securityName": "HDFC Bank Ltd",
                        "weighting": 8.45,
                        "marketValue": 1250000000.0,
                        "holdingTypeId": "E",
                        "holdingType": "Equity",
                        "isin": "INE040A01034",
                    },
                    {
                        "securityName": "Infosys Ltd",
                        "weighting": 5.65,
                        "marketValue": 840000000.0,
                        "holdingTypeId": "E",
                        "holdingType": "Equity",
                        "isin": "INE009A01021",
                    },
                ]
            }
        }

        mock_http_client = MagicMock()
        mock_http_client.get.return_value = mock_response
        mock_httpx_cls.return_value.__enter__.return_value = mock_http_client

        importer = HdfcImporter()
        source = ("118989", "HDFC_FLEXI_CAP", "INF179K01AY2", "F00000PD0B")
        rows = importer.parse_source(source, mock_http_client)

        assert len(rows) == 2
        r1 = rows[0]
        assert r1["scheme_code"] == "118989"
        assert r1["fund_name"] == "HDFC_FLEXI_CAP"
        assert r1["security_name"] == "HDFC Bank Ltd"
        assert r1["pct_of_nav"] == 8.45
        assert r1["market_value_cr"] == 125.0
        assert r1["isin"] == "INE040A01034"
        assert r1["asset_type"] == "equity"
        assert isinstance(r1["as_of_month"], date)
