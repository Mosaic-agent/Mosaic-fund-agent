"""
tests/test_kotak_importer.py
────────────────────────────
Unit & integration tests for Kotak Mahindra Mutual Fund holdings importer (KotakImporter).

All tests are offline — HTTP calls and database calls are mocked.

Run:
    pytest tests/test_kotak_importer.py -v
"""

from datetime import date
from unittest.mock import MagicMock, patch
import pytest

from src.data_importer.amc_holdings.importers.kotak import (
    KOTAK_FUNDS,
    KotakImporter,
    _COLUMNS,
)
from src.data_importer.amc_holdings.factory import create_importer, REGISTRY


class TestKotakImporterCatalogue:
    def test_catalogue_length(self):
        """Verify Kotak catalogue contains active schemes."""
        assert len(KOTAK_FUNDS) == 9

    def test_registered_in_factory(self):
        """Verify 'kotak' is registered in fund_imports factory."""
        assert "kotak" in REGISTRY
        assert REGISTRY["kotak"] == KotakImporter

    def test_key_schemes_in_catalogue(self):
        """Verify core Kotak schemes are in catalogue."""
        fund_names = [f[1] for f in KOTAK_FUNDS]
        assert "KOTAK_SMALL_CAP" in fund_names
        assert "KOTAK_EMERGING_EQUITY" in fund_names
        assert "KOTAK_FLEXICAP" in fund_names
        assert "KOTAK_BLUECHIP" in fund_names
        assert "KOTAK_MULTICAP" in fund_names
        assert "KOTAK_BALANCED_ADVANTAGE" in fund_names


class TestKotakImporterParams:
    def test_default_params(self):
        """Verify KotakImporter default arguments."""
        importer = KotakImporter()
        assert importer.from_year == 2020
        assert importer.full_reimport is False
        assert importer.fund_name() == "Kotak Mahindra Mutual Fund"
        assert importer.table_name() == "market_data.mf_holdings"
        assert importer.column_names() == _COLUMNS

    def test_custom_params(self):
        """Verify custom from_year and full_reimport arguments."""
        importer = KotakImporter(full_reimport=True, from_year=2022)
        assert importer.from_year == 2022
        assert importer.full_reimport is True

    def test_fetch_sources_returns_catalogue(self):
        """Verify fetch_sources returns all Kotak schemes."""
        importer = KotakImporter()
        sources = importer.fetch_sources()
        assert len(sources) == 9
        assert sources == KOTAK_FUNDS


class TestKotakImporterParsing:
    @patch("httpx.Client")
    def test_parse_source(self, mock_httpx_cls):
        """Mock Morningstar API response and verify parse_source outputs correctly formatted rows."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "equityHoldingPage": {
                "holdingList": [
                    {
                        "securityName": "ICICI Bank Ltd",
                        "weighting": 7.85,
                        "marketValue": 1050000000.0,
                        "holdingTypeId": "E",
                        "holdingType": "Equity",
                        "isin": "INE090A01021",
                    },
                    {
                        "securityName": "Bharti Airtel Ltd",
                        "weighting": 5.42,
                        "marketValue": 720000000.0,
                        "holdingTypeId": "E",
                        "holdingType": "Equity",
                        "isin": "INE397D01024",
                    },
                ]
            }
        }

        mock_http_client = MagicMock()
        mock_http_client.get.return_value = mock_response
        mock_httpx_cls.return_value.__enter__.return_value = mock_http_client

        importer = KotakImporter()
        source = ("119810", "KOTAK_FLEXICAP", "INF174K01LS2", "F00000PD3E")
        rows = importer.parse_source(source, mock_http_client)

        assert len(rows) == 2
        r1 = rows[0]
        assert r1["scheme_code"] == "119810"
        assert r1["fund_name"] == "KOTAK_FLEXICAP"
        assert r1["security_name"] == "ICICI Bank Ltd"
        assert r1["pct_of_nav"] == 7.85
        assert r1["market_value_cr"] == 105.0
        assert r1["isin"] == "INE090A01021"
        assert r1["asset_type"] == "equity"
        assert isinstance(r1["as_of_month"], date)
