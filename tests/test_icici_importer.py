"""
tests/test_icici_importer.py
────────────────────────────
Unit & integration tests for ICICI Prudential AMC holdings importer (IciciMFImporter).

All tests are offline — HTTP calls and database calls are mocked.

Run:
    pytest tests/test_icici_importer.py -v
"""

from datetime import date
from unittest.mock import MagicMock, patch
import pytest

from src.scripts.fund_imports.importers.icici_mf import (
    ICICI_FUNDS,
    IciciMFImporter,
    _COLUMNS,
)
from src.scripts.fund_imports.factory import create_importer, REGISTRY


class TestIciciMFImporterCatalogue:
    def test_icici_esg_in_catalogue(self):
        """Verify ICICI_ESG entry is in ICICI_FUNDS catalogue."""
        esg_entries = [f for f in ICICI_FUNDS if f[1] == "ICICI_ESG"]
        assert len(esg_entries) == 1
        code, name, isin, sec_id = esg_entries[0]
        assert code == "148516"
        assert name == "ICICI_ESG"
        assert isin == "INF109KC1O09"
        assert sec_id == "F000015Q0S"

    def test_catalogue_length(self):
        """Verify ICICI catalogue contains all 12 schemes."""
        assert len(ICICI_FUNDS) == 12

    def test_registered_in_factory(self):
        """Verify 'icici' is registered in fund_imports factory."""
        assert "icici" in REGISTRY
        assert REGISTRY["icici"] == IciciMFImporter


class TestIciciMFImporterParams:
    def test_default_params(self):
        """Verify IciciMFImporter default arguments."""
        importer = IciciMFImporter()
        assert importer.from_year == 2020
        assert importer.full_reimport is False
        assert importer.fund_name() == "ICICI Prudential AMC"
        assert importer.table_name() == "market_data.mf_holdings"
        assert importer.column_names() == _COLUMNS

    def test_custom_params(self):
        """Verify custom from_year and full_reimport arguments."""
        importer = IciciMFImporter(full_reimport=True, from_year=2022)
        assert importer.from_year == 2022
        assert importer.full_reimport is True

    def test_fetch_sources_returns_catalogue(self):
        """Verify fetch_sources returns all 12 ICICI schemes."""
        importer = IciciMFImporter()
        sources = importer.fetch_sources()
        assert len(sources) == 12
        assert sources == ICICI_FUNDS


class TestIciciMFImporterParsing:
    @patch("httpx.Client")
    def test_parse_source(self, mock_httpx_cls):
        """Mock Morningstar API response and verify parse_source outputs correctly formatted rows."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "equityHoldingPage": {
                "holdingList": [
                    {
                        "securityName": "Advanced Enzyme Technologies Ltd",
                        "weighting": 1.84,
                        "marketValue": 247500000.0,
                        "holdingTypeId": "E",
                        "holdingType": "Equity",
                        "isin": "INE837H01020",
                    },
                    {
                        "securityName": "HDFC Bank Ltd",
                        "weighting": 7.59,
                        "marketValue": 1018200000.0,
                        "holdingTypeId": "E",
                        "holdingType": "Equity",
                        "isin": "INE040A01034",
                    },
                ]
            }
        }

        mock_http_client = MagicMock()
        mock_http_client.get.return_value = mock_response
        mock_httpx_cls.return_value.__enter__.return_value = mock_http_client

        importer = IciciMFImporter()
        source = ("148516", "ICICI_ESG", "INF109KC1O09", "F000015Q0S")
        rows = importer.parse_source(source, mock_http_client)

        assert len(rows) == 2
        r1 = rows[0]
        assert r1["scheme_code"] == "148516"
        assert r1["fund_name"] == "ICICI_ESG"
        assert r1["security_name"] == "Advanced Enzyme Technologies Ltd"
        assert r1["pct_of_nav"] == 1.84
        assert r1["market_value_cr"] == 24.75
        assert r1["isin"] == "INE837H01020"
        assert r1["asset_type"] == "equity"
        assert isinstance(r1["as_of_month"], date)
