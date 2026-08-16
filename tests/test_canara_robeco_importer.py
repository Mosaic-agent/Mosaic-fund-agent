"""
tests/test_canara_robeco_importer.py
─────────────────────────────────────
Unit tests for Canara Robeco Mutual Fund holdings importer (CanaraRobecoImporter).

All tests are offline — HTTP calls and database calls are mocked.

Run:
    pytest tests/test_canara_robeco_importer.py -v
"""

import io
from datetime import date
from unittest.mock import MagicMock
import pandas as pd
import pytest

from src.data_importer.amc_holdings.factory import create_importer, REGISTRY
from src.data_importer.amc_holdings.importers.canara_robeco import (
    CanaraRobecoImporter,
    SCHEME_MAP,
    PREFIX_MAP,
    _normalise_fund_identity,
)


class TestCanaraRobecoCatalogue:
    def test_registered_in_factory(self):
        """Verify 'canara', 'canara_robeco', and 'canara-robeco' are registered in factory."""
        assert "canara" in REGISTRY
        assert "canara_robeco" in REGISTRY
        assert "canara-robeco" in REGISTRY
        assert REGISTRY["canara"] == CanaraRobecoImporter

    def test_factory_instantiation(self):
        """Verify create_importer('canara') returns a CanaraRobecoImporter instance."""
        imp = create_importer("canara", full_reimport=True)
        assert isinstance(imp, CanaraRobecoImporter)
        assert imp.full_reimport is True

    def test_scheme_map_coverage(self):
        """Verify key Canara Robeco schemes are present in SCHEME_MAP."""
        assert "Canara Robeco Small Cap Fund" in SCHEME_MAP
        assert "Canara Robeco Flexi Cap Fund" in SCHEME_MAP
        assert "Canara Robeco Large and Mid Cap Fund" in SCHEME_MAP
        assert "Canara Robeco Multi Cap Fund" in SCHEME_MAP
        assert "Canara Robeco Mid Cap Fund" in SCHEME_MAP
        assert "Canara Robeco Value Fund" in SCHEME_MAP

        assert SCHEME_MAP["Canara Robeco Small Cap Fund"] == ("CANARA_SMALL_CAP", "Canara Robeco Small Cap Fund")
        assert SCHEME_MAP["Canara Robeco Flexi Cap Fund"] == ("CANARA_FLEXI_CAP", "Canara Robeco Flexi Cap Fund")
        assert SCHEME_MAP["Canara Robeco Value Fund"] == ("CANARA_VALUE", "Canara Robeco Value Fund")


class TestCanaraRobecoNormalisation:
    def test_normalise_fund_identity(self):
        code, name = _normalise_fund_identity("SC – Canara Robeco Small Cap Fund – July-2026", "SC-–-Canara-Robeco-Small-Cap-Fund-–-July-2026.xlsx")
        assert code == "CANARA_SMALL_CAP"
        assert name == "Canara Robeco Small Cap Fund"

        code, name = _normalise_fund_identity("VF – Canara Robeco Value Fund – July-2026", "VF-–-Canara-Robeco-Value-Fund-–-July-2026.xlsx")
        assert code == "CANARA_VALUE"
        assert name == "Canara Robeco Value Fund"

        code, name = _normalise_fund_identity("IF – Canara Robeco Income Fund – July-2026", "IF-–-Canara-Robeco-Income-Fund-–-July-2026.xlsx")
        assert code == "CANARA_INCOME"
        assert name == "Canara Robeco Income Fund"

        code, name = _normalise_fund_identity("MI – Canara Robeco Conservative Hybrid Fund – July-2026", "MI–CRCHF–July-2026.xlsx")
        assert code == "CANARA_CONSERVATIVE_HYBRID"
        assert name == "Canara Robeco Conservative Hybrid Fund"

        code, name = _normalise_fund_identity("Canara Robeco New Fund", "new.xlsx")
        assert code == "CANARA_NEW"


class TestCanaraRobecoImporterParams:
    def test_default_params(self):
        importer = CanaraRobecoImporter()
        assert importer.full_reimport is False
        assert importer.fund_name() == "Canara Robeco Mutual Fund"
        assert importer.table_name() == "market_data.mf_holdings"
        assert importer.watermark_source() == "mf_holdings"


class TestCanaraRobecoParsing:
    def test_parse_source_mock_excel(self):
        """Test parsing an in-memory Excel workbook for Canara Robeco."""
        data = [
            ["", "CANARA ROBECO SMALL CAP FUND", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", ""],
            ["", "Monthly Portfolio Statement as on July 31, 2026", "", "", "", "", "", ""],
            ["", "Name of the Instrument", "ISIN", "Industry / Rating", "Quantity", "Market/Fair Value\n (Rs. in Lacs)", "% to Net\n Assets", "Yield %"],
            ["", "Amber Enterprises India Ltd", "INE371P01015", "Consumer Durables", "746266", "35442.0", "2.49", ""],
            ["", "Torrent Pharmaceuticals Ltd", "INE685A01028", "Pharmaceuticals", "500000", "35370.0", "2.49", ""],
            ["", "Outlier Holding", "INE123A01010", "Other", "1000", "500000.0", "75.0", ""],  # >50% guard rail
        ]
        df = pd.DataFrame(data)
        out = io.BytesIO()
        with pd.ExcelWriter(out, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="SC", header=False, index=False)
        excel_bytes = out.getvalue()

        mock_http = MagicMock()
        mock_resp = MagicMock()
        mock_resp.content = excel_bytes
        mock_http.get.return_value = mock_resp

        importer = CanaraRobecoImporter()
        source = (date(2026, 7, 31), "SC – Canara Robeco Small Cap Fund – July-2026", "SC.xlsx", "https://example.com/SC.xlsx")
        rows = importer.parse_source(source, mock_http)

        # 3 rows total, 1 skipped (>50%) -> 2 rows
        assert len(rows) == 2

        r0 = rows[0]
        assert r0["scheme_code"] == "CANARA_SMALL_CAP"
        assert r0["fund_name"] == "Canara Robeco Small Cap Fund"
        assert r0["as_of_month"] == date(2026, 7, 31)
        assert r0["isin"] == "INE371P01015"
        assert r0["security_name"] == "Amber Enterprises India Ltd"
        assert r0["asset_type"] == "equity"
        assert r0["market_value_cr"] == 354.42  # 35442.0 Lacs / 100
        assert r0["pct_of_nav"] == 2.49

        r1 = rows[1]
        assert r1["isin"] == "INE685A01028"
        assert r1["market_value_cr"] == 353.70
        assert r1["pct_of_nav"] == 2.49

        watermarks = importer.watermark_rows(rows)
        assert watermarks == [("CANARA_ROBECO_MONTHLY", date(2026, 7, 31))]

    def test_fractional_pct_scaling(self):
        """Verify decimal fractions (e.g. 0.0249) get scaled by 100 to 2.49%."""
        data = [
            ["", "CANARA ROBECO SMALL CAP FUND", "", "", "", "", "", ""],
            ["", "Monthly Portfolio Statement as on July 31, 2026", "", "", "", "", "", ""],
            ["", "Name of the Instrument", "ISIN", "Industry / Rating", "Quantity", "Market/Fair Value\n (Rs. in Lacs)", "% to Net\n Assets", ""],
            ["", "Amber Enterprises India Ltd", "INE371P01015", "Consumer Durables", "746266", "35442.0", "0.0249", ""],
            ["", "Torrent Pharmaceuticals Ltd", "INE685A01028", "Pharmaceuticals", "500000", "35370.0", "0.0249", ""],
        ]
        df = pd.DataFrame(data)
        out = io.BytesIO()
        with pd.ExcelWriter(out, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="SC", header=False, index=False)
        excel_bytes = out.getvalue()

        mock_http = MagicMock()
        mock_resp = MagicMock()
        mock_resp.content = excel_bytes
        mock_http.get.return_value = mock_resp

        importer = CanaraRobecoImporter()
        source = (date(2026, 7, 31), "SC – Canara Robeco Small Cap Fund – July-2026", "SC.xlsx", "https://example.com/SC.xlsx")
        rows = importer.parse_source(source, mock_http)

        assert len(rows) == 2
        assert rows[0]["pct_of_nav"] == 2.49
        assert rows[1]["pct_of_nav"] == 2.49
