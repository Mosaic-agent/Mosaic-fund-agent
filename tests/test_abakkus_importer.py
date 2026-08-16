"""
tests/test_abakkus_importer.py
───────────────────────────────
Unit tests for Abakkus Mutual Fund holdings importer (AbakkusImporter).

All tests are offline — HTTP calls and database calls are mocked.

Run:
    pytest tests/test_abakkus_importer.py -v
"""

import io
from datetime import date
from unittest.mock import MagicMock, patch
import pandas as pd
import pytest

from src.data_importer.amc_holdings.factory import create_importer, REGISTRY
from src.data_importer.amc_holdings.importers.abakkus import (
    AbakkusImporter,
    SCHEME_MAP,
    _parse_disclosure_date,
    _normalise_fund_identity,
)


class TestAbakkusCatalogue:
    def test_registered_in_factory(self):
        """Verify 'abakkus' and 'abacus' are registered in factory."""
        assert "abakkus" in REGISTRY
        assert "abacus" in REGISTRY
        assert REGISTRY["abakkus"] == AbakkusImporter
        assert REGISTRY["abacus"] == AbakkusImporter

    def test_factory_instantiation(self):
        """Verify create_importer('abakkus') returns an AbakkusImporter instance."""
        imp = create_importer("abakkus", full_reimport=True)
        assert isinstance(imp, AbakkusImporter)
        assert imp.full_reimport is True

    def test_scheme_map_coverage(self):
        """Verify key Abakkus schemes are present in SCHEME_MAP."""
        assert "ABAFC" in SCHEME_MAP
        assert "ABASC" in SCHEME_MAP
        assert "ABALI" in SCHEME_MAP
        assert "ABALMC" in SCHEME_MAP

        assert SCHEME_MAP["ABAFC"] == ("ABAKKUS_FLEXI_CAP", "Abakkus Flexi Cap Fund")
        assert SCHEME_MAP["ABASC"] == ("ABAKKUS_SMALL_CAP", "Abakkus Small Cap Fund")
        assert SCHEME_MAP["ABALI"] == ("ABAKKUS_LIQUID", "Abakkus Liquid Fund")


class TestAbakkusDateParsing:
    def test_parse_disclosure_date_formats(self):
        assert _parse_disclosure_date("December 31, 2025", "AbakkusMF.xlsx") == date(2025, 12, 31)
        assert _parse_disclosure_date("January 15, 2026", "IN_MF.xls") == date(2026, 1, 15)
        assert _parse_disclosure_date("31st March 2026", "portfolio.xls") == date(2026, 3, 31)
        assert _parse_disclosure_date("July 31, 2026", "Final.xls") == date(2026, 7, 31)
        assert _parse_disclosure_date("", "Abakkus_Mutual_Fund_31.05.2026.xls") == date(2026, 5, 31)


class TestAbakkusNormalisation:
    def test_normalise_fund_identity(self):
        code, name = _normalise_fund_identity("ABAFC")
        assert code == "ABAKKUS_FLEXI_CAP"
        assert name == "Abakkus Flexi Cap Fund"

        code, name = _normalise_fund_identity("ABASC")
        assert code == "ABAKKUS_SMALL_CAP"
        assert name == "Abakkus Small Cap Fund"

        code, name = _normalise_fund_identity("Custom", "Abakkus Flexi Cap Fund")
        assert code == "ABAKKUS_FLEXI_CAP"

        code, name = _normalise_fund_identity("NEW_FUND", "")
        assert code == "ABAKKUS_NEW_FUND"


class TestAbakkusImporterParams:
    def test_default_params(self):
        importer = AbakkusImporter()
        assert importer.full_reimport is False
        assert importer.fund_name() == "Abakkus Mutual Fund"
        assert importer.table_name() == "market_data.mf_holdings"
        assert importer.watermark_source() == "mf_holdings"


class TestAbakkusParsing:
    def test_parse_source_mock_excel(self):
        """Test parsing an in-memory Excel workbook."""
        # Create a mock Excel file
        data = [
            ["ABAFC", "Abakkus Mutual Fund", "", "", "", "", "", ""],
            ["", "Abakkus Flexi Cap Fund", "", "", "", "", "", ""],
            ["", "Monthly Portfolio Statement as on July 31, 2026", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", ""],
            ["", "Name of the Instrument", "ISIN", "Industry* / Rating", "Quantity", "Market/Fair Value (Rs. in Lakhs)", "% to Net Assets", ""],
            ["", "Equity & Equity related", "", "", "", "", "", ""],
            ["", "(a) Listed / awaiting listing on Stock Exchanges", "", "", "", "", "", ""],
            ["IBCL05", "ICICI Bank Limited", "INE090A01021", "Banks", "2450000", "35167.3", "5.55", ""],
            ["HDFB03", "HDFC Bank Limited", "INE040A01034", "Banks", "4000000", "29926.0", "4.72", ""],
            ["FAKE01", "Rogue Holding", "INE123A01010", "Other", "1000", "1000000.0", "65.0", ""],  # > 50% guard
        ]
        df = pd.DataFrame(data)
        out = io.BytesIO()
        with pd.ExcelWriter(out, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="ABAFC", header=False, index=False)
        excel_bytes = out.getvalue()

        mock_http = MagicMock()
        mock_resp = MagicMock()
        mock_resp.content = excel_bytes
        mock_http.get.return_value = mock_resp

        importer = AbakkusImporter()
        source = (date(2026, 7, 31), "Final_Monthly_Portfolio_Jul_31.xls", "https://example.com/file.xls")
        rows = importer.parse_source(source, mock_http)

        # 3 rows total, 1 skipped due to >50% guard -> 2 rows
        assert len(rows) == 2

        r0 = rows[0]
        assert r0["scheme_code"] == "ABAKKUS_FLEXI_CAP"
        assert r0["fund_name"] == "Abakkus Flexi Cap Fund"
        assert r0["as_of_month"] == date(2026, 7, 31)
        assert r0["isin"] == "INE090A01021"
        assert r0["security_name"] == "ICICI Bank Limited"
        assert r0["asset_type"] == "equity"
        assert r0["market_value_cr"] == 351.673  # 35167.3 / 100
        assert r0["pct_of_nav"] == 5.55

        r1 = rows[1]
        assert r1["isin"] == "INE040A01034"
        assert r1["market_value_cr"] == 299.26
        assert r1["pct_of_nav"] == 4.72

        watermarks = importer.watermark_rows(rows)
        assert watermarks == [("ABAKKUS_MONTHLY", date(2026, 7, 31))]

    def test_fractional_pct_scaling(self):
        """Verify decimal fractions (e.g. 0.0555) get scaled by 100 to 5.55%."""
        data = [
            ["ABASC", "Abakkus Mutual Fund", "", "", "", "", "", ""],
            ["", "Abakkus Small Cap Fund", "", "", "", "", "", ""],
            ["", "Monthly Portfolio Statement as on March 31, 2026", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", ""],
            ["", "Name of the Instrument", "ISIN", "Industry* / Rating", "Quantity", "Market/Fair Value (Rs. in Lakhs)", "% to Net Assets", ""],
            ["", "Equity & Equity related", "", "", "", "", "", ""],
            ["KAVY06", "Karur Vysya Bank Limited", "INE036D01028", "Banks", "1000000", "3000.0", "0.0350", ""],
            ["DLPL01", "Dr. Lal Path Labs Limited", "INE600L01024", "Healthcare Services", "100000", "2000.0", "0.0250", ""],
        ]
        df = pd.DataFrame(data)
        out = io.BytesIO()
        with pd.ExcelWriter(out, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="ABASC", header=False, index=False)
        excel_bytes = out.getvalue()

        mock_http = MagicMock()
        mock_resp = MagicMock()
        mock_resp.content = excel_bytes
        mock_http.get.return_value = mock_resp

        importer = AbakkusImporter()
        source = (date(2026, 3, 31), "March.xls", "https://example.com/march.xls")
        rows = importer.parse_source(source, mock_http)

        assert len(rows) == 2
        assert rows[0]["pct_of_nav"] == 3.50
        assert rows[1]["pct_of_nav"] == 2.50
