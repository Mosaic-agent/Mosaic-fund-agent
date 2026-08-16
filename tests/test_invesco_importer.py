"""
tests/test_invesco_importer.py
───────────────────────────────
Unit tests for Invesco Mutual Fund holdings importer (InvescoImporter).

All tests are offline — HTTP calls and database calls are mocked.

Run:
    pytest tests/test_invesco_importer.py -v
"""

import io
from datetime import date
from unittest.mock import MagicMock, patch
import pandas as pd
import pytest

from src.data_importer.amc_holdings.factory import create_importer, REGISTRY
from src.data_importer.amc_holdings.importers.invesco import (
    InvescoImporter,
    SCHEME_MAP,
    _normalise_fund_identity,
)


class TestInvescoCatalogue:
    def test_registered_in_factory(self):
        """Verify 'invesco' is registered in factory."""
        assert "invesco" in REGISTRY
        assert REGISTRY["invesco"] == InvescoImporter

    def test_factory_instantiation(self):
        """Verify create_importer('invesco') returns an InvescoImporter instance."""
        imp = create_importer("invesco", full_reimport=True)
        assert isinstance(imp, InvescoImporter)
        assert imp.full_reimport is True

    def test_scheme_map_coverage(self):
        """Verify key Invesco schemes are present in SCHEME_MAP."""
        assert "Invesco India Smallcap Fund" in SCHEME_MAP
        assert "Invesco India Contra Fund" in SCHEME_MAP
        assert "Invesco India Multi Asset Allocation Fund" in SCHEME_MAP
        assert "Invesco India Flexi Cap Fund" in SCHEME_MAP
        assert "Invesco India Arbitrage Fund" in SCHEME_MAP

        assert SCHEME_MAP["Invesco India Smallcap Fund"] == ("INVESCO_SMALL_CAP", "Invesco India Smallcap Fund")
        assert SCHEME_MAP["Invesco India Contra Fund"] == ("INVESCO_CONTRA", "Invesco India Contra Fund")
        assert SCHEME_MAP["Invesco India Multi Asset Allocation Fund"] == ("INVESCO_MULTI_ASSET_ALLOCATION", "Invesco India Multi Asset Allocation Fund")


class TestInvescoNormalisation:
    def test_normalise_fund_identity(self):
        code, name = _normalise_fund_identity("Invesco India Smallcap Fund")
        assert code == "INVESCO_SMALL_CAP"
        assert name == "Invesco India Smallcap Fund"

        code, name = _normalise_fund_identity("Invesco India Contra Fund")
        assert code == "INVESCO_CONTRA"
        assert name == "Invesco India Contra Fund"

        code, name = _normalise_fund_identity("Invesco India Multi Asset Allocation Fund")
        assert code == "INVESCO_MULTI_ASSET_ALLOCATION"
        assert name == "Invesco India Multi Asset Allocation Fund"

        code, name = _normalise_fund_identity("Invesco India New Fund")
        assert code == "INVESCO_NEW"


class TestInvescoImporterParams:
    def test_default_params(self):
        importer = InvescoImporter()
        assert importer.full_reimport is False
        assert importer.fund_name() == "Invesco Mutual Fund"
        assert importer.table_name() == "market_data.mf_holdings"
        assert importer.watermark_source() == "mf_holdings"


class TestInvescoParsing:
    def test_parse_source_mock_excel(self):
        """Test parsing an in-memory Excel workbook for Invesco."""
        data = [
            ["ISCF", "INVESCO MUTUAL FUND", "", "", "", "", "", ""],
            ["", "Monthly Portfolio Statement as on July 31, 2026", "", "", "", "", "", ""],
            ["", "Invesco India Smallcap Fund", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", ""],
            ["", "Name of the Instrument", "ISIN", "Industry*", "Quantity", "Market/Fair Value (Rs. in Lakhs)", "% to Net Assets", "YTM"],
            ["", "Equity & Equity related", "", "", "", "", "", ""],
            ["", "(a) Listed / awaiting listing on Stock Exchanges", "", "", "", "", "", ""],
            ["SAPH01", "Sai Life Sciences Limited", "INE570L01029", "Pharmaceuticals & Biotechnology", "5440617", "71549.55", "4.94", ""],
            ["ZMPL01", "Eternal Limited", "INE758T01015", "Retailing", "22096970", "66832.29", "4.62", ""],
            ["FAKE01", "Outlier Holding", "INE123A01010", "Other", "1000", "500000.0", "75.0", ""],  # >50% guard rail
        ]
        df = pd.DataFrame(data)
        out = io.BytesIO()
        with pd.ExcelWriter(out, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="Smallcap", header=False, index=False)
        excel_bytes = out.getvalue()

        mock_http = MagicMock()
        mock_resp = MagicMock()
        mock_resp.content = excel_bytes
        mock_http.get.return_value = mock_resp

        importer = InvescoImporter()
        source = (date(2026, 7, 31), "Invesco India Smallcap Fund", "smallcap.xlsx", "https://example.com/smallcap.xlsx")
        rows = importer.parse_source(source, mock_http)

        # 3 rows total, 1 skipped (>50%) -> 2 rows
        assert len(rows) == 2

        r0 = rows[0]
        assert r0["scheme_code"] == "INVESCO_SMALL_CAP"
        assert r0["fund_name"] == "Invesco India Smallcap Fund"
        assert r0["as_of_month"] == date(2026, 7, 31)
        assert r0["isin"] == "INE570L01029"
        assert r0["security_name"] == "Sai Life Sciences Limited"
        assert r0["asset_type"] == "equity"
        assert r0["market_value_cr"] == 715.4955  # 71549.55 / 100
        assert r0["pct_of_nav"] == 4.94

        r1 = rows[1]
        assert r1["isin"] == "INE758T01015"
        assert r1["market_value_cr"] == 668.3229
        assert r1["pct_of_nav"] == 4.62

        watermarks = importer.watermark_rows(rows)
        assert watermarks == [("INVESCO_MONTHLY", date(2026, 7, 31))]

    def test_fractional_pct_scaling(self):
        """Verify decimal fractions (e.g. 0.0494) get scaled by 100 to 4.94%."""
        data = [
            ["", "INVESCO MUTUAL FUND", "", "", "", "", "", ""],
            ["", "Monthly Portfolio Statement as on July 31, 2026", "", "", "", "", "", ""],
            ["", "Invesco India Smallcap Fund", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", ""],
            ["", "Name of the Instrument", "ISIN", "Industry*", "Quantity", "Market/Fair Value (Rs. in Lakhs)", "% to Net Assets", ""],
            ["", "Sai Life Sciences Limited", "INE570L01029", "Pharmaceuticals", "5440617", "71549.55", "0.0494", ""],
            ["", "Eternal Limited", "INE758T01015", "Retailing", "22096970", "66832.29", "0.0462", ""],
        ]
        df = pd.DataFrame(data)
        out = io.BytesIO()
        with pd.ExcelWriter(out, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="Smallcap", header=False, index=False)
        excel_bytes = out.getvalue()

        mock_http = MagicMock()
        mock_resp = MagicMock()
        mock_resp.content = excel_bytes
        mock_http.get.return_value = mock_resp

        importer = InvescoImporter()
        source = (date(2026, 7, 31), "Invesco India Smallcap Fund", "smallcap.xlsx", "https://example.com/smallcap.xlsx")
        rows = importer.parse_source(source, mock_http)

        assert len(rows) == 2
        assert rows[0]["pct_of_nav"] == 4.94
        assert rows[1]["pct_of_nav"] == 4.62
