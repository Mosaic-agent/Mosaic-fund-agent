"""
tests/test_mirae_importer.py
─────────────────────────────
Unit tests for Mirae Asset Mutual Fund holdings importer (MiraeImporter).

All tests are offline — HTTP calls and database calls are mocked.

Run:
    pytest tests/test_mirae_importer.py -v
"""

import io
from datetime import date
from unittest.mock import MagicMock
import pandas as pd
import pytest

from src.data_importer.amc_holdings.factory import create_importer, REGISTRY
from src.data_importer.amc_holdings.importers.mirae import (
    MiraeImporter,
    SCHEME_MAP,
    _parse_disclosure_date,
    _normalise_fund_identity,
)


class TestMiraeCatalogue:
    def test_registered_in_factory(self):
        """Verify 'mirae', 'mirae_asset', and 'mirae-asset' are registered in factory."""
        assert "mirae" in REGISTRY
        assert "mirae_asset" in REGISTRY
        assert "mirae-asset" in REGISTRY
        assert REGISTRY["mirae"] == MiraeImporter

    def test_factory_instantiation(self):
        """Verify create_importer('mirae') returns a MiraeImporter instance."""
        imp = create_importer("mirae", full_reimport=True)
        assert isinstance(imp, MiraeImporter)
        assert imp.full_reimport is True

    def test_scheme_map_coverage(self):
        """Verify key Mirae Asset schemes are present in SCHEME_MAP."""
        assert "Mirae Asset Small Cap Fund" in SCHEME_MAP
        assert "Mirae Asset Large Cap Fund" in SCHEME_MAP
        assert "Mirae Asset Flexi Cap Fund" in SCHEME_MAP
        assert "Mirae Asset Multi Asset Allocation Fund" in SCHEME_MAP
        assert "Mirae Asset Midcap Fund" in SCHEME_MAP
        assert "Mirae Asset ELSS Tax Saver Fund" in SCHEME_MAP

        assert SCHEME_MAP["Mirae Asset Small Cap Fund"] == ("MIRAE_SMALL_CAP", "Mirae Asset Small Cap Fund")
        assert SCHEME_MAP["Mirae Asset Large Cap Fund"] == ("MIRAE_LARGE_CAP", "Mirae Asset Large Cap Fund")
        assert SCHEME_MAP["Mirae Asset Multi Asset Allocation Fund"] == ("MIRAE_MULTI_ASSET_ALLOCATION", "Mirae Asset Multi Asset Allocation Fund")


class TestMiraeDateParsing:
    def test_parse_disclosure_date_formats(self):
        assert _parse_disclosure_date("Portfolio Details as on 31st July 2026 for Mirae Asset Small Cap Fund") == date(2026, 7, 31)
        assert _parse_disclosure_date("Portfolio Details as on 30th June 2026 for Mirae Asset Large Cap Fund") == date(2026, 6, 30)
        assert _parse_disclosure_date("Portfolio Details as on 28th February 2026 for Mirae Asset Midcap Fund") == date(2026, 2, 28)
        assert _parse_disclosure_date("Mirae Asset Multi Asset Allocation Fund - July 2026") == date(2026, 7, 31)


class TestMiraeNormalisation:
    def test_normalise_fund_identity(self):
        code, name = _normalise_fund_identity("Portfolio Details as on 31st July 2026 for Mirae Asset Small Cap Fund", "mascf-july2026.xlsx")
        assert code == "MIRAE_SMALL_CAP"
        assert name == "Mirae Asset Small Cap Fund"

        code, name = _normalise_fund_identity("Portfolio Details as on 31st July 2026 for Mirae Asset Multi Asset Allocation Fund", "mamulti-july2026.xlsx")
        assert code == "MIRAE_MULTI_ASSET_ALLOCATION"
        assert name == "Mirae Asset Multi Asset Allocation Fund"

        code, name = _normalise_fund_identity("Portfolio Details as on 31st July 2026 for Mirae Asset New Fund", "new.xlsx")
        assert code == "MIRAE_NEW"


class TestMiraeImporterParams:
    def test_default_params(self):
        importer = MiraeImporter()
        assert importer.full_reimport is False
        assert importer.fund_name() == "Mirae Asset Mutual Fund"
        assert importer.table_name() == "market_data.mf_holdings"
        assert importer.watermark_source() == "mf_holdings"


class TestMiraeParsing:
    def test_parse_source_mock_excel(self):
        """Test parsing an in-memory Excel workbook for Mirae Asset."""
        data = [
            ["", "Mirae Asset Small Cap Fund", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", ""],
            ["", "(Small Cap Fund - An open ended equity scheme)", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", ""],
            ["", "Mirae Asset Small Cap Fund", "MI072", "", "2026-07-31", "", "", ""],
            ["", "", "", "", "", "", "", ""],
            ["", "Monthly Portfolio Statement as on July 31, 2026", "", "", "", "", "", ""],
            ["", "Name of the Instrument", "ISIN", "Industry ^/ Rating", "Quantity", "Market/Fair Value \n(Rs. in Lakhs)", "% to Net Assets", "YTM"],
            ["", "Welspun Corp Ltd.", "INE191B01025", "Industrial Products", "962128", "15880.88", "3.0418", ""],
            ["", "Kirloskar Oil Engines Ltd", "INE146L01010", "Industrial Products", "610000", "13357.78", "2.5585", ""],
            ["", "Outlier Holding", "INE123A01010", "Other", "1000", "500000.0", "120.0", ""],  # >105% guard rail
        ]
        df = pd.DataFrame(data)
        out = io.BytesIO()
        with pd.ExcelWriter(out, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="MASCF", header=False, index=False)
        excel_bytes = out.getvalue()

        mock_http = MagicMock()
        mock_resp = MagicMock()
        mock_resp.content = excel_bytes
        mock_http.get.return_value = mock_resp

        importer = MiraeImporter()
        source = (date(2026, 7, 31), "Portfolio Details as on 31st July 2026 for Mirae Asset Small Cap Fund", "mascf-july2026.xlsx", "https://example.com/mascf.xlsx")
        rows = importer.parse_source(source, mock_http)

        # 3 rows total, 1 skipped (>105%) -> 2 rows
        assert len(rows) == 2

        r0 = rows[0]
        assert r0["scheme_code"] == "MIRAE_SMALL_CAP"
        assert r0["fund_name"] == "Mirae Asset Small Cap Fund"
        assert r0["as_of_month"] == date(2026, 7, 31)
        assert r0["isin"] == "INE191B01025"
        assert r0["security_name"] == "Welspun Corp Ltd."
        assert r0["asset_type"] == "equity"
        assert r0["market_value_cr"] == 158.8088  # 15880.88 Lakhs / 100
        assert r0["pct_of_nav"] == 3.0418

        r1 = rows[1]
        assert r1["isin"] == "INE146L01010"
        assert r1["market_value_cr"] == 133.5778
        assert r1["pct_of_nav"] == 2.5585

        watermarks = importer.watermark_rows(rows)
        assert watermarks == [("MIRAE_ASSET_MONTHLY", date(2026, 7, 31))]

    def test_fractional_pct_scaling(self):
        """Verify decimal fractions (e.g. 0.0304) get scaled by 100 to 3.04%."""
        data = [
            ["", "Mirae Asset Small Cap Fund", "", "", "", "", "", ""],
            ["", "Monthly Portfolio Statement as on July 31, 2026", "", "", "", "", "", ""],
            ["", "Name of the Instrument", "ISIN", "Industry ^/ Rating", "Quantity", "Market/Fair Value \n(Rs. in Lakhs)", "% to Net Assets", ""],
            ["", "Welspun Corp Ltd.", "INE191B01025", "Industrial Products", "962128", "15880.88", "0.030418", ""],
            ["", "Kirloskar Oil Engines Ltd", "INE146L01010", "Industrial Products", "610000", "13357.78", "0.025585", ""],
        ]
        df = pd.DataFrame(data)
        out = io.BytesIO()
        with pd.ExcelWriter(out, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="MASCF", header=False, index=False)
        excel_bytes = out.getvalue()

        mock_http = MagicMock()
        mock_resp = MagicMock()
        mock_resp.content = excel_bytes
        mock_http.get.return_value = mock_resp

        importer = MiraeImporter()
        source = (date(2026, 7, 31), "Portfolio Details as on 31st July 2026 for Mirae Asset Small Cap Fund", "mascf-july2026.xlsx", "https://example.com/mascf.xlsx")
        rows = importer.parse_source(source, mock_http)

        assert len(rows) == 2
        assert rows[0]["pct_of_nav"] == 3.0418
        assert rows[1]["pct_of_nav"] == 2.5585
