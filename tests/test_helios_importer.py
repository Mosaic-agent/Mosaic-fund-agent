"""
tests/test_helios_importer.py
──────────────────────────────
Unit tests for Helios Mutual Fund holdings importer (HeliosImporter).

All tests are offline — HTTP calls and database calls are mocked.

Run:
    pytest tests/test_helios_importer.py -v
"""

import io
from datetime import date
from unittest.mock import MagicMock, patch
import pandas as pd
import pytest

from src.data_importer.amc_holdings.factory import create_importer, REGISTRY
from src.data_importer.amc_holdings.importers.helios import (
    HeliosImporter,
    SCHEME_MAP,
    _parse_disclosure_date,
    _normalise_fund_identity,
)


class TestHeliosCatalogue:
    def test_registered_in_factory(self):
        """Verify 'helios' is registered in factory."""
        assert "helios" in REGISTRY
        assert REGISTRY["helios"] == HeliosImporter

    def test_factory_instantiation(self):
        """Verify create_importer('helios') returns a HeliosImporter instance."""
        imp = create_importer("helios", full_reimport=True)
        assert isinstance(imp, HeliosImporter)
        assert imp.full_reimport is True

    def test_scheme_map_coverage(self):
        """Verify key Helios schemes are present in SCHEME_MAP."""
        assert "HSCF" in SCHEME_MAP
        assert "HFCF" in SCHEME_MAP
        assert "HMCF" in SCHEME_MAP
        assert "HLMCF" in SCHEME_MAP
        assert "HBAF" in SCHEME_MAP
        assert "HFSF" in SCHEME_MAP
        assert "HARF" in SCHEME_MAP
        assert "HOF" in SCHEME_MAP

        assert SCHEME_MAP["HSCF"] == ("HELIOS_SMALL_CAP", "Helios Small Cap Fund")
        assert SCHEME_MAP["HFCF"] == ("HELIOS_FLEXI_CAP", "Helios Flexi Cap Fund")
        assert SCHEME_MAP["HMCF"] == ("HELIOS_MID_CAP", "Helios Mid Cap Fund")


class TestHeliosDateParsing:
    def test_parse_disclosure_date_formats(self):
        assert _parse_disclosure_date("July 31, 2026", "helios-small-cap-fund-monthly-portfolio-as-on-31st-july-2026.xlsx") == date(2026, 7, 31)
        assert _parse_disclosure_date("August 31, 2025", "Helios-Overnight-Fund-Monthly-Portfolio-as-on-31st-August-2025.xlsx") == date(2025, 8, 31)
        assert _parse_disclosure_date("March 31, 2026", "Helios-Arbitrage-Fund-Monthly-Portfolio-as-on-31st-March-2026.xlsx") == date(2026, 3, 31)
        assert _parse_disclosure_date("June 30, 2026", "Helios-Flexi-Cap-Fund-Monthly-Portfolio-as-on-30th-June-2026.xlsx") == date(2026, 6, 30)
        assert _parse_disclosure_date("January 31, 2026", "Helios-Small-Cap-Fund-Monthly-Portfolio-as-on-31st-January-2026.xlsx") == date(2026, 1, 31)


class TestHeliosNormalisation:
    def test_normalise_fund_identity(self):
        code, name = _normalise_fund_identity("HSCF")
        assert code == "HELIOS_SMALL_CAP"
        assert name == "Helios Small Cap Fund"

        code, name = _normalise_fund_identity("HFCF")
        assert code == "HELIOS_FLEXI_CAP"
        assert name == "Helios Flexi Cap Fund"

        code, name = _normalise_fund_identity("Sheet1", "helios-small-cap-fund-monthly-portfolio.xlsx")
        assert code == "HELIOS_SMALL_CAP"
        assert name == "Helios Small Cap Fund"

        code, name = _normalise_fund_identity("NEW_FUND", "")
        assert code == "HELIOS_NEW_FUND"


class TestHeliosImporterParams:
    def test_default_params(self):
        importer = HeliosImporter()
        assert importer.full_reimport is False
        assert importer.fund_name() == "Helios Mutual Fund"
        assert importer.table_name() == "market_data.mf_holdings"
        assert importer.watermark_source() == "mf_holdings"


class TestHeliosParsing:
    def test_parse_source_mock_excel(self):
        """Test parsing an in-memory Excel workbook for Helios."""
        data = [
            ["", "", "", "", "", "", "", "", "", "", ""],
            ["", "", "Helios Mutual Fund", "", "", "", "", "", "", "", ""],
            ["", "", "SCHEME NAME :", "Helios Small Cap Fund", "", "", "", "", "", "", ""],
            ["", "", "PORTFOLIO STATEMENT AS ON :", "2026-07-31 00:00:00", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", "", ""],
            ["", "", "Name of the Instrument / Issuer", "ISIN", "Rating / Industry^", "Quantity", "Market value\n(Rs. in Lakhs)", "% to AUM", "YTM %", "YTC % ##", "Notes & Symbols"],
            ["", "", "EQUITY & EQUITY RELATED", "", "", "", "", "", "", "", ""],
            ["", "", "Listed/awaiting listing on Stock Exchanges", "", "", "", "", "", "", "", ""],
            ["", "102744.0", "Shadowfax Technologies Ltd.", "INE12UN01015", "Transport Services", "1495211", "3631.87", "2.50", "", "", ""],
            ["", "101345.0", "CarTrade Tech Ltd.", "INE290S01011", "Retailing", "112080", "3048.13", "2.09", "", "", ""],
            ["", "999999.0", "Outlier Holding", "INE123A01010", "Other", "1000", "500000.0", "75.0", "", "", ""],  # >50% guard rail
        ]
        df = pd.DataFrame(data)
        out = io.BytesIO()
        with pd.ExcelWriter(out, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="HSCF", header=False, index=False)
        excel_bytes = out.getvalue()

        mock_http = MagicMock()
        mock_resp = MagicMock()
        mock_resp.content = excel_bytes
        mock_http.get.return_value = mock_resp

        importer = HeliosImporter()
        source = (date(2026, 7, 31), "helios-small-cap-fund-monthly-portfolio-as-on-31st-july-2026.xlsx", "https://example.com/hscf.xlsx")
        rows = importer.parse_source(source, mock_http)

        # 3 rows total, 1 skipped (>50%) -> 2 rows
        assert len(rows) == 2

        r0 = rows[0]
        assert r0["scheme_code"] == "HELIOS_SMALL_CAP"
        assert r0["fund_name"] == "Helios Small Cap Fund"
        assert r0["as_of_month"] == date(2026, 7, 31)
        assert r0["isin"] == "INE12UN01015"
        assert r0["security_name"] == "Shadowfax Technologies Ltd."
        assert r0["asset_type"] == "equity"
        assert r0["market_value_cr"] == 36.3187  # 3631.87 / 100
        assert r0["pct_of_nav"] == 2.50

        r1 = rows[1]
        assert r1["isin"] == "INE290S01011"
        assert r1["market_value_cr"] == 30.4813
        assert r1["pct_of_nav"] == 2.09

        watermarks = importer.watermark_rows(rows)
        assert watermarks == [("HELIOS_MONTHLY", date(2026, 7, 31))]

    def test_fractional_pct_scaling(self):
        """Verify decimal fractions (e.g. 0.0250) get scaled by 100 to 2.50%."""
        data = [
            ["", "", "", "", "", "", "", "", "", "", ""],
            ["", "", "Helios Mutual Fund", "", "", "", "", "", "", "", ""],
            ["", "", "SCHEME NAME :", "Helios Small Cap Fund", "", "", "", "", "", "", ""],
            ["", "", "PORTFOLIO STATEMENT AS ON :", "2026-07-31 00:00:00", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", "", ""],
            ["", "", "Name of the Instrument / Issuer", "ISIN", "Rating / Industry^", "Quantity", "Market value (Rs. in Lakhs)", "% to AUM", "", "", ""],
            ["", "102744.0", "Ather Energy Ltd.", "INE0LEZ01016", "Automobiles", "234615", "2956.85", "0.0203", "", "", ""],
            ["", "100200.0", "Page Industries Ltd.", "INE761H01022", "Textiles & Apparels", "6953", "2807.27", "0.0193", "", "", ""],
        ]
        df = pd.DataFrame(data)
        out = io.BytesIO()
        with pd.ExcelWriter(out, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="HSCF", header=False, index=False)
        excel_bytes = out.getvalue()

        mock_http = MagicMock()
        mock_resp = MagicMock()
        mock_resp.content = excel_bytes
        mock_http.get.return_value = mock_resp

        importer = HeliosImporter()
        source = (date(2026, 7, 31), "helios-small-cap.xlsx", "https://example.com/hscf.xlsx")
        rows = importer.parse_source(source, mock_http)

        assert len(rows) == 2
        assert rows[0]["pct_of_nav"] == 2.03
        assert rows[1]["pct_of_nav"] == 1.93
