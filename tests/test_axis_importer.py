"""
tests/test_axis_importer.py
────────────────────────────
Unit test suite for Axis Mutual Fund holdings importer.
"""

from __future__ import annotations

import io
from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.data_importer.amc_holdings.factory import REGISTRY, create_importer
from src.data_importer.amc_holdings.importers.axis import (
    AxisImporter,
    SCHEME_MAP,
    _normalise_fund_identity,
    _parse_disclosure_date,
)


class TestAxisImporterSchemeMapping:
    def test_scheme_map_known_funds(self):
        assert "Axis Small Cap Fund" in SCHEME_MAP
        assert SCHEME_MAP["Axis Small Cap Fund"] == ("AXIS_SMALL_CAP", "Axis Small Cap Fund")
        assert "Axis Multi Asset Allocation Fund" in SCHEME_MAP
        assert SCHEME_MAP["Axis Multi Asset Allocation Fund"] == ("AXIS_MULTI_ASSET_ALLOCATION", "Axis Multi Asset Allocation Fund")
        assert "Axis Flexi Cap Fund" in SCHEME_MAP
        assert SCHEME_MAP["Axis Flexi Cap Fund"] == ("AXIS_FLEXI_CAP", "Axis Flexi Cap Fund")
        assert "Axis Gold ETF" in SCHEME_MAP
        assert SCHEME_MAP["Axis Gold ETF"] == ("AXIS_GOLD_ETF", "Axis Gold ETF")

    def test_parse_disclosure_date(self):
        assert _parse_disclosure_date("Monthly Portfolio Statement as on July 31, 2026") == date(2026, 7, 31)
        assert _parse_disclosure_date("Monthly Portfolio-31 07 26") == date(2026, 7, 31)
        assert _parse_disclosure_date("Monthly Portfolio - Axis Small Cap Fund - 31 July 2026") == date(2026, 7, 31)
        assert _parse_disclosure_date("Monthly_Portfolio_31072026_8a12978eff.xlsx") == date(2026, 7, 31)
        assert _parse_disclosure_date("monthly_20portfolio-31_2003_2026.xlsx") == date(2026, 3, 31)
        assert _parse_disclosure_date("monthly_20portfolio-28_2002_2026.xlsx") == date(2026, 2, 28)

    def test_normalise_fund_identity(self):
        code, name = _normalise_fund_identity("Axis Small Cap Fund")
        assert code == "AXIS_SMALL_CAP"
        assert name == "Axis Small Cap Fund"

        code, name = _normalise_fund_identity("Axis Multi Asset Allocation Fund")
        assert code == "AXIS_MULTI_ASSET_ALLOCATION"

        # Fallback for unknown fund
        code, name = _normalise_fund_identity("Axis Next Gen Alpha Fund")
        assert code.startswith("AXIS_")
        assert "ALPHA" in code


class TestAxisImporterStructure:
    def test_importer_metadata(self):
        importer = AxisImporter()
        assert importer.fund_name() == "Axis Mutual Fund"
        assert importer.table_name() == "market_data.mf_holdings"
        assert importer.watermark_source() == "mf_holdings"
        cols = importer.column_names()
        assert "scheme_code" in cols
        assert "market_value_cr" in cols
        assert "pct_of_nav" in cols

    def test_factory_registration(self):
        assert "axis" in REGISTRY
        assert "axis_mf" in REGISTRY
        assert "axis-mf" in REGISTRY
        importer = create_importer("axis")
        assert isinstance(importer, AxisImporter)


class TestAxisExcelParsing:
    def test_parse_mock_multi_sheet_excel(self):
        # Create in-memory multi-sheet Excel matching Axis format
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            # Index sheet
            index_df = pd.DataFrame([
                ["Sr No.", "Short Name", "Scheme Name", "Exchange"],
                [1, "AXISSCF", "Axis Small Cap Fund", None],
                [2, "AXISTAF", "Axis Multi Asset Allocation Fund", None],
            ])
            index_df.to_excel(writer, sheet_name="Index", header=False, index=False)

            # Axis Small Cap sheet
            sc_data = [
                ["AXISSCF", "Axis Small Cap Fund", None, None, None, None, None],
                [None, None, None, None, None, None, None],
                ["", "Monthly Portfolio Statement as on July 31, 2026", None, None, None, None, None],
                [None, "Name of the Instrument", "ISIN", "Industry", "Quantity", "Market/Fair Value (Rs. in Lakhs)", "% to Net Assets"],
                [None, "Equity & Equity related", None, None, None, None, None],
                ["KIMS01", "Krishna Institute Of Medical Sciences Limited", "INE967H01025", "Healthcare Services", 100000, 81523.0, 0.0271],
                ["TORR01", "Torrent Pharmaceuticals Limited", "INE685A01028", "Pharmaceuticals & Biotechnology", 50000, 77981.0, 0.0259],
                [None, "Sub Total", None, None, None, 159504.0, 0.0530],
                [None, "Treps / Reverse Repo", None, None, None, 10000.0, 0.0050],
            ]
            pd.DataFrame(sc_data).to_excel(writer, sheet_name="AXISSCF", header=False, index=False)

            # Axis Multi Asset sheet
            ta_data = [
                ["AXISTAF", "Axis Multi Asset Allocation Fund", None, None, None, None, None],
                [None, None, None, None, None, None, None],
                ["", "Monthly Portfolio Statement as on July 31, 2026", None, None, None, None, None],
                [None, "Name of the Instrument", "ISIN", "Industry", "Quantity", "Market/Fair Value (Rs. in Lakhs)", "% to Net Assets"],
                ["ICIC01", "ICICI Bank Limited", "INE090A01021", "Banks", 50000, 16246.0, 0.0650],
                ["GOLD01", "Axis Gold ETF", "INF846K01347", "Gold ETF", 10000, 5000.0, 0.0200],
            ]
            pd.DataFrame(ta_data).to_excel(writer, sheet_name="AXISTAF", header=False, index=False)

        output.seek(0)
        content = output.getvalue()

        importer = AxisImporter()
        mock_http = MagicMock()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = content
        mock_http.get.return_value = mock_resp

        source = (date(2026, 7, 31), "Monthly Portfolio-31 07 26", "Monthly_Portfolio_31072026.xlsx", "https://example.com/test.xlsx")
        rows = importer.parse_source(source, mock_http)

        assert len(rows) >= 4
        # Verify Small Cap holdings
        sc_rows = [r for r in rows if r["scheme_code"] == "AXIS_SMALL_CAP"]
        assert len(sc_rows) >= 2
        kims = next(r for r in sc_rows if "Krishna" in r["security_name"])
        assert kims["isin"] == "INE967H01025"
        assert kims["market_value_cr"] == 815.23  # 81523 Lakhs / 100
        assert kims["pct_of_nav"] == 2.71        # 0.0271 * 100
        assert kims["asset_type"] == "equity"

        # Verify Multi Asset holdings
        ta_rows = [r for r in rows if r["scheme_code"] == "AXIS_MULTI_ASSET_ALLOCATION"]
        assert len(ta_rows) >= 2
        icici = next(r for r in ta_rows if "ICICI" in r["security_name"])
        assert icici["market_value_cr"] == 162.46
        assert icici["pct_of_nav"] == 6.50

    def test_parse_mock_debt_format_excel(self):
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            headers = [
                "Scheme Code", "Scheme Name", "ISIN", "Security Name", "Security Type",
                "Settled Quantity", "Total Market Value (Rs.)", "Market Value as % of Net Asset",
                "Yield", "Residual Maturity", "Modified Duration", "Macaulay Duration",
                "Average Maturity", "Rating / Industry", "Sector Classification",
                "Coupon Rate", "Face Value", "ISIN Description", "Asset Class",
                "Instrument Type", "Rating Agency", "Macro Economic Sector"
            ]
            row1 = [
                "AXISTAA", "Axis Treasury Advantage Fund", "INE040A01034", "HDFC Bank Limited", "CD",
                1000, 500000000.0, 0.05, 7.15, 90, 0.25, 0.25, 0.25, "CRISIL A1+", "Banks",
                7.0, 100, "CD", "Money Market", "CD", "CRISIL", "Financial Services"
            ]
            pd.DataFrame([headers, row1]).to_excel(writer, sheet_name="Debt", header=False, index=False)

        output.seek(0)
        content = output.getvalue()

        importer = AxisImporter()
        mock_http = MagicMock()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = content
        mock_http.get.return_value = mock_resp

        source = (date(2026, 7, 31), "Axis Treasury Advantage Fund", "AXISTAA.xlsx", "https://example.com/AXISTAA.xlsx")
        rows = importer.parse_source(source, mock_http)

        assert len(rows) == 1
        r = rows[0]
        assert r["scheme_code"] == "AXIS_TREASURY_ADVANTAGE"
        assert r["market_value_cr"] == 50.0  # 500,000,000 Rs / 10,000,000 = 50.0 Cr
        assert r["pct_of_nav"] == 5.0        # 0.05 * 100 = 5.0%

    def test_outlier_rejection(self):
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            data = [
                ["AXISSCF", "Axis Small Cap Fund", None, None, None, None, None],
                [None, None, None, None, None, None, None],
                ["", "Monthly Portfolio Statement as on July 31, 2026", None, None, None, None, None],
                [None, "Name of the Instrument", "ISIN", "Industry", "Quantity", "Market/Fair Value (Rs. in Lakhs)", "% to Net Assets"],
                ["ABC", "Normal Stock", "INE123A01010", "IT", 1000, 1000.0, 0.05],
                ["XYZ", "Total Outlier Row", "INE999A01010", "Total", 9999, 999999.0, 1.50],  # 150% -> outlier
            ]
            pd.DataFrame(data).to_excel(writer, sheet_name="AXISSCF", header=False, index=False)

        output.seek(0)
        importer = AxisImporter()
        mock_http = MagicMock()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = output.getvalue()
        mock_http.get.return_value = mock_resp

        source = (date(2026, 7, 31), "Axis Small Cap Fund", "test.xlsx", "https://example.com/test.xlsx")
        rows = importer.parse_source(source, mock_http)

        assert len(rows) == 1
        assert rows[0]["security_name"] == "Normal Stock"
