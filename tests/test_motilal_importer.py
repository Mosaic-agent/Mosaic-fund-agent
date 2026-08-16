"""
tests/test_motilal_importer.py
──────────────────────────────
Unit tests for Motilal Oswal Mutual Fund portfolio holdings importer.
"""

from __future__ import annotations

import io
from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.data_importer.amc_holdings.factory import REGISTRY, create_importer
from src.data_importer.amc_holdings.importers.motilal import (
    MotilalOswalImporter,
    _clean_scheme_name,
    _parse_disclosure_date,
    _parse_sheet_date,
)


class TestMotilalCatalogue:
    def test_registered_in_factory(self):
        assert "motilal" in REGISTRY
        assert REGISTRY["motilal"] is MotilalOswalImporter

    def test_aliases_registered(self):
        assert "motilal_oswal" in REGISTRY
        assert "motilal-oswal" in REGISTRY
        assert REGISTRY["motilal_oswal"] is MotilalOswalImporter
        assert REGISTRY["motilal-oswal"] is MotilalOswalImporter

    def test_factory_instantiation(self):
        imp = create_importer("motilal", from_year=2024, full_reimport=True)
        assert isinstance(imp, MotilalOswalImporter)
        assert imp.from_year == 2024
        assert imp.full_reimport is True


class TestMotilalDateParsing:
    def test_parse_disclosure_date_formats(self):
        assert _parse_disclosure_date("scheme portfolio details july 2026", "/content/dam/.../Monthly Portfolio 31-07-2026-Final.xlsx") == date(2026, 7, 31)
        assert _parse_disclosure_date("Scheme Portfolio Details June 2026", "/content/dam/.../Scheme Portfolio Details June 20261.xlsx") == date(2026, 6, 30)
        assert _parse_disclosure_date("scheme portfolio details - february 2026", "/content/dam/.../8c1a9-scheme-portfolio-details-february-26.xlsx") == date(2026, 2, 28)
        assert _parse_disclosure_date("month end portfolio - may 2025", "/content/dam/.../27945-month-end-portfolio-may-2025.xlsx") == date(2025, 5, 31)

    def test_parse_sheet_date(self):
        assert _parse_sheet_date("MONTHLY PORTFOLIO STATEMENT AS ON JULY 31, 2026") == date(2026, 7, 31)
        assert _parse_sheet_date("Statement As On June 30 2026") == date(2026, 6, 30)
        assert _parse_sheet_date("as at March 31, 2025") == date(2025, 3, 31)
        assert _parse_sheet_date("Invalid text") is None


class TestMotilalNormalisation:
    def test_clean_scheme_name(self):
        assert _clean_scheme_name("Motilal Oswal Midcap Fund (Formerly known as Motilal Oswal MOSt Focused Midcap 30 Fund)") == "Motilal Oswal Midcap Fund"
        assert _clean_scheme_name("Small Cap Fund") == "Motilal Oswal Small Cap Fund"
        assert _clean_scheme_name("Motilal Oswal Flexi Cap Fund\nAdditional text") == "Motilal Oswal Flexi Cap Fund"


class TestMotilalImporterParams:
    def test_default_params(self):
        imp = MotilalOswalImporter()
        assert imp.from_year == 2017
        assert imp.full_reimport is False
        assert imp._target_month is None
        assert imp.table_name() == "market_data.mf_holdings"
        assert imp.watermark_source() == "mf_holdings"

    def test_custom_params(self):
        imp = MotilalOswalImporter(from_year=2024, full_reimport=True, target_month=date(2026, 7, 1))
        assert imp.from_year == 2024
        assert imp.full_reimport is True
        assert imp._target_month == date(2026, 7, 1)


class TestMotilalExcelParsing:
    def _create_mock_motilal_excel(self) -> bytes:
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            # Index sheet
            index_df = pd.DataFrame([
                ["", "", "Sr No.", "Fund Name", "Fund Code"],
                ["", "", 1, "Motilal Oswal Small Cap Fund", "YO46"],
                ["", "", 2, "Motilal Oswal Midcap Fund", "YO07"],
            ])
            index_df.to_excel(writer, sheet_name="Index", header=False, index=False)

            # Scheme YO46 sheet
            yo46_df = pd.DataFrame([
                ["Back to Index", "", "", "", "", "", "", "", "", ""],
                ["Motilal Oswal Asset Management Company Limited", "", "", "", "", "", "", "", "", ""],
                ["(Investment Manager for Motilal Oswal Mutual Fund)", "", "", "", "", "", "", "", "", ""],
                ["Registered Office: Mumbai", "", "", "", "", "", "", "", "", ""],
                ["● CIN: U67120MH2008PLC188186", "", "", "", "", "", "", "", "", ""],
                ["MONTHLY PORTFOLIO STATEMENT AS ON JULY 31, 2026", "", "", "", "", "", "", "", "", ""],
                ["", "", "", "", "", "", "", "", "", ""],
                ["Motilal Oswal Small Cap Fund", "", "", "", "", "", "", "", "", ""],
                ["(An open ended equity scheme)", "", "", "", "", "", "", "", "", ""],
                ["Sr. No.", "Name of the Instrument", "ISIN", "Industry*", "Quantity", "Market/Fair Value (Rs. in Lakhs)", "% to Net Assets", "", "", "Sector / Rating", "Percent"],
                ["(A)", "Equity & Equity related", "", "", "", "", "", "", "", "Consumer Durables", "0.1114"],
                ["", "Listed / awaiting listing on Stock Exchanges", "", "", "", "", "", "", "", "Retailing", "0.0864"],
                [1, "Rubicon Research Limited", "INE506V01022", "Pharmaceuticals & Biotechnology", 2257865, 33664.77, 4.43, "", "", "Healthcare Services", "0.0789"],
                [2, "VA Tech Wabag Limited", "INE956G01038", "Other Utilities", 1550805, 30663.0, 4.03, "", "", "", ""],
                ["", "Total", "", "", "", 64327.77, 8.46, "", "", "", ""],
            ])
            yo46_df.to_excel(writer, sheet_name="YO46", header=False, index=False)

        return output.getvalue()

    def test_parse_mock_multi_sheet_excel(self):
        excel_bytes = self._create_mock_motilal_excel()
        importer = MotilalOswalImporter()

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = excel_bytes

        source = (date(2026, 7, 31), "scheme portfolio details july 2026", "Monthly Portfolio 31-07-2026-Final.xlsx", "https://mock.url/file.xlsx")

        with patch("httpx.Client.get", return_value=mock_resp):
            rows = importer.parse_source(source)

        assert len(rows) == 2

        rubicon = next(r for r in rows if r["isin"] == "INE506V01022")
        assert rubicon["scheme_code"] == "MOTILAL_SMALL_CAP_FUND"
        assert rubicon["fund_name"] == "Motilal Oswal Small Cap Fund"
        assert rubicon["security_name"] == "Rubicon Research Limited"
        assert rubicon["market_value_cr"] == 336.6477  # 33664.77 Lakhs -> 336.6477 Crores
        assert rubicon["pct_of_nav"] == 4.43
        assert rubicon["asset_type"] == "equity"
        assert rubicon["as_of_month"] == date(2026, 7, 31)

        wabag = next(r for r in rows if r["isin"] == "INE956G01038")
        assert wabag["market_value_cr"] == 306.63
        assert wabag["pct_of_nav"] == 4.03

    def test_outlier_rejection(self):
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df = pd.DataFrame([
                ["", "", "", "", "", "", ""],
                ["", "", "", "", "", "", ""],
                ["", "", "", "", "", "", ""],
                ["", "", "", "", "", "", ""],
                ["", "", "", "", "", "", ""],
                ["MONTHLY PORTFOLIO STATEMENT AS ON JULY 31, 2026", "", "", "", "", "", ""],
                ["", "", "", "", "", "", ""],
                ["Motilal Oswal Flexi Cap Fund", "", "", "", "", "", ""],
                ["", "", "", "", "", "", ""],
                ["Sr. No.", "Name of the Instrument", "ISIN", "Industry*", "Quantity", "Market/Fair Value (Rs. in Lakhs)", "% to Net Assets"],
                ["(A)", "Equity & Equity related", "", "", "", "", ""],
                ["", "Total", "", "", "", "", ""],
                ["", "Grand Total", "", "", "", 100000.0, 100.0],
                ["", "", "", "", "", "", ""],
                [1, "Valid Stock Limited", "INE123A01010", "Finance", 50000, 500.0, 2.5],
            ])
            df.to_excel(writer, sheet_name="YO08", header=False, index=False)

        excel_bytes = output.getvalue()
        importer = MotilalOswalImporter()

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = excel_bytes

        source = (date(2026, 7, 31), "scheme portfolio details july 2026", "Monthly Portfolio 31-07-2026-Final.xlsx", "https://mock.url/file.xlsx")

        with patch("httpx.Client.get", return_value=mock_resp):
            rows = importer.parse_source(source)

        assert len(rows) == 1
        assert rows[0]["security_name"] == "Valid Stock Limited"
        assert rows[0]["market_value_cr"] == 5.0
        assert rows[0]["pct_of_nav"] == 2.5
