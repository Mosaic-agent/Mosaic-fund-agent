"""
tests/test_mf_sector_analyzer.py
───────────────────────────────────
Unit tests for src/tools/mf_sector_analyzer.py
"""

from src.tools.mf_sector_analyzer import classify_sector, get_mf_sector_report, analyze_mf_sectors


def test_classify_sector():
    assert classify_sector("HDFC Bank Limited") == "BFSI (Banking & Financial Services)"
    assert classify_sector("Aurobindo Pharma Ltd") == "Healthcare & Pharmaceuticals"
    assert classify_sector("HFCL Limited") == "Telecom & Digital Infrastructure"
    assert classify_sector("Bharat Heavy Electricals Ltd") == "Capital Goods, Power & Engineering"
    assert classify_sector("Adani Green Energy Ltd") == "Adani Conglomerate"
    assert classify_sector("Random XYZ Ltd") == "Other Sectors / Specialized Industrials"


def test_get_mf_sector_report_single_amc():
    report = get_mf_sector_report(amc_name="QUANT", top_n_stocks=2)
    assert isinstance(report, str)
    assert "# 🏛️ QUANT Mutual Fund" in report
    assert "Sector Allocation Breakdown" in report


def test_get_mf_sector_report_all_amcs():
    report = get_mf_sector_report(amc_name="ALL")
    assert isinstance(report, str)
    assert "# 🏛️ Multi-AMC Sector Allocation Matrix" in report
