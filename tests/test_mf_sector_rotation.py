"""
tests/test_mf_sector_rotation.py
───────────────────────────────────
Unit tests for src/tools/mf_sector_rotation.py
"""

from src.tools.mf_sector_rotation import get_sector_rotation_report, detect_amc_sector_rotation, get_exhaustive_shift_ledger, audit_exhaustive_stock_shifts


def test_get_sector_rotation_report_quant():
    report = get_sector_rotation_report(amc_name="QUANT")
    assert isinstance(report, str)
    assert "# 🔄 QUANT AMC Sector Rotation Detection Report" in report
    assert "Sector Rotation Matrix" in report


def test_get_sector_rotation_report_dsp():
    report = get_sector_rotation_report(amc_name="DSP")
    assert isinstance(report, str)
    assert "# 🔄 DSP AMC Sector Rotation Detection Report" in report


def test_audit_exhaustive_stock_shifts():
    report = get_exhaustive_shift_ledger(amc_name="QUANT", lookback_months=12)
    assert isinstance(report, str)
    assert "# 📋 EXHAUSTIVE STOCK ADDITIONS & SUBTRACTIONS LEDGER: QUANT AMC" in report
    assert "100% EXHAUSTIVE STOCK ADDITIONS & NEW ENTRIES" in report
    assert "100% EXHAUSTIVE STOCK SUBTRACTIONS & COMPLETE EXITS" in report
