"""
tests/test_mf_rotation_thesis.py
───────────────────────────────────
Unit tests for src/tools/mf_rotation_thesis.py
"""

from src.tools.mf_rotation_thesis import get_rotation_thesis_report, explain_rotation_thesis


def test_get_rotation_thesis_telecom():
    report = get_rotation_thesis_report(amc_name="QUANT", sector_or_stock="Telecom")
    assert isinstance(report, str)
    assert "# 💡 Investment Thesis & Rotation Rationale: QUANT AMC ➔ Telecom" in report
    assert "Telecom Tariff Hikes & 5G Manufacturing Capex Inflow" in report


def test_get_rotation_thesis_adani():
    report = get_rotation_thesis_report(amc_name="QUANT", sector_or_stock="Adani")
    assert isinstance(report, str)
    assert "Khavda Renewable Commissioning & Airport Demerger Value Unlock" in report
