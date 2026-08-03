"""
tests/test_smallcap_pattern_analyzer.py
────────────────────────────────────────
Unit and integration tests for SmallcapPatternAnalyzer module.
"""

import pytest
from src.scripts.portfolio.smallcap_pattern_analyzer import SmallcapPatternAnalyzer, run_smallcap_analysis


def test_smallcap_analyzer_all_amcs():
    analyzer = SmallcapPatternAnalyzer()
    report = analyzer.analyze(amc="all")

    assert report.amc == "ALL"
    assert not report.top_holdings.empty, "Top holdings DataFrame should not be empty"
    assert not report.mom_additions.empty, "MoM additions DataFrame should not be empty"
    assert not report.cross_conviction.empty, "Cross conviction DataFrame should not be empty"
    assert report.price_metrics.latest_close > 0, "Price metrics latest close should be positive"
    assert report.price_metrics.symbol == "SMALLCAP"


def test_smallcap_analyzer_dsp():
    analyzer = SmallcapPatternAnalyzer()
    report = analyzer.analyze(amc="dsp")

    assert report.amc == "DSP"
    assert not report.top_holdings.empty
    assert "Thangamayil" in report.top_holdings.iloc[0]["security_name"] or report.top_holdings.iloc[0]["total_market_value_cr"] > 0


def test_smallcap_analyzer_nippon():
    analyzer = SmallcapPatternAnalyzer()
    report = analyzer.analyze(amc="nippon")

    assert report.amc == "NIPPON"
    assert not report.top_holdings.empty


def test_smallcap_analyzer_dashboard_renderer():
    analyzer = SmallcapPatternAnalyzer()
    report = analyzer.analyze(amc="all")
    dashboard = analyzer.render_ascii_dashboard(report)

    assert "SMALL CAP ETF PRICE TREND" in dashboard
    assert "SMALL CAP TOP EQUITY HOLDINGS" in dashboard
    assert "MULTI-AMC CROSS-CONVICTION" in dashboard
