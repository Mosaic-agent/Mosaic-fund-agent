import pandas as pd
import pytest

from src.scripts.portfolio.concentration_risk import analyze_fund, build_parser


class StubPool:
    def __init__(self, frame):
        self.frame = frame
        self.query = None
        self.parameters = None

    def query_df(self, query, parameters=None):
        self.query = query
        self.parameters = parameters
        return self.frame


def test_analyze_fund_uses_roster_filter_and_its_latest_month():
    pool = StubPool(pd.DataFrame([
        {"isin": "INE1", "security_name": "Alpha Bank", "pct_of_nav": 6.0,
         "selected_fund_name": "TEST_MULTI_ASSET"},
        {"isin": "INE2", "security_name": "Beta Tech", "pct_of_nav": 4.0,
         "selected_fund_name": "TEST_MULTI_ASSET"},
    ]))

    result = analyze_fund(pool, fund_filter="scheme_code = '123456'")

    assert result["fund_name"] == "TEST_MULTI_ASSET"
    assert "WHERE (scheme_code = '123456')" in pool.query
    assert "SELECT max(as_of_month)" in pool.query
    assert "AS fund_name" not in pool.query
    assert pool.parameters == {}
    assert result["top_holdings"]["security_name"].tolist() == ["Alpha Bank", "Beta Tech"]


def test_analyze_fund_parameterizes_cli_fund_name():
    pool = StubPool(pd.DataFrame([
        {"isin": "INE1", "security_name": "Alpha Bank", "pct_of_nav": 10.0,
         "selected_fund_name": "Test Fund"},
    ]))

    analyze_fund(pool, fund_name="Test Fund') OR 1=1 --")

    assert "Test Fund') OR 1=1 --" not in pool.query
    assert pool.parameters == {"fund_pattern": "%Test Fund') OR 1=1 --%"}


def test_parser_rejects_ambiguous_fund_and_scheme_selectors():
    with pytest.raises(SystemExit):
        build_parser().parse_args(["--fund", "DSP_MULTI_ASSET", "--scheme", "120821"])
