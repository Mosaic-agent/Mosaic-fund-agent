from unittest.mock import patch

from src.tools.runners import run_mf_concentration_risk


@patch("src.tools.runners._run_cmd", return_value="report")
def test_runner_uses_fund_selector(mock_run):
    assert run_mf_concentration_risk.invoke({"fund": "DSP_SMALL_CAP"}) == "report"
    mock_run.assert_called_once_with([
        "src/scripts/portfolio/concentration_risk.py", "--fund", "DSP_SMALL_CAP"
    ])


def test_runner_rejects_ambiguous_selectors():
    result = run_mf_concentration_risk.invoke({
        "fund": "DSP_SMALL_CAP", "scheme_code": "119212"
    })

    assert result.startswith("SELECTOR_REQUIRED:")
