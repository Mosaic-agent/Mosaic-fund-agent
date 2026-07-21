from datetime import date
from unittest.mock import MagicMock, patch

import pytest
from src.importer.fetchers.indian_macro_fetcher import IndianMacroFetcher

# Mock HTML page with one table containing a single metric row and date headers.
MOCK_HTML = """
<html>
<body>
<table>
  <thead>
    <tr role="row">
      <th class="firstcol"></th>
      <th></th>
      <th class="text-right pr-10">May 26</th>
      <th class="text-right pr-10">Apr 26</th>
    </tr>
  </thead>
  <tbody>
    <tr parent="1" myid="CPI" index="1">
      <td class="indexop1 oprow firstcol showhand">
        <div class="nameofmetriccol" data-unit="%">CPI</div>
      </td>
      <td class="yoy__graph text-right">
        <div class="data-lock">
          <i class="fa fa-line-chart"></i>
        </div>
      </td>
      <td class="knowledge numericvalue text-right">3.9</td>
      <td>
        <div class="data-lock">locked</div>
      </td>
    </tr>
  </tbody>
</table>
</body>
</html>
"""

def test_indian_macro_fetcher_parses_valid_row():
    fetcher = IndianMacroFetcher()

    with patch("urllib.request.urlopen") as mock_urlopen:
        mock_response = MagicMock()
        mock_response.read.return_value = MOCK_HTML.encode("utf-8")
        mock_urlopen.return_value.__enter__.return_value = mock_response

        # Fetch with a range covering the dates
        rows = fetcher.fetch(date(2026, 4, 1), date(2026, 6, 1))

        # May 26 is 2026-05-01 (in range). Value is 3.9.
        # Apr 26 is 2026-04-01 (in range) but locked. Should be omitted.
        assert len(rows) == 1
        record = rows[0]
        assert record["as_of_date"] == date(2026, 5, 1)
        assert record["indicator_code"] == "CPI"
        assert record["indicator_name"] == "CPI"
        assert record["parent_code"] == "1"
        assert record["value"] == 3.9
        assert record["unit"] == "%"


def test_indian_macro_fetcher_filters_by_date():
    fetcher = IndianMacroFetcher()

    with patch("urllib.request.urlopen") as mock_urlopen:
        mock_response = MagicMock()
        mock_response.read.return_value = MOCK_HTML.encode("utf-8")
        mock_urlopen.return_value.__enter__.return_value = mock_response

        # Range excludes May 26
        rows = fetcher.fetch(date(2026, 5, 10), date(2026, 6, 1))
        assert len(rows) == 0


def test_indian_macro_max_date():
    fetcher = IndianMacroFetcher()
    rows = [
        {"as_of_date": date(2026, 5, 1), "value": 3.9},
        {"as_of_date": date(2026, 4, 1), "value": 3.5},
    ]
    assert fetcher.max_date(rows) == date(2026, 5, 1)

    with pytest.raises(ValueError):
        fetcher.max_date([])

