"""
tests/test_nse_delivery_fetcher.py
───────────────────────────
Unit tests for the NSE delivery-position (bhavcopy) fetcher.
"""

from datetime import date
from unittest.mock import MagicMock, patch

from src.data_importer.fetchers.nse_delivery_fetcher import fetch_nse_delivery

_SAMPLE_CSV = (
    "SYMBOL, SERIES, DATE1, PREV_CLOSE, OPEN_PRICE, HIGH_PRICE, LOW_PRICE, "
    "LAST_PRICE, CLOSE_PRICE, AVG_PRICE, TTL_TRD_QNTY, TURNOVER_LACS, "
    "NO_OF_TRADES, DELIV_QTY, DELIV_PER\n"
    "20MICRONS, EQ, 14-Aug-2026, 189.07, 190.01, 190.99, 187.22, 188.00, "
    "188.53, 188.70, 68386, 129.04, 1783, 36394, 53.22\n"
    "3IINFOLTD, BE, 14-Aug-2026, 26.93, 26.80, 27.50, 25.59, 27.00, "
    "26.95, 26.16, 734406, 192.11, 1547, -, -\n"
)


def _mock_response(status_code=200, content=_SAMPLE_CSV.encode("utf-8")):
    resp = MagicMock()
    resp.status_code = status_code
    resp.content = content
    return resp


class TestFetchNseDelivery:

    @patch("httpx.Client.get")
    def test_parses_csv_including_null_delivery(self, mock_get):
        mock_get.return_value = _mock_response()

        rows = fetch_nse_delivery(date(2026, 8, 14), date(2026, 8, 14))

        assert len(rows) == 2
        by_symbol = {r["symbol"]: r for r in rows}

        eq_row = by_symbol["20MICRONS"]
        assert eq_row["trade_date"] == date(2026, 8, 14)
        assert eq_row["series"] == "EQ"
        assert eq_row["deliv_qty"] == 36394
        assert eq_row["deliv_per"] == 53.22
        assert eq_row["source"] == "nse"

        # "-" delivery fields must become None (SQL NULL), not 0 — distinguishes
        # "NSE doesn't publish delivery for this series" from "0% delivered".
        be_row = by_symbol["3IINFOLTD"]
        assert be_row["deliv_qty"] is None
        assert be_row["deliv_per"] is None

    @patch("httpx.Client.get")
    def test_skips_unavailable_days_without_raising(self, mock_get):
        # Simulates a weekend/holiday where NSE has no file for that date.
        mock_get.return_value = _mock_response(status_code=404, content=b"")

        rows = fetch_nse_delivery(date(2026, 8, 15), date(2026, 8, 16))

        assert rows == []

    @patch("httpx.Client.get")
    def test_get_raises_is_handled_gracefully(self, mock_get):
        mock_get.side_effect = Exception("network down")

        rows = fetch_nse_delivery(date(2026, 8, 14), date(2026, 8, 14))

        assert rows == []
