import sys
import os
import unittest
from unittest.mock import patch, MagicMock
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.tools.skills_tools import import_symbol_data, import_symbol_data_impl

class TestCustomImport(unittest.TestCase):
    @patch('src.importer.fetchers.adapters.ShoonyaFetcher')
    @patch('src.importer.clickhouse.ClickHouseImporter')
    def test_import_symbol_data_custom_dates_shoonya(self, mock_clickhouse_importer, mock_shoonya_fetcher):
        # Setup mocks
        mock_fetcher_instance = MagicMock()
        mock_shoonya_fetcher.return_value = mock_fetcher_instance
        
        # Mock rows returned by the fetcher
        mock_fetcher_instance.fetch.return_value = [
            {"symbol": "GOLDBEES", "trade_date": date(2019, 6, 3), "open": 29.5, "high": 30.0, "low": 29.0, "close": 29.8, "volume": 1000}
        ]
        
        # Mock ClickHouseImporter instance behavior
        mock_importer_instance = MagicMock()
        mock_clickhouse_importer.return_value = mock_importer_instance
        mock_importer_instance.insert_prices.return_value = 1

        # Run import with custom start/end date
        res = import_symbol_data_impl(
            "GOLDBEES",
            start_date="2019-06-01",
            end_date="2019-06-05",
            data_source="shoonya",
        )
        
        # Verify ShoonyaFetcher was instantiated with category "etfs" and correct mapping
        mock_shoonya_fetcher.assert_called_once_with("etfs", [("GOLDBEES", "GOLDBEES.NS")])
        
        # Verify fetch was called with the parsed dates
        mock_fetcher_instance.fetch.assert_called_once_with(date(2019, 6, 1), date(2019, 6, 5))
        
        # Verify ClickHouseImporter was initialized and insert_prices called
        mock_importer_instance.ensure_schema.assert_called_once()
        mock_importer_instance.insert_prices.assert_called_once()
        
        # Verify successful output message
        self.assertIn("Imported GOLDBEES: 1 rows inserted", res)

    @patch('src.importer.fetchers.yfinance_fetcher.fetch_ohlcv')
    @patch('src.importer.clickhouse.ClickHouseImporter')
    def test_import_symbol_data_custom_dates_yfinance_fallback(self, mock_clickhouse_importer, mock_yf_fetch):
        # Test non-stock/non-etf fallback (e.g. indices or commodities)
        mock_yf_fetch.return_value = [
            {"symbol": "NIFTY50", "trade_date": date(2019, 6, 3), "open": 11000.0, "high": 11100.0, "low": 10900.0, "close": 11050.0, "volume": 0}
        ]
        
        # Mock ClickHouseImporter instance behavior
        mock_importer_instance = MagicMock()
        mock_clickhouse_importer.return_value = mock_importer_instance
        mock_importer_instance.insert_prices.return_value = 1

        # Run import on an index (which goes to indices category/yfinance)
        res = import_symbol_data_impl(
            "NIFTY50",
            start_date="2019-06-01",
            end_date="2019-06-05",
            data_source="yfinance",
        )
        
        # Verify yfinance fetch_ohlcv was called
        mock_yf_fetch.assert_called_once_with([("NIFTY50", "^NSEI")], "indices", date(2019, 6, 1), date(2019, 6, 5))
        
        # Verify ClickHouseImporter was initialized and insert_prices called
        mock_importer_instance.ensure_schema.assert_called_once()
        mock_importer_instance.insert_prices.assert_called_once()
        self.assertIn("Imported NIFTY50: 1 rows inserted", res)

    @patch('src.importer.fetchers.adapters.NSElibFetcher')
    @patch('src.importer.clickhouse.ClickHouseImporter')
    def test_import_symbol_data_uses_nse_source(self, mock_clickhouse_importer, mock_nse_fetcher):
        mock_fetcher_instance = MagicMock()
        mock_nse_fetcher.return_value = mock_fetcher_instance
        mock_fetcher_instance.fetch.return_value = [
            {
                "symbol": "RELIANCE",
                "trade_date": date(2026, 6, 9),
                "open": 1400.0,
                "high": 1420.0,
                "low": 1390.0,
                "close": 1410.0,
                "volume": 1000,
            }
        ]
        mock_importer_instance = MagicMock()
        mock_clickhouse_importer.return_value = mock_importer_instance
        mock_importer_instance.insert_prices.return_value = 1

        res = import_symbol_data_impl("RELIANCE", days=5, data_source="nse")

        mock_nse_fetcher.assert_called_once_with(
            "stocks", [("RELIANCE", "RELIANCE.NS")]
        )
        mock_importer_instance.set_watermark.assert_called_once_with(
            "nse", "RELIANCE", date(2026, 6, 9)
        )
        self.assertIn("Imported RELIANCE: 1 rows inserted", res)

    @patch(
        "src.importer.source_preference.resolve_data_source",
        return_value=("", False),
    )
    def test_import_symbol_requires_data_source(self, _mock_resolve):
        res = import_symbol_data.invoke({"symbol": "GOLDBEES"})

        self.assertIn("DATA_SOURCE_REQUIRED", res)
        self.assertIn("1. Shoonya", res)
        self.assertIn("2. NSE", res)
        self.assertIn("3. yfinance", res)

    @patch(
        "src.importer.source_preference.resolve_data_source",
        return_value=("nse", True),
    )
    @patch("src.tools.skills_tools.import_symbol_data_impl")
    def test_import_symbol_reuses_saved_data_source(self, mock_import, _mock_resolve):
        mock_import.return_value = "Imported RELIANCE"

        res = import_symbol_data.invoke({"symbol": "RELIANCE"})

        self.assertEqual(res, "Imported RELIANCE")
        mock_import.assert_called_once_with("RELIANCE", 365, "", "", "nse")

    @patch("src.tools.company_resolver.resolve_company_info")
    @patch("src.tools.skills_tools.import_symbol_data_impl")
    def test_import_symbol_us_stock_bypasses_source_prompt(self, mock_import, mock_resolve_info):
        mock_resolve_info.return_value = {
            "symbol": "PCOR",
            "nse_symbol": None,
            "yf_symbol": "PCOR",
            "exchange": "NYQ",
            "market": "US",
            "company_name": "Procore Technologies",
            "currency": "USD",
            "source": "yahoo_search",
        }
        mock_import.return_value = "Imported PCOR"

        res = import_symbol_data.invoke({"symbol": "PCOR"})

        self.assertEqual(res, "Imported PCOR")
        mock_import.assert_called_once_with("PCOR", 365, "", "", "yfinance")

class TestQueryDateRangeParser(unittest.TestCase):
    def test_parse_single_year(self):
        from src.commands.chat_cmd import parse_query_date_range
        start, end = parse_query_date_range("goldbees 2019")
        self.assertEqual(start, "2019-01-01")
        self.assertEqual(end, "2019-12-31")

    def test_parse_year_range(self):
        from src.commands.chat_cmd import parse_query_date_range
        start, end = parse_query_date_range("nifty 2019 to 2026")
        self.assertEqual(start, "2019-01-01")
        self.assertEqual(end, "2026-12-31")

    def test_parse_month_year(self):
        from src.commands.chat_cmd import parse_query_date_range
        start, end = parse_query_date_range("reliance june 2019")
        self.assertEqual(start, "2019-06-01")
        self.assertEqual(end, "2019-06-30")

    def test_parse_explicit_dates(self):
        from src.commands.chat_cmd import parse_query_date_range
        start, end = parse_query_date_range("import goldbees 2019-06-01 to 2019-06-15")
        self.assertEqual(start, "2019-06-01")
        self.assertEqual(end, "2019-06-15")

    def test_parse_no_dates(self):
        from src.commands.chat_cmd import parse_query_date_range
        start, end = parse_query_date_range("import goldbees")
        self.assertEqual(start, "")
        self.assertEqual(end, "")

if __name__ == '__main__':
    unittest.main()
