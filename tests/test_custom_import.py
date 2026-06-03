import sys
import os
import unittest
from unittest.mock import patch, MagicMock
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.tools.skills_tools import import_symbol_data_impl

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
        res = import_symbol_data_impl("GOLDBEES", start_date="2019-06-01", end_date="2019-06-05")
        
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
        res = import_symbol_data_impl("NIFTY50", start_date="2019-06-01", end_date="2019-06-05")
        
        # Verify yfinance fetch_ohlcv was called
        mock_yf_fetch.assert_called_once_with([("NIFTY50", "^NSEI")], "indices", date(2019, 6, 1), date(2019, 6, 5))
        
        # Verify ClickHouseImporter was initialized and insert_prices called
        mock_importer_instance.ensure_schema.assert_called_once()
        mock_importer_instance.insert_prices.assert_called_once()
        self.assertIn("Imported NIFTY50: 1 rows inserted", res)

if __name__ == '__main__':
    unittest.main()
