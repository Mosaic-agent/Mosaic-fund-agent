"""
tests/test_fast_path_router.py
───────────────────────────────
Unit tests for the Zero-Latency Deterministic Fast-Path Router.
"""

import unittest
from unittest.mock import patch, MagicMock
from src.agents.fast_path_router import try_fast_path

class TestFastPathRouter(unittest.TestCase):

    @patch("src.importer.fetchers.nse_inav_fetcher.fetch_inav_snapshots", return_value=[{"symbol": "GOLDBEES", "inav": 60.0}])
    @patch("src.importer.fetchers.shoonya_fetcher.get_shoonya_api")
    def test_inav_lookup_pattern(self, mock_shoonya, mock_inav):
        mock_api = MagicMock()
        mock_shoonya.return_value = mock_api
        mock_api.searchscrip.return_value = {"values": [{"token": "12345"}]}
        mock_api.get_quotes.return_value = {"stat": "Ok", "lp": 60.50}

        res = try_fast_path("inav GOLDBEES")
        self.assertIsNotNone(res)
        self.assertTrue(res.get("handled"))
        self.assertEqual(res.get("intent"), "fast_path_inav")
        self.assertIn("GOLDBEES", res.get("response", ""))

    @patch("src.importer.fetchers.shoonya_fetcher.get_shoonya_api")
    def test_intraday_lookup_pattern(self, mock_shoonya):
        mock_api = MagicMock()
        mock_shoonya.return_value = mock_api
        mock_api.searchscrip.return_value = {"values": [{"token": "12345"}]}
        mock_api.get_quotes.return_value = {"stat": "Ok", "lp": 2500.0, "c": 2480.0, "ap": 2490.0, "v": 1000, "tbq": 500, "tsq": 400}

        res = try_fast_path("intraday RELIANCE")
        self.assertIsNotNone(res)
        self.assertTrue(res.get("handled"))
        self.assertEqual(res.get("intent"), "fast_path_intraday")
        self.assertIn("RELIANCE", res.get("response", ""))

    def test_complex_query_passes_to_planner(self):
        res = try_fast_path("what is the 2 year outlook for goldbees and macro supercycle?")
        self.assertIsNone(res)

if __name__ == "__main__":
    unittest.main()
