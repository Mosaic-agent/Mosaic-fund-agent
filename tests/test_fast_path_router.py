"""
tests/test_fast_path_router.py
───────────────────────────────
Unit tests for the Zero-Latency Deterministic Fast-Path Router.
"""

import unittest
from src.agents.fast_path_router import try_fast_path

class TestFastPathRouter(unittest.TestCase):

    def test_inav_lookup_pattern(self):
        res = try_fast_path("inav GOLDBEES")
        self.assertIsNotNone(res)
        self.assertTrue(res.get("handled"))
        self.assertEqual(res.get("intent"), "fast_path_inav")
        self.assertIn("GOLDBEES", res.get("response", ""))

    def test_intraday_lookup_pattern(self):
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
