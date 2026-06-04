"""
tests/test_shoonya_tools.py
───────────────────────────
Tests for Shoonya tools (get_shoonya_quotes and get_shoonya_live_tick)
to ensure they function correctly and that any data returned to the LLM
is structured as a valid JSON string.

Run with:
    .venv/bin/python tests/test_shoonya_tools.py
"""
import os
import sys
import json
import unittest

# Add project root to sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.tools.shoonya_tools import get_shoonya_quotes, get_shoonya_live_tick
from src.importer.fetchers.shoonya_fetcher import get_shoonya_api

class TestShoonyaTools(unittest.TestCase):
    def setUp(self):
        # Ensure we have active credentials before running tests
        api = get_shoonya_api()
        if not api:
            self.skipTest("Shoonya API not authenticated. Run shoonya_login.py first.")

    def test_get_shoonya_quotes_is_json(self):
        """Verify that get_shoonya_quotes returns a valid JSON string."""
        print("\nTesting get_shoonya_quotes for GOLDBEES...")
        result = get_shoonya_quotes.invoke({"symbol": "GOLDBEES"})
        print(f"Result: {result[:200]}...")
        
        # Verify it can be loaded as JSON
        try:
            data = json.loads(result)
            self.assertIsInstance(data, dict)
            self.assertEqual(data.get("stat"), "Ok")
            self.assertEqual(data.get("tsym"), "GOLDBEES-EQ")
        except json.JSONDecodeError as e:
            self.fail(f"get_shoonya_quotes did not return a valid JSON string: {e}")

    def test_get_shoonya_live_tick_is_json(self):
        """Verify that get_shoonya_live_tick returns a valid JSON string (or expected timeout JSON)."""
        print("\nTesting get_shoonya_live_tick for GOLDBEES...")
        result = get_shoonya_live_tick.invoke({"symbol": "GOLDBEES"})
        print(f"Result: {result[:200]}...")
        
        # If it returns a string, verify if it is valid JSON
        # Note: If market is closed or there is no tick, it might return a timeout message.
        # But wait! Let's make sure the tool outputs JSON even for errors or timeouts to keep it robust.
        try:
            data = json.loads(result)
            self.assertIsInstance(data, dict)
        except json.JSONDecodeError:
            # If the tool returned a raw text error, let's fail the test to enforce that ALL data to LLM is JSON.
            self.fail(f"get_shoonya_live_tick did not return a valid JSON string: {result}")

if __name__ == "__main__":
    unittest.main()
